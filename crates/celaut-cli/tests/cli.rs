//! The `celaut` command against an in-process gRPC server that answers as a node does:
//! `StartService`, `StopService` and `GetMetrics`.

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::instance::{Uri, UriSlot};
use celaut_proto::celaut::service::{api::Slot, Api};
use celaut_proto::celaut::{
    configuration_file::NetworkResolution, service::Network, Amount, ConfigurationFile, Instance,
    Metrics, Refund, ServiceInstance, Sysresources,
};
use prost::Message;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

#[derive(Default)]
struct Counts {
    started: AtomicUsize,
    stopped: AtomicUsize,
}

fn instance(ip: &str, port: i32) -> Instance {
    Instance {
        api: Some(Api {
            slot: vec![Slot {
                port: 8000,
                ..Default::default()
            }],
            ..Default::default()
        }),
        uri_slot: vec![UriSlot {
            internal_port: 8000,
            uri: vec![Uri {
                ip: ip.into(),
                port,
            }],
        }],
    }
}

fn mu(n: u32) -> Option<Amount> {
    Some(Amount { n: n.to_string() })
}

#[derive(Clone)]
struct Server(Arc<Counts>);

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "celaut.Gateway";
}

struct Method(String, Arc<Counts>);

impl tonic::server::StreamingService<Buffer> for Method {
    type Response = Buffer;
    type ResponseStream = Reply;
    type Future = BoxFuture<tonic::Response<Reply>, Status>;

    fn call(&mut self, request: tonic::Request<Streaming<Buffer>>) -> Self::Future {
        let (tx, rx) = mpsc::channel(16);
        let (path, counts) = (self.0.clone(), self.1.clone());
        let mut request = request.into_inner();
        tokio::spawn(async move {
            // The first piece of the request is enough to answer.
            let first = request.message().await;
            let answer = match path.as_str() {
                "/celaut.Gateway/ResolveNetwork" => {
                    let mut parser = Parser::new(ParseOptions::accepting([1]));
                    let part = parser
                        .push(first.unwrap().unwrap())
                        .unwrap()
                        .into_iter()
                        .next()
                        .unwrap();
                    let network = Network::decode(part.data.as_slice()).unwrap();
                    // The node declared db.engine=postgresql, and refuses the rest.
                    let body = String::from_utf8(network.formal.clone()).unwrap();
                    if !body.lines().any(|l| l == "db.engine=postgresql") {
                        let _ = tx
                            .send(Err(Status::permission_denied("a fixed key changed")))
                            .await;
                        return;
                    }
                    // A member with no gRPC port: postgresql, and a second port.
                    let mut member = instance("10.0.0.9", 5432);
                    let proto = |t: &str| celaut_proto::celaut::service::api::Protocol {
                        tags: vec![t.into()],
                        ..Default::default()
                    };
                    // Only 5432 is declared: 9187 has no protocol, and is not guessed.
                    member.api = Some(Api {
                        slot: vec![Slot {
                            port: 5432,
                            transport: Some(proto("tcp")),
                            protocol_stack: vec![proto("postgresql")],
                            ..Default::default()
                        }],
                        ..Default::default()
                    });
                    member.uri_slot[0].internal_port = 5432;
                    member.uri_slot.push(UriSlot {
                        internal_port: 9187,
                        uri: vec![Uri {
                            ip: "10.0.0.9".into(),
                            port: 9187,
                        }],
                    });
                    NetworkResolution {
                        tags: network.tags,
                        peer_instances: vec![member],
                    }
                    .encode_to_vec()
                }
                "/celaut.Gateway/StartService" => {
                    let n = counts.started.fetch_add(1, Ordering::SeqCst);
                    ServiceInstance {
                        token: format!("token-{n}"),
                        instance: Some(instance("10.0.0.9", 9000 + n as i32)),
                        receipt: None,
                    }
                    .encode_to_vec()
                }
                "/celaut.Gateway/StopService" => {
                    counts.stopped.fetch_add(1, Ordering::SeqCst);
                    Refund { amount: mu(7) }.encode_to_vec()
                }
                "/celaut.Gateway/GetMetrics" => Metrics {
                    balance: mu(42),
                    suspended: false,
                }
                .encode_to_vec(),
                other => {
                    let _ = tx.send(Err(Status::unimplemented(other.to_string()))).await;
                    return;
                }
            };
            for b in serialize_part(&Part::new(1, answer)) {
                let _ = tx.send(Ok(b)).await;
            }
        });
        let reply: Reply = Box::pin(ReceiverStream::new(rx));
        Box::pin(async move { Ok(tonic::Response::new(reply)) })
    }
}

impl<B> Service<http::Request<B>> for Server
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let method = Method(request.uri().path().to_string(), self.0.clone());
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(method, request).await)
        })
    }
}

/// A node, and a directory with its `__config__` and the pool file.
async fn node() -> (Arc<Counts>, PathBuf) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let counts = Arc::new(Counts::default());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server(counts.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    (counts, dir_for(port))
}

fn dir_for(port: u16) -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "celaut-cli-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let file = ConfigurationFile {
        gateway: Some(instance("127.0.0.1", port.into())),
        initial_sysresources: Some(Sysresources {
            mem_limit: Some(1 << 20),
            ..Default::default()
        }),
        ..Default::default()
    };
    std::fs::write(dir.join("__config__"), file.encode_to_vec()).unwrap();
    dir
}

/// Run `celaut` with the config and the pool of `dir`: the exit code, and the JSON of
/// stdout (or of stderr if it failed).
async fn celaut(dir: &Path, args: &[&str]) -> (i32, Value) {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_celaut"))
        .arg("--config")
        .arg(dir.join("__config__"))
        .arg("--state")
        .arg(dir.join("pool.json"))
        .args(args)
        .output()
        .await
        .unwrap();
    let code = out.status.code().unwrap();
    let text = if code == 0 { out.stdout } else { out.stderr };
    (code, serde_json::from_slice(&text).unwrap())
}

const SERVICE: &str = "abababababababababababababababababababababababababababababababab";

#[tokio::test(flavor = "multi_thread")]
async fn an_instance_is_reused_between_commands_and_stopped_at_the_end() {
    let (counts, dir) = node().await;
    let dep = ["--service", SERVICE, "--root", "/nonexistent"];

    let (code, first) = celaut(&dir, &[&["acquire"][..], &dep].concat()).await;
    assert_eq!(code, 0, "{first}");
    assert_eq!(first["uri"], "10.0.0.9:9000");
    assert_eq!(first["new"], true);
    let lease = first["lease"].to_string();

    let (_, metrics) = celaut(&dir, &["metrics", &lease]).await;
    assert_eq!(metrics["balance_mu"], "42");

    let (code, _) = celaut(&dir, &["release", &lease, "--outcome", "success"]).await;
    assert_eq!(code, 0);
    let (_, again) = celaut(&dir, &[&["acquire"][..], &dep].concat()).await;
    assert_eq!(again["lease"], first["lease"]);
    assert_eq!(again["new"], false);
    assert_eq!(counts.started.load(Ordering::SeqCst), 1);

    // Another configuration is another instance.
    let (_, other) = celaut(&dir, &[&["acquire", "--env", "A=1"][..], &dep].concat()).await;
    assert_eq!(other["new"], true);

    let (_, list) = celaut(&dir, &["list"]).await;
    assert_eq!(list["instances"].as_array().unwrap().len(), 2);
    assert!(
        !list.to_string().contains("token-"),
        "no token leaves the pool"
    );

    let (_, report) = celaut(&dir, &["shutdown"]).await;
    assert_eq!(report["stopped"], 2);
    assert_eq!(report["leaked"], 0);
    assert_eq!(counts.stopped.load(Ordering::SeqCst), 2);
    let (_, list) = celaut(&dir, &["list"]).await;
    assert!(list["instances"].as_array().unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_gives_the_refund_and_forgets_the_lease() {
    let (_, dir) = node().await;
    let (_, lease) = celaut(&dir, &["acquire", "--service", SERVICE, "--root", "/x"]).await;
    let lease = lease["lease"].to_string();
    let (code, stopped) = celaut(&dir, &["stop", &lease]).await;
    assert_eq!(code, 0);
    assert_eq!(stopped["refund_mu"], "7");
    let (code, error) = celaut(&dir, &["metrics", &lease]).await;
    assert_eq!(code, 1);
    assert_eq!(error["kind"], "failed");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_does_not_answer_is_exit_code_3() {
    // A port that nobody listens on.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = dir_for(port);
    let (code, error) = celaut(
        &dir,
        &[
            "acquire",
            "--service",
            SERVICE,
            "--root",
            "/x",
            "--attempts",
            "1",
        ],
    )
    .await;
    assert_eq!(code, 3, "{error}");
    assert_eq!(error["unreachable"], true);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_arguments_are_exit_code_2() {
    let dir = dir_for(1);
    let (code, error) = celaut(&dir, &["acquire"]).await;
    assert_eq!(code, 2);
    assert_eq!(error["kind"], "usage");
    let (code, config) = celaut(&dir, &["config"]).await;
    assert_eq!(code, 0);
    assert_eq!(config["mem_limit"], 1 << 20);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_gives_every_slot_of_a_member_with_its_internal_port() {
    let (_, dir) = node().await;
    let (code, out) = celaut(
        &dir,
        &[
            "resolve",
            "--tag",
            "postgres:personal-nutrition",
            "--formal",
            "db.schema=personal-nutrition",
            "--formal",
            "db.engine=postgresql",
            "--formal",
            "db.cluster=abc",
            "--protocol",
            "postgresql",
        ],
    )
    .await;
    assert_eq!(code, 0, "{out}");
    assert_eq!(out["tags"][0], "postgres:personal-nutrition");
    let slots = &out["peers"][0]["slots"];
    assert_eq!(slots[0]["internal_port"], 5432);
    assert_eq!(slots[0]["uris"][0], "10.0.0.9:5432");
    assert_eq!(slots[0]["transport"][0], "tcp");
    assert_eq!(slots[0]["protocol_stack"][0][0], "postgresql");
    assert_eq!(slots[1]["internal_port"], 9187);
    assert!(slots[1]["protocol_stack"].as_array().unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_refuses_an_unfilled_variable_before_the_node_and_a_changed_key_at_the_node() {
    let (_, dir) = node().await;
    let (code, error) = celaut(
        &dir,
        &[
            "resolve",
            "--tag",
            "t",
            "--formal",
            "db.cluster=${NUTRITION_DB_CLUSTER}",
        ],
    )
    .await;
    assert_eq!(code, 1, "{error}");
    assert!(
        error["error"].as_str().unwrap().contains("variable"),
        "{error}"
    );

    let (code, error) = celaut(
        &dir,
        &["resolve", "--tag", "t", "--formal", "db.engine=mysql"],
    )
    .await;
    assert_eq!(code, 1, "{error}");
    assert_eq!(error["unreachable"], false);

    let (code, error) = celaut(&dir, &["resolve", "--formal", "a=b"]).await;
    assert_eq!(code, 2, "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_with_no_node_is_exit_code_3() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = dir_for(port);
    let (code, error) = celaut(&dir, &["resolve", "--tag", "t"]).await;
    assert_eq!(code, 3, "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn config_keeps_every_slot_of_a_member_that_has_no_grpc_port() {
    let dir = dir_for(1);
    let mut file =
        ConfigurationFile::decode(std::fs::read(dir.join("__config__")).unwrap().as_slice())
            .unwrap();
    // A postgresql member: no gRPC slot, so `peers` drops it, and `members` keeps it.
    let mut member = instance("10.0.0.9", 5432);
    member.api = None;
    member.uri_slot[0].internal_port = 5432;
    file.network_resolution = vec![NetworkResolution {
        tags: vec!["postgres:x".into()],
        peer_instances: vec![member],
    }];
    std::fs::write(dir.join("__config__"), file.encode_to_vec()).unwrap();
    let (code, config) = celaut(&dir, &["config"]).await;
    assert_eq!(code, 0, "{config}");
    let network = &config["networks"][0];
    assert_eq!(network["peers"].as_array().unwrap().len(), 0);
    assert_eq!(network["members"][0]["slots"][0]["internal_port"], 5432);
    assert_eq!(
        network["members"][0]["slots"][0]["uris"][0],
        "10.0.0.9:5432"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
