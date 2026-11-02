//! The `celaut` command against an in-process gRPC server that answers as a node does:
//! `StartService`, `StopService` and `GetMetrics`.

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, Part};
use celaut_proto::celaut::instance::{Uri, UriSlot};
use celaut_proto::celaut::service::{api::Slot, Api};
use celaut_proto::celaut::{
    Amount, ConfigurationFile, Instance, Metrics, Refund, ServiceInstance, Sysresources,
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
            let _ = request.message().await;
            let answer = match path.as_str() {
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
