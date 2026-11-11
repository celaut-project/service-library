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
    configuration_file::NetworkResolution, service::Network, Amount, AmountKeyValue,
    BalanceMovement, BalanceStatement, BalanceStatementRequest, ConfigurationFile, Instance,
    Metrics, Peer, Refund, ServiceInstance, Sysresources,
};
use prost::Message;
use serde_json::Value;
use service_library::statement::{account_ref, genesis_hash, movement_hash, sign_with_seed};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

const SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];
const PEER_ID: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";

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
                "/celaut.Gateway/GetPeerInfo" => {
                    let rate = |key: &str, n: &str| AmountKeyValue {
                        key: key.into(),
                        value: Some(Amount { n: n.into() }),
                    };
                    Peer {
                        public_key: PEER_ID.into(),
                        mu_per_call: vec![
                            rate("statement_query_mu", "100"),
                            rate("statement_row_mu", "1"),
                        ],
                        ..Default::default()
                    }
                    .encode_to_vec()
                }
                "/celaut.Gateway/GetBalanceStatement" => {
                    let mut parser = Parser::new(ParseOptions::accepting([1]));
                    let part = parser
                        .push(first.unwrap().unwrap())
                        .unwrap()
                        .into_iter()
                        .next()
                        .unwrap();
                    let request = BalanceStatementRequest::decode(part.data.as_slice()).unwrap();
                    // The account of every instance: one credit of 42 MU, signed by the node.
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    let account = account_ref(&request.token);
                    let row = BalanceMovement {
                        seq: 1,
                        ts: now - 10,
                        amount: mu(42),
                        balance_after: mu(42),
                        kind: "credit".into(),
                        instance_ref: String::new(),
                        detail: String::new(),
                        prev_hash: genesis_hash(&account),
                    };
                    let mut statement = BalanceStatement {
                        account_ref: account,
                        from_seq: request.from_seq,
                        to_seq: request.to_seq,
                        head_seq: 1,
                        head_hash: movement_hash(&row),
                        movements: if request.from_seq == 1 {
                            vec![row]
                        } else {
                            vec![]
                        },
                        balance: mu(42),
                        oldest_seq: 1,
                        ts: now,
                        ..Default::default()
                    };
                    sign_with_seed(&mut statement, &SEED).unwrap();
                    statement.encode_to_vec()
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
                "/celaut.Gateway/Observe" => {
                    use celaut_proto::celaut::observe_event::{Packet, Payload, Session};
                    use celaut_proto::celaut::ObserveEvent;
                    let events = [
                        ObserveEvent {
                            time: "10:00:00".into(),
                            kind: "session".into(),
                            payload: Some(Payload::Session(Session {
                                instance_id: "token-0".into(),
                                tag: "ping".into(),
                                capture_mode: "pcap".into(),
                                ..Default::default()
                            })),
                        },
                        ObserveEvent {
                            time: "10:00:01".into(),
                            kind: "packet".into(),
                            payload: Some(Payload::Packet(Packet {
                                direction: "IN".into(),
                                protocol: "tcp".into(),
                                src: "93.184.216.34:80".into(),
                                peer_kind: "external".into(),
                                peer_host: "93.184.216.34".into(),
                                raw_frame: vec![1, 2, 3],
                                ..Default::default()
                            })),
                        },
                    ];
                    for e in events {
                        for b in serialize_part(&Part::new(1, e.encode_to_vec())) {
                            let _ = tx.send(Ok(b)).await;
                        }
                    }
                    return;
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

#[tokio::test(flavor = "multi_thread")]
async fn verify_reads_the_signed_statement_of_the_instances_of_the_pool() {
    let (_, dir) = node().await;
    let dep = ["--service", SERVICE, "--root", "/nonexistent"];
    let (_, first) = celaut(&dir, &[&["acquire"][..], &dep].concat()).await;
    let lease = first["lease"].to_string();

    let (code, report) = celaut(&dir, &["verify", "--budget", "100000"]).await;

    assert_eq!(code, 0, "{report}");
    assert_eq!(report["identity_pinned"], false);
    let instance = &report["instances"][0];
    assert_eq!(instance["lease"].to_string(), lease);
    assert_eq!(instance["report"]["state"]["state"], "alive");
    assert_eq!(instance["report"]["balance_mu"], 42);
    assert_eq!(instance["report"]["findings"].as_array().unwrap().len(), 0);
    assert_eq!(instance["report"]["depth"], 0);
    let spent = report["spent_mu"].as_u64().unwrap();
    assert!(spent > 0 && spent + report["budget_left_mu"].as_u64().unwrap() == 100000);

    // One lease only, and the pinned identity.
    let (code, one) = celaut(
        &dir,
        &["--peer-id", PEER_ID, "verify", "--budget", "100000", &lease],
    )
    .await;
    assert_eq!(code, 0, "{one}");
    assert_eq!(one["identity_pinned"], true);
    assert_eq!(one["instances"].as_array().unwrap().len(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_reads_nothing_beyond_its_budget_and_needs_one() {
    let (_, dir) = node().await;
    let dep = ["--service", SERVICE, "--root", "/nonexistent"];
    celaut(&dir, &[&["acquire"][..], &dep].concat()).await;

    let (code, error) = celaut(&dir, &["verify"]).await;
    assert_eq!(code, 2, "{error}");
    assert_eq!(error["kind"], "usage");

    let (code, report) = celaut(&dir, &["verify", "--budget", "0"]).await;
    assert_eq!(code, 0, "{report}");
    assert_eq!(report["spent_mu"], 0);
    assert_eq!(
        report["instances"][0]["report"]["findings"][0]["kind"],
        "budget_exhausted"
    );

    let (code, error) = celaut(&dir, &["verify", "--budget", "10", "99"]).await;
    assert_eq!(code, 1, "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn observe_gives_the_fields_of_each_event_as_json() {
    let (_, dir) = node().await;
    let dep = ["--service", SERVICE, "--root", "/nonexistent"];
    let (_, first) = celaut(&dir, &[&["acquire"][..], &dep].concat()).await;
    let lease = first["lease"].to_string();

    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_celaut"))
        .arg("--config")
        .arg(dir.join("__config__"))
        .arg("--state")
        .arg(dir.join("pool.json"))
        .args(["observe", &lease, "--packets"])
        .output()
        .await
        .unwrap();

    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["kind"], "session");
    assert_eq!(lines[0]["data"]["capture_mode"], "pcap");
    assert_eq!(lines[0]["data"]["instance_id"], "token-0");
    assert_eq!(lines[1]["kind"], "packet");
    assert_eq!(lines[1]["data"]["direction"], "IN");
    assert_eq!(lines[1]["data"]["peer_kind"], "external");
    assert_eq!(lines[1]["data"]["peer_host"], "93.184.216.34");
    assert!(
        lines[1]["data"].get("raw_frame").is_none(),
        "the frame bytes are left out"
    );
    assert!(lines[1]["payload"].is_string(), "the old field stays");
    std::fs::remove_dir_all(dir).unwrap();
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A file that stands for the device of the notice, with `lines` in it.
fn device(dir: &Path, lines: &str) -> PathBuf {
    let path = dir.join("hvc0");
    std::fs::write(&path, lines).unwrap();
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_gives_each_line_of_the_node_as_json() {
    let (_, dir) = node().await;
    let path = device(
        &dir,
        &format!("garbage\nSTOP {}\nHIBERNATE 1\n", now_secs() + 30),
    );
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_celaut"))
        .arg("--config")
        .arg(dir.join("__config__"))
        .args(["notice", "--device"])
        .arg(&path)
        .output()
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let lines: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        lines.len(),
        2,
        "one JSON for each notice, and not for garbage"
    );
    assert_eq!(lines[0]["verb"], "stop");
    assert!(lines[0]["remaining_secs"].as_u64().unwrap() <= 30);
    assert_eq!(lines[1]["verb"], "hibernate");
    assert_eq!(lines[1]["deadline"], 1);
    assert_eq!(lines[1]["remaining_secs"], 0);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_once_ends_after_the_first() {
    let (_, dir) = node().await;
    let path = device(&dir, &format!("STOP {}\nSTOP 2\n", now_secs() + 30));
    let (code, line) = celaut(
        &dir,
        &["notice", "--once", "--device", path.to_str().unwrap()],
    )
    .await;
    assert_eq!(code, 0, "{line}");
    assert_eq!(line["verb"], "stop");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_shutdown_stops_the_instances_of_the_pool_within_the_time_of_the_notice() {
    let (counts, dir) = node().await;
    for env in ["A=1", "A=2"] {
        let (code, _) = celaut(
            &dir,
            &[
                "acquire",
                "--service",
                SERVICE,
                "--root",
                "/x",
                "--env",
                env,
            ],
        )
        .await;
        assert_eq!(code, 0);
    }
    let path = device(&dir, &format!("STOP {}\nSTOP 2\n", now_secs() + 60));
    let (code, line) = celaut(
        &dir,
        &["notice", "--shutdown", "--device", path.to_str().unwrap()],
    )
    .await;
    assert_eq!(code, 0, "{line}");
    assert_eq!(line["verb"], "stop");
    assert_eq!(line["stopped"], 2);
    assert_eq!(line["leaked"], 0);
    assert!(line.get("timed_out").is_none());
    assert_eq!(counts.stopped.load(Ordering::SeqCst), 2);
    let (_, list) = celaut(&dir, &["list"]).await;
    assert!(list["instances"].as_array().unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_shutdown_with_no_time_left_keeps_the_instances_in_the_pool() {
    let (_, dir) = node().await;
    let (code, _) = celaut(&dir, &["acquire", "--service", SERVICE, "--root", "/x"]).await;
    assert_eq!(code, 0);
    // The time of the notice has passed: nothing can be stopped in it, and nothing is lost.
    let path = device(&dir, &format!("HIBERNATE {}\n", now_secs() - 5));
    let (code, line) = celaut(
        &dir,
        &["notice", "--shutdown", "--device", path.to_str().unwrap()],
    )
    .await;
    assert_eq!(code, 0, "{line}");
    assert_eq!(line["timed_out"], true);
    assert_eq!(line["stopped"], 0);
    assert_eq!(line["leaked"], 1);
    let (_, list) = celaut(&dir, &["list"]).await;
    assert_eq!(
        list["instances"].as_array().unwrap().len(),
        1,
        "a later shutdown can try again"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_with_no_device_says_what_the_service_has_to_declare() {
    let (_, dir) = node().await;
    let (code, error) = celaut(&dir, &["notice", "--device", "/nonexistent/hvc0"]).await;
    assert_eq!(code, 1);
    assert_eq!(error["kind"], "notice");
    assert!(error["error"]
        .as_str()
        .unwrap()
        .contains("notice_declaration"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_shutdown_with_an_empty_pool_needs_no_gateway() {
    // No node and no configuration: the service has no dependencies, so there is nothing to ask.
    let dir = std::env::temp_dir().join(format!("celaut-cli-empty-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = device(&dir, &format!("STOP {}\n", now_secs() + 30));
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_celaut"))
        .arg("--config")
        .arg(dir.join("no-such-config"))
        .arg("--state")
        .arg(dir.join("pool.json"))
        .args(["notice", "--shutdown", "--device"])
        .arg(&path)
        .output()
        .await
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let line: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(line["stopped"], 0);
    assert_eq!(line["leaked"], 0);
    std::fs::remove_dir_all(dir).unwrap();
}
