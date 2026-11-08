//! The verifier against an in-process node that keeps a signed ledger for each account: the
//! statements of an instance, of its children and of its grandchildren (nodo#631), and what
//! the pool says about an instance that the service stopped.
//!
//! The node signs with a fixed key, builds each statement from a script of movements (the
//! chain, the balance and the head are computed, so a statement is valid unless a test breaks
//! it), and counts every read.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::instance::{Uri, UriSlot};
use celaut_proto::celaut::service::{api::Slot, Api};
use celaut_proto::celaut::{
    Amount, AmountKeyValue, BalanceMovement, BalanceStatement, BalanceStatementRequest, Instance,
    Peer, Refund, ServiceInstance, TokenMessage,
};
use prost::Message;
use service_library::gateway::Gateway;
use service_library::manager::{DependencyManager, Limits};
use service_library::statement::{self, account_ref, genesis_hash, instance_ref, movement_hash};
use service_library::verifier::{
    AccountState, Expectation, Finding, RefusalKind, Verifier, VerifierConfig, VerifierError,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

const SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];
const PEER_ID: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

/// One movement of a script.
#[derive(Clone)]
struct Mv {
    kind: &'static str,
    amount: i128,
    instance_ref: String,
    detail: String,
}

fn mv(kind: &'static str, amount: i128) -> Mv {
    Mv {
        kind,
        amount,
        instance_ref: String::new(),
        detail: String::new(),
    }
}

fn credit(n: i128) -> Mv {
    mv("credit", n)
}

fn bound(child: &str) -> Mv {
    Mv {
        kind: "child_bound",
        amount: 0,
        instance_ref: child.to_string(),
        detail: format!(r#"{{"service_id":"{}","starts_seq":"1"}}"#, "ab".repeat(32)),
    }
}

fn refund(n: i128, detail: &str) -> Mv {
    Mv {
        detail: detail.to_string(),
        ..mv("refund", -n)
    }
}

fn stop(reason: &str, leftover: u128) -> Mv {
    Mv {
        detail: format!(r#"{{"leftover":"{leftover}","reason":"{reason}"}}"#),
        ..mv("stop", 0)
    }
}

/// How the node answers a read of one account.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Honest,
    /// The signature is of another text.
    BadSignature,
    /// The call fails with `PERMISSION_DENIED`.
    Refuse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Read {
    instance_ref: String,
    from_seq: u64,
    to_seq: u64,
    hops: u32,
}

struct Fake {
    /// `token:<token>` for an instance of the service, `ref:<instance_ref>` for a descendant.
    accounts: Mutex<HashMap<String, Vec<Mv>>>,
    modes: Mutex<HashMap<String, Mode>>,
    reads: Mutex<Vec<Read>>,
    started: AtomicUsize,
    /// What `StopService` gives back, and writes in the ledger of the instance.
    stop_refund: Mutex<u128>,
    /// A stop that gives back this much in the answer and less in the ledger.
    ledger_refund: Mutex<Option<u128>>,
    rates: bool,
    key: Mutex<String>,
    /// The date of the first movement. The rows keep their date between reads, as a real
    /// ledger does: the hash of a row covers it.
    base_ts: i64,
}

impl Fake {
    fn new() -> Arc<Fake> {
        Arc::new(Fake {
            accounts: Mutex::new(HashMap::new()),
            modes: Mutex::new(HashMap::new()),
            reads: Mutex::new(Vec::new()),
            started: AtomicUsize::new(0),
            stop_refund: Mutex::new(500),
            ledger_refund: Mutex::new(None),
            rates: true,
            key: Mutex::new(PEER_ID.to_string()),
            base_ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 1000,
        })
    }

    fn set(&self, key: &str, script: Vec<Mv>) {
        self.accounts
            .lock()
            .unwrap()
            .insert(key.to_string(), script);
    }

    fn push(&self, key: &str, more: Vec<Mv>) {
        self.accounts
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .extend(more);
    }

    fn mode(&self, key: &str, mode: Mode) {
        self.modes.lock().unwrap().insert(key.to_string(), mode);
    }

    fn reads(&self) -> Vec<Read> {
        self.reads.lock().unwrap().clone()
    }

    fn peer(&self) -> Peer {
        let rate = |key: &str, n: &str| AmountKeyValue {
            key: key.into(),
            value: Some(Amount { n: n.into() }),
        };
        Peer {
            public_key: self.key.lock().unwrap().clone(),
            mu_per_call: if self.rates {
                vec![
                    rate("statement_query_mu", "100"),
                    rate("statement_row_mu", "1"),
                ]
            } else {
                vec![]
            },
            ..Default::default()
        }
    }

    fn statement(
        &self,
        request: &BalanceStatementRequest,
        now: i64,
    ) -> Result<BalanceStatement, Status> {
        let key = if request.instance_ref.is_empty() {
            format!("token:{}", request.token)
        } else {
            format!("ref:{}", request.instance_ref)
        };
        self.reads.lock().unwrap().push(Read {
            instance_ref: request.instance_ref.clone(),
            from_seq: request.from_seq,
            to_seq: request.to_seq,
            hops: request.remaining_hops.unwrap_or(0),
        });
        let mode = self
            .modes
            .lock()
            .unwrap()
            .get(&key)
            .copied()
            .unwrap_or(Mode::Honest);
        if mode == Mode::Refuse {
            return Err(Status::permission_denied(
                "No instance that this token may reach has that descendant.",
            ));
        }
        let script = self
            .accounts
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| Status::not_found("no such account"))?;
        let account = if request.instance_ref.is_empty() {
            account_ref(&request.token)
        } else {
            account_ref(&format!("descendant:{}", request.instance_ref))
        };
        // The whole chain, then the rows that the range asks for.
        let mut prev = genesis_hash(&account);
        let mut balance = 0i128;
        let mut all = Vec::new();
        for (i, m) in script.iter().enumerate() {
            balance += m.amount;
            let mut row = BalanceMovement {
                seq: i as u64 + 1,
                ts: self.base_ts + i as i64,
                amount: Some(Amount {
                    n: m.amount.to_string(),
                }),
                balance_after: Some(Amount {
                    n: balance.to_string(),
                }),
                kind: m.kind.into(),
                instance_ref: m.instance_ref.clone(),
                detail: m.detail.clone(),
                prev_hash: prev.clone(),
            };
            row.prev_hash = prev.clone();
            prev = movement_hash(&row);
            all.push(row);
        }
        let head_seq = all.len() as u64;
        let head_hash = all
            .last()
            .map(movement_hash)
            .unwrap_or_else(|| genesis_hash(&account));
        let rows: Vec<BalanceMovement> = all
            .into_iter()
            .filter(|m| {
                request.from_seq > 0
                    && m.seq >= request.from_seq
                    && (request.to_seq == 0 || m.seq <= request.to_seq)
            })
            .collect();
        let mut s = BalanceStatement {
            account_ref: account,
            from_seq: request.from_seq,
            to_seq: request.to_seq,
            movements: rows,
            balance: Some(Amount {
                n: balance.to_string(),
            }),
            head_seq,
            head_hash,
            oldest_seq: if head_seq == 0 { 0 } else { 1 },
            retention_days: 0,
            ts: now,
            signature: String::new(),
            relayed: false,
            service_id: if request.instance_ref.is_empty() {
                String::new()
            } else {
                "ab".repeat(32)
            },
            instance_ref: request.instance_ref.clone(),
        };
        statement::sign_with_seed(&mut s, &SEED).unwrap();
        if mode == Mode::BadSignature {
            s.ts += 1; // the digest no longer matches the signature
        }
        Ok(s)
    }
}

#[derive(Clone)]
struct Server(Arc<Fake>);

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "celaut.Gateway";
}

struct Method(String, Arc<Fake>);

impl tonic::server::StreamingService<Buffer> for Method {
    type Response = Buffer;
    type ResponseStream = Reply;
    type Future = BoxFuture<tonic::Response<Reply>, Status>;

    fn call(&mut self, request: tonic::Request<Streaming<Buffer>>) -> Self::Future {
        let (tx, rx) = mpsc::channel(16);
        let path = self.0.clone();
        let fake = self.1.clone();
        let mut request = request.into_inner();
        tokio::spawn(async move {
            let mut parser = Parser::new(ParseOptions::accepting(0..16));
            let first = loop {
                match request.message().await {
                    Ok(Some(b)) => {
                        if let Some(p) = parser.push(b).unwrap().into_iter().next() {
                            break p;
                        }
                    }
                    _ => return,
                }
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let answer: Result<Vec<u8>, Status> = match path.as_str() {
                "/celaut.Gateway/GetPeerInfo" => Ok(fake.peer().encode_to_vec()),
                "/celaut.Gateway/GetBalanceStatement" => {
                    let r = BalanceStatementRequest::decode(first.data.as_slice()).unwrap();
                    fake.statement(&r, now).map(|s| s.encode_to_vec())
                }
                "/celaut.Gateway/StartService" => {
                    let n = fake.started.fetch_add(1, Ordering::SeqCst);
                    let token = format!("token-{n}");
                    fake.set(&format!("token:{token}"), vec![credit(1_000_000)]);
                    let slot = UriSlot {
                        internal_port: 8000,
                        uri: vec![Uri {
                            ip: "10.0.0.1".into(),
                            port: 8000,
                        }],
                    };
                    Ok(ServiceInstance {
                        token,
                        instance: Some(Instance {
                            api: Some(Api {
                                slot: vec![Slot {
                                    port: 8000,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            uri_slot: vec![slot],
                        }),
                        ..Default::default()
                    }
                    .encode_to_vec())
                }
                "/celaut.Gateway/StopService" => {
                    let token = TokenMessage::decode(first.data.as_slice()).unwrap().token;
                    let said = *fake.stop_refund.lock().unwrap();
                    let wrote = fake.ledger_refund.lock().unwrap().unwrap_or(said);
                    fake.push(
                        &format!("token:{token}"),
                        vec![
                            refund(wrote as i128, r#"{"to":"father"}"#),
                            stop("manual_kill", 0),
                        ],
                    );
                    Ok(Refund {
                        amount: Some(Amount {
                            n: said.to_string(),
                        }),
                    }
                    .encode_to_vec())
                }
                other => Err(Status::unimplemented(other.to_string())),
            };
            match answer {
                Ok(bytes) => {
                    for b in serialize_part(&Part::new(1, bytes)) {
                        let _ = tx.send(Ok(b)).await;
                    }
                }
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                }
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
        let path = request.uri().path().to_string();
        let fake = self.0.clone();
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(Method(path, fake), request).await)
        })
    }
}

async fn gateway(fake: &Arc<Fake>) -> Gateway {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server(fake.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Gateway::from_channel(channel)
}

async fn verifier(fake: &Arc<Fake>, budget: u128) -> Verifier {
    Verifier::connect(gateway(fake).await, VerifierConfig::with_budget(budget))
        .await
        .unwrap()
}

fn child(n: &str) -> String {
    instance_ref(&format!("child-token-{n}"))
}

/// A family: the instance `root`, a child, and a grandchild of that child.
fn family(fake: &Fake, root: &str) -> (String, String) {
    let (c, g) = (child("1"), child("2"));
    fake.set(&format!("token:{root}"), vec![credit(1_000_000), bound(&c)]);
    fake.set(&format!("ref:{c}"), vec![credit(5_000), bound(&g)]);
    fake.set(&format!("ref:{g}"), vec![credit(1_000)]);
    (c, g)
}

#[tokio::test]
async fn an_instance_is_read_with_its_children_and_grandchildren() {
    let fake = Fake::new();
    let (c, g) = family(&fake, "root");
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    assert_eq!(report.state, AccountState::Alive);
    assert_eq!(report.balance_mu, Some(1_000_000));
    assert_eq!(report.depth, 0);
    assert_eq!(report.children.len(), 1);
    assert_eq!(report.children[0].instance_ref, c);
    assert_eq!(report.children[0].depth, 1);
    assert_eq!(report.children[0].state, AccountState::Alive);
    assert_eq!(
        report.children[0].service_id.as_deref(),
        Some("ab".repeat(32).as_str())
    );
    assert_eq!(report.children[0].children[0].instance_ref, g);
    assert_eq!(report.children[0].children[0].depth, 2);
    assert!(
        report.all_findings().is_empty(),
        "{:?}",
        report.all_findings()
    );
    assert_eq!(fake.reads().len(), 3, "one read for each account");
    let (spent, left) = v.budget();
    assert!(spent > 0 && spent + left == 1_000_000);
}

#[tokio::test]
async fn a_descendant_is_read_with_the_hops_of_its_depth() {
    let fake = Fake::new();
    let (c, g) = family(&fake, "root");
    let v = verifier(&fake, 1_000_000).await;
    v.check("root", Expectation::default()).await;
    let reads = fake.reads();
    assert_eq!(reads[0].instance_ref, "");
    assert_eq!(reads[0].hops, 1);
    assert_eq!(
        (reads[1].instance_ref.as_str(), reads[1].hops),
        (c.as_str(), 2)
    );
    assert_eq!(
        (reads[2].instance_ref.as_str(), reads[2].hops),
        (g.as_str(), 3)
    );
}

#[tokio::test]
async fn the_depth_limit_leaves_the_grandchildren_out() {
    let fake = Fake::new();
    family(&fake, "root");
    let mut config = VerifierConfig::with_budget(1_000_000);
    config.max_depth = 1;
    let v = Verifier::connect(gateway(&fake).await, config)
        .await
        .unwrap();

    let report = v.check("root", Expectation::default()).await;

    assert_eq!(fake.reads().len(), 2);
    assert!(report.children[0].children.is_empty());
    assert!(report.children[0]
        .findings
        .iter()
        .any(|f| matches!(f, Finding::TooDeep { children: 1, .. })));
}

#[tokio::test]
async fn a_second_pass_reads_only_what_is_new_and_a_stopped_account_is_not_read_again() {
    let fake = Fake::new();
    let (c, _) = family(&fake, "root");
    let v = verifier(&fake, 1_000_000).await;
    v.check("root", Expectation::default()).await;
    fake.push("token:root", vec![mv("maintenance", -10)]);
    fake.push(
        &format!("ref:{c}"),
        vec![refund(5_000, r#"{"to":"father"}"#), stop("manual_kill", 0)],
    );

    let report = v.check("root", Expectation::default()).await;

    let reads = fake.reads();
    assert_eq!(reads[3].from_seq, 3, "the root goes on after its two rows");
    assert_eq!(reads[4].from_seq, 3, "the child goes on after its two rows");
    assert_eq!(report.balance_mu, Some(999_990));
    assert!(matches!(
        report.children[0].state,
        AccountState::Stopped { .. }
    ));

    fake.reads.lock().unwrap().clear();
    v.check("root", Expectation::default()).await;
    let again = fake.reads();
    assert!(
        again
            .iter()
            .all(|r| r.instance_ref.is_empty() || r.instance_ref != c),
        "nothing follows a stop, so the child is not read again: {again:?}"
    );
}

#[tokio::test]
async fn a_stop_that_the_service_did_not_ask_for_is_reported() {
    let fake = Fake::new();
    fake.set(
        "token:root",
        vec![
            credit(100),
            refund(100, r#"{"to":"father"}"#),
            stop("node_shutdown", 0),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    assert!(report.findings.contains(&Finding::StoppedWithoutUs {
        instance_ref: instance_ref("root"),
        reason: "node_shutdown".into(),
    }));
    assert!(matches!(report.state, AccountState::Stopped { .. }));
}

#[tokio::test]
async fn a_stop_that_the_service_asked_for_is_clean_when_the_refund_is_in_the_ledger() {
    let fake = Fake::new();
    fake.set(
        "token:root",
        vec![
            credit(1_000),
            refund(500, r#"{"to":"father"}"#),
            stop("manual_kill", 0),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;
    let expected = Expectation {
        stopped_by_us: true,
        refund_claimed_mu: Some(500),
        stopped_secs_ago: 5,
    };

    let report = v.check("root", expected).await;

    assert!(
        report.all_findings().is_empty(),
        "{:?}",
        report.all_findings()
    );
}

#[tokio::test]
async fn a_refund_that_the_node_claimed_and_the_ledger_does_not_show_is_reported() {
    let fake = Fake::new();
    fake.set(
        "token:root",
        vec![
            credit(1_000),
            refund(200, r#"{"to":"father"}"#),
            stop("manual_kill", 0),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;
    let expected = Expectation {
        stopped_by_us: true,
        refund_claimed_mu: Some(500),
        stopped_secs_ago: 5,
    };

    let report = v.check("root", expected).await;

    assert!(report.findings.contains(&Finding::RefundMismatch {
        instance_ref: instance_ref("root"),
        claimed_mu: 500,
        ledger_mu: 200,
    }));
}

#[tokio::test]
async fn another_stop_reason_than_the_one_the_service_asked_for_is_reported() {
    let fake = Fake::new();
    fake.set(
        "token:root",
        vec![
            credit(1_000),
            refund(500, r#"{"to":"father"}"#),
            stop("out_of_balance", 0),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;
    let expected = Expectation {
        stopped_by_us: true,
        refund_claimed_mu: None,
        stopped_secs_ago: 5,
    };

    let report = v.check("root", expected).await;

    assert!(report.findings.contains(&Finding::UnexpectedStopReason {
        instance_ref: instance_ref("root"),
        reason: "out_of_balance".into(),
    }));
}

#[tokio::test]
async fn an_account_with_no_stop_after_the_grace_time_is_reported() {
    let fake = Fake::new();
    fake.set("token:root", vec![credit(1_000)]);
    let v = verifier(&fake, 1_000_000).await;
    let soon = Expectation {
        stopped_by_us: true,
        refund_claimed_mu: Some(0),
        stopped_secs_ago: 5,
    };
    let late = Expectation {
        stopped_secs_ago: 600,
        ..soon
    };

    assert!(v.check("root", soon).await.findings.is_empty());
    let report = v.check("root", late).await;

    assert!(report.findings.contains(&Finding::NotStopped {
        instance_ref: instance_ref("root")
    }));
}

#[tokio::test]
async fn a_child_that_died_with_mu_because_its_father_was_gone_is_a_fault() {
    let fake = Fake::new();
    let (c, _) = family(&fake, "root");
    fake.push(
        &format!("ref:{c}"),
        vec![
            refund(700, r#"{"lost":"father_gone"}"#),
            stop("manual_kill", 700),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    let child = &report.children[0];
    assert!(child.findings.contains(&Finding::StoppedWithMu {
        instance_ref: c,
        reason: "manual_kill".into(),
        leftover_mu: 700,
        father_gone: true,
    }));
    assert!(
        !report
            .findings
            .iter()
            .any(|f| matches!(f, Finding::StoppedWithoutUs { .. })),
        "the service does not control a child"
    );
}

#[tokio::test]
async fn a_stop_that_gave_the_mu_to_a_live_father_is_not_a_fault() {
    let fake = Fake::new();
    let (c, _) = family(&fake, "root");
    fake.push(
        &format!("ref:{c}"),
        vec![refund(700, r#"{"to":"father"}"#), stop("manual_kill", 700)],
    );
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    assert!(
        report.children[0].findings.is_empty(),
        "{:?}",
        report.children[0].findings
    );
}

#[tokio::test]
async fn a_budget_of_zero_reads_nothing() {
    let fake = Fake::new();
    family(&fake, "root");
    let v = verifier(&fake, 0).await;

    let report = v.check("root", Expectation::default()).await;

    assert!(fake.reads().is_empty());
    assert!(matches!(
        report.findings[0],
        Finding::BudgetExhausted { .. }
    ));
    assert_eq!(report.state, AccountState::Unknown);
    assert_eq!(v.budget(), (0, 0));
}

#[tokio::test]
async fn the_budget_is_never_overspent() {
    let fake = Fake::new();
    family(&fake, "root");
    // One read of the root costs at most 100 + 100 rows. Two do not fit.
    let v = verifier(&fake, 250).await;

    let report = v.check("root", Expectation::default()).await;

    assert_eq!(
        fake.reads().len(),
        1,
        "the root was read, its child was not"
    );
    assert!(report
        .all_findings()
        .iter()
        .any(|f| matches!(f, Finding::BudgetExhausted { .. })));
    let (spent, left) = v.budget();
    assert!(spent <= 250 && spent + left == 250);
}

#[tokio::test]
async fn a_statement_with_a_bad_signature_is_a_finding_and_the_read_counts() {
    let fake = Fake::new();
    family(&fake, "root");
    fake.mode("token:root", Mode::BadSignature);
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    assert!(
        matches!(report.findings[0], Finding::InvalidStatement { .. }),
        "{:?}",
        report.findings
    );
    assert!(
        report.children.is_empty(),
        "nothing is read below an account that failed"
    );
    assert!(v.budget().0 > 0, "the node did charge the read");
}

#[tokio::test]
async fn a_refusal_of_the_node_is_a_finding_with_its_kind() {
    let fake = Fake::new();
    let (_, g) = family(&fake, "root");
    fake.mode(&format!("ref:{g}"), Mode::Refuse);
    let v = verifier(&fake, 1_000_000).await;

    let report = v.check("root", Expectation::default()).await;

    let grand = &report.children[0].children[0];
    assert!(
        grand.findings.iter().any(|f| matches!(
            f,
            Finding::StatementRefused {
                refusal: RefusalKind::NotLinked,
                ..
            }
        )),
        "{:?}",
        grand.findings
    );
    assert_eq!(grand.state, AccountState::Unknown);
}

#[tokio::test]
async fn a_node_that_signs_with_another_key_than_the_pinned_one_is_refused_at_connect() {
    let fake = Fake::new();
    let gateway = gateway(&fake).await.with_peer_id("aa".repeat(32));

    let error = Verifier::connect(gateway, VerifierConfig::with_budget(1))
        .await
        .unwrap_err();

    assert!(
        matches!(error, VerifierError::IdentityMismatch { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn a_pinned_identity_is_checked_and_a_statement_of_another_key_fails() {
    let fake = Fake::new();
    family(&fake, "root");
    // The node advertises the pinned key, but its statements are signed by SEED, which is it.
    let gateway = gateway(&fake).await.with_peer_id(PEER_ID);
    let v = Verifier::connect(gateway, VerifierConfig::with_budget(1_000_000))
        .await
        .unwrap();
    assert!(v.identity_pinned());
    assert!(v
        .check("root", Expectation::default())
        .await
        .all_findings()
        .is_empty());
}

#[tokio::test]
async fn without_a_pinned_identity_it_takes_the_advertised_key_and_says_so() {
    let fake = Fake::new();
    let v = verifier(&fake, 1).await;
    assert!(!v.identity_pinned());
}

#[tokio::test]
async fn a_node_with_no_rates_cannot_be_verified() {
    let mut fake = Fake::new();
    Arc::get_mut(&mut fake).unwrap().rates = false;
    let error = Verifier::connect(gateway(&fake).await, VerifierConfig::with_budget(1_000_000))
        .await
        .unwrap_err();
    assert!(matches!(error, VerifierError::NoRates), "{error}");
}

// ─── The pool ─────────────────────────────────────────────────────────────

fn limits() -> Limits {
    Limits {
        idle_timeout: Duration::from_secs(3600),
        pass_timeout_times: 3,
        failed_attempts: 3,
    }
}

async fn pool(
    fake: &Arc<Fake>,
) -> (
    Arc<DependencyManager<Gateway>>,
    service_library::manager::ServiceHandle<Gateway>,
) {
    let manager = DependencyManager::new(gateway(fake).await, limits());
    let handle = manager.add_service([7u8; 32], Default::default());
    (manager, handle)
}

#[tokio::test]
async fn the_pool_settles_an_instance_that_it_stopped_when_the_ledger_agrees() {
    let fake = Fake::new();
    let (manager, handle) = pool(&fake).await;
    let lease = handle.acquire().await.unwrap();
    lease.stop().await.unwrap();
    assert_eq!(manager.tracked_count(), 1);
    let v = verifier(&fake, 1_000_000).await;

    let report = manager.verify(&v).await;

    assert!(report.findings().is_empty(), "{:?}", report.findings());
    assert_eq!(manager.tracked_count(), 0, "settled: no more reads for it");
    assert!(matches!(
        report.instances[0].state,
        AccountState::Stopped { .. }
    ));
    assert!(manager.findings().is_empty());
}

#[tokio::test]
async fn the_pool_reports_a_refund_that_the_ledger_does_not_show() {
    let fake = Fake::new();
    *fake.stop_refund.lock().unwrap() = 900;
    *fake.ledger_refund.lock().unwrap() = Some(100);
    let (manager, handle) = pool(&fake).await;
    handle.acquire().await.unwrap().stop().await.unwrap();
    let v = verifier(&fake, 1_000_000).await;

    let report = manager.verify(&v).await;

    assert!(report.findings().iter().any(|f| matches!(
        f,
        Finding::RefundMismatch {
            claimed_mu: 900,
            ledger_mu: 100,
            ..
        }
    )));
    assert_eq!(
        manager.tracked_count(),
        1,
        "a finding keeps it for the next pass"
    );
    assert_eq!(manager.findings().len(), 1);
    manager.verify(&v).await;
    assert_eq!(manager.findings().len(), 1, "a finding is kept once");
}

#[tokio::test]
async fn the_pool_reports_an_instance_that_the_node_stopped_without_the_service() {
    let fake = Fake::new();
    let (manager, handle) = pool(&fake).await;
    let lease = handle.acquire().await.unwrap();
    // The node kills the instance behind the back of the service.
    fake.push(
        "token:token-0",
        vec![
            refund(1_000_000, r#"{"to":"father"}"#),
            stop("node_decision", 0),
        ],
    );
    let v = verifier(&fake, 1_000_000).await;

    let report = manager.verify(&v).await;

    assert!(report.findings().iter().any(|f| matches!(
        f,
        Finding::StoppedWithoutUs { reason, .. } if reason == "node_decision"
    )));
    drop(lease);
}

#[tokio::test]
async fn the_background_verifier_runs_at_once_and_stays_in_its_budget() {
    let fake = Fake::new();
    let (manager, handle) = pool(&fake).await;
    let _lease = handle.acquire().await.unwrap();
    let v = Arc::new(verifier(&fake, 300).await);

    let task = manager.spawn_verifier(v.clone(), Duration::from_millis(30));
    tokio::time::sleep(Duration::from_millis(400)).await;
    task.abort();

    assert!(!fake.reads().is_empty(), "the first pass is at once");
    let (spent, left) = v.budget();
    assert!(spent <= 300 && spent + left == 300);
    assert!(
        manager
            .findings()
            .iter()
            .any(|f| matches!(f, Finding::BudgetExhausted { .. })),
        "the budget ran out and it said so"
    );
}
