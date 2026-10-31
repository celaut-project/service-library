//! A quote that carries the service (`relay::call_with_service`), against an in-process
//! gRPC server that answers as nodo does
//! (`nodo:src/gateway/iterables/abstract_input_service_iterable.py`):
//!
//! - a node that has the service answers after the Hash: a signal, the answer, a signal,
//!   and it then reads the rest of the request to its end and drops it;
//! - a node that does not have it answers after the whole Service, once it stored it;
//! - cancel markers (blocks that intersect) make it refuse the request.
//!
//! The server records what it got and how the request ended: a clean end, or a reset.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bee_rpc::buffer::buffer::Head;
use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::{Amount, Configuration, EstimatedCost};
use prost::Message;
use service_library::counter::SendCounter;
use service_library::relay::{call_with_service, Buffers, EARLY_ANSWER_WAIT};
use service_library::skips::Skips;
use service_library::Error as WireError;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

const PATH: &str = "/celaut.Gateway/GetServiceEstimatedCost";
const ID: [u8; 32] = [7u8; 32];
const MIB: usize = 1024 * 1024;

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

/// How the request ended, for the server.
#[derive(Debug, Clone, PartialEq)]
enum End {
    /// A clean end of the request.
    Clean,
    /// The request was reset, or failed.
    Reset(String),
    /// The server refused what it parsed (the cancel markers).
    Refused(String),
}

#[derive(Debug, Default)]
struct Seen {
    /// The Buffers after the Hash: the Metadata and the Service.
    after_hash: u64,
    /// The head indices of the parts that the server parsed, in order.
    indices: Vec<i32>,
    /// The size of the Service part, if it came whole.
    service_len: Option<usize>,
    /// The bytes of the block of the Service that the node holds, as it read them.
    block_bytes: usize,
    /// The end markers of that block that the node read.
    block_ends: usize,
    end: Option<End>,
}

#[derive(Clone, Copy, PartialEq)]
enum Node {
    /// It has the service: it answers after the Hash.
    Has,
    /// It does not have the service: it answers after the Service.
    Lacks,
    /// It has the service and refuses the quote after the Hash.
    Refuses,
    /// It does not have the service, but it holds the block that the Service carries
    /// ([`HELD`]): it asks the client to skip it as soon as the block starts, and does not
    /// read more for a moment. It answers at the end of the request.
    Holds,
}

/// The id of the block that a [`Node::Holds`] holds.
const HELD: [u8; 32] = [9u8; 32];

fn held_marker() -> Buffer {
    use bee_rpc::buffer::buffer::{block::Hash, Block};
    Buffer {
        block: Some(Block {
            hashes: vec![Hash {
                r#type: bee_rpc::hash::HashAlgorithm::Sha3_256.hash_type(),
                value: HELD.to_vec(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn answer() -> Vec<Buffer> {
    let cost = EstimatedCost {
        cost: Some(Amount { n: "42".into() }),
        ..Default::default()
    };
    let mut out = vec![Buffer {
        signal: Some(true),
        ..Default::default()
    }];
    out.extend(serialize_part(&Part::new(1, cost.encode_to_vec())));
    out.push(Buffer {
        signal: Some(true),
        ..Default::default()
    });
    out
}

/// The handler: reads the request, as nodo does, and answers on `tx`.
async fn node(
    kind: Node,
    mut request: Streaming<Buffer>,
    tx: mpsc::Sender<Result<Buffer, Status>>,
    seen: Arc<Mutex<Seen>>,
) {
    let mut parser = Parser::new(ParseOptions::accepting([1, 2, 3, 4, 5, 6]));
    let mut hash_seen = false;
    let mut answered = false;
    let mut in_service = false;
    let mut asked = false;
    loop {
        let buffer = match request.message().await {
            Ok(Some(buffer)) => buffer,
            Ok(None) => {
                // The parser checks the blocks at the end too (the markers).
                if let Err(e) = parser.finish() {
                    seen.lock().unwrap().end = Some(End::Refused(e.to_string()));
                    let _ = tx
                        .send(Err(Status::unknown(format!(
                            "Exception iterating requests: {e}"
                        ))))
                        .await;
                    return;
                }
                seen.lock().unwrap().end = Some(End::Clean);
                if kind == Node::Holds {
                    for b in answer() {
                        let _ = tx.send(Ok(b)).await;
                    }
                    return;
                }
                if !answered {
                    let _ = tx
                        .send(Err(Status::unknown("This node does not have the service")))
                        .await;
                }
                return;
            }
            Err(status) => {
                seen.lock().unwrap().end = Some(End::Reset(status.to_string()));
                return;
            }
        };
        if kind == Node::Holds && buffer.head.as_ref().is_some_and(|h| h.index == 6) {
            in_service = true;
        }
        if in_service {
            // Raw, as nodo reads a block that it holds: the bytes are read and dropped.
            if buffer.block.is_some() {
                if asked {
                    seen.lock().unwrap().block_ends += 1;
                } else {
                    asked = true;
                    let skip =
                        bee_rpc::skip::skip_buffer(&HELD, bee_rpc::hash::HashAlgorithm::Sha3_256);
                    let _ = tx.send(Ok(skip)).await;
                    // The client goes on until the request arrives: this node does not
                    // read for a moment, so that what it gets is what the client had
                    // already sent, and not what the speed of the loopback decides.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
            seen.lock().unwrap().block_bytes += buffer.chunk.as_ref().map_or(0, Vec::len);
            continue;
        }
        if hash_seen {
            seen.lock().unwrap().after_hash += 1;
            if kind != Node::Lacks {
                // nodo drains what it does not need and drops it.
                continue;
            }
        }
        let parts = match parser.push(buffer) {
            Ok(parts) => parts,
            Err(e) => {
                seen.lock().unwrap().end = Some(End::Refused(e.to_string()));
                let _ = tx
                    .send(Err(Status::unknown(format!(
                        "Exception iterating requests: {e}"
                    ))))
                    .await;
                return;
            }
        };
        for part in parts {
            seen.lock().unwrap().indices.push(part.index);
            match (part.index, kind) {
                (4, Node::Has) => {
                    hash_seen = true;
                    answered = true;
                    for b in answer() {
                        let _ = tx.send(Ok(b)).await;
                    }
                }
                (4, Node::Refuses) => {
                    let _ = tx
                        .send(Ok(Buffer {
                            signal: Some(true),
                            ..Default::default()
                        }))
                        .await;
                    let _ = tx
                        .send(Err(Status::unknown("This node is closed now")))
                        .await;
                    // The handler raised: the call ends.
                    return;
                }
                (4, Node::Lacks | Node::Holds) => hash_seen = true,
                (6, Node::Lacks) => {
                    seen.lock().unwrap().service_len = Some(part.data.len());
                    answered = true;
                    for b in answer() {
                        let _ = tx.send(Ok(b)).await;
                    }
                }
                _ => {}
            }
        }
    }
}

#[derive(Clone)]
struct Server(Node, Arc<Mutex<Seen>>);

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "celaut.Gateway";
}

struct Method(Node, Arc<Mutex<Seen>>);

impl tonic::server::StreamingService<Buffer> for Method {
    type Response = Buffer;
    type ResponseStream = Reply;
    type Future = BoxFuture<tonic::Response<Reply>, Status>;

    fn call(&mut self, request: tonic::Request<Streaming<Buffer>>) -> Self::Future {
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(node(self.0, request.into_inner(), tx, self.1.clone()));
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
        let (kind, seen) = (self.0, self.1.clone());
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(Method(kind, seen), request).await)
        })
    }
}

/// Serve a node of `kind` on the loopback. Gives a channel and what the node sees.
async fn serve(kind: Node) -> (tonic::transport::Channel, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server(kind, seen.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, seen)
}

/// The end of the request, as the server saw it (it can come a moment after the call).
async fn end_of(seen: &Arc<Mutex<Seen>>) -> End {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(end) = seen.lock().unwrap().end.clone() {
            return end;
        }
        assert!(
            Instant::now() < deadline,
            "the server saw no end of the request"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn head() -> Vec<Part> {
    service_library::gateway::start_parts(ID, &Configuration::default())
}

/// A service of `mib` MiB with the indices of a node: the Metadata (5), then the Service
/// (6) in chunks of 1 MiB, made when they are sent. With `fail_after`, the stream gives an
/// error after that many chunks (a cancel, or a file that cannot be read).
fn service(mib: usize, fail_after: Option<usize>) -> Buffers {
    let metadata = serialize_part(&Part::new(5, b"metadata".to_vec())).collect::<Vec<_>>();
    let start = Buffer {
        head: Some(Head {
            index: 6,
            partitions: Vec::new(),
        }),
        ..Default::default()
    };
    let chunks = (0..mib).map(move |i| {
        if fail_after == Some(i) {
            return Err(WireError::Protocol("the transfer was cancelled".into()));
        }
        Ok(Buffer {
            chunk: Some(vec![(i % 251) as u8; MIB]),
            ..Default::default()
        })
    });
    let end = Buffer {
        separator: Some(true),
        ..Default::default()
    };
    let all = metadata
        .into_iter()
        .chain([start])
        .map(Ok)
        .chain(chunks)
        .chain([Ok(end)]);
    Box::pin(tokio_stream::iter(all))
}

/// A service whose Service part is one block of `mib` MiB, in chunks of 1 MiB: the Metadata
/// (5), the head of the Service (6), the start marker of the block, its chunks, its end
/// marker, and the end of the part.
fn service_with_block(mib: usize) -> Buffers {
    let metadata = serialize_part(&Part::new(5, b"metadata".to_vec())).collect::<Vec<_>>();
    let start = Buffer {
        head: Some(Head {
            index: 6,
            partitions: Vec::new(),
        }),
        ..Default::default()
    };
    let chunks = (0..mib).map(|i| {
        Ok(Buffer {
            chunk: Some(vec![(i % 251) as u8; MIB]),
            ..Default::default()
        })
    });
    let end = Buffer {
        separator: Some(true),
        ..Default::default()
    };
    let all = metadata
        .into_iter()
        .chain([start, held_marker()])
        .map(Ok)
        .chain(chunks)
        .chain([Ok(held_marker()), Ok(end)]);
    Box::pin(tokio_stream::iter(all))
}

fn cost_of(parts: &[Part]) -> EstimatedCost {
    let part = parts.iter().find(|p| p.index == 1).expect("an answer");
    EstimatedCost::decode(part.data.as_slice()).unwrap()
}

const TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_lacks_the_service_gets_all_of_it_and_a_clean_end() {
    let (channel, seen) = serve(Node::Lacks).await;
    let counter = SendCounter::new();
    let started = Instant::now();
    let parts = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(service(3, None)),
        &[1],
        TIMEOUT,
        EARLY_ANSWER_WAIT,
        &Skips::new(),
    )
    .await
    .unwrap();
    assert_eq!(cost_of(&parts).cost.unwrap().n, "42");
    assert!(counter.ended());
    let whole = SendCounter::new();
    let mut all = whole.wrap(service(3, None));
    while all.next().await.is_some() {}
    assert_eq!(counter.bytes(), whole.bytes());
    assert_eq!(end_of(&seen).await, End::Clean);
    let seen = seen.lock().unwrap();
    // Client, guard, hash, Metadata, Service: the whole service, in order.
    assert_eq!(seen.indices, [4, 5, 6]);
    assert_eq!(seen.service_len, Some(3 * MIB));
    // The node said nothing before the end, so the call waited the whole wait once.
    assert!(started.elapsed() >= EARLY_ANSWER_WAIT);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_has_the_service_answers_first_and_gets_none_of_it() {
    let (channel, seen) = serve(Node::Has).await;
    let counter = SendCounter::new();
    let total = 1024;
    let parts = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(service(total, None)),
        &[1],
        TIMEOUT,
        EARLY_ANSWER_WAIT,
        &Skips::new(),
    )
    .await
    .unwrap();
    assert_eq!(cost_of(&parts).cost.unwrap().n, "42");
    // The answer came in the wait after the hash: no Buffer of the service left.
    assert!(!counter.ended());
    assert_eq!(counter.buffers(), 0, "{} bytes went", counter.bytes());
    // The request is reset, never ended cleanly after a cut service.
    let end = end_of(&seen).await;
    assert!(matches!(end, End::Reset(_)), "{end:?}");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.indices, [4]);
    assert_eq!(seen.after_hash, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn with_no_wait_the_service_stops_when_the_answer_comes() {
    let (channel, seen) = serve(Node::Has).await;
    let counter = SendCounter::new();
    let total = 1024; // 1 GiB: far more than goes in the time of the answer.
    let parts = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(service(total, None)),
        &[1],
        TIMEOUT,
        Duration::ZERO,
        &Skips::new(),
    )
    .await
    .unwrap();
    assert_eq!(cost_of(&parts).cost.unwrap().n, "42");
    assert!(!counter.ended());
    let sent = counter.bytes();
    assert!(sent < (total * MIB / 4) as u64, "{sent} bytes went");
    let end = end_of(&seen).await;
    assert!(matches!(end, End::Reset(_)), "{end:?}");
    // The node got no more than the app took (one Buffer can be on its way).
    let got = seen.lock().unwrap().after_hash;
    assert!(
        got <= counter.buffers(),
        "the node got {got}, the app took {}",
        counter.buffers()
    );
    println!("with no wait: {sent} bytes of the service went, {got} Buffers reached the node");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_still_sends_the_markers_and_the_node_refuses() {
    let (channel, seen) = serve(Node::Lacks).await;
    let counter = SendCounter::new();
    let error = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(service(4, Some(1))),
        &[1],
        TIMEOUT,
        EARLY_ANSWER_WAIT,
        &Skips::new(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, WireError::Protocol(ref t) if t == "the transfer was cancelled"),
        "{error}"
    );
    // The node parsed the markers and refused the request: no Service. (The parser of
    // bee-rpc-rs can see the cut block at the end before it sees the blocks intersect;
    // nodo refuses them with IntersectionError.)
    //
    // On a slow machine (the CI runners) the reset of the cancel can reach the node before
    // the node reads the markers, and h2 drops the data that it did not read yet: the end is
    // then a reset, with the same result for the app (no Service).
    let end = end_of(&seen).await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.service_len, None);
    match end {
        End::Refused(_) => assert_eq!(seen.indices, [4, 5]),
        End::Reset(_) => assert!(seen.indices.starts_with(&[1, 2]), "{:?}", seen.indices),
        End::Clean => panic!("the node took the cut service as complete"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_after_the_hash_is_the_error_and_stops_the_service() {
    let (channel, seen) = serve(Node::Refuses).await;
    let counter = SendCounter::new();
    let error = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(service(1024, None)),
        &[1],
        TIMEOUT,
        EARLY_ANSWER_WAIT,
        &Skips::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("This node is closed now"),
        "{error}"
    );
    assert!(!counter.ended());
    assert_eq!(counter.buffers(), 0);
    assert_eq!(seen.lock().unwrap().indices, [4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_block_that_the_node_holds_stops_when_it_asks() {
    let total = 64;
    // The node says that it holds the block, and the stream is not wrapped: a call that
    // does not take the requests sends the whole block, as to a node of before.
    let (channel, seen) = serve(Node::Holds).await;
    let skips = Skips::new();
    let parts = call_with_service(
        channel,
        PATH,
        head(),
        service_with_block(total),
        &[1],
        TIMEOUT,
        Duration::ZERO,
        &skips,
    )
    .await
    .unwrap();
    assert_eq!(cost_of(&parts).cost.unwrap().n, "42");
    assert_eq!(end_of(&seen).await, End::Clean);
    assert_eq!(seen.lock().unwrap().block_bytes, total * MIB);
    assert_eq!(skips.blocks(), 1);

    // The same node, with the stream wrapped: the body stops, the markers do not.
    let (channel, seen) = serve(Node::Holds).await;
    let counter = SendCounter::new();
    let skips = Skips::new();
    let parts = call_with_service(
        channel,
        PATH,
        head(),
        counter.wrap(skips.wrap(service_with_block(total))),
        &[1],
        TIMEOUT,
        Duration::ZERO,
        &skips,
    )
    .await
    .unwrap();
    assert_eq!(cost_of(&parts).cost.unwrap().n, "42");
    assert_eq!(end_of(&seen).await, End::Clean);
    assert!(counter.ended());
    assert_eq!(skips.blocks(), 1);
    let got = seen.lock().unwrap().block_bytes;
    assert!(
        got < total * MIB / 2,
        "the node read {got} bytes of the block"
    );
    assert!(
        counter.bytes() < (total * MIB / 2) as u64,
        "the call sent {} bytes",
        counter.bytes()
    );
    // The end marker of the block went, once, so that the node can close it.
    assert_eq!(seen.lock().unwrap().block_ends, 1);
}
