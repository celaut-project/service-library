//! The receipts of a `ServiceTunnel` stream (nodo#419), against an in-process gRPC
//! server that relays as nodo does (`nodo:src/tunneling/rpc_tunnel.py`): it echoes each
//! message of the caller, puts each message in the chain of its direction, and signs a
//! checkpoint after each echo and a close when the request ends. The key is the key of
//! the test vectors of nodo.
//!
//! A node that lies (it signs bytes that it did not relay) must show as a mismatch, a
//! stream that the client cancels must end with no close and no error, and index 2 must
//! not break the byte stream.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::{Amount, ExecutionReceipt, TokenMessage};
use prost::Message;
use service_library::receipt::{self, Chain, DIRECTION_IN, DIRECTION_OUT};
use service_library::tunnel::open;
use service_library::tunnel::{ReceiptEvent, ReceiptSink, StreamReceipt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

const KEY: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];
const PEER_ID: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
const TOKEN: &str = "instance-token-for-vectors";
const SESSION: &str = "00112233445566778899aabbccddeeff";

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

/// How the node behaves.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Node {
    /// Relays and signs what it relayed.
    Honest,
    /// Sends the caller its message in upper case, but signs the message it got.
    SignsOtherOut,
    /// Signs one more message from the caller than it got.
    SignsOtherIn,
    /// Sends a checkpoint before the echo, so its `in` chain counts fewer messages than
    /// the caller sent.
    Early,
    /// Signs with a token that is not the token of the stream.
    OtherToken,
}

fn part(index: i32, data: Vec<u8>) -> Vec<Buffer> {
    serialize_part(&Part::new(index, data)).collect()
}

struct Relay {
    node: Node,
    ins: Chain,
    outs: Chain,
    seq: u64,
}

impl Relay {
    fn receipt(&mut self, kind: &str) -> Vec<Buffer> {
        self.seq += 1;
        let (mut ins, outs) = (self.ins.clone(), self.outs.clone());
        if self.node == Node::SignsOtherIn {
            ins.push(DIRECTION_IN, b"never sent");
        }
        let token = if self.node == Node::OtherToken {
            "other"
        } else {
            TOKEN
        };
        let mut r = ExecutionReceipt {
            kind: kind.into(),
            execution_name: receipt::execution_name(token, PEER_ID).unwrap(),
            ts: 1798329600,
            plane: receipt::PLANE_TUNNEL.into(),
            session: SESSION.into(),
            slot: "8080".into(),
            seq: self.seq,
            in_events: ins.events,
            in_bytes: ins.bytes,
            in_head: ins.head,
            out_events: outs.events,
            out_bytes: outs.bytes,
            out_head: outs.head,
            mu_charged: Some(Amount { n: "10000".into() }),
            coverage: receipt::COVERAGE_CLIENT_TUNNEL.into(),
            ..Default::default()
        };
        receipt::sign_with_seed(&mut r, &KEY).unwrap();
        part(2, r.encode_to_vec())
    }
}

async fn node(
    kind: Node,
    mut request: Streaming<Buffer>,
    tx: mpsc::Sender<Result<Buffer, Status>>,
    asked: Arc<Mutex<Option<bool>>>,
) {
    let mut parser = Parser::new(ParseOptions::accepting([0, 1]));
    let mut relay = Relay {
        node: kind,
        ins: Chain::default(),
        outs: Chain::default(),
        seq: 0,
    };
    let send = |buffers: Vec<Buffer>| {
        let tx = tx.clone();
        async move {
            for b in buffers {
                if tx.send(Ok(b)).await.is_err() {
                    return false;
                }
            }
            true
        }
    };
    while let Ok(Some(buffer)) = request.message().await {
        for p in parser.push(buffer).unwrap() {
            if p.index == 1 {
                *asked.lock().unwrap() =
                    Some(TokenMessage::decode(p.data.as_slice()).unwrap().receipts);
                continue;
            }
            if kind == Node::Early {
                // Signed before the node saw this message.
                if !send(relay.receipt(receipt::KIND_CHECKPOINT)).await {
                    return;
                }
            }
            relay.ins.push(DIRECTION_IN, &p.data);
            let echo = if kind == Node::SignsOtherOut {
                p.data.to_ascii_uppercase()
            } else {
                p.data.clone()
            };
            relay.outs.push(DIRECTION_OUT, &p.data);
            if !send(part(0, echo)).await {
                return;
            }
            if !send(relay.receipt(receipt::KIND_CHECKPOINT)).await {
                return;
            }
        }
    }
    // The caller stopped sending: the stream stops normally, with a close.
    let _ = send(relay.receipt(receipt::KIND_CLOSE)).await;
}

#[derive(Clone)]
struct Server(Node, Arc<Mutex<Option<bool>>>);

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "celaut.Gateway";
}

struct Method(Node, Arc<Mutex<Option<bool>>>);

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
        let (kind, asked) = (self.0, self.1.clone());
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(Method(kind, asked), request).await)
        })
    }
}

async fn serve(kind: Node) -> (tonic::transport::Channel, Arc<Mutex<Option<bool>>>) {
    let asked = Arc::new(Mutex::new(None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server(kind, asked.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, asked)
}

fn sink() -> (ReceiptSink, Arc<Mutex<Vec<ReceiptEvent>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let kept = events.clone();
    (Arc::new(move |e| kept.lock().unwrap().push(e)), events)
}

/// Wait for the end of the stream in the sink.
async fn ended(events: &Arc<Mutex<Vec<ReceiptEvent>>>) -> Vec<ReceiptEvent> {
    for _ in 0..500 {
        {
            let events = events.lock().unwrap();
            if matches!(events.last(), Some(ReceiptEvent::Ended { .. })) {
                return events.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the stream did not end");
}

fn receipts(events: &[ReceiptEvent]) -> Vec<StreamReceipt> {
    events
        .iter()
        .filter_map(|e| match e {
            ReceiptEvent::Receipt(r) => Some(r.clone()),
            ReceiptEvent::Ended { .. } => None,
        })
        .collect()
}

/// Send `messages`, half-close, and read the bytes to the end.
async fn exchange(
    kind: Node,
    messages: &[&[u8]],
    sink: Option<ReceiptSink>,
) -> (Vec<u8>, Option<bool>) {
    let (channel, asked) = serve(kind).await;
    let mut tunnel = open(channel, Some(PEER_ID), TOKEN, 8080, sink);
    for m in messages {
        tunnel.to_service.send(m.to_vec()).await.unwrap();
    }
    drop(tunnel.to_service);
    let mut got = Vec::new();
    while let Some(item) = tunnel.from_service.recv().await {
        got.extend(item.unwrap());
    }
    let asked = *asked.lock().unwrap();
    (got, asked)
}

#[tokio::test]
async fn an_honest_node_gives_checkpoints_and_a_close_that_match() {
    let (sink, events) = sink();
    let (got, asked) = exchange(Node::Honest, &[b"hello", b"world"], Some(sink)).await;
    assert_eq!(asked, Some(true), "the client asks for receipts");
    assert_eq!(got, b"helloworld", "index 2 does not get into the bytes");
    let events = ended(&events).await;
    let rs = receipts(&events);
    assert_eq!(rs.len(), 3);
    assert!(rs.iter().all(StreamReceipt::is_good), "{rs:?}");
    assert_eq!(rs[2].receipt.kind, receipt::KIND_CLOSE);
    assert_eq!((rs[2].receipt.in_events, rs[2].receipt.out_bytes), (2, 10));
    assert_eq!(
        events.last(),
        Some(&ReceiptEvent::Ended {
            session: Some(SESSION.into()),
            closed: true,
            cancelled: false,
        })
    );
}

#[tokio::test]
async fn index_2_with_no_sink_does_not_break_the_bytes() {
    let (got, _) = exchange(Node::Honest, &[b"a", b"b", b"c"], None).await;
    assert_eq!(got, b"abc");
}

#[tokio::test]
async fn a_node_that_signs_other_bytes_out_is_a_mismatch() {
    let (sink, events) = sink();
    let (got, _) = exchange(Node::SignsOtherOut, &[b"hello"], Some(sink)).await;
    assert_eq!(got, b"HELLO");
    let rs = receipts(&ended(&events).await);
    assert!(rs[0].problems.is_empty(), "the signature is good");
    assert!(rs[0].got_mismatch && !rs[0].sent_mismatch, "{rs:?}");
}

#[tokio::test]
async fn a_node_that_signs_a_message_that_was_never_sent_is_a_mismatch() {
    let (sink, events) = sink();
    exchange(Node::SignsOtherIn, &[b"hello"], Some(sink)).await;
    let rs = receipts(&ended(&events).await);
    assert!(
        rs.iter().all(|r| r.sent_mismatch && !r.got_mismatch),
        "{rs:?}"
    );
}

#[tokio::test]
async fn a_checkpoint_that_counts_fewer_sent_messages_matches() {
    let (sink, events) = sink();
    exchange(Node::Early, &[b"one", b"two", b"three"], Some(sink)).await;
    let rs = receipts(&ended(&events).await);
    assert_eq!(rs.len(), 7);
    assert!(rs.iter().all(StreamReceipt::is_good), "{rs:?}");
}

#[tokio::test]
async fn a_receipt_for_another_token_does_not_verify() {
    let (sink, events) = sink();
    exchange(Node::OtherToken, &[b"x"], Some(sink)).await;
    let rs = receipts(&ended(&events).await);
    assert!(rs.iter().all(|r| r
        .problems
        .contains(&"execution_name is not the name of this token on this node".to_string())));
}

#[tokio::test]
async fn a_cancel_gives_no_close_and_no_error() {
    let (sink, events) = sink();
    let (channel, _) = serve(Node::Honest).await;
    let mut tunnel = open(channel, Some(PEER_ID), TOKEN, 8080, Some(sink));
    tunnel.to_service.send(b"ping".to_vec()).await.unwrap();
    let first = tunnel.from_service.recv().await.unwrap().unwrap();
    assert_eq!(first, b"ping");
    // The reader goes away before the stream ends. The request stays open, so the node
    // does not end the stream on its own.
    let service_library::tunnel::Tunnel {
        to_service,
        from_service,
    } = tunnel;
    drop(from_service);
    let events = ended(&events).await;
    drop(to_service);
    match events.last() {
        Some(ReceiptEvent::Ended {
            closed, cancelled, ..
        }) => assert!(!closed && *cancelled),
        other => panic!("{other:?}"),
    }
    assert!(receipts(&events).iter().all(StreamReceipt::is_good));
}
