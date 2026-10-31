//! `Observe` and `Chat` against an in-process gRPC server that answers as nodo does: a
//! stream of events for `Observe`, one `ChatAck` for `Chat`.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::{ChatAck, ChatMessage, ObserveEvent, ObserveRequest};
use prost::Message;
use service_library::gateway::Gateway;
use service_library::observe::ObserveOptions;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

fn part(index: i32, data: Vec<u8>) -> Vec<Buffer> {
    serialize_part(&Part::new(index, data)).collect()
}

fn event(kind: &str) -> ObserveEvent {
    ObserveEvent {
        time: "12:00:00".into(),
        kind: kind.into(),
        ..Default::default()
    }
}

/// Read the first message of the request, then answer for `path`.
async fn node(
    path: String,
    mut request: Streaming<Buffer>,
    tx: mpsc::Sender<Result<Buffer, Status>>,
) {
    let mut parser = Parser::new(ParseOptions::accepting([1]));
    let first = loop {
        match request.message().await {
            Ok(Some(buffer)) => {
                let parts = parser.push(buffer).unwrap();
                if let Some(p) = parts.into_iter().next() {
                    break p;
                }
            }
            _ => return,
        }
    };
    let send = |buffers: Vec<Buffer>| {
        let tx = tx.clone();
        async move {
            for b in buffers {
                let _ = tx.send(Ok(b)).await;
            }
        }
    };
    match path.as_str() {
        "/celaut.Gateway/Observe" => {
            let request = ObserveRequest::decode(first.data.as_slice()).unwrap();
            if request.instance_id == "gone" {
                send(part(1, event("notice").encode_to_vec())).await;
                return;
            }
            let mut kinds = vec!["session", "metrics"];
            if request.include_packets {
                kinds.push("packet");
            }
            for kind in kinds {
                send(part(1, event(kind).encode_to_vec())).await;
            }
            // The stream stops when the instance stops: the answer ends here.
        }
        "/celaut.Gateway/Chat" => {
            let message = ChatMessage::decode(first.data.as_slice()).unwrap();
            let ack = if message.body.is_empty() {
                ChatAck {
                    stored: false,
                    reason: "empty".into(),
                }
            } else {
                ChatAck {
                    stored: true,
                    reason: String::new(),
                }
            };
            send(part(1, ack.encode_to_vec())).await;
        }
        other => {
            let _ = tx.send(Err(Status::unimplemented(other.to_string()))).await;
        }
    }
}

#[derive(Clone)]
struct Server;

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "celaut.Gateway";
}

struct Method(String);

impl tonic::server::StreamingService<Buffer> for Method {
    type Response = Buffer;
    type ResponseStream = Reply;
    type Future = BoxFuture<tonic::Response<Reply>, Status>;

    fn call(&mut self, request: tonic::Request<Streaming<Buffer>>) -> Self::Future {
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(node(self.0.clone(), request.into_inner(), tx));
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
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(Method(path), request).await)
        })
    }
}

async fn gateway() -> Gateway {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Gateway::from_channel(channel)
}

async fn kinds(mut observation: service_library::observe::Observation) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(item) = observation.events.recv().await {
        out.push(item.unwrap().kind);
    }
    out
}

#[tokio::test]
async fn an_observation_gives_the_events_in_order_and_ends_with_the_instance() {
    let g = gateway().await;
    let observation = g.observe("tok", ObserveOptions::default()).await.unwrap();
    assert_eq!(kinds(observation).await, ["session", "metrics"]);
}

#[tokio::test]
async fn packets_are_asked_for_in_the_request() {
    let g = gateway().await;
    let observation = g
        .observe(
            "tok",
            ObserveOptions {
                packets: true,
                receipts: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(kinds(observation).await, ["session", "metrics", "packet"]);
}

#[tokio::test]
async fn an_observation_that_does_not_start_is_one_notice() {
    let g = gateway().await;
    let observation = g.observe("gone", ObserveOptions::default()).await.unwrap();
    assert_eq!(kinds(observation).await, ["notice"]);
}

#[tokio::test]
async fn a_chat_message_is_stored_or_the_reason_is_given() {
    let g = gateway().await;
    assert!(g.chat("c1", "hello", Some("conv")).await.unwrap().stored);
    let refused = g.chat("c1", "", None).await.unwrap();
    assert!(!refused.stored);
    assert_eq!(refused.reason, "empty");
}
