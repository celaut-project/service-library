//! `ResolveNetwork` against an in-process node that checks the request as nodo does: the
//! service declared `db.cluster=${CLUSTER}`, `db.engine=postgresql` and
//! `db.schema=personal-nutrition`. A request must keep the fixed keys, fill the variable, and
//! may add keys.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use celaut_proto::celaut::configuration_file::NetworkResolution;
use celaut_proto::celaut::instance::{Uri, UriSlot};
use celaut_proto::celaut::service::Network;
use celaut_proto::celaut::Instance;
use prost::Message;
use service_library::gateway::Gateway;
use service_library::network;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;

/// Why the node refuses `network`, or nothing if it fits the declaration.
fn refusal(network: &Network) -> Option<&'static str> {
    let body = String::from_utf8(network.formal.clone()).ok()?;
    let get = |key: &str| {
        body.lines()
            .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
    };
    if body.contains("${") {
        return Some("unfilled variable");
    }
    if get("db.engine") != Some("postgresql") || get("db.schema") != Some("personal-nutrition") {
        return Some("a fixed key changed or is missing");
    }
    if get("db.cluster").is_none() {
        return Some("the variable is not filled");
    }
    None
}

fn member() -> Instance {
    let slot = |port: i32, host: &str| UriSlot {
        internal_port: port,
        uri: vec![Uri {
            ip: host.into(),
            port,
        }],
    };
    // No gRPC slot: this member speaks postgresql, and has a second port.
    Instance {
        api: None,
        uri_slot: vec![slot(5432, "10.0.0.9"), slot(9187, "10.0.0.9")],
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
        let (tx, rx) = mpsc::channel(16);
        let path = self.0.clone();
        let mut request = request.into_inner();
        tokio::spawn(async move {
            let mut parser = Parser::new(ParseOptions::accepting([1]));
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
            if path != "/celaut.Gateway/ResolveNetwork" {
                let _ = tx.send(Err(Status::unimplemented(path))).await;
                return;
            }
            assert_eq!(first.index, 1, "the Network is part 1");
            let network = Network::decode(first.data.as_slice()).unwrap();
            if let Some(why) = refusal(&network) {
                let _ = tx.send(Err(Status::permission_denied(why))).await;
                return;
            }
            let answer = NetworkResolution {
                tags: network.tags,
                peer_instances: vec![member()],
            };
            for b in serialize_part(&Part::new(1, answer.encode_to_vec())) {
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

const FIXED: [(&str, &str); 2] = [
    ("db.engine", "postgresql"),
    ("db.schema", "personal-nutrition"),
];

fn ask(pairs: &[(&str, &str)]) -> Network {
    network::request(&["postgres:personal-nutrition"], "", pairs, &["postgresql"]).unwrap()
}

#[tokio::test]
async fn a_request_that_fits_is_answered_with_every_port() {
    let g = gateway().await;
    let mut pairs = FIXED.to_vec();
    pairs.push(("db.cluster", "abc"));
    let resolved = g.resolve_network(ask(&pairs)).await.unwrap();
    assert_eq!(resolved.tags, ["postgres:personal-nutrition"]);
    let slots = network::slots(&resolved.peer_instances[0]);
    let ports: Vec<_> = slots.iter().map(|s| s.internal_port).collect();
    assert_eq!(ports, [5432, 9187]);
    assert_eq!(slots[0].uris, ["10.0.0.9:5432"]);
}

#[tokio::test]
async fn keys_may_be_added() {
    let g = gateway().await;
    let mut pairs = FIXED.to_vec();
    pairs.extend([("db.cluster", "abc"), ("db.extra", "1")]);
    assert!(g.resolve_network(ask(&pairs)).await.is_ok());
}

#[tokio::test]
async fn a_request_that_changes_or_drops_a_fixed_key_is_refused() {
    let g = gateway().await;
    let changed = [
        ("db.cluster", "abc"),
        ("db.engine", "mysql"),
        ("db.schema", "personal-nutrition"),
    ];
    let dropped = [("db.cluster", "abc"), ("db.engine", "postgresql")];
    for pairs in [&changed[..], &dropped[..]] {
        let e = g.resolve_network(ask(pairs)).await.unwrap_err();
        assert!(!e.is_unreachable(), "{e}");
        assert!(e.to_string().contains("fixed key"), "{e}");
    }
}

#[tokio::test]
async fn a_variable_that_is_not_filled_never_reaches_the_node() {
    let mut pairs = FIXED.to_vec();
    pairs.push(("db.cluster", "${NUTRITION_DB_CLUSTER}"));
    assert!(network::request(&["t"], "", &pairs, &[]).is_err());

    // And if a caller builds the body by hand, the node refuses it.
    let g = gateway().await;
    let mut raw = ask(&FIXED);
    raw.formal = b"db.cluster=${X}\ndb.engine=postgresql\ndb.schema=personal-nutrition".to_vec();
    let e = g.resolve_network(raw).await.unwrap_err();
    assert!(e.to_string().contains("unfilled variable"), "{e}");
}
