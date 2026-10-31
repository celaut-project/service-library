//! The calls of a service to the gateway of its node.
//!
//! A service is an *instance* of the node, so it needs no `Client`: each instance that it
//! starts has the service as its father, and the token of the instance is the credential
//! (`token` auth of `docs/celaut/wire.md` in chatui, and `token+client` where the father is
//! a node or an instance). The connection is plain HTTP/2 to the address of the config
//! file: it is local to the node.

use std::future::Future;
use std::time::Duration;

use bee_rpc::frame::{ParseOptions, Part};
use celaut_proto::celaut::metadata::hash_tag::Hash;
use celaut_proto::celaut::{
    Amount, ChatAck, ChatMessage, Client, Configuration, EstimatedCost, Metrics,
    ModifyDepositInput, ModifyDepositOutput, ModifyServiceSystemResourcesInput,
    ModifyServiceSystemResourcesOutput, Refund, ServiceInstance, Sysresources, TokenMessage,
};
use prost::Message;
use tonic::transport::{Channel, Endpoint};

use crate::error::is_unreachable;
use crate::Error;

/// The type id of SHA3-256: the digest of the empty input.
pub const SHA3_256_TYPE: [u8; 32] = [
    0xa7, 0xff, 0xc6, 0xf8, 0xbf, 0x1e, 0xd7, 0x66, 0x51, 0xc1, 0x47, 0x56, 0xa0, 0x61, 0xd6, 0x62,
    0xf5, 0x80, 0xff, 0x4d, 0xe4, 0x3b, 0x49, 0xfa, 0x82, 0xd8, 0x0a, 0x4b, 0x80, 0xf8, 0x43, 0x4a,
];

/// How many times [`Gateway::stop_with_retries`] tries. Unbounded retries turn an
/// unreachable gateway into a hung task instead of a reportable error.
pub const STOP_MAX_ATTEMPTS: u32 = 5;

/// The time limit of a simple call.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The time limit of `StartService`: the node can build the service and boot it before it
/// answers.
pub const START_TIMEOUT: Duration = Duration::from_secs(600);

/// What the pool needs from a node: start an instance, and stop it. [`Gateway`] is the real
/// one, and a test can give another.
pub trait Node: Send + Sync + 'static {
    /// Start the service `service_hash` (SHA3-256, 32 bytes) with `configuration`.
    fn start(
        &self,
        service_hash: [u8; 32],
        configuration: Configuration,
    ) -> impl Future<Output = Result<ServiceInstance, Error>> + Send;

    /// Start the service of the `.bee` file at `path`, sending the file: for a node that does
    /// not have it. `info` comes from [`crate::package::inspect`]. A node that has the
    /// service already answers after the hash and the file stops there; a node that has
    /// only some of its blocks asks for them to be skipped (nodo#584).
    fn start_package(
        &self,
        path: &std::path::Path,
        info: &crate::package::PackageInfo,
        configuration: Configuration,
    ) -> impl Future<Output = Result<ServiceInstance, Error>> + Send;

    /// Stop the instance with `token`. It gives back the MU that the instance did not use.
    fn stop(&self, token: &str) -> impl Future<Output = Result<u128, Error>> + Send;
}

/// The gateway of the node of this service.
#[derive(Clone)]
pub struct Gateway {
    channel: Channel,
    peer_id: Option<String>,
}

impl Gateway {
    /// A gateway at `address` (`ip:port`). It connects at the first call.
    pub fn new(address: &str) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .map_err(|e| Error::Config(format!("the gateway address {address}: {e}")))?
            .tcp_nodelay(true);
        Ok(Gateway {
            channel: endpoint.connect_lazy(),
            peer_id: None,
        })
    }

    async fn call(
        &self,
        method: &str,
        parts: Vec<Part>,
        accepted: &[i32],
        timeout: Duration,
    ) -> Result<Vec<Part>, bee_rpc::Error> {
        let options = bee_rpc::grpc::CallOptions {
            parse: ParseOptions::accepting(accepted.iter().copied()),
            block_skip: false,
            timeout: Some(timeout),
        };
        let path = format!("/celaut.Gateway/{method}");
        bee_rpc::grpc::call(self.channel.clone(), &path, parts, options).await
    }

    pub(crate) fn failed(operation: &str, attempts: u32, error: bee_rpc::Error) -> Error {
        Error::Gateway {
            operation: operation.to_string(),
            attempts,
            unreachable: is_unreachable(&error),
            message: error.to_string(),
        }
    }

    /// `GetServiceEstimatedCost` for a service that the node already has.
    pub async fn estimated_cost(
        &self,
        service_hash: [u8; 32],
        configuration: Configuration,
    ) -> Result<EstimatedCost, Error> {
        let answer = self
            .call(
                "GetServiceEstimatedCost",
                start_parts(service_hash, &configuration),
                &[1],
                CALL_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("estimating the cost of a service", 1, e))?;
        find(&answer, "EstimatedCost")
    }

    /// `GetMetrics` of an instance: its balance in MU. No part means a balance of 0.
    pub async fn metrics(&self, token: &str) -> Result<Metrics, Error> {
        let answer = self
            .call("GetMetrics", token_parts(token), &[1], CALL_TIMEOUT)
            .await
            .map_err(|e| Self::failed("reading the metrics of an instance", 1, e))?;
        match answer.iter().find(|p| p.index == 1) {
            Some(part) => decode(part, "Metrics"),
            None => Ok(Metrics::default()),
        }
    }

    /// `ModifyDeposit`: move `difference_mu` MU between this service and the instance
    /// `token`. A positive value moves balance to the instance, a negative one takes it
    /// back. The node says in `message` why it moved nothing.
    pub async fn modify_deposit(
        &self,
        token: &str,
        difference_mu: i128,
    ) -> Result<ModifyDepositOutput, Error> {
        let input = ModifyDepositInput {
            difference: Some(Amount {
                n: difference_mu.to_string(),
            }),
            service_token: token.to_string(),
        };
        let answer = self
            .call(
                "ModifyDeposit",
                vec![Part::new(1, input.encode_to_vec())],
                &[1],
                CALL_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("moving the balance of an instance", 1, e))?;
        match answer.iter().find(|p| p.index == 1) {
            Some(part) => decode(part, "ModifyDepositOutput"),
            // A false bool and an empty text have no bytes.
            None => Ok(ModifyDepositOutput::default()),
        }
    }

    /// A gateway on a channel that the caller made: for a test, or for a connection with its
    /// own settings.
    pub fn from_channel(channel: Channel) -> Self {
        Gateway {
            channel,
            peer_id: None,
        }
    }

    /// `Chat`: send a text message to the operator of the node. `client_id` is the id of
    /// this service as a client of the node that gets the message, which must have it bound
    /// to a peer. The answer says if the node stored the message and, if not, why.
    pub async fn chat(
        &self,
        client_id: &str,
        body: &str,
        conversation_id: Option<&str>,
    ) -> Result<ChatAck, Error> {
        let message = ChatMessage {
            client_id: client_id.to_string(),
            body: body.to_string(),
            conversation_id: conversation_id.map(str::to_string),
            service: None,
        };
        let answer = self
            .call(
                "Chat",
                vec![Part::new(1, message.encode_to_vec())],
                &[1],
                CALL_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("sending a chat message", 1, e))?;
        match answer.iter().find(|p| p.index == 1) {
            Some(part) => decode(part, "ChatAck"),
            // A false bool and an empty text have no bytes.
            None => Ok(ChatAck::default()),
        }
    }

    /// The channel of the gateway, for the tunnel.
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// The `peer_id` that the receipts of a tunnel must name. A service does not know the
    /// identity of its node unless it was told: see [`Gateway::with_peer_id`].
    pub fn peer_id(&self) -> Option<&str> {
        self.peer_id.as_deref()
    }

    /// The same gateway, with the `peer_id` of the node that the receipts must name.
    pub fn with_peer_id(mut self, peer_id: impl Into<String>) -> Self {
        self.peer_id = Some(peer_id.into());
        self
    }

    /// `ServiceTunnel`: send `payload` to the port `slot` of the local instance `token`, close
    /// the write side, and give everything that comes back. For an instance that has no
    /// address that the service can reach. The node takes the traffic cost from the balance
    /// of the instance. It holds the whole answer in memory: for a stream, open the tunnel
    /// with `bee_rpc` directly.
    pub async fn tunnel(&self, token: &str, slot: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let answer = self
            .call(
                "ServiceTunnel",
                tunnel_parts(token, slot, payload),
                &[0],
                START_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("using the tunnel of an instance", 1, e))?;
        Ok(answer
            .into_iter()
            .filter(|p| p.index == 0)
            .flat_map(|p| p.data)
            .collect())
    }

    /// `ModifyServiceSystemResources`: ask the node for the resources `max` for this
    /// service. Only a local instance may call it, and the node takes a fee from the
    /// balance of the instance. It gives the resources that the service has now, and its
    /// balance in MU.
    pub async fn modify_resources(
        &self,
        max: Sysresources,
    ) -> Result<ModifyServiceSystemResourcesOutput, Error> {
        let input = ModifyServiceSystemResourcesInput {
            min_sysreq: None,
            max_sysreq: Some(max),
        };
        let answer = self
            .call(
                "ModifyServiceSystemResources",
                vec![Part::new(1, input.encode_to_vec())],
                &[1],
                CALL_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("changing the resources of the service", 1, e))?;
        match answer.iter().find(|p| p.index == 1) {
            Some(part) => decode(part, "ModifyServiceSystemResourcesOutput"),
            None => Ok(ModifyServiceSystemResourcesOutput::default()),
        }
    }

    /// [`Node::stop`], tried up to [`STOP_MAX_ATTEMPTS`] times with a pause that grows.
    /// If every attempt fails, the instance can still be running and billing: the error
    /// says so, with the last error of the node.
    pub async fn stop_with_retries(&self, token: &str) -> Result<u128, Error> {
        let mut last = None;
        for attempt in 1..=STOP_MAX_ATTEMPTS {
            match self
                .call("StopService", token_parts(token), &[1], CALL_TIMEOUT)
                .await
            {
                Ok(answer) => {
                    return match answer.iter().find(|p| p.index == 1) {
                        Some(part) => {
                            let refund: Refund = decode(part, "Refund")?;
                            amount(refund.amount.as_ref())
                        }
                        // A Refund of 0 has no bytes.
                        None => Ok(0),
                    };
                }
                Err(e) => {
                    last = Some(e);
                    if attempt < STOP_MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
                    }
                }
            }
        }
        Err(Self::failed(
            &format!("stopping the instance {token} (it may still be running)"),
            STOP_MAX_ATTEMPTS,
            last.expect("at least one attempt"),
        ))
    }
}

impl Node for Gateway {
    async fn start(
        &self,
        service_hash: [u8; 32],
        configuration: Configuration,
    ) -> Result<ServiceInstance, Error> {
        let answer = self
            .call(
                "StartService",
                start_parts(service_hash, &configuration),
                &[1],
                START_TIMEOUT,
            )
            .await
            .map_err(|e| Self::failed("starting a service", 1, e))?;
        let instance: ServiceInstance = find(&answer, "ServiceInstance")?;
        if instance.token.is_empty() {
            return Err(Error::Protocol(
                "the node gave an instance with no token".to_string(),
            ));
        }
        Ok(instance)
    }

    async fn start_package(
        &self,
        path: &std::path::Path,
        info: &crate::package::PackageInfo,
        configuration: Configuration,
    ) -> Result<ServiceInstance, Error> {
        use tokio_stream::StreamExt;
        // The head: the Configuration (3) and the Hash (4). Then the Metadata and the
        // Service of the file, without the blocks that the node says it holds. A node that
        // has the service answers after the Hash, and the call stops sending the file.
        let head = start_parts(info.service_id, &configuration);
        let file: crate::relay::Buffers = Box::pin(
            crate::package::read(path.to_path_buf(), info.layout)
                .map(|item| item.map_err(|e| Error::Protocol(format!("the package: {e}")))),
        );
        let skips = crate::skips::Skips::new();
        let answer = crate::relay::call_with_service(
            self.channel.clone(),
            "/celaut.Gateway/StartService",
            head,
            skips.wrap(file),
            &[1],
            START_TIMEOUT,
            crate::relay::EARLY_ANSWER_WAIT,
            &skips,
        )
        .await?;
        let instance: ServiceInstance = find(&answer, "ServiceInstance")?;
        if instance.token.is_empty() {
            return Err(Error::Protocol(
                "the node gave an instance with no token".to_string(),
            ));
        }
        Ok(instance)
    }

    async fn stop(&self, token: &str) -> Result<u128, Error> {
        self.stop_with_retries(token).await
    }
}

/// The bytes of one message of a tunnel: the node reads the bytes at index 0 in pieces.
pub const TUNNEL_PIECE: usize = 64 * 1024;

/// The parts of a `ServiceTunnel` request: the handshake (1), then `payload` at index 0 in
/// pieces of [`TUNNEL_PIECE`]. The end of the request half-closes the connection to the
/// service.
pub fn tunnel_parts(token: &str, slot: u16, payload: &[u8]) -> Vec<Part> {
    let handshake = TokenMessage {
        token: token.to_string(),
        slot: Some(slot.to_string()),
        receipts: false,
    };
    let mut parts = vec![Part::new(1, handshake.encode_to_vec())];
    parts.extend(
        payload
            .chunks(TUNNEL_PIECE)
            .map(|c| Part::new(0, c.to_vec())),
    );
    parts
}

/// The parts of `StartService` for a service that the node has: the Configuration (3, only
/// if it has bytes: a receiver drops a part with none) and the Hash (4). A service has no
/// `Client` (index 1) and sends no `RecursionGuard` (2): the node knows who calls.
pub fn start_parts(service_hash: [u8; 32], configuration: &Configuration) -> Vec<Part> {
    let mut parts = Vec::new();
    let bytes = configuration.encode_to_vec();
    if !bytes.is_empty() {
        parts.push(Part::new(3, bytes));
    }
    let hash = Hash {
        r#type: SHA3_256_TYPE.to_vec(),
        value: service_hash.to_vec(),
    };
    parts.push(Part::new(4, hash.encode_to_vec()));
    parts
}

fn token_parts(token: &str) -> Vec<Part> {
    let message = TokenMessage {
        token: token.to_string(),
        slot: None,
        receipts: false,
    };
    vec![Part::new(1, message.encode_to_vec())]
}

/// `client_id` as a `Client` part, for the calls that a service makes on behalf of a
/// client (a service that holds a `client_id` on another node).
pub fn client_part(index: i32, client_id: &str) -> Part {
    let client = Client {
        client_id: client_id.to_string(),
        ..Default::default()
    };
    Part::new(index, client.encode_to_vec())
}

fn decode<M: Message + Default>(part: &Part, what: &str) -> Result<M, Error> {
    M::decode(part.data.as_slice())
        .map_err(|e| Error::Protocol(format!("the {what} does not decode: {e}")))
}

fn find<M: Message + Default>(answer: &[Part], what: &str) -> Result<M, Error> {
    match answer.iter().find(|p| p.index == 1) {
        Some(part) => decode(part, what),
        None => Err(Error::Protocol(format!("the node gave no {what}"))),
    }
}

/// An `Amount` in MU. No amount is 0.
pub fn amount(amount: Option<&Amount>) -> Result<u128, Error> {
    match amount {
        None => Ok(0),
        Some(a) if a.n.is_empty() => Ok(0),
        Some(a) => {
            a.n.parse()
                .map_err(|_| Error::Protocol(format!("the amount {:?} is not a number", a.n)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_start_has_the_hash_at_four_and_no_client() {
        let parts = start_parts([7; 32], &Configuration::default());
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].index, 4);
        let hash = Hash::decode(parts[0].data.as_slice()).unwrap();
        assert_eq!(hash.r#type, SHA3_256_TYPE);
        assert_eq!(hash.value, vec![7; 32]);
    }

    #[test]
    fn a_configuration_with_bytes_goes_at_three_before_the_hash() {
        let configuration = Configuration {
            initial_mu: Some(Amount { n: "5".into() }),
            ..Default::default()
        };
        let parts = start_parts([1; 32], &configuration);
        assert_eq!(
            parts.iter().map(|p| p.index).collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn a_tunnel_request_has_the_handshake_then_the_bytes_in_pieces() {
        let payload = vec![7u8; TUNNEL_PIECE + 10];
        let parts = tunnel_parts("tok", 8080, &payload);
        assert_eq!(
            parts.iter().map(|p| p.index).collect::<Vec<_>>(),
            vec![1, 0, 0]
        );
        let handshake = TokenMessage::decode(parts[0].data.as_slice()).unwrap();
        assert_eq!(handshake.token, "tok");
        assert_eq!(handshake.slot.as_deref(), Some("8080"));
        assert_eq!(parts[1].data.len(), TUNNEL_PIECE);
        assert_eq!(parts[2].data.len(), 10);
    }

    #[test]
    fn an_empty_payload_sends_only_the_handshake() {
        assert_eq!(tunnel_parts("t", 1, &[]).len(), 1);
    }

    #[test]
    fn amounts_parse_and_a_missing_one_is_zero() {
        assert_eq!(amount(None).unwrap(), 0);
        assert_eq!(amount(Some(&Amount { n: "42".into() })).unwrap(), 42);
        assert!(amount(Some(&Amount { n: "x".into() })).is_err());
    }
}
