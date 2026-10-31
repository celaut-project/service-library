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
    Amount, Client, Configuration, EstimatedCost, Metrics, ModifyDepositInput, ModifyDepositOutput,
    Refund, ServiceInstance, TokenMessage,
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

    /// Stop the instance with `token`. It gives back the MU that the instance did not use.
    fn stop(&self, token: &str) -> impl Future<Output = Result<u128, Error>> + Send;
}

/// The gateway of the node of this service.
#[derive(Clone)]
pub struct Gateway {
    channel: Channel,
}

impl Gateway {
    /// A gateway at `address` (`ip:port`). It connects at the first call.
    pub fn new(address: &str) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .map_err(|e| Error::Config(format!("the gateway address {address}: {e}")))?
            .tcp_nodelay(true);
        Ok(Gateway {
            channel: endpoint.connect_lazy(),
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

    fn failed(operation: &str, attempts: u32, error: bee_rpc::Error) -> Error {
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

    async fn stop(&self, token: &str) -> Result<u128, Error> {
        self.stop_with_retries(token).await
    }
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
    fn amounts_parse_and_a_missing_one_is_zero() {
        assert_eq!(amount(None).unwrap(), 0);
        assert_eq!(amount(Some(&Amount { n: "42".into() })).unwrap(), 42);
        assert!(amount(Some(&Amount { n: "x".into() })).is_err());
    }
}
