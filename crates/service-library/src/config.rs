//! What the node tells the service: the `ConfigurationFile` at `/__config__`.

use std::path::Path;

use celaut_proto::celaut::{ConfigurationFile, Instance};
use prost::Message;

use crate::Error;

/// Where a node writes the configuration of a service.
pub const DEFAULT_CONFIG_PATH: &str = "/__config__";

/// The part of the `ConfigurationFile` that the library uses.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    /// `ip:port` of the gateway of the node, for plain gRPC over HTTP/2.
    pub gateway: String,
    /// The memory limit at the start, in bytes. 0 if the file has none.
    pub mem_limit: u64,
    /// The whole file, for the parts that a service reads itself (`config`,
    /// `network_resolution`).
    pub file: ConfigurationFile,
}

impl NodeConfig {
    /// Read the file at [`DEFAULT_CONFIG_PATH`].
    pub fn load() -> Result<Self, Error> {
        Self::load_from(DEFAULT_CONFIG_PATH)
    }

    /// Read the file at `path`.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let bytes =
            std::fs::read(path).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        Self::from_bytes(&bytes)
    }

    /// Read a `ConfigurationFile` that is already in memory.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let file = ConfigurationFile::decode(bytes)
            .map_err(|e| Error::Config(format!("it does not decode: {e}")))?;
        let gateway = file
            .gateway
            .as_ref()
            .ok_or_else(|| Error::Config("it has no gateway".to_string()))?;
        let uri = grpc_uri(gateway)?;
        Ok(NodeConfig {
            gateway: uri,
            mem_limit: file
                .initial_sysresources
                .as_ref()
                .and_then(|s| s.mem_limit)
                .unwrap_or(0),
            file,
        })
    }
}

/// The `ip:port` of the gRPC port of an instance: the first address of the first port that
/// the interface of the service declares (`get_grpc_uri` of the Python library).
pub fn grpc_uri(instance: &Instance) -> Result<String, Error> {
    let api = instance.api.as_ref();
    for slot in api.map(|a| a.slot.as_slice()).unwrap_or_default() {
        for uri_slot in &instance.uri_slot {
            if uri_slot.internal_port == slot.port {
                if let Some(uri) = uri_slot.uri.first() {
                    return Ok(format!("{}:{}", uri.ip, uri.port));
                }
            }
        }
    }
    Err(Error::Protocol(
        "no gRPC address on this instance".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use celaut_proto::celaut::instance::{Uri, UriSlot};
    use celaut_proto::celaut::service::{api::Slot, Api};
    use celaut_proto::celaut::Sysresources;

    fn gateway() -> Instance {
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
                    ip: "10.0.0.1".into(),
                    port: 5000,
                }],
            }],
        }
    }

    #[test]
    fn the_gateway_and_the_memory_come_from_the_file() {
        let file = ConfigurationFile {
            gateway: Some(gateway()),
            initial_sysresources: Some(Sysresources {
                mem_limit: Some(1 << 20),
                ..Default::default()
            }),
            ..Default::default()
        };
        let config = NodeConfig::from_bytes(&file.encode_to_vec()).unwrap();
        assert_eq!(config.gateway, "10.0.0.1:5000");
        assert_eq!(config.mem_limit, 1 << 20);
    }

    #[test]
    fn a_file_with_no_gateway_is_an_error() {
        let bytes = ConfigurationFile::default().encode_to_vec();
        assert!(matches!(
            NodeConfig::from_bytes(&bytes),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn a_missing_file_is_an_error() {
        assert!(matches!(
            NodeConfig::load_from("/nonexistent/__config__"),
            Err(Error::Config(_))
        ));
    }
}
