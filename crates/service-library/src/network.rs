//! Networks: ask the node for the members of a `Service.Network` (`ResolveNetwork`).
//!
//! A service declares networks in its `.service`. The node resolves them at the start, unless
//! a `${VAR}` in the `formal` field was not set: then the service finishes the network later
//! with [`crate::gateway::Gateway::resolve_network`], or asks again for the members of a
//! domain.

use celaut_proto::celaut::service::{api::Protocol, Network};
use celaut_proto::celaut::Instance;

use crate::Error;

/// The `formal` field of a network, built the way nodo does: the lines `key=value` sorted by
/// key, in UTF-8, joined with `\n`, and nothing else (no final newline). The node compares
/// this field byte for byte with the one that the service declared.
///
/// A value that is still `${...}` is refused: the node refuses it too, and the caller must
/// give the real value. So is a key or a value that cannot be one line, an empty key, or a
/// key given twice.
pub fn formal(pairs: &[(&str, &str)]) -> Result<Vec<u8>, Error> {
    let mut sorted: Vec<(&str, &str)> = pairs.to_vec();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    for (i, (key, value)) in sorted.iter().enumerate() {
        if key.is_empty() || key.contains(['=', '\n', '\r']) {
            return Err(Error::Protocol(format!(
                "{key:?} is not a key of a network (it is empty, or has '=' or a line break)"
            )));
        }
        if value.contains(['\n', '\r']) {
            return Err(Error::Protocol(format!(
                "the value of {key} is not on one line"
            )));
        }
        if value.contains("${") {
            return Err(Error::Protocol(format!(
                "the value of {key} is still a ${{...}} variable: give its value"
            )));
        }
        if i > 0 && sorted[i - 1].0 == *key {
            return Err(Error::Protocol(format!("the key {key} is given twice")));
        }
    }
    Ok(sorted
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("\n")
        .into_bytes())
}

/// A `Service.Network` for [`crate::gateway::Gateway::resolve_network`]. `protocols` are the
/// tags of the protocols that the peers must speak, one layer each (`protocol_stack`).
pub fn request(
    tags: &[&str],
    prose: &str,
    pairs: &[(&str, &str)],
    protocols: &[&str],
) -> Result<Network, Error> {
    Ok(Network {
        tags: tags.iter().map(|t| t.to_string()).collect(),
        prose: prose.to_string(),
        formal: formal(pairs)?,
        protocol_stack: protocols
            .iter()
            .map(|p| Protocol {
                tags: vec![p.to_string()],
                ..Default::default()
            })
            .collect(),
    })
}

/// One port of a member and where to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    /// The port inside the member. The caller picks the one of the protocol it needs.
    pub internal_port: i32,
    /// The tags of the transport that the member declares for this port (`tcp`, `udp`).
    /// Empty if the member declares none, or no port of its interface matches.
    pub transport: Vec<String>,
    /// The protocols on the transport that the member declares for this port, bottom to
    /// top, each layer with its tags (`[["postgresql"]]`). The caller reads the protocol
    /// here: the number of the port says nothing, because nobody agrees on it.
    pub protocol_stack: Vec<Vec<String>>,
    /// The addresses, as `ip:port`. Empty if the node cannot give one.
    pub uris: Vec<String>,
}

/// All the ports of `instance` with their addresses and the protocols that the member
/// declares for each one (`api.slot` with the same port), in the order of the node. Unlike
/// [`crate::config::grpc_uri`], it does not look for a gRPC port: a member can speak
/// another protocol.
pub fn slots(instance: &Instance) -> Vec<Slot> {
    instance
        .uri_slot
        .iter()
        .map(|s| {
            let declared = instance
                .api
                .as_ref()
                .and_then(|a| a.slot.iter().find(|d| d.port == s.internal_port));
            Slot {
                internal_port: s.internal_port,
                transport: declared
                    .and_then(|d| d.transport.as_ref())
                    .map(|t| t.tags.clone())
                    .unwrap_or_default(),
                protocol_stack: declared
                    .map(|d| d.protocol_stack.iter().map(|p| p.tags.clone()).collect())
                    .unwrap_or_default(),
                uris: s
                    .uri
                    .iter()
                    .map(|u| format!("{}:{}", u.ip, u.port))
                    .collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_body_is_sorted_lines_without_a_final_newline() {
        let body = formal(&[
            ("db.schema", "personal-nutrition"),
            ("db.cluster", "abc"),
            ("db.engine", "postgresql"),
        ])
        .unwrap();
        assert_eq!(
            body,
            b"db.cluster=abc\ndb.engine=postgresql\ndb.schema=personal-nutrition"
        );
    }

    #[test]
    fn no_pairs_is_no_bytes() {
        assert!(formal(&[]).unwrap().is_empty());
    }

    #[test]
    fn a_value_keeps_its_equals_signs() {
        assert_eq!(formal(&[("k", "a=b")]).unwrap(), b"k=a=b");
    }

    #[test]
    fn a_variable_that_is_not_filled_is_refused() {
        assert!(formal(&[("db.cluster", "${NUTRITION_DB_CLUSTER}")]).is_err());
        assert!(formal(&[("db.cluster", "x${A}")]).is_err());
    }

    #[test]
    fn a_bad_key_or_value_is_refused() {
        assert!(formal(&[("", "v")]).is_err());
        assert!(formal(&[("a=b", "v")]).is_err());
        assert!(formal(&[("a\nb", "v")]).is_err());
        assert!(formal(&[("a", "x\ny")]).is_err());
        assert!(formal(&[("a", "1"), ("a", "2")]).is_err());
    }

    #[test]
    fn a_slot_has_the_protocols_that_the_member_declares_for_its_port() {
        use celaut_proto::celaut::instance::{Uri, UriSlot};
        use celaut_proto::celaut::service::{api::Slot as Declared, Api};
        let proto = |t: &str| Protocol {
            tags: vec![t.into()],
            ..Default::default()
        };
        let uri_slot = |port: i32| UriSlot {
            internal_port: port,
            uri: vec![Uri {
                ip: "10.0.0.9".into(),
                port: 40000 + port,
            }],
        };
        let instance = Instance {
            api: Some(Api {
                slot: vec![Declared {
                    // Not the usual port: the protocol comes from the declaration.
                    port: 7777,
                    transport: Some(proto("tcp")),
                    protocol_stack: vec![proto("postgresql")],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            uri_slot: vec![uri_slot(7777), uri_slot(5432)],
        };
        let s = slots(&instance);
        assert_eq!(s[0].transport, ["tcp"]);
        assert_eq!(s[0].protocol_stack, [["postgresql"]]);
        assert_eq!(s[0].uris, ["10.0.0.9:47777"]);
        // A port that the interface does not declare has no protocol: it is not guessed.
        assert!(s[1].transport.is_empty() && s[1].protocol_stack.is_empty());
    }

    #[test]
    fn the_request_has_the_tags_the_body_and_the_protocols() {
        let n = request(&["postgres:x"], "p", &[("a", "1")], &["postgresql"]).unwrap();
        assert_eq!(n.tags, ["postgres:x"]);
        assert_eq!(n.prose, "p");
        assert_eq!(n.formal, b"a=1");
        assert_eq!(n.protocol_stack[0].tags, ["postgresql"]);
    }
}
