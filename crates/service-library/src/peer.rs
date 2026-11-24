//! The signature of a `Peer` (`GetPeerInfo`), as nodo makes and checks it
//! (`nodo:src/identity/node_identity.py`, `canonical_peer_content_digest` and
//! `canonical_peer_payload`, and `manager.verified_peer_public_key`).
//!
//! The node signs `<public_key>|<ts>|<content_digest>` (no prefix: the first part is the
//! key). The digest covers everything that the node advertises, built field by field and
//! never from the protobuf bytes: the parts are `signature_scheme`, `uri`,
//! `payment_contracts`, `rates`, `limits`, `resources`, `devices` and `activity_window`, in
//! the order of their numbers in `celaut.proto`, joined by `|`. `public_key`, `ts` and
//! `signature` are not in it: the signed text has the key and the time before the digest.
//! Every repeated element is sorted, so the digest does not depend on the order that a node
//! enumerated them in. A key/value list reads as the last entry of each key, sorted by key.
//!
//! [`verify`] is the check that a node does before it registers a peer. A service that
//! reads `GetPeerInfo` ([`crate::gateway::Gateway::peer_info`]) calls it to know that the
//! rates and the identity are what the node signed.

use std::collections::BTreeMap;

use celaut_proto::celaut::peer::signature_scheme::Protocol as SchemeComponent;
use celaut_proto::celaut::peer::uri::Protocol as UriProtocol;
use celaut_proto::celaut::service::container::Architecture;
use celaut_proto::celaut::{
    ArchitectureResources, Contract, ContractRate, Device, Peer, Sysresources,
};

use crate::receipt::{blake2b_256_hex, scheme_is_ours};
use crate::signing;

fn component(formal: &[u8], tags: &[String], prose: &str) -> String {
    let mut tags = tags.to_vec();
    tags.sort();
    format!("{}~{}~{}", hex::encode(formal), tags.join(","), prose)
}

fn uri_protocol(p: &Option<UriProtocol>) -> String {
    let p = p.clone().unwrap_or_default();
    component(&p.formal, &p.tags, &p.prose)
}

fn architecture(a: &Option<Architecture>) -> String {
    let a = a.clone().unwrap_or_default();
    component(&a.formal, &a.tags, &a.prose)
}

/// Deterministic encoding of one `Contract` (`canonical_contract_message`): the ledger, the
/// protocol and the xattrs, as `formal~tags~prose` of each and then `key=hex(value)` of each
/// key, sorted, joined by `;`. A `ReputationProof` uses it for its `ledger_proofs`.
pub fn canonical_contract(c: &Contract) -> String {
    let ledger = c.ledger.clone().unwrap_or_default();
    let protocol = c.protocol.clone().unwrap_or_default();
    let xattrs: BTreeMap<&str, &[u8]> = c
        .xattrs
        .iter()
        .map(|e| (e.key.as_str(), e.value.as_deref().unwrap_or(&[])))
        .collect();
    let xattrs = xattrs
        .iter()
        .map(|(k, v)| format!("{k}={}", hex::encode(v)))
        .collect::<Vec<_>>()
        .join(";");
    [
        hex::encode(&ledger.formal),
        sorted_tags(&ledger.tags),
        ledger.prose,
        hex::encode(&protocol.formal),
        sorted_tags(&protocol.tags),
        protocol.prose,
        xattrs,
    ]
    .join("~")
}

fn sorted_tags(tags: &[String]) -> String {
    let mut tags = tags.to_vec();
    tags.sort();
    tags.join(",")
}

fn contract_rate(r: &ContractRate) -> String {
    let contract = r.contract.clone().unwrap_or_default();
    format!(
        "{}~{}",
        canonical_contract(&contract),
        r.mu_per_unit.as_ref().map_or("", |a| a.n.as_str())
    )
}

fn optional_number(value: &Option<u64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

/// One `Sysresources`: its scalars with their presence (an unset limit is not a limit of 0),
/// and its benchmark, sorted.
fn sysresources(s: &Sysresources) -> String {
    let scalars = [
        ("blkio_weight", &s.blkio_weight),
        ("cpu_period", &s.cpu_period),
        ("cpu_quota", &s.cpu_quota),
        ("mem_limit", &s.mem_limit),
        ("disk_space", &s.disk_space),
    ]
    .iter()
    .map(|(name, v)| format!("{name}={}", optional_number(v)))
    .collect::<Vec<_>>()
    .join(",");
    let mut benchmark: Vec<String> = s
        .benchmark
        .iter()
        .map(|e| format!("{}={}", e.key, optional_number(&e.value)))
        .collect();
    benchmark.sort();
    format!("{scalars}~{}", benchmark.join(";"))
}

fn architecture_resources(e: &ArchitectureResources) -> String {
    format!(
        "{}~{}",
        architecture(&e.architecture),
        e.resources.as_ref().map_or("-".to_string(), sysresources)
    )
}

fn device(d: &Device) -> String {
    let halves = match &d.resources {
        Some(r) => [&r.at_init, &r.at_most]
            .iter()
            .map(|half| half.as_ref().map_or("-".to_string(), sysresources))
            .collect::<Vec<_>>()
            .join("~"),
        None => "-".to_string(),
    };
    let mut attach = d.attach.clone();
    attach.sort();
    format!(
        "{}~{}~{}",
        architecture(&d.architecture),
        halves,
        attach
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn sorted_join(mut parts: Vec<String>, separator: &str) -> String {
    parts.sort();
    parts.join(separator)
}

/// The text that [`content_digest`] hashes.
pub fn canonical_peer(peer: &Peer) -> String {
    let scheme = sorted_join(
        peer.signature_scheme
            .iter()
            .flat_map(|s| &s.components)
            .map(|c: &SchemeComponent| component(&c.formal, &c.tags, &c.prose))
            .collect(),
        ";",
    );
    let uris = sorted_join(
        peer.uri
            .iter()
            .map(|u| {
                let stack = sorted_join(
                    u.protocol_stack
                        .iter()
                        .map(|p| component(&p.formal, &p.tags, &p.prose))
                        .collect(),
                    ";",
                );
                [
                    u.ip.clone(),
                    u.port.to_string(),
                    u.expiry_unix_timestamp.to_string(),
                    uri_protocol(&u.transport),
                    stack,
                ]
                .join("~")
            })
            .collect(),
        "/",
    );
    let contracts = sorted_join(
        peer.payment_contracts.iter().map(contract_rate).collect(),
        "/",
    );
    let rates: BTreeMap<&str, &str> = peer
        .rates
        .iter()
        .map(|e| {
            (
                e.key.as_str(),
                e.value.as_ref().map_or("", |a| a.n.as_str()),
            )
        })
        .collect();
    let rates = rates
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";");
    let limits: BTreeMap<&str, u64> = peer
        .limits
        .iter()
        .map(|e| (e.key.as_str(), e.value.unwrap_or(0)))
        .collect();
    let limits = limits
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";");
    let resources = sorted_join(
        peer.resources.iter().map(architecture_resources).collect(),
        "/",
    );
    let devices = sorted_join(peer.devices.iter().map(device).collect(), "/");
    let window = match &peer.activity_window {
        Some(w) => {
            let spans = sorted_join(
                w.spans
                    .iter()
                    .map(|s| format!("{}-{}", s.start_minute, s.end_minute))
                    .collect(),
                ",",
            );
            format!("w:{}:{spans}", w.on_close)
        }
        None => "-".to_string(),
    };
    [
        scheme, uris, contracts, rates, limits, resources, devices, window,
    ]
    .join("|")
}

/// The digest of everything that a peer advertises (`canonical_peer_content_digest`):
/// Blake2b-256 hex of [`canonical_peer`].
pub fn content_digest(peer: &Peer) -> String {
    blake2b_256_hex(canonical_peer(peer).as_bytes())
}

/// The text that a node signs for a `Peer` (`canonical_peer_payload`).
pub fn signed_text(public_key: &str, ts: i64, content_digest: &str) -> String {
    format!("{public_key}|{ts}|{content_digest}")
}

/// Why a `Peer` is not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerFault {
    #[error("the peer has no public_key or no signature")]
    Unsigned,
    #[error("the peer signs with a scheme that this library does not speak")]
    UnknownScheme,
    #[error("the public_key is not 64 lowercase hex digits")]
    NonCanonicalKey,
    #[error("the signature does not verify for the public_key")]
    BadSignature,
}

/// What a node checks before it registers a peer (`manager.verified_peer_public_key`): it
/// has a key and a signature, it signs in the Ed25519 scheme, the key has its one spelling,
/// and the signature is over [`signed_text`]. It gives the key, which is the `peer_id`.
///
/// It does not judge the `ts` or the expiry of the addresses: a node keeps the last `ts`
/// that it accepted and refuses one that is not newer, and that state is the caller's.
pub fn verify(peer: &Peer) -> Result<String, PeerFault> {
    if peer.public_key.is_empty() || peer.signature.is_empty() {
        return Err(PeerFault::Unsigned);
    }
    if !scheme_is_ours(peer.signature_scheme.as_ref()) {
        return Err(PeerFault::UnknownScheme);
    }
    if crate::receipt::normalize_key(&peer.public_key).as_deref() != Some(peer.public_key.as_str())
    {
        return Err(PeerFault::NonCanonicalKey);
    }
    let text = signed_text(&peer.public_key, peer.ts, &content_digest(peer));
    if signing::verifies(&peer.public_key, &text, &peer.signature) {
        Ok(peer.public_key.clone())
    } else {
        Err(PeerFault::BadSignature)
    }
}

/// Sign `peer` in place with the Ed25519 key of `seed` at `ts`, as a node does
/// (`gateway.utils._sign_peer`): it sets `public_key`, `ts` and `signature`. The scheme
/// and the rest are the caller's. A service never signs a peer: this is for tests and tools.
pub fn sign_with_seed(peer: &mut Peer, ts: i64, seed: &[u8; 32]) -> Result<(), String> {
    peer.public_key = signing::sign(seed, "")?.0;
    peer.ts = ts;
    let text = signed_text(&peer.public_key, ts, &content_digest(peer));
    peer.signature = signing::sign(seed, &text)?.1;
    Ok(())
}
