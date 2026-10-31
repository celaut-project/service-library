//! Execution receipts (nodo#419, `nodo:docs/EXECUTION_RECEIPTS.md`): what a node signs
//! about one execution that it runs. The client keeps them as evidence against the node.
//!
//! A receipt is an `ExecutionReceipt` of one of three kinds:
//!
//! - `start`: in the answer of `StartService`. It names the service, a salted digest of
//!   the environment variables that the service got, and what the start charged.
//! - `checkpoint`: on a `ServiceTunnel` stream (index 2), from time to time. It gives the
//!   counters and two hash chains of the stream: the messages into the instance, and the
//!   messages out of it.
//! - `close`: the last checkpoint of a stream. The node sends it when the stream stops
//!   normally. A stream that the caller cancels gets no close, and that is not a fault.
//!
//! This is a port of `nodo:src/identity/execution_receipt.py`. Each encoding must give
//! the same bytes as the Python code: the tests of this module use the vectors of
//! `nodo:tests/identity/test_execution_receipt.py` (key `000102..1f`).
//!
//! The node signs the UTF-8 of `celaut-execution-receipt:<peer_id>|<receipt_digest>`
//! with its identity key (Ed25519). `receipt_digest` is Blake2b-256 over a canonical
//! text that is made field by field ([`canonical_receipt`]), never over the protobuf
//! bytes. Every free text is the lowercase hex of its UTF-8, every number is decimal,
//! and every hash is lowercase hex. [`problems`] refuses a hash field that is not hex,
//! so no field can contain the `|` between the fields.

use std::collections::{BTreeMap, BTreeSet};

use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};
use celaut_proto::celaut::peer::signature_scheme::Protocol as SchemeComponent;
use celaut_proto::celaut::peer::SignatureScheme;
use celaut_proto::celaut::{BytesKeyValue, ExecutionReceipt};
use ring::signature::{UnparsedPublicKey, ED25519};

/// The prefix of the signed text (`node_identity._RECEIPT_PREFIX`).
pub const PREFIX: &str = "celaut-execution-receipt:";

pub const KIND_START: &str = "start";
pub const KIND_CHECKPOINT: &str = "checkpoint";
pub const KIND_CLOSE: &str = "close";
const KINDS: [&str; 3] = [KIND_START, KIND_CHECKPOINT, KIND_CLOSE];

pub const PLANE_TUNNEL: &str = "tunnel";
pub const PLANE_OBSERVE: &str = "observe";
const PLANES: [&str; 2] = [PLANE_TUNNEL, PLANE_OBSERVE];

/// All the billed bytes of one tunnel, and nothing else of the instance.
pub const COVERAGE_CLIENT_TUNNEL: &str = "client-tunnel";
/// The packets of the instance that the capture of the node got.
pub const COVERAGE_TAP: &str = "instance-tap";
/// Counts and lengths only.
pub const COVERAGE_COUNTERS: &str = "counters-only";

/// The chain of the messages into the instance (from the caller to the service).
pub const DIRECTION_IN: &str = "in";
/// The chain of the messages out of the instance (from the service to the caller).
pub const DIRECTION_OUT: &str = "out";

/// The head of a chain with no event.
pub const CHAIN_GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The name that the node takes out of the environment variables before it starts the
/// service. It names the instance; it is not a variable of the service.
pub const RESERVED_ENV: &str = "__nodo_instance_name";

/// The `formal` of the one component of the signature scheme of nodo
/// (`node_identity.SIGNATURE_SCHEME_COMPONENTS`).
pub const ED25519_FORMAL: &str = "algorithm=eddsa\ncontext=none\ncurve=edwards25519\nmessage=as-given\nprehash=none\nprivate_key_bytes=32\npublic_key_bytes=32\npublic_key_encoding=hex-lowercase\nsignature_bytes=64\nsignature_encoding=hex-lowercase\nsignature_layout=R||S\nspec=RFC8032\nvariant=pure\nverification=RFC8032-5.1.7";

/// The length of `envs_salt` (`execution_receipt.ENVS_SALT_BYTES`). A verifier refuses
/// another length: nothing separates the salt from the canonical text.
pub const ENVS_SALT_BYTES: usize = 16;

const DIGEST_HEX_LEN: usize = 64;
/// Ed25519: 64 bytes, in lowercase hex.
const SIGNATURE_HEX_LEN: usize = 128;

fn blake2b_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Blake2b::<U32>::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Blake2b-256 of `data`, in lowercase hex.
pub fn blake2b_256_hex(data: &[u8]) -> String {
    hex::encode(blake2b_256(data))
}

/// The lowercase hex of the UTF-8 of `text`; empty for an empty text.
fn hex_text(text: &str) -> String {
    hex::encode(text.as_bytes())
}

/// Lowercase hex, an even number of digits, and `len` digits if given.
fn is_hex(value: &str, len: Option<usize>) -> bool {
    len.is_none_or(|n| value.len() == n)
        && value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `normalize_public_key_hex`: the one spelling of a public key (64 lowercase hex
/// digits), or `None` if `text` is not a key.
pub fn normalize_key(text: &str) -> Option<String> {
    let key = text.trim().to_ascii_lowercase();
    is_hex(&key, Some(DIGEST_HEX_LEN)).then_some(key)
}

// ─── The parts of a receipt ───────────────────────────────────────────────

/// `blake2b_256(UTF-8(token) || the 32 bytes of the key)`, lowercase hex: how a receipt
/// names an execution. The key is the raw bytes, not its hex text, so the spelling of
/// the hex does not change the name. `None` if `peer_id` is not a key.
pub fn execution_name(token: &str, peer_id: &str) -> Option<String> {
    let key = hex::decode(normalize_key(peer_id)?).ok()?;
    let mut data = token.as_bytes().to_vec();
    data.extend_from_slice(&key);
    Some(blake2b_256_hex(&data))
}

/// Where an environment variable came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The caller sent it. Today every variable is this.
    Client,
    /// The node added it.
    Node,
}

impl Origin {
    fn text(self) -> &'static str {
        match self {
            Origin::Client => "client",
            Origin::Node => "node",
        }
    }
}

/// The environment variables that the service gets from these entries of
/// `Configuration.environment_variables`, as the node reads them (`keyvalue.to_dict`):
/// the last entry of a name wins, an entry with no value has the empty value, and the
/// reserved instance name is not a variable.
pub fn service_envs(entries: &[BytesKeyValue]) -> BTreeMap<String, Vec<u8>> {
    let mut envs: BTreeMap<String, Vec<u8>> = entries
        .iter()
        .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
        .collect();
    envs.remove(RESERVED_ENV);
    envs
}

/// The canonical text of an environment-variable map: one entry per variable,
/// `origin "~" hex(UTF-8(name)) "~" hex(value)`, sorted by the UTF-8 of the name,
/// joined by `/`. `value` is the raw bytes, never a decoded text. Every variable has
/// the origin [`Origin::Client`] unless `origins` gives another one.
pub fn canonical_envs(
    envs: &BTreeMap<String, Vec<u8>>,
    origins: &BTreeMap<String, Origin>,
) -> String {
    // A BTreeMap of String is in the order of the UTF-8 bytes.
    envs.iter()
        .map(|(name, value)| {
            let origin = origins.get(name).copied().unwrap_or(Origin::Client);
            format!(
                "{}~{}~{}",
                origin.text(),
                hex_text(name),
                hex::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// `blake2b_256(salt || UTF-8(canonical_envs))`, lowercase hex.
///
/// The salt is always [`ENVS_SALT_BYTES`] long, and a verifier must refuse another
/// length ([`envs_match`]): nothing separates the salt from the text, so a longer salt
/// could hold the first entries of the map and open the digest with fewer.
pub fn envs_digest(
    envs: &BTreeMap<String, Vec<u8>>,
    salt: &[u8],
    origins: &BTreeMap<String, Origin>,
) -> String {
    let mut data = salt.to_vec();
    data.extend_from_slice(canonical_envs(envs, origins).as_bytes());
    blake2b_256_hex(&data)
}

/// `envs_match`: whether `envs` is the map that the `start` receipt `r` names.
///
/// `salt` defaults to the `envs_salt` of the receipt. A salt of another length than
/// [`ENVS_SALT_BYTES`] never matches. This only compares the digest: check the
/// signature with [`verify`].
pub fn envs_match(
    r: &ExecutionReceipt,
    envs: &BTreeMap<String, Vec<u8>>,
    origins: &BTreeMap<String, Origin>,
    salt: Option<&[u8]>,
) -> bool {
    let salt = salt.unwrap_or(&r.envs_salt);
    r.kind == KIND_START
        && salt.len() == ENVS_SALT_BYTES
        && envs_digest(envs, salt, origins) == r.envs_digest
}

/// The Blake2b-256 of one payload, in hex.
pub fn payload_digest(payload: &[u8]) -> String {
    blake2b_256_hex(payload)
}

/// The head of a chain after one more event:
/// `blake2b_256(UTF-8(prev "|" seq "|" direction "|" length "|" payload_digest))`.
/// `seq` counts the events of that chain from 1.
pub fn chain_link(prev: &str, seq: u64, direction: &str, length: u64, digest: &str) -> String {
    blake2b_256_hex(format!("{prev}|{seq}|{direction}|{length}|{digest}").as_bytes())
}

/// One hash chain of a stream: the count of events, their bytes and the head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub events: u64,
    pub bytes: u64,
    pub head: String,
}

impl Default for Chain {
    fn default() -> Self {
        Chain {
            events: 0,
            bytes: 0,
            head: CHAIN_GENESIS.to_string(),
        }
    }
}

impl Chain {
    /// Add one message of `direction`.
    pub fn push(&mut self, direction: &str, payload: &[u8]) {
        let length = payload.len() as u64;
        self.events += 1;
        self.bytes += length;
        self.head = chain_link(
            &self.head,
            self.events,
            direction,
            length,
            &payload_digest(payload),
        );
    }
}

/// `replay_chain`: the chain of `direction` over these payloads, in their order.
pub fn replay_chain<'a>(payloads: impl IntoIterator<Item = &'a [u8]>, direction: &str) -> Chain {
    let mut chain = Chain::default();
    for payload in payloads {
        chain.push(direction, payload);
    }
    chain
}

/// The scheme as the digest of a Peer encodes it:
/// `";".join(sorted(hex(formal) "~" ",".join(sorted(tags)) "~" prose))`.
pub fn canonical_scheme(scheme: &SignatureScheme) -> String {
    let mut parts: Vec<String> = scheme
        .components
        .iter()
        .map(|c| {
            let mut tags = c.tags.clone();
            tags.sort();
            format!("{}~{}~{}", hex::encode(&c.formal), tags.join(","), c.prose)
        })
        .collect();
    parts.sort();
    parts.join(";")
}

/// The scheme that a receipt of nodo carries: the Ed25519 component, without the prose.
pub fn receipt_scheme() -> SignatureScheme {
    SignatureScheme {
        components: vec![SchemeComponent {
            tags: vec!["ed25519".to_string()],
            prose: String::new(),
            formal: ED25519_FORMAL.as_bytes().to_vec(),
        }],
    }
}

/// One component that names the Ed25519 component of nodo (`same_component`): the
/// `formal` decides if it has one, else the tag `ed25519`.
fn scheme_is_ours(scheme: Option<&SignatureScheme>) -> bool {
    let Some(scheme) = scheme else {
        return false;
    };
    let [component] = scheme.components.as_slice() else {
        return false;
    };
    if !component.formal.is_empty() {
        return component.formal == ED25519_FORMAL.as_bytes();
    }
    component.tags.iter().any(|t| t == "ed25519")
}

/// The text that [`receipt_digest`] hashes: every field but `envs_salt` and
/// `signature`, in field order, joined by `|`:
///
/// ```text
/// hex(kind) | peer_id | execution_name | ts
/// | hex(service_id.type) ~ hex(service_id.value) | envs_digest | hex(initial_mu.n)
/// | hex(plane) | session | hex(slot) | seq
/// | in_events | in_bytes | in_head | out_events | out_bytes | out_head
/// | hex(mu_charged.n) | hex(coverage) | hex(capture_mode) | hex(degraded_reason)
/// | blake2b_256_hex(UTF-8(canonical_scheme(signature_scheme)))
/// ```
pub fn canonical_receipt(r: &ExecutionReceipt) -> String {
    let (service_type, service_value) = r
        .service_id
        .as_ref()
        .map(|s| (hex::encode(&s.r#type), hex::encode(&s.value)))
        .unwrap_or_default();
    let amount = |a: &Option<celaut_proto::celaut::Amount>| {
        hex_text(a.as_ref().map(|a| a.n.as_str()).unwrap_or(""))
    };
    let scheme = r
        .signature_scheme
        .as_ref()
        .map(canonical_scheme)
        .unwrap_or_default();
    [
        hex_text(&r.kind),
        r.peer_id.clone(),
        r.execution_name.clone(),
        r.ts.to_string(),
        format!("{service_type}~{service_value}"),
        r.envs_digest.clone(),
        amount(&r.initial_mu),
        hex_text(&r.plane),
        r.session.clone(),
        hex_text(&r.slot),
        r.seq.to_string(),
        r.in_events.to_string(),
        r.in_bytes.to_string(),
        r.in_head.clone(),
        r.out_events.to_string(),
        r.out_bytes.to_string(),
        r.out_head.clone(),
        amount(&r.mu_charged),
        hex_text(&r.coverage),
        hex_text(&r.capture_mode),
        hex_text(&r.degraded_reason),
        blake2b_256_hex(scheme.as_bytes()),
    ]
    .join("|")
}

/// Blake2b-256 hex of the UTF-8 of [`canonical_receipt`].
pub fn receipt_digest(r: &ExecutionReceipt) -> String {
    blake2b_256_hex(canonical_receipt(r).as_bytes())
}

/// The exact bytes that the signature is over.
pub fn signed_bytes(r: &ExecutionReceipt) -> Vec<u8> {
    format!("{PREFIX}{}|{}", r.peer_id, receipt_digest(r)).into_bytes()
}

/// Fill `peer_id` and `signature_scheme` from the Ed25519 key with this `seed`, and
/// sign, as a node does. The app never signs a receipt: this is for tests and tools.
pub fn sign_with_seed(r: &mut ExecutionReceipt, seed: &[u8; 32]) -> Result<(), String> {
    let pair =
        ring::signature::Ed25519KeyPair::from_seed_unchecked(seed).map_err(|e| e.to_string())?;
    r.peer_id = hex::encode(ring::signature::KeyPair::public_key(&pair).as_ref());
    r.signature_scheme = Some(receipt_scheme());
    r.signature = hex::encode(pair.sign(&signed_bytes(r)).as_ref());
    Ok(())
}

// ─── Verification ─────────────────────────────────────────────────────────

/// The signed fields that only some kinds or planes use. A field that a receipt does
/// not use must be empty or zero.
const START_FIELDS: [&str; 3] = ["service_id", "envs_digest", "initial_mu"];
const STREAM_FIELDS: [&str; 14] = [
    "plane",
    "session",
    "slot",
    "seq",
    "in_events",
    "in_bytes",
    "in_head",
    "out_events",
    "out_bytes",
    "out_head",
    "mu_charged",
    "coverage",
    "capture_mode",
    "degraded_reason",
];
const TUNNEL_FIELDS: [&str; 2] = ["slot", "mu_charged"];
const OBSERVE_FIELDS: [&str; 2] = ["capture_mode", "degraded_reason"];

/// Whether `field` adds anything to the canonical text.
fn is_set(r: &ExecutionReceipt, field: &str) -> bool {
    let amount =
        |a: &Option<celaut_proto::celaut::Amount>| a.as_ref().is_some_and(|a| !a.n.is_empty());
    match field {
        "service_id" => r
            .service_id
            .as_ref()
            .is_some_and(|s| !s.r#type.is_empty() || !s.value.is_empty()),
        "envs_digest" => !r.envs_digest.is_empty(),
        "initial_mu" => amount(&r.initial_mu),
        "plane" => !r.plane.is_empty(),
        "session" => !r.session.is_empty(),
        "slot" => !r.slot.is_empty(),
        "seq" => r.seq != 0,
        "in_events" => r.in_events != 0,
        "in_bytes" => r.in_bytes != 0,
        "in_head" => !r.in_head.is_empty(),
        "out_events" => r.out_events != 0,
        "out_bytes" => r.out_bytes != 0,
        "out_head" => !r.out_head.is_empty(),
        "mu_charged" => amount(&r.mu_charged),
        "coverage" => !r.coverage.is_empty(),
        "capture_mode" => !r.capture_mode.is_empty(),
        "degraded_reason" => !r.degraded_reason.is_empty(),
        _ => false,
    }
}

/// The coverages that a plane can have.
fn plane_coverages(plane: &str) -> &'static [&'static str] {
    match plane {
        PLANE_TUNNEL => &[COVERAGE_CLIENT_TUNNEL],
        PLANE_OBSERVE => &[COVERAGE_TAP, COVERAGE_COUNTERS],
        _ => &[],
    }
}

/// Why `r` is not a valid receipt; empty when it is.
///
/// `peer_id`, if given, is the node that must sign it. `token`, if given, is the token
/// that the client holds: the name must be its name. The shape checks come before the
/// signature, because they make the canonical text have one reading.
pub fn problems(r: &ExecutionReceipt, peer_id: Option<&str>, token: Option<&str>) -> Vec<String> {
    let mut found = Vec::new();
    if !KINDS.contains(&r.kind.as_str()) {
        found.push(format!(
            "kind {:?} is not one of {}",
            r.kind,
            KINDS.join(", ")
        ));
    }
    if r.peer_id.is_empty() || normalize_key(&r.peer_id).as_deref() != Some(r.peer_id.as_str()) {
        found.push("peer_id is not a lowercase 64-hex public key".into());
    }
    if !is_hex(&r.execution_name, Some(DIGEST_HEX_LEN)) {
        found.push("execution_name is not a 64-hex digest".into());
    }
    for (field, value) in [
        ("envs_digest", &r.envs_digest),
        ("in_head", &r.in_head),
        ("out_head", &r.out_head),
    ] {
        if !value.is_empty() && !is_hex(value, Some(DIGEST_HEX_LEN)) {
            found.push(format!("{field} is not a 64-hex digest"));
        }
    }
    if !r.session.is_empty() && !is_hex(&r.session, None) {
        found.push("session is not lowercase hex".into());
    }
    // hex::decode reads uppercase too: one spelling only, as the proto says.
    if !is_hex(&r.signature, Some(SIGNATURE_HEX_LEN)) {
        found.push("signature is not 128 lowercase hex".into());
    }
    if !r.envs_salt.is_empty() && (r.kind != KIND_START || r.envs_salt.len() != ENVS_SALT_BYTES) {
        found.push(format!(
            "envs_salt is not {ENVS_SALT_BYTES} bytes of a start receipt"
        ));
    }

    let listed = |fields: &[&'static str]| -> Vec<&'static str> {
        fields.iter().filter(|f| is_set(r, f)).copied().collect()
    };
    if r.kind == KIND_START {
        let service = r.service_id.as_ref();
        if service.is_none_or(|s| s.r#type.is_empty() || s.value.is_empty())
            || r.envs_digest.is_empty()
            || !is_set(r, "initial_mu")
        {
            found.push("a start receipt needs service_id, envs_digest and initial_mu".into());
        }
        let used = listed(&STREAM_FIELDS);
        if !used.is_empty() {
            found.push(format!(
                "a start receipt has no stream fields (set: {})",
                used.join(", ")
            ));
        }
    } else if r.kind == KIND_CHECKPOINT || r.kind == KIND_CLOSE {
        if !PLANES.contains(&r.plane.as_str()) {
            found.push(format!(
                "plane {:?} is not one of {}",
                r.plane,
                PLANES.join(", ")
            ));
        } else if !plane_coverages(&r.plane).contains(&r.coverage.as_str()) {
            found.push(format!(
                "coverage {:?} is not one of {} for plane {}",
                r.coverage,
                plane_coverages(&r.plane).join(", "),
                r.plane
            ));
        }
        if r.session.is_empty() || r.seq < 1 || r.in_head.is_empty() || r.out_head.is_empty() {
            found.push("a checkpoint needs session, seq >= 1, in_head and out_head".into());
        }
        let used = listed(&START_FIELDS);
        if !used.is_empty() {
            found.push(format!(
                "a checkpoint has no start fields (set: {})",
                used.join(", ")
            ));
        }
        if r.plane == PLANE_TUNNEL {
            if r.slot.is_empty() || !is_set(r, "mu_charged") {
                found.push("a tunnel checkpoint needs slot and mu_charged".into());
            }
            let used = listed(&OBSERVE_FIELDS);
            if !used.is_empty() {
                found.push(format!(
                    "a tunnel checkpoint has no capture fields (set: {})",
                    used.join(", ")
                ));
            }
        } else if r.plane == PLANE_OBSERVE {
            if r.capture_mode.is_empty() {
                found.push("an observe checkpoint needs capture_mode".into());
            } else if r.coverage == COVERAGE_TAP && r.capture_mode != "pcap" {
                found.push(format!("coverage {COVERAGE_TAP} needs capture_mode pcap"));
            }
            let used = listed(&TUNNEL_FIELDS);
            if !used.is_empty() {
                found.push(format!(
                    "an observe checkpoint has no tunnel fields (set: {})",
                    used.join(", ")
                ));
            }
        }
    }

    if !scheme_is_ours(r.signature_scheme.as_ref()) {
        found.push("signature_scheme is not the ed25519 scheme this code verifies".into());
    }
    if let Some(expected) = peer_id {
        if normalize_key(expected).as_deref() != Some(r.peer_id.as_str()) {
            found.push("peer_id is not the expected node".into());
        }
    }
    if let Some(token) = token {
        if found.is_empty()
            && execution_name(token, &r.peer_id).as_deref() != Some(r.execution_name.as_str())
        {
            found.push("execution_name is not the name of this token on this node".into());
        }
    }

    if found.is_empty() && !signature_verifies(r) {
        found.push("the signature does not verify".into());
    }
    found
}

fn signature_verifies(r: &ExecutionReceipt) -> bool {
    let (Ok(key), Ok(signature)) = (hex::decode(&r.peer_id), hex::decode(&r.signature)) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&signed_bytes(r), &signature)
        .is_ok()
}

/// Whether `r` is well formed and signed by its `peer_id`.
pub fn verify(r: &ExecutionReceipt, peer_id: Option<&str>, token: Option<&str>) -> bool {
    problems(r, peer_id, token).is_empty()
}

// ─── A set of receipts ────────────────────────────────────────────────────

/// One stream in a [`SequenceReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSummary {
    pub plane: String,
    pub slot: String,
    pub coverage: String,
    pub last_seq: u64,
    pub closed: bool,
    pub in_events: u64,
    pub in_bytes: u64,
    pub out_events: u64,
    pub out_bytes: u64,
    pub mu_charged: String,
}

/// What [`check_sequence`] finds in a set of receipts of one execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SequenceReport {
    /// The problems of each receipt, in the order of the set.
    pub receipts: Vec<Vec<String>>,
    /// Two valid receipts for the same thing (the start of a name, or one `seq` of one
    /// stream) with different content: the node signed two stories.
    pub contradictions: Vec<String>,
    /// What a valid set cannot have.
    pub errors: Vec<String>,
    /// Missing `seq` ranges, `(session, from, to)`. Not an error.
    pub gaps: Vec<(String, u64, u64)>,
    /// Per stream: its last seq, and whether a close came.
    pub streams: BTreeMap<String, StreamSummary>,
    pub valid: bool,
}

fn mu(a: &Option<celaut_proto::celaut::Amount>) -> u128 {
    a.as_ref().and_then(|a| a.n.parse().ok()).unwrap_or(0)
}

/// `check_sequence`: check a set of receipts of one execution, as a client keeps them.
pub fn check_sequence(
    receipts: &[ExecutionReceipt],
    peer_id: Option<&str>,
    token: Option<&str>,
) -> SequenceReport {
    let mut report = SequenceReport::default();
    let mut valid: Vec<&ExecutionReceipt> = Vec::new();
    for r in receipts {
        let found = problems(r, peer_id, token);
        if found.is_empty() {
            valid.push(r);
        }
        report.receipts.push(found);
    }
    if valid
        .iter()
        .map(|r| &r.peer_id)
        .collect::<BTreeSet<_>>()
        .len()
        > 1
    {
        report
            .errors
            .push("the receipts are signed by more than one node".into());
    }
    if valid
        .iter()
        .map(|r| &r.execution_name)
        .collect::<BTreeSet<_>>()
        .len()
        > 1
    {
        report
            .errors
            .push("the receipts name more than one execution".into());
    }

    let mut seen: BTreeMap<(String, String, bool, String, u64), String> = BTreeMap::new();
    for r in &valid {
        let key = (
            r.peer_id.clone(),
            r.execution_name.clone(),
            r.kind == KIND_START,
            r.session.clone(),
            r.seq,
        );
        let digest = receipt_digest(r);
        match seen.get(&key) {
            Some(first) if *first != digest => report.contradictions.push(format!(
                "two receipts for {} {} seq {} with different content",
                r.kind, r.session, r.seq
            )),
            Some(_) => {}
            None => {
                seen.insert(key, digest);
            }
        }
    }

    let mut sessions: BTreeMap<String, Vec<&ExecutionReceipt>> = BTreeMap::new();
    for r in &valid {
        if r.kind != KIND_START {
            sessions.entry(r.session.clone()).or_default().push(r);
        }
    }
    for (session, mut items) in sessions {
        items.sort_by_key(|r| r.seq);
        let Some(last) = items.last().copied() else {
            continue;
        };
        let close = items.iter().find(|r| r.kind == KIND_CLOSE);
        if close.is_some_and(|c| c.seq != last.seq) {
            report
                .errors
                .push(format!("stream {session}: a checkpoint after its close"));
        }
        if items
            .iter()
            .map(|r| (&r.plane, &r.slot, &r.coverage))
            .collect::<BTreeSet<_>>()
            .len()
            > 1
        {
            report
                .errors
                .push(format!("stream {session}: plane, slot or coverage changes"));
        }
        let mut expected = 1u64;
        let mut previous: Option<&ExecutionReceipt> = None;
        for r in &items {
            if r.seq > expected {
                report.gaps.push((session.clone(), expected, r.seq - 1));
            }
            expected = r.seq + 1;
            if let Some(p) = previous {
                if p.seq != r.seq {
                    if r.in_events < p.in_events
                        || r.in_bytes < p.in_bytes
                        || r.out_events < p.out_events
                        || r.out_bytes < p.out_bytes
                    {
                        report.errors.push(format!(
                            "stream {session}: a counter goes down at seq {}",
                            r.seq
                        ));
                    }
                    if mu(&r.mu_charged) < mu(&p.mu_charged) {
                        report.errors.push(format!(
                            "stream {session}: the charge goes down at seq {}",
                            r.seq
                        ));
                    }
                    if (r.in_events == p.in_events && r.in_head != p.in_head)
                        || (r.out_events == p.out_events && r.out_head != p.out_head)
                    {
                        report.errors.push(format!(
                            "stream {session}: a head changes with no new event at seq {}",
                            r.seq
                        ));
                    }
                }
            }
            previous = Some(r);
        }
        report.streams.insert(
            session,
            StreamSummary {
                plane: last.plane.clone(),
                slot: last.slot.clone(),
                coverage: last.coverage.clone(),
                last_seq: last.seq,
                closed: close.is_some(),
                in_events: last.in_events,
                in_bytes: last.in_bytes,
                out_events: last.out_events,
                out_bytes: last.out_bytes,
                mu_charged: last
                    .mu_charged
                    .as_ref()
                    .map(|a| a.n.clone())
                    .unwrap_or_default(),
            },
        );
    }
    report.valid = !receipts.is_empty()
        && report.receipts.iter().all(Vec::is_empty)
        && report.errors.is_empty()
        && report.contradictions.is_empty();
    report
}

/// What is wrong with the start receipt of an instance that this service started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartFault {
    /// The receipt does not verify: the signature, the shape, the node or the token.
    Invalid { reason: String },
    /// The service of the receipt is not the service that was asked for.
    Service,
    /// The digest of the environment variables is not the one of the variables sent.
    Envs,
    /// The node signed a start amount that is not the one that was asked for.
    Amount { signed: String, asked: String },
}

impl std::fmt::Display for StartFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartFault::Invalid { reason } => write!(f, "the receipt is invalid: {reason}"),
            StartFault::Service => write!(f, "the receipt is for another service"),
            StartFault::Envs => write!(f, "the receipt has the digest of other variables"),
            StartFault::Amount { signed, asked } => {
                write!(
                    f,
                    "the receipt has the amount {signed}, and {asked} was asked"
                )
            }
        }
    }
}

/// Check the start receipt of an instance: signed by the node `peer_id` (when it is known;
/// `None` checks the signature of the key that the receipt carries, which proves only that
/// some key signed it), named by `token`, for the service `service_id`, with the digest of
/// the variables `envs` that were sent, and, if a start balance was asked, that amount.
/// An empty list means that the receipt is good.
pub fn check_start(
    r: &ExecutionReceipt,
    peer_id: Option<&str>,
    token: &str,
    service_id: &[u8; 32],
    envs: &[BytesKeyValue],
    initial_mu: Option<&str>,
) -> Vec<StartFault> {
    let mut faults: Vec<StartFault> = problems(r, peer_id, Some(token))
        .into_iter()
        .map(|reason| StartFault::Invalid { reason })
        .collect();
    if faults.is_empty() && r.kind != KIND_START {
        faults.push(StartFault::Invalid {
            reason: format!("the answer of the start has a {} receipt", r.kind),
        });
    }
    if !faults.is_empty() {
        return faults;
    }
    if r.service_id
        .as_ref()
        .is_none_or(|s| s.r#type != crate::gateway::SHA3_256_TYPE || s.value != service_id)
    {
        faults.push(StartFault::Service);
    }
    if !envs_match(
        r,
        &service_envs(envs),
        &BTreeMap::<String, Origin>::new(),
        None,
    ) {
        faults.push(StartFault::Envs);
    }
    let signed = r
        .initial_mu
        .as_ref()
        .map(|a| a.n.clone())
        .unwrap_or_default();
    if let Some(asked) = initial_mu.filter(|a| !a.is_empty()) {
        if signed != asked {
            faults.push(StartFault::Amount {
                signed,
                asked: asked.to_string(),
            });
        }
    }
    faults
}

#[cfg(test)]
mod tests {
    //! The vectors of `nodo:tests/identity/test_execution_receipt.py` (nodo#593). A fixed
    //! key and fixed fields give these exact texts, digests and signatures. Ed25519 is
    //! deterministic (RFC 8032), so the signature is fixed too.

    use super::*;
    use crate::gateway::SHA3_256_TYPE;
    use celaut_proto::celaut::metadata::hash_tag::Hash;
    use celaut_proto::celaut::Amount;

    const KEY: [u8; 32] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ];
    const TOKEN: &str = "instance-token-for-vectors";
    /// SHA3-256 of "service".
    const SERVICE_VALUE: &str = "63e5b5a4137cfa77cb9d10adae081d0df082a826d8441721460d5933f5800056";
    const SALT: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    const INITIAL_MU: &str = "12345678";
    const TS: i64 = 1798329600;
    const SESSION: &str = "00112233445566778899aabbccddeeff";

    const PEER_ID: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
    const EXECUTION_NAME: &str = "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a";
    const CANONICAL_ENVS: &str = "client~41~31/client~42~ff00";
    const ENVS_DIGEST: &str = "e684ff3022c2a14c62addd0129cd4a5d3dc95b2d13a924ce4fc57aa7e9842a45";
    const START_CANONICAL: &str = concat!(
        "7374617274",
        "|03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8",
        "|b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
        "|1798329600",
        "|a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
        "~63e5b5a4137cfa77cb9d10adae081d0df082a826d8441721460d5933f5800056",
        "|e684ff3022c2a14c62addd0129cd4a5d3dc95b2d13a924ce4fc57aa7e9842a45",
        "|3132333435363738",
        "||||0|0|0||0|0||||||",
        "94c3da627446ed1f3c3736d21ded39451ec00c4ccc687acc42b9d0c61dfcf573",
    );
    const START_DIGEST: &str = "59ecb3f5e113da7bb9f76d41059a183bfd7d355e9ba32a53d848826be5502531";
    const START_SIGNATURE: &str = concat!(
        "f79ac345e0d9290dd803edf6b90cf16ef2680ba4353cf44f9a825b38b47be96c",
        "de20b2d09453e24c965a25bbe1b846b7256fbc53fe79f921e88e70d967aa3d0b",
    );
    const IN_HEAD: &str = "4cc36e6c5ce771828a06755610180b0db14686b7b88ec9aca2d94dc67cb82b58";
    const OUT_HEAD: &str = "721ba2dbda02e1f8d3f9bf151175163f77ece18af91d85a2357a438d36f2ed85";
    const CHECKPOINT_DIGEST: &str =
        "94157eae3e1f9aef97bd3347e33e9ed85db5b8c515253881a55e5763f8f2b840";
    const CHECKPOINT_SIGNATURE: &str = concat!(
        "98bbc219739d19bc7663f99ae68e46d198795cad15d2e9fc5ddd9a8c6b47aef7",
        "a4320777ed11f2d9ffd6fb43c40520e32f14be82193ab64b6cdc6ab74ee6900b",
    );
    const CHECKPOINT_CANONICAL: &str = concat!(
        "636865636b706f696e74",
        "|03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8",
        "|b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
        "|1798329600",
        "|~||",
        "|74756e6e656c",
        "|00112233445566778899aabbccddeeff",
        "|38303830",
        "|1",
        "|1|4|4cc36e6c5ce771828a06755610180b0db14686b7b88ec9aca2d94dc67cb82b58",
        "|2|5|721ba2dbda02e1f8d3f9bf151175163f77ece18af91d85a2357a438d36f2ed85",
        "|37",
        "|636c69656e742d74756e6e656c",
        "||",
        "|94c3da627446ed1f3c3736d21ded39451ec00c4ccc687acc42b9d0c61dfcf573",
    );

    fn envs() -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([
            ("B".to_string(), vec![0xff, 0x00]),
            ("A".to_string(), b"1".to_vec()),
        ])
    }

    fn no_origins() -> BTreeMap<String, Origin> {
        BTreeMap::new()
    }

    fn signed(mut r: ExecutionReceipt) -> ExecutionReceipt {
        sign_with_seed(&mut r, &KEY).unwrap();
        r
    }

    /// `start_receipt` of the vectors.
    fn start() -> ExecutionReceipt {
        signed(ExecutionReceipt {
            kind: KIND_START.into(),
            execution_name: execution_name(TOKEN, PEER_ID).unwrap(),
            ts: TS,
            service_id: Some(Hash {
                r#type: SHA3_256_TYPE.to_vec(),
                value: hex::decode(SERVICE_VALUE).unwrap(),
            }),
            envs_digest: envs_digest(&envs(), &SALT, &no_origins()),
            envs_salt: SALT.to_vec(),
            initial_mu: Some(Amount {
                n: INITIAL_MU.into(),
            }),
            ..Default::default()
        })
    }

    /// A tunnel checkpoint at seq 1 after "ping" in, "pong" and "!" out, charged 7.
    fn checkpoint_with(
        kind: &str,
        seq: u64,
        ins: &[&[u8]],
        outs: &[&[u8]],
        mu: &str,
    ) -> ExecutionReceipt {
        let i = replay_chain(ins.iter().copied(), DIRECTION_IN);
        let o = replay_chain(outs.iter().copied(), DIRECTION_OUT);
        signed(ExecutionReceipt {
            kind: kind.into(),
            execution_name: execution_name(TOKEN, PEER_ID).unwrap(),
            ts: TS,
            plane: PLANE_TUNNEL.into(),
            session: SESSION.into(),
            slot: "8080".into(),
            seq,
            in_events: i.events,
            in_bytes: i.bytes,
            in_head: i.head,
            out_events: o.events,
            out_bytes: o.bytes,
            out_head: o.head,
            mu_charged: Some(Amount { n: mu.into() }),
            coverage: COVERAGE_CLIENT_TUNNEL.into(),
            ..Default::default()
        })
    }

    fn checkpoint() -> ExecutionReceipt {
        checkpoint_with(KIND_CHECKPOINT, 1, &[b"ping"], &[b"pong", b"!"], "7")
    }

    #[test]
    fn the_key_of_the_vectors() {
        assert_eq!(start().peer_id, PEER_ID);
    }

    #[test]
    fn the_execution_name_is_blake2b_of_the_token_and_the_raw_key() {
        assert_eq!(execution_name(TOKEN, PEER_ID).unwrap(), EXECUTION_NAME);
        // The spelling of the key does not change the name.
        assert_eq!(
            execution_name(TOKEN, &PEER_ID.to_uppercase()).unwrap(),
            EXECUTION_NAME
        );
        assert_eq!(execution_name(TOKEN, "not-a-key"), None);
    }

    #[test]
    fn the_envs() {
        assert_eq!(canonical_envs(&envs(), &no_origins()), CANONICAL_ENVS);
        assert_eq!(envs_digest(&envs(), &SALT, &no_origins()), ENVS_DIGEST);
        let mut data = SALT.to_vec();
        data.extend_from_slice(CANONICAL_ENVS.as_bytes());
        assert_eq!(blake2b_256_hex(&data), ENVS_DIGEST);
    }

    #[test]
    fn the_start_receipt() {
        let r = start();
        assert_eq!(canonical_receipt(&r), START_CANONICAL);
        assert_eq!(receipt_digest(&r), START_DIGEST);
        assert_eq!(
            signed_bytes(&r),
            format!("celaut-execution-receipt:{PEER_ID}|{START_DIGEST}").into_bytes()
        );
        assert_eq!(r.signature, START_SIGNATURE);
        assert_eq!(
            problems(&r, Some(PEER_ID), Some(TOKEN)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn the_checkpoint_receipt() {
        let r = checkpoint();
        assert_eq!(canonical_receipt(&r), CHECKPOINT_CANONICAL);
        assert_eq!(r.in_head, IN_HEAD);
        assert_eq!(r.out_head, OUT_HEAD);
        assert_eq!(receipt_digest(&r), CHECKPOINT_DIGEST);
        assert_eq!(r.signature, CHECKPOINT_SIGNATURE);
        assert!(verify(&r, None, Some(TOKEN)));
    }

    #[test]
    fn the_chain_heads_by_hand() {
        let ping = blake2b_256_hex(b"ping");
        let text = format!("{CHAIN_GENESIS}|1|in|4|{ping}");
        assert_eq!(blake2b_256_hex(text.as_bytes()), IN_HEAD);
    }

    #[test]
    fn the_digest_reads_raw_bytes_and_the_salt_and_origin_count() {
        let one = |v: &[u8]| BTreeMap::from([("K".to_string(), v.to_vec())]);
        assert_ne!(
            envs_digest(&one(b"\xff"), &SALT, &no_origins()),
            envs_digest(&one(b"\xfe"), &SALT, &no_origins())
        );
        let base = envs_digest(&envs(), &SALT, &no_origins());
        assert_ne!(base, envs_digest(&envs(), &[0u8; 16], &no_origins()));
        let node = BTreeMap::from([("A".to_string(), Origin::Node)]);
        assert_ne!(base, envs_digest(&envs(), &SALT, &node));
        assert_eq!(canonical_envs(&BTreeMap::new(), &no_origins()), "");
        assert_eq!(
            envs_digest(&BTreeMap::new(), &SALT, &no_origins()),
            blake2b_256_hex(&SALT)
        );
    }

    #[test]
    fn the_service_envs_are_those_that_the_node_reads() {
        let kv = |k: &str, v: Option<&[u8]>| BytesKeyValue {
            key: k.into(),
            value: v.map(<[u8]>::to_vec),
        };
        let envs = service_envs(&[
            kv("B", Some(b"old")),
            kv(RESERVED_ENV, Some(b"name")),
            kv("A", Some(b"1")),
            kv("B", Some(&[0xff, 0x00])),
            kv("E", None),
        ]);
        let mut expected = super::tests::envs();
        expected.insert("E".into(), Vec::new());
        assert_eq!(envs, expected);
    }

    /// Every field but the salt and the signature is signed: a change breaks the
    /// signature (`TamperTests`).
    #[test]
    fn each_signed_field_is_covered() {
        type Change = fn(&mut ExecutionReceipt);
        let start_changes: [(&str, Change); 10] = [
            ("kind", |r| r.kind.push('x')),
            ("peer_id", |r| r.peer_id.replace_range(0..1, "1")),
            ("execution_name", |r| {
                r.execution_name.replace_range(0..1, "1")
            }),
            ("ts", |r| r.ts += 1),
            ("service_id", |r| {
                r.service_id.as_mut().unwrap().value.push(b'x')
            }),
            ("envs_digest", |r| r.envs_digest.replace_range(0..1, "1")),
            ("initial_mu", |r| {
                r.initial_mu = Some(Amount {
                    n: "12345679".into(),
                })
            }),
            ("signature_scheme", |r| {
                r.signature_scheme.as_mut().unwrap().components[0]
                    .tags
                    .push("other".into())
            }),
            ("plane", |r| r.plane = PLANE_TUNNEL.into()),
            ("seq", |r| r.seq += 1),
        ];
        for (field, change) in start_changes {
            let mut r = start();
            let digest = receipt_digest(&r);
            change(&mut r);
            assert_ne!(receipt_digest(&r), digest, "{field}");
            assert!(!verify(&r, None, None), "{field}");
        }
        let checkpoint_changes: [(&str, Change); 14] = [
            ("plane", |r| r.plane.push('x')),
            ("session", |r| r.session.replace_range(0..1, "1")),
            ("slot", |r| r.slot.push('x')),
            ("seq", |r| r.seq += 1),
            ("in_events", |r| r.in_events += 1),
            ("in_bytes", |r| r.in_bytes += 1),
            ("in_head", |r| r.in_head.replace_range(0..1, "1")),
            ("out_events", |r| r.out_events += 1),
            ("out_bytes", |r| r.out_bytes += 1),
            ("out_head", |r| r.out_head.replace_range(0..1, "1")),
            ("mu_charged", |r| {
                r.mu_charged = Some(Amount { n: "8".into() })
            }),
            ("coverage", |r| r.coverage.push('x')),
            ("capture_mode", |r| r.capture_mode.push('x')),
            ("degraded_reason", |r| r.degraded_reason.push('x')),
        ];
        for (field, change) in checkpoint_changes {
            let mut r = checkpoint();
            let digest = receipt_digest(&r);
            change(&mut r);
            assert_ne!(receipt_digest(&r), digest, "{field}");
            assert!(!verify(&r, None, None), "{field}");
        }
        // Not signed: the salt and the signature itself. Another salt of the same
        // length (a salt of another length is refused).
        let mut r = start();
        let digest = receipt_digest(&r);
        r.envs_salt[0] ^= 1;
        assert_eq!(receipt_digest(&r), digest);
        assert!(verify(&r, None, None));
        assert!(!envs_match(&r, &envs(), &no_origins(), None));
        // Only a start receipt carries a salt.
        let mut r = checkpoint();
        let digest = receipt_digest(&r);
        r.envs_salt = vec![1; ENVS_SALT_BYTES];
        assert_eq!(receipt_digest(&r), digest);
        assert!(!verify(&r, None, None));
    }

    /// Final review of nodo#593: nothing separates the salt from the text, so a salt
    /// that holds the first entry opens the same digest with the rest of the map only.
    #[test]
    fn a_longer_salt_cannot_open_the_envs_with_fewer_variables() {
        let r = start();
        assert!(envs_match(&r, &envs(), &no_origins(), None));
        let mut rest = envs();
        let (first, value) = rest.pop_first().unwrap();
        let mut long_salt = r.envs_salt.clone();
        let one = BTreeMap::from([(first, value)]);
        long_salt.extend_from_slice(format!("{}/", canonical_envs(&one, &no_origins())).as_bytes());
        // The digest alone has two readings...
        assert_eq!(envs_digest(&rest, &long_salt, &no_origins()), r.envs_digest);
        // ...so the check refuses any salt that is not 16 bytes.
        assert!(!envs_match(&r, &rest, &no_origins(), Some(&long_salt)));
        let mut r = r;
        r.envs_salt = long_salt;
        assert!(!envs_match(&r, &rest, &no_origins(), None));
        assert!(problems(&r, None, None).contains(&format!(
            "envs_salt is not {ENVS_SALT_BYTES} bytes of a start receipt"
        )));
    }

    #[test]
    fn envs_match_needs_the_right_map_and_a_start() {
        let r = start();
        let mut more = envs();
        more.insert("extra".into(), b"1".to_vec());
        assert!(!envs_match(&r, &more, &no_origins(), None));
        assert!(!envs_match(&checkpoint(), &envs(), &no_origins(), None));
        // Published without the salt.
        let mut stripped = r.clone();
        stripped.envs_salt.clear();
        assert!(verify(&stripped, None, None));
        assert!(envs_match(
            &stripped,
            &envs(),
            &no_origins(),
            Some(&r.envs_salt)
        ));
    }

    /// Final review of nodo#593: the hex decoder reads uppercase (and Python's reads
    /// spaces), so the same signature verified in other spellings.
    #[test]
    fn the_signature_has_one_spelling() {
        let good = start();
        assert!(verify(&good, None, None));
        let spaced = (0..128)
            .step_by(2)
            .map(|i| &good.signature[i..i + 2])
            .collect::<Vec<_>>()
            .join(" ");
        for spelling in [
            good.signature.to_uppercase(),
            spaced,
            format!("{}00", good.signature),
            String::new(),
        ] {
            let mut r = start();
            r.signature = spelling.clone();
            assert!(
                problems(&r, None, None)
                    .contains(&"signature is not 128 lowercase hex".to_string()),
                "{spelling:.20}"
            );
        }
    }

    #[test]
    fn a_separator_cannot_move_and_another_spelling_of_the_key_is_refused() {
        let mut r = start();
        r.envs_digest.replace_range(10..11, "|");
        assert!(
            problems(&r, None, None).contains(&"envs_digest is not a 64-hex digest".to_string())
        );
        let mut r = start();
        r.peer_id = r.peer_id.to_uppercase();
        assert!(!verify(&r, None, None));
    }

    #[test]
    fn the_wrong_token_node_or_key() {
        let r = start();
        assert!(!verify(&r, None, Some("another-token")));
        let mut other = ExecutionReceipt::default();
        sign_with_seed(&mut other, &[0u8; 32]).unwrap();
        assert!(!verify(&r, Some(&other.peer_id), None));
        // Signed by another key under this peer_id.
        let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&[0u8; 32]).unwrap();
        let mut forged = start();
        forged.signature = hex::encode(pair.sign(&signed_bytes(&forged)).as_ref());
        assert!(
            problems(&forged, None, None).contains(&"the signature does not verify".to_string())
        );
        // A scheme that this code does not verify.
        let mut r = start();
        r.signature_scheme.as_mut().unwrap().components[0].formal = b"algorithm=other".to_vec();
        let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&KEY).unwrap();
        r.signature = hex::encode(pair.sign(&signed_bytes(&r)).as_ref());
        assert!(!verify(&r, None, None));
    }

    #[test]
    fn the_shapes() {
        let refused = |r: ExecutionReceipt, expected: &str| {
            let found = problems(&signed(r), None, None);
            assert!(
                found.iter().any(|p| p.contains(expected)),
                "{expected}: {found:?}"
            );
        };
        let mut r = start();
        r.slot = "8080".into();
        refused(r, "a start receipt has no stream fields (set: slot)");
        let mut r = start();
        r.service_id.as_mut().unwrap().r#type.clear();
        refused(r, "a start receipt needs service_id");
        let mut r = checkpoint();
        r.initial_mu = Some(Amount { n: "1".into() });
        refused(r, "a checkpoint has no start fields (set: initial_mu)");
        let mut r = checkpoint();
        r.capture_mode = "pcap".into();
        refused(
            r,
            "a tunnel checkpoint has no capture fields (set: capture_mode)",
        );
        let mut r = checkpoint();
        r.mu_charged = None;
        refused(r, "a tunnel checkpoint needs slot and mu_charged");
        let mut r = checkpoint();
        r.coverage = COVERAGE_TAP.into();
        refused(r, "is not one of client-tunnel for plane tunnel");
    }

    #[test]
    fn a_client_rebuilds_each_chain_from_its_own_messages() {
        let sent: [&[u8]; 3] = [b"a", b"bb", b"ccc"];
        let big = vec![b'x'; 70000];
        let got: [&[u8]; 2] = [&big, b"y"];
        let i = replay_chain(sent, DIRECTION_IN);
        assert_eq!((i.events, i.bytes), (3, 6));
        let o = replay_chain(got, DIRECTION_OUT);
        assert_eq!((o.events, o.bytes), (2, 70001));
        // Dropping, moving or adding an event, or another direction, changes the head.
        let head = replay_chain([b"1" as &[u8], b"2", b"3"], DIRECTION_IN).head;
        for changed in [
            vec![b"1" as &[u8], b"3"],
            vec![b"2", b"1", b"3"],
            vec![b"1", b"2", b"3", b"4"],
        ] {
            assert_ne!(replay_chain(changed, DIRECTION_IN).head, head);
        }
        assert_ne!(
            replay_chain([b"1" as &[u8], b"2", b"3"], DIRECTION_OUT).head,
            head
        );
        assert_eq!(Chain::default().head, CHAIN_GENESIS);
    }

    #[test]
    fn a_set_of_receipts() {
        let ins: [&[u8]; 3] = [b"m", b"m", b"m"];
        let stream: Vec<ExecutionReceipt> = (1..=3)
            .map(|n| {
                checkpoint_with(
                    KIND_CHECKPOINT,
                    n,
                    &ins[..n as usize],
                    &[],
                    &(n - 1).to_string(),
                )
            })
            .chain([checkpoint_with(KIND_CLOSE, 4, &ins, &[], "3")])
            .collect();
        let mut all = vec![start()];
        all.extend(stream.clone());
        let report = check_sequence(&all, Some(PEER_ID), Some(TOKEN));
        assert!(report.valid, "{report:?}");
        let s = &report.streams[SESSION];
        assert!(s.closed);
        assert_eq!(s.last_seq, 4);

        // Two signed receipts for one seq: the node lied.
        let mut forged = stream[1].clone();
        forged.in_bytes += 100;
        forged.in_events += 1;
        let forged = signed(forged);
        let mut set = stream.clone();
        set.push(forged);
        let report = check_sequence(&set, None, None);
        assert_eq!(report.contradictions.len(), 1);
        assert!(!report.valid);

        // A gap is not an error.
        let report = check_sequence(
            &[stream[0].clone(), stream[2].clone(), stream[3].clone()],
            None,
            None,
        );
        assert_eq!(report.gaps, vec![(SESSION.to_string(), 2, 2)]);
        assert!(report.valid);

        // A counter that goes down.
        let mut down = stream[..3].to_vec();
        down[2].in_bytes = 0;
        down[2] = signed(down[2].clone());
        assert!(check_sequence(&down, None, None)
            .errors
            .iter()
            .any(|e| e.contains("goes down")));

        // A changed stored receipt.
        let mut changed = stream.clone();
        changed[1].out_bytes += 1;
        let report = check_sequence(&changed, None, None);
        assert!(!report.valid);
        assert!(!report.receipts[1].is_empty());
        assert!(!check_sequence(&[], None, None).valid);
    }

    fn sent_envs() -> Vec<BytesKeyValue> {
        envs()
            .into_iter()
            .map(|(key, value)| BytesKeyValue {
                key,
                value: Some(value),
            })
            .collect()
    }

    fn service_id() -> [u8; 32] {
        hex::decode(SERVICE_VALUE).unwrap().try_into().unwrap()
    }

    #[test]
    fn a_good_start_receipt_has_no_faults() {
        let faults = check_start(
            &start(),
            Some(PEER_ID),
            TOKEN,
            &service_id(),
            &sent_envs(),
            Some(INITIAL_MU),
        );
        assert_eq!(faults, vec![]);
    }

    #[test]
    fn a_start_receipt_for_another_service_or_variables_or_amount_is_a_fault() {
        let faults = check_start(
            &start(),
            Some(PEER_ID),
            TOKEN,
            &[9; 32],
            &[BytesKeyValue {
                key: "A".into(),
                value: Some(b"2".to_vec()),
            }],
            Some("1"),
        );
        assert!(faults.contains(&StartFault::Service));
        assert!(faults.contains(&StartFault::Envs));
        assert!(faults
            .iter()
            .any(|f| matches!(f, StartFault::Amount { .. })));
    }

    #[test]
    fn a_receipt_of_another_token_or_node_is_invalid() {
        let other_token = check_start(
            &start(),
            Some(PEER_ID),
            "other",
            &service_id(),
            &sent_envs(),
            None,
        );
        assert!(matches!(other_token[0], StartFault::Invalid { .. }));
        let other_node = check_start(
            &start(),
            Some(&"ab".repeat(32)),
            TOKEN,
            &service_id(),
            &sent_envs(),
            None,
        );
        assert!(matches!(other_node[0], StartFault::Invalid { .. }));
    }

    #[test]
    fn a_checkpoint_is_not_a_start_receipt() {
        let cp = checkpoint_with(KIND_CHECKPOINT, 1, &[b"ping"], &[b"pong"], "7");
        let faults = check_start(&cp, Some(PEER_ID), TOKEN, &service_id(), &sent_envs(), None);
        assert!(!faults.is_empty());
    }
}
