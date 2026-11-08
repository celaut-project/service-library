//! The balance statements of `GetBalanceStatement`, with `instance_ref` (nodo#631).
//!
//! The rules are in chatui, `docs/celaut/descendants.md` (ported from `celaut-wire`). This module builds the request, and
//! checks the answer: the `instance_ref`, the signature of the node of the connection, the
//! hash chain and the head. Each text and each hash follows `nodo:src/manager/
//! balance_statement.py` and `nodo:src/database/ledger.py`.
//!
//! # Cost
//!
//! A read costs MU and writes a `statement_query` movement on the node. This module never
//! reads by itself. The caller passes a `max_mu` budget to
//! [`crate::gateway::Gateway::balance_statement`], and the call sends nothing when
//! [`StatementRequest::max_mu_needed`] is more than `max_mu`. A service that wants this
//! checked on a schedule uses [`crate::verifier::Verifier`], which keeps a budget.
//!
//! # What this module refuses
//!
//! - An answer whose `instance_ref` is not the one of the request. A node that does not
//!   know the field gives an empty one: that is [`StatementError::NoDescendants`].
//! - An answer whose `from_seq` or `to_seq` is not the one of the request.
//! - A signature that is empty, not in canonical form, or not from the node of the
//!   connection.
//! - A chain that is broken, a head that does not match, or a head that does not agree
//!   with a head that the caller kept.

use celaut_proto::celaut::{
    Amount, BalanceMovement, BalanceStatement, BalanceStatementRequest, Peer,
};
use ring::signature::{UnparsedPublicKey, ED25519};

use crate::error::Error as WireError;
use crate::receipt::{blake2b_256_hex, normalize_key};

/// The text that the node signs, before the peer id and the digest.
pub const SIGNATURE_PREFIX: &str = "celaut-balance-statement:";
/// The prefix of the hash of a movement.
pub const MOVEMENT_PREFIX: &str = "celaut-balance-movement:";
/// The prefix of the genesis hash of an account.
pub const GENESIS_PREFIX: &str = "celaut-balance-genesis:";
/// The prefix of the `account_ref` of a client id or an instance token.
pub const ACCOUNT_PREFIX: &str = "celaut-ledger-account:";
/// The prefix of the `instance_ref` of an instance token.
pub const INSTANCE_PREFIX: &str = "celaut-ledger-instance:";

/// The most movements that a node gives in one answer (`ledger.MAX_STATEMENT_ROWS`, the
/// default of nodo). An operator can set a lower limit, so this is the most that a read
/// can cost.
pub const MAX_STATEMENT_ROWS: u64 = 1000;

/// How far the clock of a node can be from the clock of this service, in seconds
/// (`statement_check.CLOCK_SKEW_SECONDS`). A statement dated further away is refused, so a
/// node cannot give an old signed statement as a new one.
pub const CLOCK_SKEW_SECONDS: u64 = 120;

/// The kind of the movement that moves nothing and links a child.
pub const KIND_CHILD_BOUND: &str = "child_bound";
/// The kind of the last movement of an account.
pub const KIND_STOP: &str = "stop";

/// A hex text of 64 lowercase digits, as `blake2b-256` gives.
fn is_digest(text: &str) -> bool {
    text.len() == 64 && is_lower_hex(text)
}

fn is_lower_hex(text: &str) -> bool {
    !text.is_empty()
        && text.len().is_multiple_of(2)
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The lowercase hex of the UTF-8 of `text`.
fn hex_text(text: &str) -> String {
    hex::encode(text.as_bytes())
}

// ─── The hashes ───────────────────────────────────────────────────────────

/// The `instance_ref` of an instance token (`ledger.instance_ref`).
pub fn instance_ref(token: &str) -> String {
    blake2b_256_hex(format!("{INSTANCE_PREFIX}{token}").as_bytes())
}

/// The `account_ref` of a client id or an instance token (`ledger.account_ref`).
pub fn account_ref(account: &str) -> String {
    blake2b_256_hex(format!("{ACCOUNT_PREFIX}{account}").as_bytes())
}

/// The hash that the first movement of an account hangs from (`ledger.genesis_hash`).
pub fn genesis_hash(account_ref: &str) -> String {
    blake2b_256_hex(format!("{GENESIS_PREFIX}{account_ref}").as_bytes())
}

fn amount_text(amount: &Option<Amount>) -> &str {
    amount.as_ref().map_or("", |a| a.n.as_str())
}

/// The hash of a movement, from its fields (`ledger.movement_hash`).
pub fn movement_hash(m: &BalanceMovement) -> String {
    let text = format!(
        "{MOVEMENT_PREFIX}{}|{}~{}~{}~{}~{}~{}~{}",
        m.prev_hash,
        m.seq,
        m.ts,
        hex_text(amount_text(&m.amount)),
        hex_text(amount_text(&m.balance_after)),
        hex_text(&m.kind),
        m.instance_ref,
        hex_text(&m.detail),
    );
    blake2b_256_hex(text.as_bytes())
}

/// The digest of a statement: every field but the signature (`statement_digest`).
/// `service_id`, `instance_ref` and `relayed` are in the text only when they are set, so
/// the digest of a statement without them does not change.
pub fn statement_digest(s: &BalanceStatement) -> String {
    let hashes: Vec<String> = s.movements.iter().map(movement_hash).collect();
    let mut text = [
        s.account_ref.clone(),
        s.from_seq.to_string(),
        s.to_seq.to_string(),
        s.ts.to_string(),
        s.retention_days.to_string(),
        s.oldest_seq.to_string(),
        s.head_seq.to_string(),
        s.head_hash.clone(),
        hex_text(amount_text(&s.balance)),
        hashes.join("/"),
    ]
    .join("|");
    if !s.service_id.is_empty() {
        text.push_str("|service_id:");
        text.push_str(&hex_text(&s.service_id));
    }
    if !s.instance_ref.is_empty() {
        text.push_str("|instance_ref:");
        text.push_str(&hex_text(&s.instance_ref));
    }
    if s.relayed {
        text.push_str("|relayed");
    }
    blake2b_256_hex(text.as_bytes())
}

/// The text that a node signs for a statement (`statement_payload`).
pub fn signed_text(peer_id: &str, digest: &str) -> String {
    format!("{SIGNATURE_PREFIX}{peer_id}|{digest}")
}

/// Whether `peer_id` signed exactly this statement (`verify`). `peer_id` must be 64
/// lowercase hex digits, and the signature 128. An empty signature is never valid.
pub fn signature_verifies(s: &BalanceStatement, peer_id: &str) -> bool {
    if normalize_key(peer_id).as_deref() != Some(peer_id)
        || s.signature.len() != 128
        || !is_lower_hex(&s.signature)
    {
        return false;
    }
    let (Ok(key), Ok(signature)) = (hex::decode(peer_id), hex::decode(&s.signature)) else {
        return false;
    };
    let text = signed_text(peer_id, &statement_digest(s));
    UnparsedPublicKey::new(&ED25519, key)
        .verify(text.as_bytes(), &signature)
        .is_ok()
}

/// Sign `s` with the Ed25519 key of this `seed`, as a node does, and give the peer id of
/// the key. A service never signs a statement: this is for tests and tools.
pub fn sign_with_seed(s: &mut BalanceStatement, seed: &[u8; 32]) -> Result<String, String> {
    let pair =
        ring::signature::Ed25519KeyPair::from_seed_unchecked(seed).map_err(|e| e.to_string())?;
    let peer_id = hex::encode(ring::signature::KeyPair::public_key(&pair).as_ref());
    let text = signed_text(&peer_id, &statement_digest(s));
    s.signature = hex::encode(pair.sign(text.as_bytes()).as_ref());
    Ok(peer_id)
}

// ─── The request and its cost ─────────────────────────────────────────────

/// The rates of a node for a statement, in MU (`statement_query_mu` and
/// `statement_row_mu` of the `Peer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rates {
    /// The fixed part of the price of a read.
    pub query_mu: u128,
    /// The part for each movement that the answer holds.
    pub row_mu: u128,
}

/// One rate that the `Peer` advertises in `mu_per_call`, in MU. `None` if it advertises
/// it not at all, or not as a decimal integer: then the cost is not known.
pub fn peer_rate(peer: &Peer, key: &str) -> Option<u128> {
    let entry = peer.mu_per_call.iter().find(|e| e.key == key)?;
    let n = &entry.value.as_ref()?.n;
    if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    n.parse().ok()
}

impl Rates {
    /// The rates that the `Peer` advertises in `mu_per_call`. `None` if it advertises
    /// one of them not at all, or not as a decimal integer: then the cost is not known
    /// the cost and must not read.
    pub fn from_peer(peer: &Peer) -> Option<Rates> {
        Some(Rates {
            query_mu: peer_rate(peer, "statement_query_mu")?,
            row_mu: peer_rate(peer, "statement_row_mu")?,
        })
    }

    /// The price of an answer with `rows` movements (`statement_price_mu`).
    pub fn price(&self, rows: u64) -> u128 {
        self.query_mu
            .saturating_add(self.row_mu.saturating_mul(u128::from(rows)))
    }
}

/// What to read: the arguments of `BalanceStatementRequest`. The `Debug` text hides the
/// token, which is a bearer secret.
#[derive(Clone, PartialEq, Eq)]
pub struct StatementRequest {
    /// The token of an instance that the caller holds: the ancestor. Empty reads the
    /// account of the client.
    pub token: String,
    /// The descendant to read, by its `instance_ref`. Empty reads the account that
    /// `token` names.
    pub instance_ref: String,
    /// The first movement. 0 asks for no movement, only the head.
    pub from_seq: u64,
    /// The last movement, included. 0 asks up to the head.
    pub to_seq: u64,
    /// The hop budget. Always set it: a node that gets none uses its own
    /// maximum, and the cost then depends on a value that the node chooses.
    pub remaining_hops: u32,
}

impl std::fmt::Debug for StatementRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementRequest")
            .field("token", &"<hidden>")
            .field("instance_ref", &self.instance_ref)
            .field("from_seq", &self.from_seq)
            .field("to_seq", &self.to_seq)
            .field("remaining_hops", &self.remaining_hops)
            .finish()
    }
}

impl StatementRequest {
    /// Why the request cannot be sent, if it cannot.
    pub fn check(&self) -> Result<(), StatementError> {
        if !self.instance_ref.is_empty() && !is_digest(&self.instance_ref) {
            return Err(StatementError::BadRequest(
                "instance_ref is not 64 lowercase hex digits".into(),
            ));
        }
        if self.remaining_hops == 0 {
            return Err(StatementError::BadRequest(
                "remaining_hops is 0: a node refuses it".into(),
            ));
        }
        if self.to_seq != 0 && self.to_seq < self.from_seq {
            return Err(StatementError::BadRequest(
                "to_seq is less than from_seq".into(),
            ));
        }
        Ok(())
    }

    /// The message to send.
    pub fn to_proto(&self) -> BalanceStatementRequest {
        BalanceStatementRequest {
            token: self.token.clone(),
            from_seq: self.from_seq,
            to_seq: self.to_seq,
            instance_ref: self.instance_ref.clone(),
            remaining_hops: Some(self.remaining_hops),
        }
    }

    /// The most movements that the answer can hold.
    pub fn most_rows(&self) -> u64 {
        if self.from_seq == 0 {
            0
        } else if self.to_seq >= self.from_seq {
            (self.to_seq - self.from_seq + 1).min(MAX_STATEMENT_ROWS)
        } else {
            MAX_STATEMENT_ROWS
        }
    }

    /// The most MU that the read can cost: `max(1, remaining_hops - 1)` reads, each with
    /// the fixed price and the price of the most rows (`docs/celaut/descendants.md`,
    /// section 8). A service that spends MU on reads keeps this under its budget.
    pub fn max_mu_needed(&self, rates: &Rates) -> u128 {
        let reads = u128::from(self.remaining_hops.saturating_sub(1).max(1));
        reads.saturating_mul(rates.price(self.most_rows()))
    }
}

// ─── Errors ───────────────────────────────────────────────────────────────

/// Why a read of a statement failed.
#[derive(Debug, thiserror::Error)]
pub enum StatementError {
    /// The maximum cost of the read is more than the budget. Nothing was sent.
    #[error("the read can cost {needed} MU, and the budget is {max} MU")]
    OverBudget {
        /// The most MU that the read can cost.
        needed: u128,
        /// The budget that the caller gave.
        max: u128,
    },
    /// The request is not correct. Nothing was sent.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// The node ignored `instance_ref`: it answered with a statement that has no
    /// `instance_ref`. The node did charge the read.
    #[error("node does not support descendants")]
    NoDescendants,
    /// The answer is not valid. The node did charge the read.
    #[error("invalid statement: {0}")]
    Invalid(String),
    /// The call failed.
    #[error(transparent)]
    Wire(#[from] WireError),
}

impl StatementError {
    /// True if the request went out and the answer was refused, so the node may have
    /// charged the read. The caller must account for it.
    pub fn may_have_charged(&self) -> bool {
        matches!(
            self,
            StatementError::NoDescendants | StatementError::Invalid(_)
        )
    }
}

fn invalid<T>(text: impl Into<String>) -> Result<T, StatementError> {
    Err(StatementError::Invalid(text.into()))
}

// ─── The movements, typed ─────────────────────────────────────────────────

/// The detail of a `child_bound` movement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildBound {
    /// The `seq` of the `child_start` that this movement completes.
    pub starts_seq: Option<u64>,
    /// The service of the child, as lowercase hex. Old rows do not have it.
    pub service_id: Option<String>,
}

/// The detail of a `stop` movement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop {
    /// The death reason of the instance.
    pub reason: String,
    /// The MU that the `refund` took out. 0 if the movement says none.
    pub leftover: u128,
    /// The price of the maintenance tick that the instance could not pay. Only for the
    /// reason `out_of_balance`.
    pub tick: Option<String>,
}

/// What a movement is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Deposit,
    Credit,
    Debit,
    Start,
    Build,
    Maintenance,
    ChildStart,
    /// The child of a `child_start` exists. It moves nothing.
    ChildBound(ChildBound),
    TopUp,
    Withdrawal,
    Refund,
    Tunnel,
    ResourceChange,
    /// The last movement of an account. It moves nothing.
    Stop(Stop),
    StatementQuery,
    /// A kind that this code does not know. A reader treats it as a charge that it did
    /// not cause.
    Unknown(String),
}

/// One movement of a statement that passed the checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Movement {
    pub seq: u64,
    /// When the node wrote it, in unix seconds, on the clock of the node.
    pub ts: i64,
    /// The change of the balance, in MU. A charge is negative.
    pub amount: i128,
    /// The balance after the movement, in MU.
    pub balance_after: i128,
    pub kind: Kind,
    /// The instance that the movement concerns, as an `instance_ref`. Empty if none.
    pub instance_ref: String,
    /// The detail, as the node wrote it. Empty if none.
    pub detail: String,
    pub prev_hash: String,
    /// The hash of this movement.
    pub hash: String,
}

/// A canonical decimal integer with an optional minus sign: the text of the number that
/// `str(int(n))` gives in nodo (`statement_check._canonical`). So `007`, `-0` and `+5` are
/// refused.
fn parse_amount(what: &str, amount: &Option<Amount>) -> Result<i128, StatementError> {
    let text = amount_text(amount);
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return invalid(format!("{what} is not a decimal integer"));
    }
    let value: i128 = text
        .parse()
        .or_else(|_| invalid(format!("{what} is too large")))?;
    if value.to_string() != text {
        return invalid(format!("{what} is not a canonical integer"));
    }
    Ok(value)
}

/// The string value of a key of a detail object. A value of another type is an error.
fn detail_text(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, StatementError> {
    match object.get(key) {
        None => Ok(None),
        Some(serde_json::Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => invalid(format!("detail {key} is not a text")),
    }
}

fn detail_object(
    seq: u64,
    detail: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, StatementError> {
    if detail.is_empty() {
        return Ok(serde_json::Map::new());
    }
    match serde_json::from_str(detail) {
        Ok(serde_json::Value::Object(object)) => Ok(object),
        _ => invalid(format!("the detail of movement {seq} is not a JSON object")),
    }
}

fn parse_kind(m: &BalanceMovement, amount: i128) -> Result<Kind, StatementError> {
    Ok(match m.kind.as_str() {
        "deposit" => Kind::Deposit,
        "credit" => Kind::Credit,
        "debit" => Kind::Debit,
        "start" => Kind::Start,
        "build" => Kind::Build,
        "maintenance" => Kind::Maintenance,
        "child_start" => Kind::ChildStart,
        KIND_CHILD_BOUND => {
            if amount != 0 {
                return invalid(format!("child_bound {} moves MU", m.seq));
            }
            if !is_digest(&m.instance_ref) {
                return invalid(format!("child_bound {} has no instance_ref", m.seq));
            }
            let detail = detail_object(m.seq, &m.detail)?;
            let starts_seq = match detail_text(&detail, "starts_seq")? {
                None => None,
                Some(t) if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) => {
                    match t.parse() {
                        Ok(n) => Some(n),
                        Err(_) => return invalid("starts_seq is too large"),
                    }
                }
                Some(_) => return invalid("starts_seq is not a decimal number"),
            };
            let service_id = detail_text(&detail, "service_id")?;
            if service_id.as_deref().is_some_and(|s| !is_lower_hex(s)) {
                return invalid(format!("service_id of child_bound {} is not hex", m.seq));
            }
            Kind::ChildBound(ChildBound {
                starts_seq,
                service_id,
            })
        }
        "top_up" => Kind::TopUp,
        "withdrawal" => Kind::Withdrawal,
        "refund" => Kind::Refund,
        "tunnel" => Kind::Tunnel,
        "resource_change" => Kind::ResourceChange,
        KIND_STOP => {
            if amount != 0 {
                return invalid(format!("stop {} moves MU", m.seq));
            }
            let detail = detail_object(m.seq, &m.detail)?;
            let leftover = match detail_text(&detail, "leftover")? {
                None => 0,
                Some(t) if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) => {
                    match t.parse() {
                        Ok(n) => n,
                        Err(_) => return invalid("leftover is too large"),
                    }
                }
                Some(_) => return invalid("leftover is not a decimal number"),
            };
            Kind::Stop(Stop {
                reason: detail_text(&detail, "reason")?.unwrap_or_default(),
                leftover,
                tick: detail_text(&detail, "tick")?,
            })
        }
        "statement_query" => Kind::StatementQuery,
        other => Kind::Unknown(other.to_string()),
    })
}

// ─── The check of an answer ───────────────────────────────────────────────

/// A head of an account that the caller kept from an earlier statement of the same node
/// and descendant. A new statement must agree with it (`head_conflict`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptHead {
    pub seq: u64,
    pub hash: String,
}

/// A statement that passed the checks.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedStatement {
    /// The statement as the node gave it.
    pub statement: BalanceStatement,
    /// The node that signed it. It is the node of the connection.
    pub peer_id: String,
    /// The movements, typed.
    pub movements: Vec<Movement>,
    /// The balance at the head, in MU.
    pub balance: i128,
    /// True if the node that signed did not write the movements: it read them from the
    /// next node and signed them again.
    pub relayed: bool,
    /// The service of the descendant, as lowercase hex. Empty if the node did not say.
    pub service_id: String,
    /// The head to keep for the next read of this descendant on this node.
    pub head: KeptHead,
    /// The first `from_seq` to ask for the rest of the range, if the node gave fewer
    /// movements than the range holds (its limit or its retention).
    pub next_from_seq: Option<u64>,
    /// True if the first movement is after `from_seq`: the node says that it deleted the
    /// movements before it (its retention). A reader cannot check this. Children or a
    /// `child_bound` movement can be in the movements that are gone. A `stop` cannot: a
    /// movement after it would not be valid.
    pub gap: bool,
}

impl VerifiedStatement {
    /// The children that the movements link: `(instance_ref, ChildBound)`.
    pub fn children(&self) -> impl Iterator<Item = (&str, &ChildBound)> {
        self.movements.iter().filter_map(|m| match &m.kind {
            Kind::ChildBound(bound) => Some((m.instance_ref.as_str(), bound)),
            _ => None,
        })
    }

    /// The `stop` movement, if the account ended.
    pub fn stop(&self) -> Option<&Stop> {
        self.movements.iter().find_map(|m| match &m.kind {
            Kind::Stop(stop) => Some(stop),
            _ => None,
        })
    }
}

/// Check an answer to `request` from the node `peer_id` (the identity that the TLS
/// connection proved), and type its movements. `kept` is the head that the caller kept
/// from an earlier read, if any. `now` is the time of this service in Unix seconds: the
/// statement must be dated within [`CLOCK_SKEW_SECONDS`] of it.
///
/// The order is: the `instance_ref` first, then the range, the account, the signature,
/// the date, the chain and the head.
///
/// The account of the answer is checked only for a read with no `instance_ref`: it must be
/// `account_ref(request.token)`. A read of a descendant has an account that the caller does
/// not know by token, so the signed `instance_ref` stands in its place. A read of the
/// account of the client (empty token) is checked by
/// [`crate::gateway::get_balance_statement`], which knows the `client_id`.
pub fn verify(
    request: &StatementRequest,
    peer_id: &str,
    answer: BalanceStatement,
    kept: Option<&KeptHead>,
    now: i64,
) -> Result<VerifiedStatement, StatementError> {
    // A node that ignores instance_ref answers with its own account and no instance_ref.
    if answer.instance_ref != request.instance_ref {
        if !request.instance_ref.is_empty() && answer.instance_ref.is_empty() {
            return Err(StatementError::NoDescendants);
        }
        return invalid("the instance_ref of the answer is not the one of the request");
    }
    if answer.from_seq != request.from_seq || answer.to_seq != request.to_seq {
        return invalid("the range of the answer is not the range of the request");
    }
    if request.instance_ref.is_empty()
        && !request.token.is_empty()
        && answer.account_ref != account_ref(&request.token)
    {
        return invalid("the account of the answer is not the account that was asked for");
    }
    if answer.signature.is_empty() {
        return invalid("the statement has no signature");
    }
    if !signature_verifies(&answer, peer_id) {
        return invalid("the signature is not valid for the node of the connection");
    }
    if !is_digest(&answer.account_ref) {
        return invalid("account_ref is not 64 lowercase hex digits");
    }
    if !answer.service_id.is_empty() && !is_lower_hex(&answer.service_id) {
        return invalid("service_id is not lowercase hex");
    }
    if answer.ts.abs_diff(now) > CLOCK_SKEW_SECONDS {
        return invalid(format!(
            "the statement is dated {}, and the time of this service is {now}",
            answer.ts
        ));
    }
    let balance = parse_amount("the balance", &answer.balance)?;
    let movements = check_chain(request, &answer)?;
    check_head(&answer, &movements, balance, kept)?;
    let gap = movements
        .first()
        .is_some_and(|first| first.seq > request.from_seq);

    let last = movements.last().map(|m| m.seq);
    let next_from_seq = match last {
        Some(seq) if seq < answer.head_seq && (request.to_seq == 0 || seq < request.to_seq) => {
            Some(seq + 1)
        }
        _ => None,
    };
    Ok(VerifiedStatement {
        peer_id: peer_id.to_string(),
        balance,
        relayed: answer.relayed,
        service_id: answer.service_id.clone(),
        head: KeptHead {
            seq: answer.head_seq,
            hash: answer.head_hash.clone(),
        },
        next_from_seq,
        gap,
        movements,
        statement: answer,
    })
}

/// The chain of the movements (`statement_check.verify_chain`): ascending seqs with no
/// gap, each `prev_hash` the hash of the movement before, each balance the one before plus
/// the amount, no movement dated after the statement, the genesis hash and a balance from
/// 0 at seq 1, a first movement that the node does not skip, and a stop that ends the
/// account.
fn check_chain(
    request: &StatementRequest,
    s: &BalanceStatement,
) -> Result<Vec<Movement>, StatementError> {
    if request.from_seq == 0 && !s.movements.is_empty() {
        return invalid("the answer holds movements, and the request asked for none");
    }
    if u64::try_from(s.movements.len()).map_or(true, |n| n > request.most_rows()) {
        return invalid("the answer holds more movements than the read can pay for");
    }
    let mut out: Vec<Movement> = Vec::with_capacity(s.movements.len());
    for m in &s.movements {
        let amount = parse_amount("an amount", &m.amount)?;
        let balance_after = parse_amount("a balance_after", &m.balance_after)?;
        if !m.instance_ref.is_empty() && !is_digest(&m.instance_ref) {
            return invalid(format!(
                "the instance_ref of movement {} is not a hash",
                m.seq
            ));
        }
        let kind = parse_kind(m, amount)?;
        if m.ts > s.ts {
            return invalid(format!("movement {} is dated after the statement", m.seq));
        }
        let expected_prev = match out.last() {
            None if m.seq == 1 => {
                if amount != balance_after {
                    return invalid("the first movement does not start from 0");
                }
                Some(genesis_hash(&s.account_ref))
            }
            None => None,
            Some(before) => {
                if before.seq.checked_add(1) != Some(m.seq) {
                    return invalid(format!("movement {} does not follow {}", m.seq, before.seq));
                }
                if matches!(before.kind, Kind::Stop(_)) {
                    return invalid(format!("movement {} comes after a stop", m.seq));
                }
                if before.balance_after.checked_add(amount) != Some(balance_after) {
                    return invalid(format!("movement {} does not add up", m.seq));
                }
                Some(before.hash.clone())
            }
        };
        if let Some(expected) = expected_prev {
            if m.prev_hash != expected {
                return invalid(format!(
                    "movement {} does not hang from the one before",
                    m.seq
                ));
            }
        } else if !is_digest(&m.prev_hash) {
            return invalid(format!("prev_hash of movement {} is not a hash", m.seq));
        }
        out.push(Movement {
            seq: m.seq,
            ts: m.ts,
            amount,
            balance_after,
            kind,
            instance_ref: m.instance_ref.clone(),
            detail: m.detail.clone(),
            prev_hash: m.prev_hash.clone(),
            hash: movement_hash(m),
        });
    }
    if let Some(first) = out.first() {
        // The node gives the movements from `from_seq`, or from the first one that it
        // holds if that is later. It never skips some of what it holds.
        let expected = request.from_seq.max(s.oldest_seq);
        if first.seq < expected {
            return invalid("the first movement is outside the range");
        }
        if first.seq != expected {
            return invalid(format!(
                "movements {expected} to {} are held and not shown",
                first.seq.saturating_sub(1)
            ));
        }
    }
    if let Some(last) = out.last() {
        if request.to_seq != 0 && last.seq > request.to_seq {
            return invalid("a movement is after to_seq");
        }
    }
    Ok(out)
}

/// The head: its hash, the balance, and the head that the caller kept.
fn check_head(
    s: &BalanceStatement,
    movements: &[Movement],
    balance: i128,
    kept: Option<&KeptHead>,
) -> Result<(), StatementError> {
    match movements.last() {
        Some(last) if last.seq > s.head_seq => {
            return invalid("a movement is after the head");
        }
        Some(last) if last.seq == s.head_seq => {
            if last.hash != s.head_hash {
                return invalid("head_hash is not the hash of the last movement");
            }
            if last.balance_after != balance {
                return invalid("the balance is not the balance after the last movement");
            }
        }
        // The range ends before the head: the node can hold fewer rows than the range.
        Some(_) => {}
        None => {
            if s.head_seq == 0 {
                if s.head_hash != genesis_hash(&s.account_ref) {
                    return invalid("head_hash is not the genesis hash of an empty account");
                }
            } else if !is_digest(&s.head_hash) {
                return invalid("head_hash is not a hash");
            }
            // A range that the head covers must hold movements.
            if s.from_seq > 0
                && s.head_seq >= s.from_seq
                && (s.to_seq == 0 || s.to_seq >= s.from_seq)
                && s.oldest_seq <= s.head_seq
            {
                return invalid("the answer holds no movement of the range");
            }
        }
    }
    if s.head_seq != 0 && !is_digest(&s.head_hash) {
        return invalid("head_hash is not a hash");
    }
    if let Some(kept) = kept {
        // An account with no movement at all (retention) is not a conflict.
        if s.head_seq != 0 {
            check_kept(s, movements, kept)?;
        }
    }
    Ok(())
}

/// The head only grows: it never goes back, a seq keeps its hash, and the movement after
/// the kept head hangs from it.
fn check_kept(
    s: &BalanceStatement,
    movements: &[Movement],
    kept: &KeptHead,
) -> Result<(), StatementError> {
    if s.head_seq < kept.seq {
        return invalid(format!(
            "head {} is below the head {} that the node signed before",
            s.head_seq, kept.seq
        ));
    }
    if s.head_seq == kept.seq && s.head_hash != kept.hash {
        return invalid(format!("head {} has another hash", kept.seq));
    }
    for m in movements {
        if m.seq == kept.seq && m.hash != kept.hash {
            return invalid(format!("movement {} has another hash", kept.seq));
        }
        if m.seq == kept.seq + 1 && m.prev_hash != kept.hash {
            return invalid(format!(
                "movement {} does not follow the head {}",
                m.seq, kept.seq
            ));
        }
    }
    Ok(())
}
