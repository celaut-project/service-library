//! The off-chain reputation proof of a node (`ReputationProof`, nodo#652 and #677;
//! `nodo:src/reputation_system/offchain_proof.py` and `fault_proofs.py`).
//!
//! A proof holds opinions about nodes, signed with the identity key of the publisher, which
//! is the id of the proof. It has a proof of work, `pow_nonce`. The signature covers
//! `celaut-reputation-proof:<publisher>|<proof_digest>`, and `proof_digest` covers every
//! field but the signature, from a canonical text and never from the protobuf bytes:
//!
//! ```text
//! base   = blake2b_256( publisher | canonical_scheme | ts | opinion/opinion/... | pointer/pointer/... )
//! digest = blake2b_256( base | pow_nonce )
//! ```
//!
//! The nonce is the last part, so a node that looks for one hashes the base once. An opinion
//! is `hex(type)~hex(object)~amount~positive~hex(content)~evidence`, where `evidence` is the
//! digest of its `FaultEvidence` ([`evidence_digest`]) or empty. The pointers are the sorted
//! [`crate::peer::canonical_contract`] of each `ledger_proofs`. The work is the number of
//! leading zero bits of the digest ([`pow_bits`]).
//!
//! This module checks the signature and the work. It does not check the fault that an
//! evidence names, or the attestation of a ledger pointer: those are judgements of a node.

use celaut_proto::celaut::{FaultEvidence, ReputationOpinion, ReputationProof};

use crate::peer::canonical_contract;
use crate::receipt::{blake2b_256_hex, canonical_scheme, hex_text, receipt_digest, scheme_is_ours};
use crate::signing;
use crate::statement::statement_digest;

/// The prefix of the signed text (`node_identity._REPUTATION_PROOF_PREFIX`).
pub const PREFIX: &str = "celaut-reputation-proof:";

/// The most bits of work that a node looks for (`MAX_POW_BITS`).
pub const MAX_POW_BITS: u32 = 32;

fn escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('|', "\\|")
}

/// The digest of a `FaultEvidence` (`fault_proofs.digest`): the reason and the accused, with
/// `|` and `\` escaped, then the digest of each statement and of each receipt, in order,
/// joined by `|`.
pub fn evidence_digest(e: &FaultEvidence) -> String {
    let mut parts = vec![escape(&e.reason), escape(&e.accused)];
    parts.extend(e.statements.iter().map(statement_digest));
    parts.extend(e.receipts.iter().map(receipt_digest));
    blake2b_256_hex(parts.join("|").as_bytes())
}

fn opinion_text(o: &ReputationOpinion) -> String {
    let evidence = o.evidence.as_ref().map(evidence_digest).unwrap_or_default();
    [
        hex_text(&o.r#type),
        hex_text(&o.object),
        o.amount.to_string(),
        if o.positive { "1" } else { "0" }.to_string(),
        hex::encode(&o.content),
        evidence,
    ]
    .join("~")
}

/// The base of the digest: every field but the nonce and the signature (`pow_base`).
pub fn pow_base(p: &ReputationProof) -> String {
    let scheme = p
        .signature_scheme
        .as_ref()
        .map(canonical_scheme)
        .unwrap_or_default();
    let opinions = p
        .opinions
        .iter()
        .map(opinion_text)
        .collect::<Vec<_>>()
        .join("/");
    let mut pointers: Vec<String> = p.ledger_proofs.iter().map(canonical_contract).collect();
    pointers.sort();
    let text = [
        p.publisher.clone(),
        scheme,
        p.ts.to_string(),
        opinions,
        pointers.join("/"),
    ]
    .join("|");
    blake2b_256_hex(text.as_bytes())
}

fn pow_hash(base: &str, nonce: u64) -> String {
    blake2b_256_hex(format!("{base}|{nonce}").as_bytes())
}

/// Blake2b-256 hex of `base|pow_nonce`: it covers every field but the signature
/// (`proof_digest`).
pub fn proof_digest(p: &ReputationProof) -> String {
    pow_hash(&pow_base(p), p.pow_nonce)
}

/// The number of leading zero bits of a digest in hex.
pub fn zero_bits(digest: &str) -> u32 {
    let mut bits = 0;
    for b in hex::decode(digest).unwrap_or_default() {
        if b == 0 {
            bits += 8;
        } else {
            return bits + b.leading_zeros();
        }
    }
    bits
}

/// The work of `p`: the leading zero bits of its digest (`pow_bits`).
pub fn pow_bits(p: &ReputationProof) -> u32 {
    zero_bits(&proof_digest(p))
}

/// Look for a `pow_nonce` that gives `p` at least `bits` of work, from 0, and set it
/// (`mine`). A node does it before it signs. `bits` is at most [`MAX_POW_BITS`].
pub fn mine(p: &mut ReputationProof, bits: u32) {
    let base = pow_base(p);
    let mut nonce = 0u64;
    while zero_bits(&pow_hash(&base, nonce)) < bits.min(MAX_POW_BITS) {
        nonce += 1;
    }
    p.pow_nonce = nonce;
}

/// The text that a node signs for a proof (`node_identity.reputation_proof_payload`).
pub fn signed_text(publisher: &str, digest: &str) -> String {
    format!("{PREFIX}{publisher}|{digest}")
}

/// Whether `proof.publisher` signed exactly this proof, in the Ed25519 scheme
/// (`offchain_proof.verify`). The signature must be in lowercase.
pub fn verify(p: &ReputationProof) -> bool {
    scheme_is_ours(p.signature_scheme.as_ref())
        && !p.signature.is_empty()
        && signing::verifies(
            &p.publisher,
            &signed_text(&p.publisher, &proof_digest(p)),
            &p.signature,
        )
}

/// Sign `p` in place with the Ed25519 key of `seed`, as a node does (`offchain_proof.sign`):
/// it sets `publisher`, the scheme of a receipt and the signature, which covers `pow_nonce`
/// (set it first, with [`mine`]). A service never signs a proof: this is for tests and tools.
pub fn sign_with_seed(p: &mut ReputationProof, seed: &[u8; 32]) -> Result<(), String> {
    p.publisher = signing::sign(seed, "")?.0;
    p.signature_scheme = Some(crate::receipt::receipt_scheme());
    let text = signed_text(&p.publisher, &proof_digest(p));
    p.signature = signing::sign(seed, &text)?.1;
    Ok(())
}
