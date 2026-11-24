//! The price offer that a node signs: `EstimatedCost` with `valid_until` and `signature`
//! (nodo#657, `nodo:src/utils/cost_functions/signed_offer.py` and `offer_honor.py`).
//!
//! The offer is a commitment of the node. A caller that sends it back in
//! `Configuration.offer` before `valid_until` gets the price of the offer, even if the load
//! of the node moved. The node keeps no state: the offer proves itself, so a node checks that
//! it signed it, that its time is not past and that its figures are the ones that it
//! computes for the service and the resources at the load of the offer. A caller cannot
//! replay the last check (it needs the service and the pricing of the node), but it can
//! check the signature and the time, and decide not to send an offer that the node would
//! ignore ([`check`]).
//!
//! The signature covers a canonical text of every field but the signature, never the
//! protobuf bytes ([`offer_digest`]). The node signs
//! `celaut-price-offer:<peer_id>|<offer_digest>`.

use std::collections::BTreeMap;

use celaut_proto::celaut::{Configuration, EstimatedCost};

use crate::receipt::{blake2b_256_hex, hex_text};
use crate::signing;

/// The prefix of the signed text (`node_identity._OFFER_PREFIX`).
pub const PREFIX: &str = "celaut-price-offer:";

/// Parts per million: the resolution of the load in an offer (`SCARCITY_SCALE`).
pub const SCARCITY_SCALE: u64 = 1_000_000;

/// The resources that the load of a valid offer names (`execution_cost.RESOURCES`): the
/// node ignores an offer that has no load for each of them.
pub const RESOURCES: [&str; 3] = ["cpu", "mem", "disk"];

fn amount_text(a: &Option<celaut_proto::celaut::Amount>) -> String {
    hex_text(a.as_ref().map_or("", |a| a.n.as_str()))
}

/// The load in `offer` in parts per million. The last entry of a key wins, as nodo reads it.
pub fn scarcity_of(offer: &EstimatedCost) -> BTreeMap<String, u64> {
    offer
        .launch_scarcity
        .iter()
        .map(|e| (e.key.clone(), e.value.unwrap_or(0)))
        .collect()
}

/// The text that [`offer_digest`] hashes: every field but `signature`, in the order of the
/// numbers of the fields in `celaut.proto`, joined by `|`:
///
/// ```text
/// hex(cost.n) | hex(init_maintenance_cost.n) | hex(max_maintenance_cost.n)
/// | maintenance_seconds_loop | launch_scarcity | price_bounded | price_volatility
/// | volatility_period_seconds | valid_until | closes_at | on_close
/// ```
///
/// `launch_scarcity` is `key=value` for each key, sorted by key and joined by `;`.
/// `price_bounded` is 1 or 0. `price_volatility` is the 4 bytes of the float, big-endian,
/// in hex. `on_close` is the number of the enum.
pub fn canonical_offer(o: &EstimatedCost) -> String {
    let scarcity = scarcity_of(o)
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";");
    [
        amount_text(&o.cost),
        amount_text(&o.init_maintenance_cost),
        amount_text(&o.max_maintenance_cost),
        o.maintenance_seconds_loop.to_string(),
        scarcity,
        if o.price_bounded { "1" } else { "0" }.to_string(),
        hex::encode(o.price_volatility.to_be_bytes()),
        o.volatility_period_seconds.to_string(),
        o.valid_until.to_string(),
        o.closes_at.to_string(),
        o.on_close.to_string(),
    ]
    .join("|")
}

/// Blake2b-256 hex of [`canonical_offer`] (`signed_offer.offer_digest`).
pub fn offer_digest(o: &EstimatedCost) -> String {
    blake2b_256_hex(canonical_offer(o).as_bytes())
}

/// The text that a node signs for an offer (`node_identity.offer_payload`).
pub fn signed_text(peer_id: &str, digest: &str) -> String {
    format!("{PREFIX}{peer_id}|{digest}")
}

/// Whether `peer_id` signed exactly this offer (`signed_offer.verify`). An offer with no
/// signature never verifies.
pub fn signature_verifies(offer: &EstimatedCost, peer_id: &str) -> bool {
    !offer.signature.is_empty()
        && signing::verifies(
            peer_id,
            &signed_text(peer_id, &offer_digest(offer)),
            &offer.signature,
        )
}

/// The time until which `peer_id` keeps the price of `offer`, or 0
/// (`signed_offer.committed_until`): 0 when the offer promises nothing, and also when it
/// names a time with no valid signature.
pub fn committed_until(offer: &EstimatedCost, peer_id: &str) -> i64 {
    if offer.valid_until <= 0 || !signature_verifies(offer, peer_id) {
        0
    } else {
        offer.valid_until
    }
}

/// Whether the time of `offer` is not past at `now` (unix seconds): `signed_offer.is_current`.
pub fn is_current(offer: &EstimatedCost, now: i64) -> bool {
    offer.valid_until > 0 && now <= offer.valid_until
}

/// Why a node would ignore an offer, and so price the start at its price of now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OfferFault {
    /// `valid_until` is 0: the node made no promise.
    #[error("the offer makes no promise (valid_until is 0)")]
    NoPromise,
    /// The offer has a time and no valid signature of the node.
    #[error("the offer is not signed by this node")]
    NotSigned,
    /// The time of the offer is past.
    #[error("the offer is past its valid_until ({valid_until}, now {now})")]
    Expired { valid_until: i64, now: i64 },
    /// The load has no entry for one of `cpu`, `mem` and `disk`.
    #[error("the offer has no load for {0}")]
    NoLoad(&'static str),
}

/// What the node checks of an offer before it keeps its price, as far as a caller can
/// (`offer_honor.honored`): it has a time, `peer_id` signed it, the time is not past and the
/// load names each resource. The last check of the node, that the figures are those of the
/// service, needs the pricing of the node and is not done here.
pub fn check(offer: &EstimatedCost, peer_id: &str, now: i64) -> Result<(), OfferFault> {
    if offer.valid_until <= 0 {
        return Err(OfferFault::NoPromise);
    }
    if !signature_verifies(offer, peer_id) {
        return Err(OfferFault::NotSigned);
    }
    if !is_current(offer, now) {
        return Err(OfferFault::Expired {
            valid_until: offer.valid_until,
            now,
        });
    }
    let load = scarcity_of(offer);
    if let Some(missing) = RESOURCES.iter().find(|k| !load.contains_key(**k)) {
        return Err(OfferFault::NoLoad(missing));
    }
    Ok(())
}

/// `configuration` with `offer` as the offer that the caller accepts
/// (`Configuration.offer`). The node uses it only if it is valid for the start.
pub fn with_offer(mut configuration: Configuration, offer: EstimatedCost) -> Configuration {
    configuration.offer = Some(offer);
    configuration
}

/// Sign `offer` in place with the Ed25519 key of `seed`, valid until `valid_until`, as a
/// node does (`signed_offer.seal`): it sets `valid_until` and the signature, and gives the
/// peer id. A service never signs an offer: this is for tests and tools.
pub fn seal_with_seed(
    offer: &mut EstimatedCost,
    valid_until: i64,
    seed: &[u8; 32],
) -> Result<String, String> {
    offer.signature.clear();
    offer.valid_until = valid_until;
    let peer_id = signing::sign(seed, "")?.0;
    let text = signed_text(&peer_id, &offer_digest(offer));
    offer.signature = signing::sign(seed, &text)?.1;
    Ok(peer_id)
}

/// An offer of a node and what a caller can say of it: see
/// [`crate::gateway::Gateway::checked_offer`].
#[derive(Debug, Clone, PartialEq)]
pub struct CheckedOffer {
    /// The `EstimatedCost` as the node sent it.
    pub cost: EstimatedCost,
    /// The identity that must have signed it. Empty when the offer makes no promise.
    pub peer_id: String,
    /// `Ok` when the node would keep the price of the offer for a start now, as far as a
    /// caller can check ([`check`]).
    pub verdict: Result<(), OfferFault>,
}

impl CheckedOffer {
    /// Whether the offer is worth sending in `Configuration.offer`.
    pub fn is_usable(&self) -> bool {
        self.verdict.is_ok()
    }

    /// `base` with the offer in `Configuration.offer` when the offer is usable, else `base`
    /// unchanged: the node ignores an offer that it would not keep, so sending it gains
    /// nothing.
    pub fn configuration(&self, base: Configuration) -> Configuration {
        if self.is_usable() {
            with_offer(base, self.cost.clone())
        } else {
            base
        }
    }
}
