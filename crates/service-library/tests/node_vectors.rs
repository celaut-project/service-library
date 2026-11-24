//! Digests and signatures of nodo, checked against the code of nodo.
//!
//! `tests/data/node_vectors.json` and `tests/data/node_receipts.json` are made by
//! `tests/data/gen_node_vectors.py`, which runs the real digest and signature functions of nodo
//! (`/home/jse/Escritorio/nodo`, `celaut.proto` sha256 4c48c295...) with the fixed key
//! `000102..1f`. Each message is stored as protobuf bytes, so the test also reads the wire of
//! nodo with the prost types of this library: a field with a different number gives a
//! different digest, and fails here.
//!
//! Every check is a cross-check: the expected value comes from Python, and this crate computes
//! it again, or verifies the signature that Python made.

use celaut_proto::celaut::{
    BalanceStatement, Configuration, EstimatedCost, ExecutionReceipt, FaultEvidence, Peer,
    ReputationProof,
};
use prost::Message;
use serde_json::Value;
use service_library::offer::{self, OfferFault};
use service_library::{peer, receipt, reputation, statement};

fn vectors() -> Value {
    serde_json::from_str(include_str!("data/node_vectors.json")).unwrap()
}

fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or_else(|| panic!("no {key}"))
}

fn decode<M: Message + Default>(v: &Value) -> M {
    M::decode(hex::decode(text(v, "bytes")).unwrap().as_slice()).unwrap()
}

const SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

#[test]
fn the_key_of_the_vectors_is_the_seed_of_the_tests() {
    let v = vectors();
    assert_eq!(text(&v, "seed"), hex::encode(SEED));
    let mut s = BalanceStatement::default();
    assert_eq!(
        statement::sign_with_seed(&mut s, &SEED).unwrap(),
        text(&v, "peer_id")
    );
}

#[test]
fn the_balance_statements_of_the_node_digest_and_verify() {
    let v = vectors();
    for case in v["statements"].as_array().unwrap() {
        let name = text(case, "name");
        let s: BalanceStatement = decode(case);
        assert_eq!(
            statement::statement_digest(&s),
            text(case, "digest"),
            "{name}"
        );
        assert_eq!(
            statement::signed_text(text(case, "peer_id"), text(case, "digest")),
            text(case, "payload"),
            "{name}"
        );
        assert_eq!(s.signature, text(case, "signature"), "{name}");
        assert!(
            statement::signature_verifies(&s, text(case, "peer_id")),
            "{name}"
        );
        assert_eq!(s.account_ref, text(case, "account_ref"), "{name}");
        assert_eq!(
            statement::account_ref(text(case, "account")),
            text(case, "account_ref"),
            "{name}"
        );
        assert_eq!(
            statement::genesis_hash(&s.account_ref),
            text(case, "genesis_hash"),
            "{name}"
        );
        let hashes: Vec<String> = s.movements.iter().map(statement::movement_hash).collect();
        let expected: Vec<&str> = case["movement_hashes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h.as_str().unwrap())
            .collect();
        assert_eq!(hashes, expected, "{name}");
        // A change of any signed field breaks the signature.
        let mut t = s.clone();
        t.ts += 1;
        assert!(
            !statement::signature_verifies(&t, text(case, "peer_id")),
            "{name}"
        );
        let mut t = s.clone();
        t.relayed = !t.relayed;
        assert!(
            !statement::signature_verifies(&t, text(case, "peer_id")),
            "{name}"
        );
    }
}

#[test]
fn the_statements_of_the_node_pass_the_chain_checks() {
    // The plain statement is a whole account: its chain, head and balance hold.
    let v = vectors();
    let case = &v["statements"][0];
    let s: BalanceStatement = decode(case);
    let request = statement::StatementRequest {
        token: String::new(),
        instance_ref: String::new(),
        from_seq: 1,
        to_seq: s.head_seq,
        remaining_hops: 0,
    };
    let verified = statement::verify(&request, text(case, "peer_id"), s.clone(), None, s.ts)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(verified.balance, 0);
    assert_eq!(verified.movements.len(), 6);
}

#[test]
fn the_hashes_and_references_of_the_node_match() {
    let v = vectors();
    let r = &v["refs"];
    let token = text(r, "token");
    let peer_id = text(r, "peer_id");
    assert_eq!(statement::instance_ref(token), text(r, "instance_ref"));
    assert_eq!(statement::account_ref(token), text(r, "account_ref"));
    assert_eq!(
        statement::genesis_hash(&statement::account_ref(token)),
        text(r, "genesis_hash")
    );
    assert_eq!(
        receipt::execution_name(token, peer_id).unwrap(),
        text(r, "execution_name")
    );
    assert_eq!(
        receipt::execution_name_by_ref(text(r, "instance_ref"), peer_id).unwrap(),
        text(r, "execution_name_by_ref")
    );
    let m = celaut_proto::celaut::BalanceMovement {
        seq: 7,
        ts: 1798329600,
        amount: Some(celaut_proto::celaut::Amount { n: "-15".into() }),
        balance_after: Some(celaut_proto::celaut::Amount { n: "985".into() }),
        kind: "tunnel".into(),
        instance_ref: "bb".repeat(32),
        detail: r#"{"open":true}"#.into(),
        prev_hash: "aa".repeat(32),
    };
    assert_eq!(statement::movement_hash(&m), text(r, "movement_hash"));
    let stop = celaut_proto::celaut::BalanceMovement {
        seq: 1,
        ts: 5,
        amount: Some(celaut_proto::celaut::Amount { n: "0".into() }),
        balance_after: Some(celaut_proto::celaut::Amount { n: "0".into() }),
        kind: "stop".into(),
        prev_hash: "aa".repeat(32),
        ..Default::default()
    };
    assert_eq!(
        statement::movement_hash(&stop),
        text(r, "movement_hash_empty")
    );
    assert_eq!(
        receipt::canonical_scheme(&receipt::receipt_scheme()),
        text(r, "scheme")
    );
}

#[test]
fn the_envs_and_the_chains_of_the_node_match() {
    let v = vectors();
    let r = &v["refs"];
    let entries: Vec<celaut_proto::celaut::BytesKeyValue> = r["envs"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, val)| celaut_proto::celaut::BytesKeyValue {
            key: k.clone(),
            value: Some(hex::decode(val.as_str().unwrap()).unwrap()),
        })
        .collect();
    let envs = receipt::service_envs(&entries);
    let origins: std::collections::BTreeMap<String, receipt::Origin> = r["origins"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, o)| {
            assert_eq!(o, "node");
            (k.clone(), receipt::Origin::Node)
        })
        .collect();
    let salt = hex::decode(text(r, "salt")).unwrap();
    assert_eq!(
        receipt::canonical_envs(&envs, &origins),
        text(r, "canonical_envs")
    );
    assert_eq!(
        receipt::envs_digest(&envs, &salt, &origins),
        text(r, "envs_digest")
    );
    assert_eq!(receipt::payload_digest(b"hello"), text(r, "payload_digest"));
    for (name, direction) in [
        ("chain_in", receipt::DIRECTION_IN),
        ("chain_out", receipt::DIRECTION_OUT),
    ] {
        let c = &r[name];
        let events: Vec<Vec<u8>> = c["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| hex::decode(e.as_str().unwrap()).unwrap())
            .collect();
        let chain = receipt::replay_chain(events.iter().map(|e| e.as_slice()), direction);
        assert_eq!(chain.events, c["count"].as_u64().unwrap(), "{name}");
        assert_eq!(chain.bytes, c["bytes"].as_u64().unwrap(), "{name}");
        assert_eq!(chain.head, text(c, "head"), "{name}");
    }
}

#[test]
fn the_receipts_of_the_node_digest_and_verify_through_the_wire() {
    let all: Value = serde_json::from_str(include_str!("data/node_receipts.json")).unwrap();
    for (name, case) in all.as_object().unwrap() {
        let r: ExecutionReceipt = decode(case);
        assert_eq!(
            receipt::canonical_receipt(&r),
            text(case, "canonical"),
            "{name}"
        );
        assert_eq!(receipt::receipt_digest(&r), text(case, "digest"), "{name}");
        assert_eq!(
            String::from_utf8(receipt::signed_bytes(&r)).unwrap(),
            text(case, "payload"),
            "{name}"
        );
        assert_eq!(r.signature, text(case, "signature"), "{name}");
        assert_eq!(
            receipt::problems(&r, None, None),
            Vec::<String>::new(),
            "{name}"
        );
        assert!(receipt::verify(&r, Some(&r.peer_id), None), "{name}");
    }
}

#[test]
fn the_offers_of_the_node_digest_and_verify() {
    let v = vectors();
    for case in v["offers"].as_array().unwrap() {
        let name = text(case, "name");
        let o: EstimatedCost = decode(case);
        let peer_id = text(case, "peer_id");
        assert_eq!(offer::offer_digest(&o), text(case, "digest"), "{name}");
        assert_eq!(
            offer::signed_text(peer_id, text(case, "digest")),
            text(case, "payload"),
            "{name}"
        );
        assert_eq!(o.signature, text(case, "signature"), "{name}");
        assert_eq!(
            offer::committed_until(&o, peer_id),
            case["committed_until"].as_i64().unwrap(),
            "{name}"
        );
        assert_eq!(
            offer::signature_verifies(&o, peer_id),
            !o.signature.is_empty(),
            "{name}"
        );
        // Every field but the signature moves the digest (and so breaks the signature).
        if !o.signature.is_empty() {
            type Change = fn(&mut EstimatedCost);
            let changes: [(&str, Change); 9] = [
                ("cost", |o| o.cost.as_mut().unwrap().n.push('1')),
                ("init", |o| {
                    o.init_maintenance_cost.as_mut().unwrap().n.push('1')
                }),
                ("max", |o| {
                    o.max_maintenance_cost.as_mut().unwrap().n.push('1')
                }),
                ("loop", |o| o.maintenance_seconds_loop += 1),
                ("bounded", |o| o.price_bounded = !o.price_bounded),
                ("volatility", |o| o.price_volatility += 0.125),
                ("period", |o| o.volatility_period_seconds += 1),
                ("valid_until", |o| o.valid_until += 1),
                ("closes_at", |o| o.closes_at += 1),
            ];
            for (field, change) in changes {
                let mut t = o.clone();
                change(&mut t);
                assert!(!offer::signature_verifies(&t, peer_id), "{name} {field}");
            }
            let mut t = o.clone();
            t.launch_scarcity[0].value = Some(t.launch_scarcity[0].value.unwrap_or(0) + 1);
            assert!(
                !offer::signature_verifies(&t, peer_id),
                "{name} launch_scarcity"
            );
            let mut t = o.clone();
            t.on_close += 1;
            assert!(!offer::signature_verifies(&t, peer_id), "{name} on_close");
        }
    }
}

#[test]
fn an_offer_is_checked_as_the_node_checks_it() {
    let v = vectors();
    let peer_id = text(&v, "peer_id").to_string();
    let signed: EstimatedCost = decode(&v["offers"][0]);
    let until = signed.valid_until;
    assert_eq!(offer::check(&signed, &peer_id, until), Ok(()));
    assert_eq!(
        offer::check(&signed, &peer_id, until + 1),
        Err(OfferFault::Expired {
            valid_until: until,
            now: until + 1
        })
    );
    assert!(offer::is_current(&signed, until) && !offer::is_current(&signed, until + 1));
    // Another node did not sign it.
    assert_eq!(
        offer::check(&signed, &"ab".repeat(32), until - 1),
        Err(OfferFault::NotSigned)
    );
    // A time that was moved after the signature.
    let mut moved = signed.clone();
    moved.valid_until += 1000;
    assert_eq!(
        offer::check(&moved, &peer_id, until),
        Err(OfferFault::NotSigned)
    );
    assert_eq!(offer::committed_until(&moved, &peer_id), 0);
    // An offer with no promise.
    let none: EstimatedCost = decode(&v["offers"][3]);
    assert_eq!(offer::check(&none, &peer_id, 0), Err(OfferFault::NoPromise));
    // The load must name each resource.
    let mut partial = signed.clone();
    partial.launch_scarcity.retain(|e| e.key != "disk");
    offer::seal_with_seed(&mut partial, until, &SEED).unwrap();
    assert_eq!(
        offer::check(&partial, &peer_id, until),
        Err(OfferFault::NoLoad("disk"))
    );
    // A sealed offer of this library is the one that Python signs.
    let mut again = signed.clone();
    assert_eq!(
        offer::seal_with_seed(&mut again, until, &SEED).unwrap(),
        peer_id
    );
    assert_eq!(again.signature, signed.signature);
}

#[test]
fn an_offer_goes_in_field_three_of_the_configuration() {
    let v = vectors();
    let signed: EstimatedCost = decode(&v["offers"][0]);
    let configuration = offer::with_offer(Configuration::default(), signed.clone());
    let bytes = configuration.encode_to_vec();
    // key 3, class 2: byte 0x1a, then the length and the bytes of the offer.
    assert_eq!(bytes[0], 0x1a);
    let back = Configuration::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.offer, Some(signed));
}

#[test]
fn the_peers_of_the_node_digest_and_verify() {
    let v = vectors();
    for case in v["peers"].as_array().unwrap() {
        let name = text(case, "name");
        let p: Peer = decode(case);
        assert_eq!(peer::content_digest(&p), text(case, "digest"), "{name}");
        assert_eq!(
            peer::signed_text(&p.public_key, p.ts, text(case, "digest")),
            text(case, "payload"),
            "{name}"
        );
        assert_eq!(p.signature, text(case, "signature"), "{name}");
        assert_eq!(
            peer::verify(&p).as_deref(),
            Ok(text(case, "peer_id")),
            "{name}"
        );
        // The signature covers the time and each advertised thing.
        let mut t = p.clone();
        t.ts += 1;
        assert!(peer::verify(&t).is_err(), "{name} ts");
        let mut t = p.clone();
        t.uri.push(Default::default());
        assert!(peer::verify(&t).is_err(), "{name} uri");
        let mut t = p.clone();
        t.rates.push(Default::default());
        assert!(peer::verify(&t).is_err(), "{name} rates");
        let mut t = p.clone();
        t.public_key = t.public_key.to_uppercase();
        assert_eq!(
            peer::verify(&t),
            Err(peer::PeerFault::NonCanonicalKey),
            "{name}"
        );
    }
    // Signing in this library gives the signature of Python.
    let case = &v["peers"][0];
    let mut p: Peer = decode(case);
    p.signature.clear();
    let ts = p.ts;
    peer::sign_with_seed(&mut p, ts, &SEED).unwrap();
    assert_eq!(p.signature, text(case, "signature"));
}

#[test]
fn a_peer_with_another_scheme_or_no_signature_is_refused() {
    let v = vectors();
    let mut p: Peer = decode(&v["peers"][0]);
    p.signature_scheme.as_mut().unwrap().components[0].formal = b"algorithm=other".to_vec();
    assert_eq!(peer::verify(&p), Err(peer::PeerFault::UnknownScheme));
    let mut p: Peer = decode(&v["peers"][0]);
    p.signature.clear();
    assert_eq!(peer::verify(&p), Err(peer::PeerFault::Unsigned));
}

#[test]
fn the_reputation_proofs_of_the_node_digest_and_verify() {
    let v = vectors();
    for case in v["proofs"].as_array().unwrap() {
        let name = text(case, "name");
        let p: ReputationProof = decode(case);
        assert_eq!(reputation::pow_base(&p), text(case, "base"), "{name}");
        assert_eq!(reputation::proof_digest(&p), text(case, "digest"), "{name}");
        assert_eq!(p.pow_nonce, case["pow_nonce"].as_u64().unwrap(), "{name}");
        assert_eq!(
            reputation::pow_bits(&p) as u64,
            case["pow_bits"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            reputation::signed_text(&p.publisher, text(case, "digest")),
            text(case, "payload"),
            "{name}"
        );
        assert_eq!(p.signature, text(case, "signature"), "{name}");
        assert!(reputation::verify(&p), "{name}");
        // The nonce and each field are signed.
        let mut t = p.clone();
        t.pow_nonce += 1;
        assert!(!reputation::verify(&t), "{name} pow_nonce");
        let mut t = p.clone();
        t.ts += 1;
        assert!(!reputation::verify(&t), "{name} ts");
        let mut t = p.clone();
        t.opinions[0].amount += 1;
        assert!(!reputation::verify(&t), "{name} opinion");
        let mut t = p.clone();
        t.ledger_proofs.push(Default::default());
        assert!(!reputation::verify(&t), "{name} ledger_proofs");
    }
    // A mined proof has the work that was asked for; this library mines the same nonce.
    let mined = &v["proofs"][2];
    let p: ReputationProof = decode(mined);
    assert!(reputation::pow_bits(&p) >= 8);
    let mut again = p.clone();
    again.pow_nonce = 0;
    reputation::mine(&mut again, 8);
    assert_eq!(again.pow_nonce, p.pow_nonce);
    reputation::sign_with_seed(&mut again, &SEED).unwrap();
    assert_eq!(again.signature, p.signature);
}

#[test]
fn the_digest_of_a_fault_evidence_matches() {
    let v = vectors();
    let e: FaultEvidence = decode(&v["fault_evidence"]);
    assert_eq!(
        reputation::evidence_digest(&e),
        text(&v["fault_evidence"], "digest")
    );
    // Its statement and receipt are signed by the accused: they verify on their own.
    assert!(statement::signature_verifies(&e.statements[0], &e.accused));
    assert!(receipt::verify(&e.receipts[0], Some(&e.accused), None));
}

#[test]
fn the_hash_of_a_service_is_the_sha3_256_of_nodo() {
    let v = vectors();
    let r = &v["refs"];
    assert_eq!(
        hex::encode(service_library::gateway::SHA3_256_TYPE),
        text(r, "sha3_256_id")
    );
    assert_eq!(
        hex::encode(bee_rpc::hash::HashAlgorithm::Sha3_256.digest(b"hello")),
        text(r, "sha3_256_hello")
    );
}
