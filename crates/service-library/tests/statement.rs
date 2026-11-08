//! Balance statements with `instance_ref` (nodo#631).
//!
//! The vectors are in chatui, `docs/celaut/descendants.md`, section 10. The harness could not run
//! the Python of nodo, so a script that follows `ledger.py` and `balance_statement.py`
//! computed them, and nothing here was checked against the output of a real node.

use celaut_proto::celaut::{
    Amount, AmountKeyValue, BalanceMovement, BalanceStatement, BalanceStatementRequest, Peer,
};
use service_library::statement::{
    self, account_ref, genesis_hash, instance_ref, movement_hash, signature_verifies, signed_text,
    statement_digest, KeptHead, Kind, Rates, StatementError, StatementRequest,
};

const SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];
const PEER_ID: &str = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
const CHILD: &str = "407e5db0b61e39e7eeeef075270dbf0d1f60f8cb6f9a56493fca5a6f464cfa61";
const ACCOUNT: &str = "707f5710da5b6e024393aa806427f0331905f9fc894d205a365c0b79bf3fc481";
const GENESIS: &str = "4e3977840f3f19220cb66feb8cde682426b9a59759d5bf3f5136b8e9b712774a";
const HASHES: [&str; 4] = [
    "d8ea596e8c3a1c055558447ac715cf0561a698ee63e3e894fcaa542e468b7552",
    "8d3fa7af31d646e24ea3a56fe6cf3ac4b5f802cb6128851dba405771b9080716",
    "ae6ca76d104952b3005d25afbaa3e6a766b444137185d5786844d4be6cda7b1e",
    "d1826e9d6c4b19f8e535061e7e9f01351adcef405318f212d0dbcc2b7515ebbf",
];
const DIGEST_NONE: &str = "1fe4d386b30d8a32a6728b8e838d0adbaf90c9a83b95543ac6f7b6815591ead4";
const DIGEST_FULL: &str = "ba57e06097700132860ca253f405a2436a45642bbdae479cff2a8ecf8f258d30";
const DIGEST_FULL_RELAYED: &str =
    "2cc1dce4e4304f8902c3a2c03492ce357dcb77be34a184bca665f1dfc1b7f351";
const DIGEST_RELAYED: &str = "33a235c568d0c346910a0e7044dea9733563e2ce561908eca84e90c3abddb9bb";
const NOW: i64 = 1700001001;
const SIGNATURE: &str = "a7af781d441f40e2ef248a39958d7bd1c363c934c1dcb254350060a8850c0906c374dfa991e833cf7d44159b1ee7be0c1b982a0291d8bd46779bd68efc77de0d";

fn service() -> String {
    "cd".repeat(32)
}

fn amount(n: &str) -> Option<Amount> {
    Some(Amount { n: n.into() })
}

fn movement(
    seq: u64,
    ts: i64,
    amt: &str,
    bal: &str,
    kind: &str,
    iref: &str,
    detail: &str,
) -> BalanceMovement {
    BalanceMovement {
        seq,
        ts,
        amount: amount(amt),
        balance_after: amount(bal),
        kind: kind.into(),
        instance_ref: iref.into(),
        detail: detail.into(),
        prev_hash: String::new(),
    }
}

/// The four movements of the vectors, chained from the genesis hash. They are for the
/// hash tests only: the first one does not start from 0, so the chain check refuses it.
fn vector_movements() -> Vec<BalanceMovement> {
    let mut list = vec![
        movement(
            1,
            1700000000,
            "-5000",
            "995000",
            "child_start",
            "",
            r#"{"start_ref":"ab"}"#,
        ),
        movement(
            2,
            1700000001,
            "0",
            "995000",
            "child_bound",
            CHILD,
            &format!(r#"{{"service_id":"{}","starts_seq":"1"}}"#, service()),
        ),
        movement(
            3,
            1700000900,
            "-30",
            "994970",
            "statement_query",
            "",
            r#"{"from_seq":"0","rows":"0","to_seq":"0"}"#,
        ),
        movement(
            4,
            1700001000,
            "0",
            "994970",
            "stop",
            "",
            r#"{"leftover":"0","reason":"stopped"}"#,
        ),
    ];
    let mut prev = GENESIS.to_string();
    for m in &mut list {
        m.prev_hash = prev;
        prev = movement_hash(m);
    }
    list
}

/// The statement of the vectors, with `service_id` and `instance_ref`, not signed. For the
/// hash tests only (see `vector_movements`).
fn vector_unsigned() -> BalanceStatement {
    BalanceStatement {
        account_ref: ACCOUNT.into(),
        from_seq: 1,
        to_seq: 4,
        movements: vector_movements(),
        balance: amount("994970"),
        head_seq: 4,
        head_hash: HASHES[3].into(),
        oldest_seq: 1,
        retention_days: 0,
        ts: 1700001001,
        signature: String::new(),
        relayed: false,
        service_id: service(),
        instance_ref: CHILD.into(),
    }
}

/// The movements of a valid chain: as the vectors, but the first one is a `credit` that
/// starts from 0, so that the chain check accepts it. Chained from the genesis hash.
fn movements() -> Vec<BalanceMovement> {
    let mut list = vector_movements();
    list[0] = movement(1, 1700000000, "995000", "995000", "credit", "", "");
    rechain(&mut list);
    list
}

fn rechain(list: &mut [BalanceMovement]) {
    let mut prev = GENESIS.to_string();
    for m in list.iter_mut() {
        m.prev_hash = prev;
        prev = movement_hash(m);
    }
}

/// The hash of movement `i` (0 to 3) of the valid chain.
fn h(i: usize) -> String {
    movement_hash(&movements()[i])
}

/// A statement that passes every check, not signed.
fn unsigned() -> BalanceStatement {
    BalanceStatement {
        movements: movements(),
        head_hash: h(3),
        ..vector_unsigned()
    }
}

fn signed(mut s: BalanceStatement) -> BalanceStatement {
    assert_eq!(statement::sign_with_seed(&mut s, &SEED).unwrap(), PEER_ID);
    s
}

fn request() -> StatementRequest {
    StatementRequest {
        token: "parent-token-1".into(),
        instance_ref: CHILD.into(),
        from_seq: 1,
        to_seq: 4,
        remaining_hops: 16,
    }
}

fn invalid(result: Result<statement::VerifiedStatement, StatementError>) -> String {
    match result {
        Err(StatementError::Invalid(text)) => text,
        other => panic!("expected Invalid, got {other:?}"),
    }
}

#[test]
fn the_hashes_match_the_vectors() {
    assert_eq!(instance_ref("child-token-1"), CHILD);
    assert_eq!(account_ref("parent-token-1"), ACCOUNT);
    assert_eq!(genesis_hash(ACCOUNT), GENESIS);
    let hashes: Vec<String> = vector_movements().iter().map(movement_hash).collect();
    assert_eq!(hashes, HASHES);
}

#[test]
fn the_digests_match_the_vectors() {
    let mut s = vector_unsigned();
    s.service_id.clear();
    s.instance_ref.clear();
    assert_eq!(statement_digest(&s), DIGEST_NONE);
    s.relayed = true;
    assert_eq!(statement_digest(&s), DIGEST_RELAYED);
    let mut s = vector_unsigned();
    assert_eq!(statement_digest(&s), DIGEST_FULL);
    s.relayed = true;
    assert_eq!(statement_digest(&s), DIGEST_FULL_RELAYED);
}

#[test]
fn the_signature_matches_the_vector() {
    assert_eq!(
        signed_text(PEER_ID, DIGEST_FULL),
        format!("celaut-balance-statement:{PEER_ID}|{DIGEST_FULL}")
    );
    let s = signed(vector_unsigned());
    assert_eq!(s.signature, SIGNATURE);
    assert!(signature_verifies(&s, PEER_ID));
}

#[test]
fn a_good_statement_is_verified_and_typed() {
    let v = statement::verify(&request(), PEER_ID, signed(unsigned()), None, NOW).unwrap();
    assert_eq!(v.movements.len(), 4);
    assert_eq!(v.balance, 994970);
    assert!(!v.relayed);
    assert_eq!(v.service_id, service());
    assert_eq!(v.head, KeptHead { seq: 4, hash: h(3) });
    assert_eq!(v.next_from_seq, None);
    assert_eq!(v.movements[0].amount, 995000);
    assert_eq!(v.movements[0].kind, Kind::Credit);
    assert_eq!(v.movements[2].kind, Kind::StatementQuery);
    let children: Vec<_> = v.children().collect();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].0, CHILD);
    assert_eq!(children[0].1.starts_seq, Some(1));
    assert_eq!(
        children[0].1.service_id.as_deref(),
        Some(service().as_str())
    );
    let stop = v.stop().unwrap();
    assert_eq!(
        (stop.reason.as_str(), stop.leftover, stop.tick.as_deref()),
        ("stopped", 0, None)
    );
}

#[test]
fn a_relayed_statement_shows_relayed() {
    let mut s = unsigned();
    s.relayed = true;
    let v = statement::verify(&request(), PEER_ID, signed(s), None, NOW).unwrap();
    assert!(v.relayed);
}

#[test]
fn a_stop_with_a_tick_and_a_leftover_is_parsed() {
    let mut s = unsigned();
    s.movements[3].detail = r#"{"leftover":"12","reason":"out_of_balance","tick":"3"}"#.into();
    let mut prev = String::new();
    for m in &mut s.movements {
        if !prev.is_empty() {
            m.prev_hash = prev.clone();
        }
        prev = movement_hash(m);
    }
    s.head_hash = prev;
    let v = statement::verify(&request(), PEER_ID, signed(s), None, NOW).unwrap();
    let stop = v.stop().unwrap();
    assert_eq!(stop.reason, "out_of_balance");
    assert_eq!(stop.leftover, 12);
    assert_eq!(stop.tick.as_deref(), Some("3"));
}

#[test]
fn a_kind_that_is_not_known_is_kept() {
    let mut s = unsigned();
    s.movements[2].kind = "something_new".into();
    let mut prev = String::new();
    for m in &mut s.movements {
        if !prev.is_empty() {
            m.prev_hash = prev.clone();
        }
        prev = movement_hash(m);
    }
    s.head_hash = prev;
    let v = statement::verify(&request(), PEER_ID, signed(s), None, NOW).unwrap();
    assert_eq!(v.movements[2].kind, Kind::Unknown("something_new".into()));
}

// ─── Tampered answers ─────────────────────────────────────────────────────

#[test]
fn a_bad_signature_is_refused() {
    let mut s = signed(unsigned());
    s.signature.replace_range(0..1, "b");
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
    // No signature.
    let mut s = signed(unsigned());
    s.signature.clear();
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("no signature"));
    // Upper case is another spelling.
    let mut s = signed(unsigned());
    s.signature = s.signature.to_uppercase();
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
    // Signed by another node than the node of the connection.
    let other = "00".repeat(32);
    let s = signed(unsigned());
    assert!(invalid(statement::verify(&request(), &other, s, None, NOW)).contains("signature"));
}

#[test]
fn another_instance_ref_is_refused() {
    let mut s = unsigned();
    s.instance_ref = instance_ref("another-child");
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("instance_ref"));
}

#[test]
fn an_answer_with_no_instance_ref_means_that_the_node_ignores_the_field() {
    let mut s = unsigned();
    s.instance_ref.clear();
    s.service_id.clear();
    let s = signed(s);
    let result = statement::verify(&request(), PEER_ID, s, None, NOW);
    assert!(
        matches!(result, Err(StatementError::NoDescendants)),
        "{result:?}"
    );
    let e = result.unwrap_err();
    assert_eq!(e.to_string(), "node does not support descendants");
    assert!(e.may_have_charged());
}

#[test]
fn an_instance_ref_in_the_answer_to_a_read_without_one_is_refused() {
    let mut r = request();
    r.instance_ref.clear();
    let s = signed(unsigned());
    assert!(invalid(statement::verify(&r, PEER_ID, s, None, NOW)).contains("instance_ref"));
}

#[test]
fn a_changed_instance_ref_after_the_signature_is_refused() {
    let mut s = signed(unsigned());
    s.instance_ref = instance_ref("another-child");
    let mut r = request();
    r.instance_ref = s.instance_ref.clone();
    assert!(invalid(statement::verify(&r, PEER_ID, s, None, NOW)).contains("signature"));
}

#[test]
fn a_flipped_relayed_breaks_the_signature() {
    let mut s = signed(unsigned());
    s.relayed = true;
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
    let mut s = unsigned();
    s.relayed = true;
    let mut s = signed(s);
    s.relayed = false;
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
}

#[test]
fn a_changed_service_id_breaks_the_signature() {
    let mut s = signed(unsigned());
    s.service_id = "ab".repeat(32);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
}

#[test]
fn a_changed_movement_breaks_the_signature() {
    let mut s = signed(unsigned());
    s.movements[0].amount = amount("-1");
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
}

#[test]
fn a_broken_chain_that_is_signed_is_refused() {
    // The node signs a statement whose movement 3 does not hang from movement 2.
    let mut s = unsigned();
    s.movements[2].prev_hash = h(0);
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("hang"));
    // A gap in the seq.
    let mut s = unsigned();
    s.movements.remove(2);
    let s = signed(s);
    assert!(
        invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("does not follow")
    );
    // The first movement of the account does not hang from the genesis hash.
    let mut s = unsigned();
    s.movements[0].prev_hash = h(3);
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("hang"));
    // The head is not the hash of the last movement.
    let mut s = unsigned();
    s.head_hash = h(2);
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("head_hash"));
    // The balance is not the balance after the last movement.
    let mut s = unsigned();
    s.balance = amount("1");
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("balance"));
}

#[test]
fn a_movement_after_a_stop_is_refused() {
    let mut s = unsigned();
    let mut extra = movement(5, 1700001001, "-1", "994969", "statement_query", "", "");
    extra.prev_hash = h(3);
    s.movements.push(extra);
    s.head_seq = 5;
    s.head_hash = movement_hash(&s.movements[4]);
    s.balance = amount("994969");
    let mut r = request();
    r.to_seq = 0;
    s.to_seq = 0;
    let s = signed(s);
    assert!(invalid(statement::verify(&r, PEER_ID, s, None, NOW)).contains("after a stop"));
}

#[test]
fn a_child_bound_that_moves_mu_or_has_no_ref_is_refused() {
    let rehash = |s: &mut BalanceStatement| {
        let mut prev = String::new();
        for m in &mut s.movements {
            if !prev.is_empty() {
                m.prev_hash = prev.clone();
            }
            prev = movement_hash(m);
        }
        s.head_hash = prev;
    };
    let mut s = unsigned();
    s.movements[1].amount = amount("-1");
    rehash(&mut s);
    assert!(
        invalid(statement::verify(&request(), PEER_ID, signed(s), None, NOW)).contains("moves MU")
    );
    let mut s = unsigned();
    s.movements[1].instance_ref.clear();
    rehash(&mut s);
    assert!(
        invalid(statement::verify(&request(), PEER_ID, signed(s), None, NOW))
            .contains("instance_ref")
    );
    let mut s = unsigned();
    s.movements[1].detail = "not json".into();
    rehash(&mut s);
    assert!(invalid(statement::verify(&request(), PEER_ID, signed(s), None, NOW)).contains("JSON"));
}

#[test]
fn the_range_of_the_answer_must_be_the_range_of_the_request() {
    let mut s = unsigned();
    s.from_seq = 2;
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("range"));
    let mut s = unsigned();
    s.to_seq = 3;
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("range"));
}

#[test]
fn a_truncated_answer_is_refused() {
    // Movements dropped after the signature.
    let mut s = signed(unsigned());
    s.movements.truncate(2);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("signature"));
    // An answer with no movement for a range that the head covers, even when signed.
    let mut s = unsigned();
    s.movements.clear();
    let s = signed(s);
    assert!(invalid(statement::verify(&request(), PEER_ID, s, None, NOW)).contains("no movement"));
}

#[test]
fn fewer_movements_than_the_range_is_paging_and_gives_the_next_seq() {
    // The node gives the first 2 rows of 1..4 and signs the head.
    let mut s = unsigned();
    s.movements.truncate(2);
    let s = signed(s);
    let v = statement::verify(&request(), PEER_ID, s, None, NOW).unwrap();
    assert_eq!(v.movements.len(), 2);
    assert_eq!(v.next_from_seq, Some(3));
}

#[test]
fn a_head_that_goes_back_or_changes_is_refused() {
    let s = signed(unsigned());
    let ok = KeptHead { seq: 2, hash: h(1) };
    assert!(statement::verify(&request(), PEER_ID, s.clone(), Some(&ok), NOW).is_ok());
    let back = KeptHead {
        seq: 9,
        hash: "11".repeat(32),
    };
    assert!(invalid(statement::verify(
        &request(),
        PEER_ID,
        s.clone(),
        Some(&back),
        NOW
    ))
    .contains("below"));
    let other = KeptHead {
        seq: 4,
        hash: "11".repeat(32),
    };
    assert!(invalid(statement::verify(
        &request(),
        PEER_ID,
        s.clone(),
        Some(&other),
        NOW
    ))
    .contains("another hash"));
    let other = KeptHead {
        seq: 3,
        hash: "11".repeat(32),
    };
    assert!(invalid(statement::verify(&request(), PEER_ID, s, Some(&other), NOW)).contains("3"));
}

#[test]
fn a_head_only_read_checks_the_genesis_of_an_empty_account() {
    let mut r = request();
    r.from_seq = 0;
    r.to_seq = 0;
    let mut s = unsigned();
    s.from_seq = 0;
    s.to_seq = 0;
    s.movements.clear();
    s.head_seq = 0;
    s.head_hash = GENESIS.into();
    s.balance = amount("0");
    assert!(statement::verify(&r, PEER_ID, signed(s.clone()), None, NOW).is_ok());
    s.head_hash = h(0);
    assert!(invalid(statement::verify(&r, PEER_ID, signed(s), None, NOW)).contains("genesis"));
    // A read for the head that comes with movements.
    let s = signed({
        let mut s = unsigned();
        s.from_seq = 0;
        s.to_seq = 0;
        s
    });
    assert!(invalid(statement::verify(&r, PEER_ID, s, None, NOW)).contains("asked for none"));
}

// ─── The request and its cost ─────────────────────────────────────────────

fn peer(entries: &[(&str, &str)]) -> Peer {
    Peer {
        mu_per_call: entries
            .iter()
            .map(|(k, v)| AmountKeyValue {
                key: (*k).into(),
                value: amount(v),
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn the_rates_come_from_the_peer() {
    let p = peer(&[
        ("statement_query_mu", "100"),
        ("statement_row_mu", "1"),
        ("build_mu", "5"),
    ]);
    assert_eq!(
        Rates::from_peer(&p),
        Some(Rates {
            query_mu: 100,
            row_mu: 1
        })
    );
    assert_eq!(
        Rates::from_peer(&peer(&[("statement_query_mu", "100")])),
        None
    );
    assert_eq!(
        Rates::from_peer(&peer(&[
            ("statement_query_mu", "x"),
            ("statement_row_mu", "1")
        ])),
        None
    );
    assert_eq!(Rates::from_peer(&Peer::default()), None);
}

#[test]
fn the_maximum_cost_follows_the_doc() {
    let rates = Rates {
        query_mu: 100,
        row_mu: 1,
    };
    // 4 rows, 16 hops: 15 reads of 104.
    assert_eq!(request().max_mu_needed(&rates), 15 * 104);
    // A head only read has no row.
    let mut r = request();
    r.from_seq = 0;
    r.to_seq = 0;
    assert_eq!(r.max_mu_needed(&rates), 15 * 100);
    // 1 and 2 hops are one read.
    r.remaining_hops = 1;
    assert_eq!(r.max_mu_needed(&rates), 100);
    r.remaining_hops = 2;
    assert_eq!(r.max_mu_needed(&rates), 100);
    r.remaining_hops = 3;
    assert_eq!(r.max_mu_needed(&rates), 200);
    // An open range is the limit of the node.
    let mut r = request();
    r.to_seq = 0;
    r.remaining_hops = 2;
    assert_eq!(r.max_mu_needed(&rates), 100 + 1000);
    // A range above the limit is the limit.
    r.from_seq = 1;
    r.to_seq = 5000;
    assert_eq!(r.max_mu_needed(&rates), 100 + 1000);
}

#[test]
fn the_request_is_built_with_all_the_fields() {
    let m = request().to_proto();
    assert_eq!(
        m,
        BalanceStatementRequest {
            token: "parent-token-1".into(),
            from_seq: 1,
            to_seq: 4,
            instance_ref: CHILD.into(),
            remaining_hops: Some(16),
        }
    );
    let mut bad = request();
    bad.instance_ref = "not a ref".into();
    assert!(matches!(bad.check(), Err(StatementError::BadRequest(_))));
    let mut bad = request();
    bad.remaining_hops = 0;
    assert!(matches!(bad.check(), Err(StatementError::BadRequest(_))));
    let mut bad = request();
    bad.from_seq = 5;
    bad.to_seq = 2;
    assert!(matches!(bad.check(), Err(StatementError::BadRequest(_))));
}
