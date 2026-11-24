"""Generate the test vectors of service-library from the code of nodo.

Run from the nodo checkout, with its interpreter:

    cd /path/to/nodo && PYTHONDONTWRITEBYTECODE=1 venv/bin/python \
        /path/to/service-library/crates/service-library/tests/data/gen_node_vectors.py OUT_DIR

It writes OUT_DIR/node_receipts.json and OUT_DIR/node_vectors.json. Every digest, hash and
signature in them is computed by the digest functions of nodo (``execution_receipt``,
``statement_signature``, ``signed_offer``, ``offchain_proof``, ``fault_proofs``,
``node_identity``, ``ledger``). The keys are fixed seeds, so the output is deterministic:
Ed25519 is deterministic and nothing here reads the clock or a random source.
"""
import json
import os
import sys

sys.path.insert(0, os.getcwd())
from tests.config_bootstrap import load_example_config  # noqa: E402

load_example_config()

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ed25519  # noqa: E402

from protos import celaut_pb2 as pb  # noqa: E402
from src.database import ledger  # noqa: E402
from src.identity import execution_receipt as er  # noqa: E402
from src.identity import node_identity as ni  # noqa: E402
from src.identity import statement_signature as ss  # noqa: E402
from src.reputation_system import fault_proofs, offchain_proof  # noqa: E402
from src.utils import hashing, keyvalue  # noqa: E402
from src.utils.cost_functions import signed_offer  # noqa: E402

OUT = sys.argv[1]
SEED = bytes(range(32))
KEY = ed25519.Ed25519PrivateKey.from_private_bytes(SEED)
PEER_ID = KEY.public_key().public_bytes(
    serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()


def sign(payload: str) -> str:
    return KEY.sign(payload.encode("utf-8")).hex()


def raw(message) -> str:
    return message.SerializeToString().hex()


# -- Receipts ------------------------------------------------------------------------------

# The inputs of the receipts: the fields that a node fills in; the rest is computed below.
CASES = {
 "start_resources_kernel": {
  "kind": "start",
  "execution_name": "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
  "ts": 1798329600,
  "service_id_type": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
  "service_id_value": "63e5b5a4137cfa77cb9d10adae081d0df082a826d8441721460d5933f5800056",
  "envs_digest": "e684ff3022c2a14c62addd0129cd4a5d3dc95b2d13a924ce4fc57aa7e9842a45",
  "envs_salt": "000102030405060708090a0b0c0d0e0f",
  "initial_mu": "12345678",
  "plane": "",
  "session": "",
  "slot": "",
  "seq": 0,
  "in_events": 0,
  "in_bytes": 0,
  "in_head": "",
  "out_events": 0,
  "out_bytes": 0,
  "out_head": "",
  "mu_charged": "",
  "coverage": "",
  "capture_mode": "",
  "degraded_reason": "",
  "cpu_period": 100000,
  "cpu_quota": 200000,
  "mem_limit": 536870912,
  "disk_space": 1073741824,
  "kernel": "6923dd1bc0460082c5d55a831908c24a282860b7f1cd6c2b79cf1bc8857c639c",
  "devices": []
 },
 "start_devices": {
  "kind": "start",
  "execution_name": "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
  "ts": 1798329600,
  "service_id_type": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
  "service_id_value": "63e5b5a4137cfa77cb9d10adae081d0df082a826d8441721460d5933f5800056",
  "envs_digest": "e684ff3022c2a14c62addd0129cd4a5d3dc95b2d13a924ce4fc57aa7e9842a45",
  "envs_salt": "000102030405060708090a0b0c0d0e0f",
  "initial_mu": "12345678",
  "plane": "",
  "session": "",
  "slot": "",
  "seq": 0,
  "in_events": 0,
  "in_bytes": 0,
  "in_head": "",
  "out_events": 0,
  "out_bytes": 0,
  "out_head": "",
  "mu_charged": "",
  "coverage": "",
  "capture_mode": "",
  "degraded_reason": "",
  "cpu_period": 0,
  "cpu_quota": 0,
  "mem_limit": 0,
  "disk_space": 0,
  "kernel": "6923dd1bc0460082c5d55a831908c24a282860b7f1cd6c2b79cf1bc8857c639c",
  "devices": [
   "10de:1ad0",
   "10de:2204"
  ]
 },
 "start_bare": {
  "kind": "start",
  "execution_name": "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
  "ts": 1798329600,
  "service_id_type": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
  "service_id_value": "63e5b5a4137cfa77cb9d10adae081d0df082a826d8441721460d5933f5800056",
  "envs_digest": "e684ff3022c2a14c62addd0129cd4a5d3dc95b2d13a924ce4fc57aa7e9842a45",
  "envs_salt": "000102030405060708090a0b0c0d0e0f",
  "initial_mu": "12345678",
  "plane": "",
  "session": "",
  "slot": "",
  "seq": 0,
  "in_events": 0,
  "in_bytes": 0,
  "in_head": "",
  "out_events": 0,
  "out_bytes": 0,
  "out_head": "",
  "mu_charged": "",
  "coverage": "",
  "capture_mode": "",
  "degraded_reason": "",
  "cpu_period": 0,
  "cpu_quota": 0,
  "mem_limit": 0,
  "disk_space": 0,
  "kernel": "",
  "devices": []
 },
 "checkpoint": {
  "kind": "checkpoint",
  "execution_name": "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
  "ts": 1798329600,
  "service_id_type": "",
  "service_id_value": "",
  "envs_digest": "",
  "envs_salt": "",
  "initial_mu": "",
  "plane": "tunnel",
  "session": "00112233445566778899aabbccddeeff",
  "slot": "8080",
  "seq": 1,
  "in_events": 1,
  "in_bytes": 4,
  "in_head": "4cc36e6c5ce771828a06755610180b0db14686b7b88ec9aca2d94dc67cb82b58",
  "out_events": 2,
  "out_bytes": 5,
  "out_head": "721ba2dbda02e1f8d3f9bf151175163f77ece18af91d85a2357a438d36f2ed85",
  "mu_charged": "7",
  "coverage": "client-tunnel",
  "capture_mode": "",
  "degraded_reason": "",
  "cpu_period": 0,
  "cpu_quota": 0,
  "mem_limit": 0,
  "disk_space": 0,
  "kernel": "",
  "devices": []
 },
 "close": {
  "kind": "close",
  "execution_name": "b08b762433f9c582f3347fbe192e523260278d24533a94e1972fbcddba42158a",
  "ts": 1798329600,
  "service_id_type": "",
  "service_id_value": "",
  "envs_digest": "",
  "envs_salt": "",
  "initial_mu": "",
  "plane": "tunnel",
  "session": "00112233445566778899aabbccddeeff",
  "slot": "8080",
  "seq": 1,
  "in_events": 1,
  "in_bytes": 4,
  "in_head": "4cc36e6c5ce771828a06755610180b0db14686b7b88ec9aca2d94dc67cb82b58",
  "out_events": 2,
  "out_bytes": 5,
  "out_head": "721ba2dbda02e1f8d3f9bf151175163f77ece18af91d85a2357a438d36f2ed85",
  "mu_charged": "7",
  "coverage": "client-tunnel",
  "capture_mode": "",
  "degraded_reason": "",
  "cpu_period": 0,
  "cpu_quota": 0,
  "mem_limit": 0,
  "disk_space": 0,
  "kernel": "",
  "devices": []
 }
}


def receipt_vectors():
    old = CASES
    out = {}
    for name, v in old.items():
        r = pb.ExecutionReceipt(
            kind=v["kind"], execution_name=v["execution_name"], ts=v["ts"],
            envs_digest=v["envs_digest"], envs_salt=bytes.fromhex(v["envs_salt"]),
            plane=v["plane"], session=v["session"], slot=v["slot"], seq=v["seq"],
            in_events=v["in_events"], in_bytes=v["in_bytes"], in_head=v["in_head"],
            out_events=v["out_events"], out_bytes=v["out_bytes"], out_head=v["out_head"],
            coverage=v["coverage"], capture_mode=v["capture_mode"],
            degraded_reason=v["degraded_reason"], cpu_period=v["cpu_period"],
            cpu_quota=v["cpu_quota"], mem_limit=v["mem_limit"], disk_space=v["disk_space"],
            kernel=v["kernel"], devices=v["devices"])
        if v["kind"] == "start":
            r.service_id.type = bytes.fromhex(v["service_id_type"])
            r.service_id.value = bytes.fromhex(v["service_id_value"])
        if v["initial_mu"]:
            r.initial_mu.n = v["initial_mu"]
        if v["mu_charged"]:
            r.mu_charged.n = v["mu_charged"]
        er.sign_with(r, KEY)
        entry = dict(v)
        entry.update(peer_id=r.peer_id, signature=r.signature,
                     canonical=er.canonical_receipt(r), digest=er.receipt_digest(r),
                     payload=er.signed_bytes(r).decode(), bytes=raw(r))
        out[name] = entry
    return out


# -- Statements ----------------------------------------------------------------------------

def chain(account: str, rows):
    """(movements, head_seq, head_hash, balance) of rows (kind, amount, ref, detail)."""
    ref = ledger.account_ref(account)
    prev, balance, movements = ledger.genesis_hash_of_ref(ref), 0, []
    for i, (kind, amount, iref, detail) in enumerate(rows, 1):
        balance += amount
        m = pb.BalanceMovement(
            seq=i, ts=1798329000 + i, kind=kind, instance_ref=iref, prev_hash=prev,
            detail=ledger.canonical_detail(detail) or "")
        m.amount.n = str(amount)
        m.balance_after.n = str(balance)
        movements.append(m)
        prev = ss.movement_hash(m)
    return ref, movements, len(rows), prev, balance


def statement(name, account, rows, **fields):
    ref, movements, head_seq, head_hash, balance = chain(account, rows)
    s = pb.BalanceStatement(ts=1798329600, account_ref=ref, from_seq=1, to_seq=head_seq,
                            head_seq=head_seq, head_hash=head_hash, **fields)
    s.movements.extend(movements)
    s.balance.n = str(balance)
    digest = ss.statement_digest(s)
    s.signature = sign(ni.statement_payload(PEER_ID, digest))
    assert ss.verify(s, PEER_ID)
    return {
        "name": name, "account": account, "bytes": raw(s), "digest": digest,
        "payload": ni.statement_payload(PEER_ID, digest), "signature": s.signature,
        "peer_id": PEER_ID, "account_ref": ref,
        "genesis_hash": ledger.genesis_hash(account),
        "movement_hashes": [ss.movement_hash(m) for m in s.movements],
    }


def statements():
    iref = ledger.instance_ref("child-token")
    rows = [
        ("start", 1000, "", {"execution_name": "ab" * 32}),
        ("maintenance", -10, "", {"seconds": "60"}),
        ("child_start", -100, iref, {"actor": "instance"}),
        ("child_bound", 0, iref, {"starts_seq": "3", "service_id": "cd" * 32}),
        ("refund", -890, "", {"refunds_seq": "1"}),
        ("stop", 0, "", {"reason": "stopped", "leftover": "0"}),
    ]
    return [
        statement("plain", "client-1", rows),
        statement("relayed_with_service_and_ref", "client-1", rows, relayed=True,
                  service_id="cd" * 32, instance_ref=iref, oldest_seq=1, retention_days=30),
        statement("empty_account", "client-2", []),
    ]


# -- Offers --------------------------------------------------------------------------------

def offer(name, valid_until=1798333200, volatility=0.5, bounded=True, scarcity=None,
          on_close=pb.ON_CLOSE_STOP, closes_at=1798340000, sign_it=True):
    o = pb.EstimatedCost(maintenance_seconds_loop=60, price_bounded=bounded,
                         price_volatility=volatility, volatility_period_seconds=3600,
                         valid_until=valid_until, closes_at=closes_at, on_close=on_close)
    o.cost.n = "100"
    o.init_maintenance_cost.n = "7"
    o.max_maintenance_cost.n = "9"
    for key, value in (scarcity if scarcity is not None else
                       (("ram", 250000), ("cpu", 1), ("disk", 1000000), ("mem", 0))):
        o.launch_scarcity.add(key=key, value=value)
    digest = signed_offer.offer_digest(o)
    if sign_it:
        o.signature = sign(ni.offer_payload(PEER_ID, digest))
        assert signed_offer.verify(o, PEER_ID)
    return {"name": name, "bytes": raw(o), "digest": digest, "peer_id": PEER_ID,
            "payload": ni.offer_payload(PEER_ID, digest), "signature": o.signature,
            "valid_until": valid_until, "committed_until": signed_offer.committed_until(o, PEER_ID)}


def offers():
    return [
        offer("signed"),
        offer("signed_volatility_tenth", volatility=0.1, scarcity=(("cpu", 5), ("mem", 6), ("disk", 7))),
        offer("unbounded_no_close", bounded=False, volatility=0.0, closes_at=0,
              on_close=pb.ON_CLOSE_UNSPECIFIED if hasattr(pb, "ON_CLOSE_UNSPECIFIED") else 0),
        offer("no_promise", valid_until=0, scarcity=(), sign_it=False),
    ]


# -- Peers ---------------------------------------------------------------------------------

def peer_vector(name, peer, signed=True):
    peer.public_key = PEER_ID
    peer.ts = 1798329600
    digest = ni.canonical_peer_content_digest(peer)
    payload = ni.canonical_peer_payload(PEER_ID, peer.ts, digest)
    peer.signature = sign(payload) if signed else ""
    return {"name": name, "bytes": raw(peer), "digest": digest, "payload": payload,
            "signature": peer.signature, "peer_id": PEER_ID}


def full_peer():
    peer = pb.Peer()
    peer.signature_scheme.CopyFrom(er.receipt_scheme())
    for tags, prose, formal in ((["tcp"], "TCP", b""), (["tls"], "", b"\x01")):
        pass
    uri = peer.uri.add(ip="1.2.3.4", port=8080, expiry_unix_timestamp=1800000000)
    uri.transport.tags.append("tcp")
    uri.protocol_stack.add(tags=["tls", "h2"], prose="TLS", formal=b"\x01")
    uri.protocol_stack.add(tags=["bee"], prose="", formal=b"")
    uri2 = peer.uri.add(ip="abc.onion", port=80, expiry_unix_timestamp=1800000001)
    uri2.transport.formal = b"\x09"
    uri2.transport.tags.extend(["tor", "onion"])
    contract = peer.payment_contracts.add()
    contract.contract.ledger.tags.extend(["ergo", "erg"])
    contract.contract.ledger.formal = b"net=main"
    contract.contract.ledger.prose = "Ergo"
    contract.contract.protocol.tags.append("p2pk")
    contract.contract.xattrs.add(key="addr", value=b"\x01\x02")
    contract.contract.xattrs.add(key="empty")
    contract.mu_per_unit.n = "12345"
    keyvalue.set_value(peer.rates, "build_mu", pb.Amount(n="10"))
    keyvalue.set_value(peer.rates, "statement_query_mu", pb.Amount(n="3"))
    keyvalue.set_value(peer.limits, "max_concurrent_connections:*", 5)
    peer.limits.add(key="zero", value=0)
    peer.limits.add(key="unset")
    res = peer.resources.add()
    res.architecture.tags.append("linux/amd64")
    res.resources.cpu_period = 100000
    res.resources.mem_limit = 1 << 30
    res.resources.cpu_quota = 0
    res.resources.benchmark.add(key="sha256_hashes_per_sec", value=1234567)
    res.resources.benchmark.add(key="unset")
    res2 = peer.resources.add()
    res2.architecture.tags.append("linux/arm64")
    dev = peer.devices.add()
    dev.architecture.tags.append("nvidia")
    dev.resources.at_init.mem_limit = 4096
    dev.resources.at_most.disk_space = 1
    dev.attach.extend([pb.Device.ATTACH_ON_REQUEST, pb.Device.ATTACH_AT_START])
    dev2 = peer.devices.add()
    dev2.architecture.tags.append("amd")
    dev2.resources.at_init.SetInParent()
    peer.activity_window.on_close = pb.ON_CLOSE_STOP
    peer.activity_window.spans.add(start_minute=1320, end_minute=360)
    peer.activity_window.spans.add(start_minute=60, end_minute=120)
    return peer


def peers():
    bare = pb.Peer()
    bare.signature_scheme.CopyFrom(er.receipt_scheme())
    empty_window = pb.Peer()
    empty_window.signature_scheme.CopyFrom(er.receipt_scheme())
    empty_window.activity_window.SetInParent()
    return [peer_vector("full", full_peer()), peer_vector("bare", bare),
            peer_vector("empty_window", empty_window)]


# -- Reputation proofs ---------------------------------------------------------------------

def proof_vector(name, proof, bits=0):
    proof.publisher = PEER_ID
    proof.signature_scheme.CopyFrom(er.receipt_scheme())
    proof.ts = 1798329600
    if bits:
        assert offchain_proof.mine(proof, bits)
    digest = offchain_proof.proof_digest(proof)
    proof.signature = sign(ni.reputation_proof_payload(PEER_ID, digest))
    assert offchain_proof.verify(proof)
    return {"name": name, "bytes": raw(proof), "base": offchain_proof.pow_base(proof),
            "digest": digest, "pow_bits": offchain_proof.pow_bits(proof),
            "pow_nonce": proof.pow_nonce, "peer_id": PEER_ID,
            "payload": ni.reputation_proof_payload(PEER_ID, digest),
            "signature": proof.signature}


def evidence():
    ev = pb.FaultEvidence(reason="killed_with_mu", accused=PEER_ID)
    ref, movements, head_seq, head_hash, balance = chain("acct", [
        ("start", 10, "", None), ("refund", -5, "", {"lost": "father_gone"})])
    s = ev.statements.add(ts=1798329600, account_ref=ref, from_seq=1, to_seq=2,
                          head_seq=head_seq, head_hash=head_hash)
    s.movements.extend(movements)
    s.balance.n = str(balance)
    s.signature = sign(ni.statement_payload(PEER_ID, ss.statement_digest(s)))
    r = ev.receipts.add(kind="checkpoint", execution_name="ee" * 32, ts=1798329700,
                        plane="tunnel", session="ff" * 8, slot="80", seq=1, in_events=1,
                        in_bytes=2, in_head="11" * 32, out_events=3, out_bytes=4,
                        out_head="22" * 32, coverage="client-tunnel")
    r.mu_charged.n = "6"
    er.sign_with(r, KEY)
    return ev


def proofs():
    simple = pb.ReputationProof()
    simple.opinions.add(type="celaut-node", object="cd" * 32, amount=40, positive=True,
                        content=b"hi")
    simple.opinions.add(type="celaut-node", object="ab" * 32, amount=0, positive=False)
    full = pb.ReputationProof()
    full.opinions.add(type="celaut-node", object="cd" * 32, amount=40, positive=True,
                      content=b"hi")
    ev = evidence()
    full.opinions.add(type="celaut-node", object=PEER_ID, amount=0, positive=False,
                      content=b'{"reason":"killed_with_mu"}', evidence=ev)
    p2 = full.ledger_proofs.add()
    p2.ledger.tags.append("ergo")
    p2.ledger.formal = b"x"
    p2.protocol.tags.append("p2pk")
    p2.xattrs.add(key="attestation", value=b"\xaa")
    p1 = full.ledger_proofs.add()
    p1.ledger.tags.append("bitcoin")
    out = [proof_vector("simple", simple), proof_vector("with_evidence_and_pointers", full),
           proof_vector("mined_8_bits", _mined())]
    for vec in out:
        pass
    return out, ev


def _mined():
    proof = pb.ReputationProof()
    proof.opinions.add(type="celaut-node", object="cd" * 32, amount=1, positive=True)
    return proof


# -- Hashes and references -----------------------------------------------------------------

def refs():
    events_in = [b"hello", b"world!"]
    events_out = [b"", b"\x00\x01"]
    count_in, bytes_in, head_in = er.replay_chain(events_in, "in")
    count_out, bytes_out, head_out = er.replay_chain(events_out, "out")
    envs = {"B": b"two", "A": b"\x00\xffone", "é": "café".encode()}
    origins = {"B": "node"}
    salt = bytes(range(16))
    token = "tok-é-123"
    return {
        "peer_id": PEER_ID,
        "instance_ref": ledger.instance_ref(token),
        "account_ref": ledger.account_ref(token),
        "genesis_hash": ledger.genesis_hash(token),
        "token": token,
        "execution_name": er.execution_name(token, PEER_ID),
        "execution_name_by_ref": er.execution_name_by_ref(ledger.instance_ref(token), PEER_ID),
        "movement_hash": ledger.movement_hash("aa" * 32, 7, 1798329600, -15, 985, "tunnel",
                                              "bb" * 32, '{"open":true}'),
        "movement_hash_empty": ledger.movement_hash("aa" * 32, 1, 5, 0, 0, "stop", "", None),
        "envs": {k: v.hex() for k, v in envs.items()},
        "origins": origins,
        "salt": salt.hex(),
        "canonical_envs": er.canonical_envs(envs, origins),
        "envs_digest": er.envs_digest(envs, salt, origins),
        "payload_digest": er.payload_digest(b"hello"),
        "chain_in": {"events": [e.hex() for e in events_in], "count": count_in,
                     "bytes": bytes_in, "head": head_in},
        "chain_out": {"events": [e.hex() for e in events_out], "count": count_out,
                      "bytes": bytes_out, "head": head_out},
        "scheme": er.canonical_scheme(er.receipt_scheme()),
        # The id of a service is the SHA3-256 of its bytes, with this type id (src/utils/hashing.py).
        "sha3_256_id": hashing.SHA3_256_ID.hex(),
        "sha3_256_hello": hashing.SHA3_256(b"hello").hex(),
    }


def main():
    proof_list, ev = proofs()
    # The mined proof needs its nonce before it is signed.
    mined = pb.ReputationProof()
    mined.opinions.add(type="celaut-node", object="cd" * 32, amount=1, positive=True)
    proof_list[2] = proof_vector("mined_8_bits", mined, bits=8)
    vectors = {
        "seed": SEED.hex(), "peer_id": PEER_ID, "statements": statements(),
        "offers": offers(), "peers": peers(), "proofs": proof_list,
        "fault_evidence": {"bytes": raw(ev), "digest": fault_proofs.digest(ev)},
        "refs": refs(),
    }
    with open(os.path.join(OUT, "node_vectors.json"), "w") as f:
        json.dump(vectors, f, indent=1, sort_keys=True)
        f.write("\n")
    with open(os.path.join(OUT, "node_receipts.json"), "w") as f:
        json.dump(receipt_vectors(), f, indent=1)
        f.write("\n")


main()
