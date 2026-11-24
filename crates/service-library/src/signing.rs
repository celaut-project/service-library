//! The one Ed25519 check of every signed text of a node (`node_identity.verify_peer_payload`).
//!
//! A node signs the UTF-8 bytes of a text, as given (RFC 8032, pure, no context, no
//! pre-hash). The key is the `peer_id`: 64 lowercase hex digits, and the signature is 128.
//! A second spelling of the key or of the signature is never accepted: it would be a second
//! identity, or a second proof, for one node.

use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

use crate::receipt::{is_hex, normalize_key};

/// Whether `signature` (128 lowercase hex digits) is the signature of `peer_id` (64
/// lowercase hex digits) over exactly `text`.
pub fn verifies(peer_id: &str, text: &str, signature: &str) -> bool {
    if normalize_key(peer_id).as_deref() != Some(peer_id) || !is_hex(signature, Some(128)) {
        return false;
    }
    let (Ok(key), Ok(signature)) = (hex::decode(peer_id), hex::decode(signature)) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key)
        .verify(text.as_bytes(), &signature)
        .is_ok()
}

/// Sign `text` with the Ed25519 key of `seed`, as a node does: `(peer_id, signature)`, both
/// in lowercase hex. A service never signs for a node: this is for tests and tools.
pub fn sign(seed: &[u8; 32], text: &str) -> Result<(String, String), String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(seed).map_err(|e| e.to_string())?;
    let peer_id = hex::encode(pair.public_key().as_ref());
    Ok((peer_id, hex::encode(pair.sign(text.as_bytes()).as_ref())))
}
