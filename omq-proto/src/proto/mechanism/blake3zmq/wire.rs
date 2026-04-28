//! BLAKE3ZMQ wire-format constants + ZMTP-command-body helpers.
//!
//! ZMTP command frames carry a body of `name_len(1) || name || payload`.
//! The mechanism state machine emits `(name, payload)` pairs; the helpers
//! here pack and unpack those into the body bytes that sit inside a
//! ZMTP command frame.

use crate::error::{Error, Result};

/// KDF context-string prefix per RFC §5.
pub const PROTOCOL_ID: &str = "BLAKE3ZMQ-1.0";

/// HELLO body without the `0x05 HELLO` name prefix:
/// `version(2) || C'(32) || padding(96) || hello_box(96)`. The padding
/// grew from 64 to 96 bytes when the cookie expanded to 152 bytes, to
/// preserve `HELLO ≥ WELCOME` (RFC §9.1 anti-amplification).
pub const HELLO_PAYLOAD_LEN: usize = 2 + 32 + 96 + 96;
/// WELCOME body without the `0x07 WELCOME` name prefix:
/// `welcome_box(216)` = `S'(32) || cookie(152) || tag(32)`.
pub const WELCOME_PAYLOAD_LEN: usize = 32 + 152 + 32;
/// Cookie inside WELCOME:
/// `nonce(24) || Encrypt(C'(32) || s'(32) || h1(32))(96) || tag(32)` = 152 B.
pub const COOKIE_LEN: usize = 24 + 32 + 32 + 32 + 32;
/// Cookie plaintext size: `C' || s' || h1` = 96 bytes.
pub const COOKIE_PLAIN_LEN: usize = 32 + 32 + 32;
/// Vouch box: `Encrypt(C'(32) || S(32))` produces 32+32+tag(32) = 96 bytes.
pub const VOUCH_BOX_LEN: usize = 32 + 32 + 32;
/// AEAD tag size (chacha20-blake3).
pub const TAG_LEN: usize = 32;
/// X25519 key size.
pub const KEY_LEN: usize = 32;
/// AEAD nonce size.
pub const NONCE_LEN: usize = 24;

/// Encode a command body: `name_len || name || payload`. Used to build
/// the body that goes inside a ZMTP command frame for the
/// HELLO/WELCOME/INITIATE/READY/ERROR exchanges.
pub fn encode_command_body(name: &str, payload: &[u8]) -> Vec<u8> {
    assert!(name.len() <= u8::MAX as usize, "command name too long");
    let mut out = Vec::with_capacity(1 + name.len() + payload.len());
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(payload);
    out
}

/// Parse a command body into `(name, payload)`. Errors if the body is
/// truncated or the name is non-ASCII.
pub fn parse_command_body(body: &[u8]) -> Result<(&str, &[u8])> {
    if body.is_empty() {
        return Err(Error::Protocol("empty command body".into()));
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len {
        return Err(Error::Protocol("command body truncated in name".into()));
    }
    let name_bytes = &body[1..1 + name_len];
    let name = std::str::from_utf8(name_bytes)
        .map_err(|_| Error::Protocol("command name not ASCII".into()))?;
    Ok((name, &body[1 + name_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let body = encode_command_body("HELLO", &[1, 2, 3]);
        // 1-byte len + 5-byte name + 3-byte payload
        assert_eq!(body.len(), 9);
        assert_eq!(body[0], 5);
        assert_eq!(&body[1..6], b"HELLO");
        assert_eq!(&body[6..], &[1, 2, 3]);

        let (name, payload) = parse_command_body(&body).unwrap();
        assert_eq!(name, "HELLO");
        assert_eq!(payload, &[1, 2, 3]);
    }

    #[test]
    fn empty_payload_roundtrip() {
        let body = encode_command_body("READY", &[]);
        let (name, payload) = parse_command_body(&body).unwrap();
        assert_eq!(name, "READY");
        assert_eq!(payload.len(), 0);
    }

    #[test]
    fn rejects_empty_body() {
        assert!(parse_command_body(&[]).is_err());
    }

    #[test]
    fn rejects_truncated_name() {
        // name_len=10 but only 4 bytes follow.
        assert!(parse_command_body(&[10, b'a', b'b', b'c', b'd']).is_err());
    }

    #[test]
    fn rejects_non_ascii_name() {
        // Invalid UTF-8 byte sequence in the name.
        assert!(parse_command_body(&[3, 0xC3, 0x28, b'a']).is_err());
    }
}
