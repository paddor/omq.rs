//! WebSocket HTTP upgrade handshake helpers (RFC 6455 section 4).
//!
//! Inline SHA-1 and base64 to avoid external deps. SHA-1 is used only for the
//! `Sec-WebSocket-Accept` computation per RFC 6455; it is not used for security.

mod address;
mod http;
pub use address::{normalize_ws_origin, validate_ws_address};
pub use http::{
    MAX_HTTP_BYTES, MAX_HTTP_FIELDS, UpgradeRequest, parse_client_upgrade, parse_server_upgrade,
};

const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Generate a random 16-byte `Sec-WebSocket-Key`, base64-encoded (24 chars).
pub fn generate_ws_key() -> String {
    use rand::Rng;
    let mut raw = [0u8; 16];
    rand::rng().fill_bytes(&mut raw);
    base64_encode(&raw)
}

/// Compute `Sec-WebSocket-Accept` from a `Sec-WebSocket-Key`.
pub fn compute_ws_accept(key: &str) -> String {
    let mut input = Vec::with_capacity(key.len() + WS_GUID.len());
    input.extend_from_slice(key.as_bytes());
    input.extend_from_slice(WS_GUID);
    let hash = sha1(&input);
    base64_encode(&hash)
}

/// Validate a `Sec-WebSocket-Accept` value against the original key.
pub fn validate_ws_accept(key: &str, accept: &str) -> bool {
    compute_ws_accept(key) == accept
}

fn valid_ws_key(key: &str) -> bool {
    key.len() == 24
        && key.bytes().enumerate().all(|(i, b)| {
            matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/')
                || (i >= 22 && b == b'=')
        })
        && key.ends_with("==")
        && matches!(key.as_bytes()[21], b'A' | b'Q' | b'g' | b'w')
}

/// Format a client HTTP upgrade request.
pub fn format_client_upgrade(host: &str, path: &str, key: &str, subprotocol: &str) -> Vec<u8> {
    let mut req = Vec::with_capacity(256);
    req.extend_from_slice(b"GET ");
    req.extend_from_slice(path.as_bytes());
    req.extend_from_slice(b" HTTP/1.1\r\n");
    req.extend_from_slice(b"Host: ");
    req.extend_from_slice(host.as_bytes());
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(b"Upgrade: websocket\r\n");
    req.extend_from_slice(b"Connection: Upgrade\r\n");
    req.extend_from_slice(b"Sec-WebSocket-Key: ");
    req.extend_from_slice(key.as_bytes());
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(b"Sec-WebSocket-Version: 13\r\n");
    req.extend_from_slice(b"Sec-WebSocket-Protocol: ");
    req.extend_from_slice(subprotocol.as_bytes());
    req.extend_from_slice(b"\r\n\r\n");
    req
}

/// Format a server HTTP 101 upgrade response.
pub fn format_server_upgrade(accept: &str, subprotocol: &str) -> Vec<u8> {
    let mut resp = Vec::with_capacity(256);
    resp.extend_from_slice(b"HTTP/1.1 101 Switching Protocols\r\n");
    resp.extend_from_slice(b"Upgrade: websocket\r\n");
    resp.extend_from_slice(b"Connection: Upgrade\r\n");
    resp.extend_from_slice(b"Sec-WebSocket-Accept: ");
    resp.extend_from_slice(accept.as_bytes());
    resp.extend_from_slice(b"\r\n");
    resp.extend_from_slice(b"Sec-WebSocket-Protocol: ");
    resp.extend_from_slice(subprotocol.as_bytes());
    resp.extend_from_slice(b"\r\n\r\n");
    resp
}

// --- Inline SHA-1 (RFC 3174) ---

#[expect(clippy::many_single_char_names, clippy::unreadable_literal)]
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];

    let bit_len = (data.len() as u64) * 8;

    let mut padded = data.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, word) in w[..16].iter_mut().enumerate() {
            *word = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let [mut a, mut b, mut c, mut d, mut e] = h;

        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999_u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, val) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&val.to_be_bytes());
    }
    out
}

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = if chunk.len() > 1 {
            u32::from(chunk[1])
        } else {
            0
        };
        let b2 = if chunk.len() > 2 {
            u32::from(chunk[2])
        } else {
            0
        };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(B64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(B64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
#[path = "ws_handshake/validation_tests.rs"]
mod validation_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_empty() {
        let hash = sha1(b"");
        let hex = hash.iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });
        assert_eq!(hex, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn sha1_abc() {
        let hash = sha1(b"abc");
        let hex = hash.iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });
        assert_eq!(hex, "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn base64_rfc4648() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn ws_accept_known_vector() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = compute_ws_accept(key);
        // Verified against Python: hashlib.sha1(key + GUID).digest() -> base64
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn validate_accept_works() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        assert!(validate_ws_accept(key, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        assert!(!validate_ws_accept(key, "wrong"));
    }

    #[test]
    fn format_client_upgrade_roundtrip() {
        let key = generate_ws_key();
        let req = format_client_upgrade("localhost:9000", "/zmtp", &key, "ZWS2.0/NULL");
        let req_str = String::from_utf8(req).unwrap();
        assert!(req_str.starts_with("GET /zmtp HTTP/1.1\r\n"));
        assert!(req_str.contains("Upgrade: websocket\r\n"));
        assert!(req_str.contains(&format!("Sec-WebSocket-Key: {key}\r\n")));
        assert!(req_str.contains("Sec-WebSocket-Protocol: ZWS2.0/NULL\r\n"));
        assert!(req_str.ends_with("\r\n\r\n"));
    }

    #[test]
    fn parse_client_upgrade_valid() {
        let req = b"GET /zmtp HTTP/1.1\r\n\
            Host: localhost:9000\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\
            Sec-WebSocket-Protocol: ZWS2.0/NULL, ZWS2.0\r\n\
            \r\n";
        let parsed = parse_client_upgrade(req).unwrap();
        assert_eq!(parsed.key, "dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(parsed.path, "/zmtp");
        assert_eq!(parsed.subprotocols, vec!["ZWS2.0/NULL", "ZWS2.0"]);
    }

    #[test]
    fn parse_client_upgrade_rejects_invalid_key() {
        let req = b"GET /zmtp HTTP/1.1\r\n\
            Host: localhost:9000\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Key: not-a-valid-key\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";
        assert!(parse_client_upgrade(req).is_err());
    }

    #[test]
    fn parse_server_upgrade_valid() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = compute_ws_accept(key);
        let resp = format_server_upgrade(&accept, "ZWS2.0/NULL");
        let proto = parse_server_upgrade(&resp, key).unwrap();
        assert_eq!(proto, "ZWS2.0/NULL");
    }

    #[test]
    fn parse_server_upgrade_bad_accept() {
        let resp = b"HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Accept: wrongvalue\r\n\
            \r\n";
        assert!(parse_server_upgrade(resp, "somekey").is_err());
    }
}
