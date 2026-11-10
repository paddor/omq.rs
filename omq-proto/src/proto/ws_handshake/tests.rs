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
