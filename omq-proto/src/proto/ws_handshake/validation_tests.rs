use super::*;

const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

fn request() -> String {
    String::from_utf8(format_client_upgrade(
        "localhost:9000",
        "/zmtp",
        KEY,
        "ZWS2.0/NULL",
    ))
    .unwrap()
}

fn response() -> String {
    String::from_utf8(format_server_upgrade(
        &compute_ws_accept(KEY),
        "ZWS2.0/NULL",
    ))
    .unwrap()
}

#[test]
fn rejects_ambiguous_or_malformed_requests() {
    let valid = request();
    let cases = [
        ("lowercase method", valid.replacen("GET", "get", 1)),
        ("old version", valid.replacen("HTTP/1.1", "HTTP/1.0", 1)),
        (
            "extra request component",
            valid.replacen("HTTP/1.1", "HTTP/1.1 extra", 1),
        ),
        ("tab separator", valid.replacen("GET ", "GET\t", 1)),
        (
            "missing Host",
            valid.replace("Host: localhost:9000\r\n", ""),
        ),
        (
            "duplicate Host",
            valid.replace(
                "Host: localhost:9000",
                "Host: localhost:9000\r\nHost: other",
            ),
        ),
        (
            "duplicate key",
            valid.replace(
                "Sec-WebSocket-Key:",
                &format!("Sec-WebSocket-Key: {KEY}\r\nSec-WebSocket-Key:"),
            ),
        ),
        (
            "duplicate version",
            valid.replace(
                "Sec-WebSocket-Version: 13",
                "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Version: 12",
            ),
        ),
        (
            "noncanonical key",
            valid.replace(KEY, "dGhlIHNhbXBsZSBub25jZR=="),
        ),
        (
            "duplicate protocol",
            valid.replace("ZWS2.0/NULL", "ZWS2.0/NULL, ZWS2.0/NULL"),
        ),
        (
            "empty protocol entry",
            valid.replace("ZWS2.0/NULL", "ZWS2.0/NULL,,ZWS2.0"),
        ),
        (
            "protocol injection",
            valid.replace("ZWS2.0/NULL", "ZWS2.0/NULL; bad"),
        ),
        (
            "no protocol",
            valid.replace("Sec-WebSocket-Protocol: ZWS2.0/NULL\r\n", ""),
        ),
        (
            "duplicate Origin",
            valid.replace(
                "Host:",
                "Origin: https://one.example\r\nOrigin: https://two.example\r\nHost:",
            ),
        ),
        ("invalid header name", valid.replace("Host:", "Ho(st:")),
        ("space before colon", valid.replace("Host:", "Host :")),
        ("folded field", valid.replace("Host:", " Host:")),
        ("header without colon", valid.replace("Host:", "Host")),
        ("bare newlines", valid.replace("\r\n", "\n")),
        (
            "missing terminator",
            valid.trim_end_matches("\r\n").to_string(),
        ),
        ("NUL value", valid.replace("localhost", "local\0host")),
        (
            "request body",
            valid.replace("Host:", "Content-Length: 1\r\nHost:"),
        ),
        (
            "transfer encoding",
            valid.replace("Host:", "Transfer-Encoding: chunked\r\nHost:"),
        ),
    ];
    let accepted: Vec<_> = cases
        .into_iter()
        .filter_map(|(name, wire)| {
            parse_client_upgrade(wire.as_bytes())
                .is_ok()
                .then_some(name)
        })
        .collect();
    assert!(
        accepted.is_empty(),
        "accepted malformed requests: {accepted:?}"
    );
}

#[test]
fn rejects_ambiguous_or_malformed_responses() {
    let valid = response();
    let cases = [
        ("old version", valid.replacen("HTTP/1.1", "HTTP/1.0", 1)),
        ("fake version", valid.replacen("HTTP/1.1", "BOGUS", 1)),
        (
            "missing Upgrade",
            valid.replace("Upgrade: websocket\r\n", ""),
        ),
        (
            "missing Connection",
            valid.replace("Connection: Upgrade\r\n", ""),
        ),
        (
            "duplicate Accept",
            valid.replace(
                "Sec-WebSocket-Accept:",
                &format!(
                    "Sec-WebSocket-Accept: {}\r\nSec-WebSocket-Accept:",
                    compute_ws_accept(KEY)
                ),
            ),
        ),
        (
            "duplicate selection",
            valid.replace(
                "Sec-WebSocket-Protocol:",
                "Sec-WebSocket-Protocol: ZWS2.0\r\nSec-WebSocket-Protocol:",
            ),
        ),
        (
            "multiple selections",
            valid.replace("ZWS2.0/NULL", "ZWS2.0/NULL,ZWS2.0"),
        ),
        (
            "unsolicited extension",
            valid.replace(
                "Upgrade: websocket",
                "Sec-WebSocket-Extensions: permessage-deflate\r\nUpgrade: websocket",
            ),
        ),
        ("header without colon", valid.replace("Upgrade:", "Upgrade")),
        ("bare newlines", valid.replace("\r\n", "\n")),
        (
            "missing terminator",
            valid.trim_end_matches("\r\n").to_string(),
        ),
    ];
    let accepted: Vec<_> = cases
        .into_iter()
        .filter_map(|(name, wire)| {
            parse_server_upgrade(wire.as_bytes(), KEY)
                .is_ok()
                .then_some(name)
        })
        .collect();
    assert!(
        accepted.is_empty(),
        "accepted malformed responses: {accepted:?}"
    );
}

#[test]
fn bounds_unknown_headers_and_protocol_offers() {
    let valid = request();
    let many_headers = valid.replace("Host:", &("X: a\r\n".repeat(65) + "Host:"));
    assert!(parse_client_upgrade(many_headers.as_bytes()).is_err());
    let large_header = valid.replace("Host:", &format!("X: {}\r\nHost:", "a".repeat(4096)));
    assert!(parse_client_upgrade(large_header.as_bytes()).is_err());
    let protocols = (0..65)
        .map(|i| format!("p{i}"))
        .collect::<Vec<_>>()
        .join(",");
    assert!(parse_client_upgrade(valid.replace("ZWS2.0/NULL", &protocols).as_bytes()).is_err());
}

#[test]
fn accepts_list_headers_ows_and_native_zws_protocol_names() {
    let response = response().replace("Switching Protocols", "Switching\tProtocols");
    assert!(parse_server_upgrade(response.as_bytes(), KEY).is_ok());
    let valid = request()
        .replace("Upgrade: websocket", "Upgrade: h2c, WebSocket")
        .replace(
            "Connection: Upgrade",
            "Connection: keep-alive\r\nConnection:\t Upgrade\t",
        )
        .replace("ZWS2.0/NULL", "ZWS2.0/NULL, ZWS2.0/PLAIN, ZWS2.0");
    let request = parse_client_upgrade(valid.as_bytes()).unwrap();
    assert_eq!(
        request.subprotocols,
        ["ZWS2.0/NULL", "ZWS2.0/PLAIN", "ZWS2.0"]
    );
}

#[test]
fn accepts_exact_header_limits_and_rejects_one_over() {
    let valid = request();
    let fields = valid.replace("Host:", &("X: a\r\n".repeat(MAX_HTTP_FIELDS - 6) + "Host:"));
    assert!(parse_client_upgrade(fields.as_bytes()).is_ok());
    let over = fields.replace("Host:", "X: a\r\nHost:");
    assert!(parse_client_upgrade(over.as_bytes()).is_err());

    let blank = valid.replace("Host:", "X-Fill: \r\nHost:");
    let full = blank.replace(
        "X-Fill: ",
        &format!("X-Fill: {}", "a".repeat(MAX_HTTP_BYTES - blank.len())),
    );
    assert_eq!(full.len(), MAX_HTTP_BYTES);
    assert!(parse_client_upgrade(full.as_bytes()).is_ok());
    assert!(parse_client_upgrade(full.replace("X-Fill: ", "X-Fill: a").as_bytes()).is_err());
}
