use super::*;
use omq_proto::Options;
use std::time::Duration;

fn request(path: &str, protocols: &str, origin: Option<&str>) -> Vec<u8> {
    let mut request = ws_handshake::format_client_upgrade(
        "localhost:80",
        path,
        "dGhlIHNhbXBsZSBub25jZQ==",
        protocols,
    );
    if let Some(origin) = origin {
        request.truncate(request.len() - 2);
        request.extend_from_slice(format!("Origin: {origin}\r\n\r\n").as_bytes());
    }
    request
}

#[test]
fn selection_requires_configured_mechanism_and_preserves_legacy_browser_profile() {
    let policy = ServerPolicy::new("/", &Options::default()).unwrap();
    for (offers, expected) in [
        ("ZWS2.0", Some("ZWS2.0")),
        ("ZWS2.0, ZWS2.0/NULL", Some("ZWS2.0/NULL")),
        ("ZWS2.0/PLAIN, ZWS2.0/NULL", Some("ZWS2.0/NULL")),
        ("ZWS2.0/PLAIN", None),
        ("ZWS2.0/CURVE", None),
        ("unknown", None),
    ] {
        let req = ws_handshake::parse_client_upgrade(&request("/", offers, None)).unwrap();
        assert_eq!(policy.select(&req).ok(), expected, "{offers}");
    }
    #[cfg(feature = "plain")]
    {
        let options = Options {
            mechanism: MechanismSetup::PlainServer {
                authenticator: omq_proto::Authenticator::new(|_| false),
            },
            ..Options::default()
        };
        let policy = ServerPolicy::new("/", &options).unwrap();
        for (offers, expected) in [
            ("ZWS2.0", Some("ZWS2.0")),
            ("ZWS2.0, ZWS2.0/PLAIN", Some("ZWS2.0/PLAIN")),
            ("ZWS2.0/NULL", None),
        ] {
            let req = ws_handshake::parse_client_upgrade(&request("/", offers, None)).unwrap();
            assert_eq!(policy.select(&req).ok(), expected, "{offers}");
        }
    }
}

#[test]
fn origin_and_resource_policy_uses_exact_normalized_origins_and_literal_targets() {
    let mut options = Options::default();
    let default = ServerPolicy::new("/app?x=1", &options).unwrap();
    let browser = request("/app?x=1", "ZWS2.0", Some("https://example.com"));
    let browser = ws_handshake::parse_client_upgrade(&browser).unwrap();
    assert!(default.select(&browser).is_err());
    options.ws.allowed_origins = vec!["https://EXAMPLE.com:443".into(), "http://[::1]".into()];
    let policy = ServerPolicy::new("/app?x=1", &options).unwrap();
    assert!(policy.select(&browser).is_ok());
    for (path, origin, allowed) in [
        ("/app?x=1", None, true),
        ("/app?x=1", Some("http://[0:0:0:0:0:0:0:1]:80"), true),
        ("/app?x=1", Some("https://example.com:444"), false),
        ("/app?x=1", Some("http://example.com"), false),
        ("/app?x=1", Some("https://example.com.evil"), false),
        ("/app?x=1", Some("https://example.com@evil"), false),
        ("/app?x=1", Some("null"), false),
        ("/app?x=1", Some(""), false),
        ("/app?x=1", Some("https://example.com https://evil"), false),
        (
            "/app?x=1",
            Some("https://example.com\r\nOrigin: https://example.com"),
            false,
        ),
        ("/app", None, false),
        ("/app?x=2", None, false),
        ("/app/../app?x=1", None, false),
        ("/%61pp?x=1", None, false),
    ] {
        let result = ws_handshake::parse_client_upgrade(&request(path, "ZWS2.0", origin))
            .and_then(|req| policy.select(&req));
        assert_eq!(result.is_ok(), allowed, "{path} {origin:?}");
    }
    options.ws.allowed_origins.push("*".into());
    assert!(ServerPolicy::new("/", &options).is_err());
}

#[tokio::test]
async fn bytewise_request_and_coalesced_frames_survive_upgrade() {
    for bytewise in [true, false] {
        let (mut server, mut client) = tokio::io::duplex(8192);
        let policy = ServerPolicy::new("/", &Options::default()).unwrap();
        let task = tokio::spawn(async move { accept(&mut server, &policy).await.unwrap() });
        let mut bytes = request("/", "ZWS2.0/NULL", None);
        if bytewise {
            for byte in bytes {
                client.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        } else {
            bytes.extend_from_slice(b"\x82\x81\x01\x02\x03\x04x");
            client.write_all(&bytes).await.unwrap();
        }
        let (head, _) = read_head(&mut client).await.unwrap();
        assert_eq!(
            ws_handshake::parse_server_upgrade(&head, "dGhlIHNhbXBsZSBub25jZQ==").unwrap(),
            "ZWS2.0/NULL"
        );
        let leftover = task.await.unwrap();
        assert_eq!(
            leftover.as_ref(),
            if bytewise {
                &b""[..]
            } else {
                &b"\x82\x81\x01\x02\x03\x04x"[..]
            }
        );
    }
}

#[tokio::test]
async fn client_rejects_unoffered_selection_and_keeps_coalesced_frames() {
    for selected in ["ZWS2.0/NULL", "ZWS2.0/PLAIN", "ZWS2.0"] {
        let (mut server, mut client) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            let (head, _) = read_head(&mut server).await.unwrap();
            let req = ws_handshake::parse_client_upgrade(&head).unwrap();
            let mut response = ws_handshake::format_server_upgrade(
                &ws_handshake::compute_ws_accept(&req.key),
                selected,
            );
            response.extend_from_slice(b"\x82\x01x");
            server.write_all(&response).await.unwrap();
        });
        let result = connect(&mut client, "localhost", "/", "ZWS2.0/NULL").await;
        if selected == "ZWS2.0/NULL" {
            assert_eq!(result.unwrap().as_ref(), b"\x82\x01x");
        } else {
            assert!(result.is_err(), "accepted unoffered {selected}");
        }
        task.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_response_write_is_covered_by_setup_deadline() {
    // One byte of capacity guarantees write_all blocks until the client reads.
    let (mut server, mut client) = tokio::io::duplex(1);
    let policy = ServerPolicy::new("/", &Options::default()).unwrap();
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(1), accept(&mut server, &policy)).await
    });
    client
        .write_all(&request("/", "ZWS2.0/NULL", None))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(client.read_u8().await.unwrap(), b'H');
    assert!(
        client.read_u8().await.is_err(),
        "timed-out transport stayed open"
    );
}
