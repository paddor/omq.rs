#![cfg(feature = "ws")]

use std::time::Duration;

use omq_tokio::{Endpoint, Message, MonitorEvent, MonitorStream, Options, Socket, SocketType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn address(endpoint: &Endpoint) -> String {
    match endpoint {
        Endpoint::Ws { host, port, .. } | Endpoint::Wss { host, port, .. } => {
            format!("{host}:{port}")
        }
        _ => unreachable!(),
    }
}

fn tls_options() -> (Options, Options) {
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let pem = cert.cert.pem().into_bytes();
    let mut server = Options::default();
    server.wss_tls.server_cert_pem = Some(pem.clone());
    server.wss_tls.server_key_pem = Some(cert.signing_key.serialize_pem().into_bytes());
    let mut client = Options::default();
    client.wss_tls.trust_system = false;
    client.wss_tls.trust_pem = Some(pem);
    (server, client)
}

async fn wait_event(monitor: &mut MonitorStream, accept: impl Fn(&MonitorEvent) -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if accept(&monitor.recv().await.unwrap()) {
                return;
            }
        }
    })
    .await
    .expect("expected monitor event did not arrive");
}

async fn eof(mut stream: TcpStream) {
    let mut data = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        (&mut stream).take(65536).read_to_end(&mut data),
    )
    .await
    .expect("setup socket stayed alive past its deadline")
    .unwrap();
    assert!(data.len() < 65536, "expected EOF, not the read cap");
}

#[tokio::test]
async fn stalled_http_and_tls_peers_do_not_serialize_accepts() {
    for scheme in ["ws", "wss"] {
        let (server_options, client_options) = tls_options();
        let server = Socket::new(SocketType::Pull, server_options);
        let endpoint = server
            .bind(format!("{scheme}://127.0.0.1:0/").parse().unwrap())
            .await
            .unwrap();
        let mut idle = TcpStream::connect(address(&endpoint)).await.unwrap();
        if scheme == "ws" {
            idle.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        }
        let client = Socket::new(SocketType::Push, client_options);
        client.connect(endpoint).await.unwrap();
        client
            .wait_connected(1, Duration::from_secs(1))
            .await
            .expect("stalled setup blocked a healthy peer");
        client.send(Message::single("alive")).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.part_slice(0).unwrap(), b"alive");
        client.close().await.unwrap();
        server.close().await.unwrap();
        eof(idle).await;
    }
}

#[tokio::test]
async fn ready_limit_is_shared_by_ws_wss_and_recovers_without_limiting_tcp() {
    let (mut server_options, client_options) = tls_options();
    server_options.ws.max_ready_peers = 1;
    let server = Socket::new(SocketType::Pull, server_options);
    let mut monitor = server.monitor();
    let ws = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let wss = server
        .bind("wss://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let tcp = server
        .bind("tcp://127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let first = Socket::new(SocketType::Push, Options::default());
    first.connect(ws.clone()).await.unwrap();
    server
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    let second = Socket::new(SocketType::Push, client_options);
    second.connect(wss).await.unwrap();
    wait_event(&mut monitor, |event| {
        matches!(event, MonitorEvent::HandshakeFailed { reason, .. } if reason == "socket peer limit reached")
    }).await;
    assert_eq!(server.ready_peer_count(), 1);

    let other = Socket::new(SocketType::Push, Options::default());
    other.connect(tcp).await.unwrap();
    server
        .wait_connected(2, Duration::from_secs(2))
        .await
        .unwrap();
    for (sender, body) in [(&first, "ws alive"), (&other, "tcp alive")] {
        sender.send(Message::single(body)).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), server.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.part_slice(0).unwrap(), body.as_bytes());
    }
    first.close().await.unwrap();
    wait_event(
        &mut monitor,
        |event| matches!(event, MonitorEvent::Disconnected { endpoint, .. } if endpoint == &ws),
    )
    .await;
    server
        .wait_connected(2, Duration::from_secs(3))
        .await
        .expect("rejected peer did not reconnect after credit returned");
    second.send(Message::single("wss recovered")).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.part_slice(0).unwrap(), b"wss recovered");
    second.close().await.unwrap();
    other.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn router_identity_handover_reuses_a_ready_ws_slot() {
    let mut options = Options::default();
    options.ws.max_ready_peers = 1;
    let router = Socket::new(SocketType::Router, options);
    let endpoint = router
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let dealer_options = Options {
        reconnect: omq_tokio::options::ReconnectPolicy::Disabled,
        ..Options::default().identity(bytes::Bytes::from_static(b"same"))
    };
    let first = Socket::new(SocketType::Dealer, dealer_options.clone());
    first.connect(endpoint.clone()).await.unwrap();
    router
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    first.send(Message::single("before")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    let mut monitor = router.monitor();
    let replacement = Socket::new(SocketType::Dealer, dealer_options);
    replacement.connect(endpoint).await.unwrap();
    wait_event(&mut monitor, |event| {
        matches!(event, MonitorEvent::HandshakeSucceeded { .. })
    })
    .await;
    assert_eq!(router.ready_peer_count(), 1);
    replacement.send(Message::single("after")).await.unwrap();
    let request = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.part_slice(0).unwrap(), b"same");
    assert_eq!(request.part_slice(1).unwrap(), b"after");
    router
        .send(Message::multipart([
            bytes::Bytes::from_static(b"same"),
            bytes::Bytes::from_static(b"reply"),
        ]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), replacement.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.part_slice(0).unwrap(), b"reply");
    replacement.close().await.unwrap();
    first.close().await.unwrap();
    router.close().await.unwrap();
}

#[tokio::test]
async fn outbound_ready_limit_spans_dialers_and_disconnect_releases_a_slot() {
    let first = Socket::new(SocketType::Pull, Options::default());
    let first_endpoint = first
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let second = Socket::new(SocketType::Pull, Options::default());
    let second_endpoint = second
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let mut options = Options::default();
    options.ws.max_ready_peers = 1;
    let sender = Socket::new(SocketType::Push, options);
    sender.connect(first_endpoint.clone()).await.unwrap();
    sender
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    let mut monitor = sender.monitor();
    sender.connect(second_endpoint).await.unwrap();
    wait_event(&mut monitor, |event| {
        matches!(event, MonitorEvent::HandshakeFailed { reason, .. } if reason == "socket peer limit reached")
    }).await;
    assert_eq!(sender.ready_peer_count(), 1);
    sender.disconnect(first_endpoint).await.unwrap();
    sender
        .wait_connected(1, Duration::from_secs(3))
        .await
        .unwrap();
    sender.send(Message::single("second route")).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), second.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(0).unwrap(), b"second route");
    sender.close().await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn accepted_http_and_tls_setup_has_a_deadline() {
    for scheme in ["ws", "wss"] {
        let (mut options, client_options) = tls_options();
        options.handshake_timeout = Some(Duration::from_millis(200));
        options.max_pending_handshakes = 1;
        let server = Socket::new(SocketType::Pull, options);
        let endpoint = server
            .bind(format!("{scheme}://127.0.0.1:0/").parse().unwrap())
            .await
            .unwrap();
        let idle = TcpStream::connect(address(&endpoint)).await.unwrap();
        eof(idle).await;
        let client = Socket::new(SocketType::Push, client_options);
        client.connect(endpoint).await.unwrap();
        client
            .wait_connected(1, Duration::from_secs(1))
            .await
            .unwrap();
        client.close().await.unwrap();
        server.close().await.unwrap();
    }
}

async fn upgrade(stream: &mut TcpStream) {
    let key = omq_proto::proto::ws_handshake::generate_ws_key();
    let request = omq_proto::proto::ws_handshake::format_client_upgrade(
        "127.0.0.1",
        "/",
        &key,
        "ZWS2.0/NULL",
    );
    stream.write_all(&request).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), async {
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            assert!(response.len() < 4096);
            response.push(stream.read_u8().await.unwrap());
        }
        response
    })
    .await
    .unwrap();
    omq_proto::proto::ws_handshake::parse_server_upgrade(&response, &key).unwrap();
}

#[tokio::test]
async fn setup_slot_spans_http_and_zmtp_and_is_shared_between_listeners() {
    let server = Socket::new(
        SocketType::Pull,
        Options {
            max_pending_handshakes: 1,
            ..Options::default()
        },
    );
    let first = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let second = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let mut pending = TcpStream::connect(address(&first)).await.unwrap();
    upgrade(&mut pending).await;
    // HTTP succeeded, but no ZMTP READY was sent. Admission must remain held.
    let mut rejected = TcpStream::connect(address(&second)).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), rejected.read_u8()).await;
    assert!(
        result.unwrap().is_err(),
        "over-cap connection received upgrade data"
    );
    drop(pending);
    let client = Socket::new(SocketType::Push, Options::default());
    client.connect(second).await.unwrap();
    client
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn http_upgrade_does_not_restart_zmtp_deadline() {
    let server = Socket::new(
        SocketType::Pull,
        Options {
            handshake_timeout: Some(Duration::from_millis(700)),
            ..Options::default()
        },
    );
    let endpoint = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(address(&endpoint)).await.unwrap();
    stream.write_all(b"G").await.unwrap();
    tokio::time::sleep(Duration::from_millis(450)).await;
    // Send the remainder of a valid request after spending most of the budget.
    let key = omq_proto::proto::ws_handshake::generate_ws_key();
    let request = omq_proto::proto::ws_handshake::format_client_upgrade(
        "127.0.0.1",
        "/",
        &key,
        "ZWS2.0/NULL",
    );
    stream.write_all(&request[1..]).await.unwrap();
    tokio::time::timeout(Duration::from_millis(450), eof(stream))
        .await
        .expect("HTTP upgrade reset the handshake deadline");
    server.close().await.unwrap();
}

#[tokio::test]
async fn unbind_cancels_pending_zmtp_but_preserves_ready_peers() {
    let server = Socket::new(SocketType::Pull, Options::default());
    let endpoint = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let client = Socket::new(SocketType::Push, Options::default());
    client.connect(endpoint.clone()).await.unwrap();
    client
        .wait_connected(1, Duration::from_secs(1))
        .await
        .unwrap();
    let mut pending = TcpStream::connect(address(&endpoint)).await.unwrap();
    upgrade(&mut pending).await;
    server.unbind(endpoint).await.unwrap();
    eof(pending).await;
    client
        .send(Message::single("still connected"))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(0).unwrap(), b"still connected");
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn outbound_http_and_tls_setup_has_a_deadline() {
    for scheme in ["ws", "wss"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("{scheme}://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let client = Socket::new(
            SocketType::Push,
            Options {
                handshake_timeout: Some(Duration::from_millis(200)),
                reconnect: omq_tokio::options::ReconnectPolicy::Disabled,
                ..Options::default()
            },
        );
        client.connect(endpoint).await.unwrap();
        let (idle, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        eof(idle).await;
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn websocket_setup_requires_a_finite_deadline() {
    let server = Socket::new(
        SocketType::Pull,
        Options {
            handshake_timeout: None,
            ..Options::default()
        },
    );
    let endpoint: Endpoint = "ws://127.0.0.1:0/".parse().unwrap();
    assert!(server.bind(endpoint.clone()).await.is_err());
    assert!(server.connect(endpoint).await.is_err());
    server.close().await.unwrap();
}

#[tokio::test]
async fn typed_ws_endpoints_reject_header_injection_before_starting_io() {
    use omq_tokio::endpoint::Host;
    let socket = Socket::new(SocketType::Pull, Options::default());
    for endpoint in [
        Endpoint::Ws {
            host: Host::Name("localhost\r\nOrigin: https://evil".into()),
            port: 80,
            path: "/".into(),
        },
        Endpoint::Ws {
            host: Host::Name("localhost".into()),
            port: 80,
            path: "/\r\nOrigin: https://evil".into(),
        },
        Endpoint::Wss {
            host: Host::Name("localhost".into()),
            port: 443,
            path: "/with space".into(),
        },
    ] {
        assert!(matches!(
            socket.bind(endpoint.clone()).await,
            Err(omq_tokio::Error::InvalidEndpoint(_))
        ));
        assert!(matches!(
            socket.connect(endpoint).await,
            Err(omq_tokio::Error::InvalidEndpoint(_))
        ));
    }
    socket.close().await.unwrap();
}

#[tokio::test]
async fn listener_applies_configured_origin_path_and_profile_before_upgrade() {
    let mut options = Options::default();
    options.ws.allowed_origins = vec!["https://app.example.com".into()];
    let server = Socket::new(SocketType::Pull, options);
    let endpoint = server
        .bind("ws://127.0.0.1:0/app?tenant=1".parse().unwrap())
        .await
        .unwrap();
    for (path, origin, profile, allowed) in [
        ("/app?tenant=1", "https://app.example.com", "ZWS2.0", true),
        ("/app?tenant=1", "https://evil.example.com", "ZWS2.0", false),
        ("/app?tenant=2", "https://app.example.com", "ZWS2.0", false),
        (
            "/app?tenant=1",
            "https://app.example.com",
            "ZWS2.0/PLAIN",
            false,
        ),
    ] {
        let mut stream = TcpStream::connect(address(&endpoint)).await.unwrap();
        let mut request = omq_proto::proto::ws_handshake::format_client_upgrade(
            "localhost",
            path,
            "dGhlIHNhbXBsZSBub25jZQ==",
            profile,
        );
        request.truncate(request.len() - 2);
        request.extend_from_slice(format!("Origin: {origin}\r\n\r\n").as_bytes());
        stream.write_all(&request).await.unwrap();
        let byte = tokio::time::timeout(Duration::from_secs(1), stream.read_u8())
            .await
            .unwrap();
        assert_eq!(byte.is_ok(), allowed, "{path} {origin} {profile}");
        if allowed {
            assert_eq!(byte.unwrap(), b'H');
        }
    }
    server.close().await.unwrap();
}

#[cfg(feature = "curve")]
#[tokio::test]
async fn native_curve_uses_matching_upgrade_profile() {
    let keypair = omq_tokio::CurveKeypair::generate();
    let public = keypair.public;
    let server = Socket::new(SocketType::Pull, Options::default().curve_server(keypair));
    let endpoint = server
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(omq_tokio::CurveKeypair::generate(), public),
    );
    client.connect(endpoint).await.unwrap();
    client
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    client.send(Message::single("encrypted")).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(0).unwrap(), b"encrypted");
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn named_ws_bind_and_connect_keep_resource_and_original_server_name() {
    let schemes = [
        "ws",
        #[cfg(feature = "lz4")]
        "lz4+ws",
    ];
    for scheme in schemes {
        let server = Socket::new(SocketType::Pull, Options::default());
        let requested: Endpoint = format!("{scheme}://localhost:0/room?x=1").parse().unwrap();
        let bound = server.bind(requested).await.unwrap();
        let (Endpoint::Ws { port, .. } | Endpoint::Wss { port, .. }) = bound.underlying_ws() else {
            unreachable!()
        };
        let client = Socket::new(SocketType::Push, Options::default());
        client
            .connect(
                format!("{scheme}://localhost:{port}/room?x=1")
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        client
            .wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        client
            .send(Message::single("named endpoint"))
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), server.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.part_slice(0).unwrap(), b"named endpoint");
        client.close().await.unwrap();
        server.close().await.unwrap();
    }
}
