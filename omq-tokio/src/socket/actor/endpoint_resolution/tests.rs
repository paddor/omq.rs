use super::*;
use crate::transport::dns::test_lookup::ScopedLookup;
use crate::{Options, Socket, SocketType};
#[cfg(feature = "ws")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static TEST_STALL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(any(feature = "ws", feature = "quic"))]
async fn wait_stalled(count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while crate::transport::dns::stalled_test_lookups() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Every named transport, with a socket type that connects and one that
/// binds it.
fn named_endpoints(port: u16) -> Vec<(Endpoint, SocketType, SocketType)> {
    let host = "omq-test-lookup.invalid";
    [
        (
            format!("tcp://{host}:{port}"),
            SocketType::Sub,
            SocketType::Sub,
        ),
        #[cfg(feature = "ws")]
        (
            format!("ws://{host}:{port}/"),
            SocketType::Sub,
            SocketType::Sub,
        ),
        #[cfg(feature = "ws")]
        (
            format!("wss://{host}:{port}/"),
            SocketType::Sub,
            SocketType::Sub,
        ),
        #[cfg(feature = "quic")]
        (
            format!("quic://{host}:{port}"),
            SocketType::Sub,
            SocketType::Sub,
        ),
        (
            format!("udp://{host}:{port}"),
            SocketType::Radio,
            SocketType::Dish,
        ),
        #[cfg(feature = "dart")]
        (
            format!("dart://{host}:{port}"),
            SocketType::Scatter,
            SocketType::Gather,
        ),
    ]
    .into_iter()
    .map(|(endpoint, connect, bind)| (endpoint.parse().unwrap(), connect, bind))
    .collect()
}

/// While DNS is out, bind and connect on every named transport fail with
/// `Error::Io`, and the failure leaves no retry loop behind.
async fn assert_initial_dns_outage_is_returned(lookup: &ScopedLookup, port: u16) {
    let mut sockets = Vec::new();
    for (endpoint, connect_type, bind_type) in named_endpoints(port) {
        let connecting = Socket::new(connect_type, Options::default());
        let connected = connecting.connect(endpoint.clone()).await;
        assert!(
            matches!(connected, Err(Error::Io(_))),
            "connect {endpoint}: {connected:?}"
        );
        let binding = Socket::new(bind_type, Options::default());
        let bound = binding.bind(endpoint.clone()).await;
        assert!(
            matches!(bound, Err(Error::Io(_))),
            "bind {endpoint}: {bound:?}"
        );
        sockets.extend([connecting, binding]);
    }
    let calls = lookup.calls();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        lookup.calls(),
        calls,
        "initial DNS failure must not create a retry loop"
    );
    for socket in sockets {
        socket.close().await.unwrap();
    }
}

#[tokio::test]
async fn initial_dns_failure_is_returned_for_bind_and_connect() {
    let lookup = ScopedLookup::new(13005, None);
    assert_initial_dns_outage_is_returned(&lookup, 13005).await;
}

#[tokio::test]
async fn initial_empty_dns_result_is_returned_for_bind_and_connect() {
    let lookup = ScopedLookup::new(13007, None);
    lookup.set_empty();
    assert_initial_dns_outage_is_returned(&lookup, 13007).await;
}

/// A DNS outage after a successful connect stays in the dialer's retry loop:
/// sends keep working, and the socket reconnects once DNS answers again.
async fn assert_reconnect_dns_outage_recovers(outage: impl FnOnce(&ScopedLookup)) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let lookup = ScopedLookup::new(address.port(), Some(address.ip()));
    let push = Socket::new(SocketType::Push, Options::default());
    let endpoint: Endpoint = format!("tcp://omq-test-lookup.invalid:{}", address.port())
        .parse()
        .unwrap();
    push.connect(endpoint).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let before = lookup.calls();
    outage(&lookup);
    drop(peer);
    tokio::time::timeout(Duration::from_secs(1), async {
        while lookup.calls() < before + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        push.send(crate::Message::single("queued")),
    )
    .await
    .expect("send blocked during a DNS outage")
    .unwrap();
    drop(listener);
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(format!("tcp://{address}")).await.unwrap();
    lookup.set_address(Some(address.ip()));
    push.wait_connected(1, Duration::from_secs(1))
        .await
        .unwrap();
    push.send(crate::Message::single("recovered"))
        .await
        .unwrap();
    for expected in ["queued", "recovered"] {
        let message = tokio::time::timeout(Duration::from_secs(1), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.part_bytes(0).unwrap(), expected.as_bytes());
    }
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_dns_failure_retries_and_recovers_without_api_errors() {
    assert_reconnect_dns_outage_recovers(|lookup| lookup.set_address(None)).await;
}

#[tokio::test]
async fn reconnect_empty_dns_result_retries_and_recovers_without_api_errors() {
    assert_reconnect_dns_outage_recovers(ScopedLookup::set_empty).await;
}

#[tokio::test]
#[cfg(feature = "quic")]
async fn stalled_quic_dns_leaves_controls_responsive() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(SocketType::Sub, Options::default());
    let before = crate::transport::dns::stalled_test_lookups();
    let connecting = tokio::spawn({
        let socket = socket.clone();
        async move { socket.connect("quic://omq-test-stall.invalid:12003").await }
    });
    wait_stalled(before + 1).await;
    let responsive =
        tokio::time::timeout(Duration::from_millis(100), socket.subscribe("news")).await;
    connecting.abort();
    assert!(
        responsive.is_ok(),
        "initial QUIC DNS blocked the socket actor"
    );
    responsive.unwrap().unwrap();
    socket.close().await.unwrap();
}

#[tokio::test]
async fn tcp_dns_uses_the_configured_setup_deadline() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(
        SocketType::Sub,
        Options {
            handshake_timeout: Some(Duration::from_millis(40)),
            ..Options::default()
        },
    );
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        socket.connect("tcp://omq-test-stall.invalid:12004"),
    )
    .await;
    socket.close().await.unwrap();
    assert!(
        matches!(result, Ok(Err(Error::HandshakeFailed(ref reason))) if reason == "DNS resolution timeout"),
        "initial TCP DNS ignored the setup deadline: {result:?}"
    );
}

#[tokio::test]
#[cfg(feature = "ws")]
async fn stalled_named_bind_and_connect_leave_controls_responsive() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(SocketType::Sub, Options::default());
    let connect_endpoint: Endpoint = "ws://omq-test-stall.invalid:12001/".parse().unwrap();
    let bind_endpoint: Endpoint = "ws://omq-test-stall.invalid:12002/".parse().unwrap();
    let first_count = crate::transport::dns::stalled_test_lookups();
    let connecting = tokio::spawn({
        let socket = socket.clone();
        let endpoint = connect_endpoint.clone();
        async move { socket.connect(endpoint).await }
    });
    wait_stalled(first_count + 1).await;
    tokio::time::timeout(Duration::from_secs(1), socket.subscribe("news"))
        .await
        .unwrap()
        .unwrap();
    let binding = tokio::spawn({
        let socket = socket.clone();
        let endpoint = bind_endpoint.clone();
        async move { socket.bind(endpoint).await }
    });
    wait_stalled(first_count + 2).await;
    let healthy: Endpoint = "ws://127.0.0.1:0/".parse().unwrap();
    let bound = tokio::time::timeout(Duration::from_secs(1), socket.bind(healthy))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(bound, Endpoint::Ws { .. }));
    tokio::time::timeout(Duration::from_secs(1), socket.disconnect(connect_endpoint))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), socket.unbind(bind_endpoint))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(connecting.await.unwrap(), Err(Error::Closed)));
    assert!(matches!(binding.await.unwrap(), Err(Error::Closed)));
    socket.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "ws")]
async fn named_connect_waits_for_dial_setup_credit_before_dns() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(
        SocketType::Sub,
        Options {
            max_pending_handshakes: 1,
            ..Options::default()
        },
    );
    let listener = socket.bind("ws://127.0.0.1:0/").await.unwrap();
    let Endpoint::Ws { host, port, .. } = listener else {
        unreachable!()
    };
    let mut pending = tokio::net::TcpStream::connect(format!("{host}:{port}"))
        .await
        .unwrap();
    let request = omq_proto::proto::ws_handshake::format_client_upgrade(
        "127.0.0.1",
        "/",
        "dGhlIHNhbXBsZSBub25jZQ==",
        "ZWS2.0/NULL",
    );
    pending.write_all(&request).await.unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        assert!(head.len() < 4096);
        head.push(pending.read_u8().await.unwrap());
    }
    // An inbound peer holding the sole inbound slot does not delay dials.
    let before = crate::transport::dns::stalled_test_lookups();
    let connect = |endpoint: Endpoint| {
        let socket = socket.clone();
        tokio::spawn(async move { socket.connect(endpoint).await })
    };
    let first: Endpoint = "ws://omq-test-stall.invalid:12001/".parse().unwrap();
    let first_connect = connect(first.clone());
    wait_stalled(before + 1).await;
    // A second named dial waits for the sole outbound slot before DNS.
    let second: Endpoint = "ws://omq-test-stall.invalid:12002/".parse().unwrap();
    let second_connect = connect(second.clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(40), async {
            while crate::transport::dns::stalled_test_lookups() == before + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    tokio::time::timeout(Duration::from_secs(1), socket.subscribe("still responsive"))
        .await
        .unwrap()
        .unwrap();
    socket.disconnect(first).await.unwrap();
    wait_stalled(before + 2).await;
    socket.disconnect(second).await.unwrap();
    assert!(matches!(first_connect.await.unwrap(), Err(Error::Closed)));
    assert!(matches!(second_connect.await.unwrap(), Err(Error::Closed)));
    drop(pending);
    socket.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "ws")]
async fn pending_endpoint_jobs_have_a_finite_socket_cap() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(SocketType::Sub, Options::default());
    let before = crate::transport::dns::stalled_test_lookups();
    let mut pending = Vec::new();
    for port in 12000..12000 + u16::try_from(MAX_PENDING_ENDPOINTS).unwrap() {
        let socket = socket.clone();
        let endpoint: Endpoint = format!("ws://omq-test-stall.invalid:{port}/")
            .parse()
            .unwrap();
        pending.push(tokio::spawn(async move { socket.connect(endpoint).await }));
    }
    wait_stalled(before + MAX_PENDING_ENDPOINTS).await;
    let excess = socket.connect("ws://omq-test-stall.invalid:13000/").await;
    assert!(
        matches!(excess, Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    socket.close().await.unwrap();
    for operation in pending {
        assert!(matches!(operation.await.unwrap(), Err(Error::Closed)));
    }
}
#[tokio::test]
#[cfg(feature = "ws")]
async fn canceled_connect_caller_releases_dns_setup_admission() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(
        SocketType::Sub,
        Options {
            max_pending_handshakes: 1,
            ..Options::default()
        },
    );
    let before = crate::transport::dns::stalled_test_lookups();
    let first = tokio::spawn({
        let socket = socket.clone();
        async move { socket.connect("ws://omq-test-stall.invalid:12001/").await }
    });
    wait_stalled(before + 1).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn({
        let socket = socket.clone();
        async move { socket.connect("ws://omq-test-stall.invalid:12002/").await }
    });
    // Reaching DNS proves the canceled caller released the only credit.
    wait_stalled(before + 2).await;
    socket.close().await.unwrap();
    assert!(matches!(second.await.unwrap(), Err(Error::Closed)));
}

#[tokio::test]
async fn named_udp_connect_resolves_without_holding_the_actor() {
    let dish = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = dish.local_addr().unwrap();
    let _lookup =
        crate::transport::dns::test_lookup::ScopedLookup::new(address.port(), Some(address.ip()));
    let radio = Socket::new(SocketType::Radio, Options::default());
    let endpoint: Endpoint = format!("udp://omq-test-lookup.invalid:{}", address.port())
        .parse()
        .unwrap();
    radio.connect(endpoint.clone()).await.unwrap();
    radio
        .send(crate::Message::with_group("weather", "hot"))
        .await
        .unwrap();
    let mut datagram = [0; 64];
    let len = tokio::time::timeout(Duration::from_secs(1), dish.recv(&mut datagram))
        .await
        .unwrap()
        .unwrap();
    let (group, body) = crate::transport::udp::decode_datagram(&datagram[..len]).unwrap();
    assert_eq!((&group[..], &body[..]), (&b"weather"[..], &b"hot"[..]));

    // The name that connected also disconnects; the caller never saw the
    // resolved address.
    radio.disconnect(endpoint).await.unwrap();

    // A lookup that never answers leaves the actor serving other commands.
    let _guard = TEST_STALL_LOCK.lock().await;
    let stalled: Endpoint = "udp://omq-test-stall.invalid:13006".parse().unwrap();
    let pending = {
        let radio = radio.clone();
        tokio::spawn(async move { radio.connect(stalled).await })
    };
    tokio::time::timeout(Duration::from_secs(1), radio.join("ignored"))
        .await
        .expect("actor waited for a UDP lookup")
        .unwrap_err();
    pending.abort();
    radio.close().await.unwrap();
}
