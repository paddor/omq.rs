use super::*;
use crate::{Options, Socket, SocketType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static TEST_STALL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn wait_stalled(count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while crate::transport::dns::stalled_test_lookups() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
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
async fn named_connect_waits_for_shared_setup_credit_before_dns() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(
        SocketType::Sub,
        Options {
            max_pending_handshakes: 1,
            ..Options::default()
        },
    );
    let listener = socket
        .bind("ws://127.0.0.1:0/".parse().unwrap())
        .await
        .unwrap();
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
    let before = crate::transport::dns::stalled_test_lookups();
    let endpoint: Endpoint = "ws://omq-test-stall.invalid:12001/".parse().unwrap();
    let connecting = tokio::spawn({
        let socket = socket.clone();
        let endpoint = endpoint.clone();
        async move { socket.connect(endpoint).await }
    });
    // No DNS request may start while the existing peer holds the sole
    // admission slot through its unfinished ZMTP handshake.
    assert!(
        tokio::time::timeout(Duration::from_millis(40), async {
            while crate::transport::dns::stalled_test_lookups() == before {
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
    drop(pending);
    wait_stalled(before + 1).await;
    socket.disconnect(endpoint).await.unwrap();
    assert!(matches!(connecting.await.unwrap(), Err(Error::Closed)));
    socket.close().await.unwrap();
}

#[tokio::test]
async fn pending_endpoint_jobs_have_a_finite_socket_cap() {
    let _serial = TEST_STALL_LOCK.lock().await;
    let socket = Socket::new(SocketType::Sub, Options::default());
    let before = crate::transport::dns::stalled_test_lookups();
    let mut pending = Vec::new();
    for port in 12000..12000 + u16::try_from(MAX_PENDING_ENDPOINTS).unwrap() {
        let socket = socket.clone();
        let endpoint = format!("ws://omq-test-stall.invalid:{port}/")
            .parse()
            .unwrap();
        pending.push(tokio::spawn(async move { socket.connect(endpoint).await }));
    }
    wait_stalled(before + MAX_PENDING_ENDPOINTS).await;
    let excess = socket
        .connect("ws://omq-test-stall.invalid:13000/".parse().unwrap())
        .await;
    assert!(
        matches!(excess, Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    socket.close().await.unwrap();
    for operation in pending {
        assert!(matches!(operation.await.unwrap(), Err(Error::Closed)));
    }
}
#[tokio::test]
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
        async move {
            socket
                .connect("ws://omq-test-stall.invalid:12001/".parse().unwrap())
                .await
        }
    });
    wait_stalled(before + 1).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn({
        let socket = socket.clone();
        async move {
            socket
                .connect("ws://omq-test-stall.invalid:12002/".parse().unwrap())
                .await
        }
    });
    // Reaching DNS proves the canceled caller released the only credit.
    wait_stalled(before + 2).await;
    socket.close().await.unwrap();
    assert!(matches!(second.await.unwrap(), Err(Error::Closed)));
}
