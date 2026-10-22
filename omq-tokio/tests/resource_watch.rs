//! Cheap resource-watch tests for bounded diagnostics and peer churn.

mod test_support;

#[cfg(target_os = "linux")]
use std::net::{IpAddr, SocketAddr};
#[cfg(target_os = "linux")]
use std::sync::LazyLock;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

use bytes::Bytes;
#[cfg(target_os = "linux")]
use omq_tokio::endpoint::Host;
use omq_tokio::{
    Endpoint, Message, MonitorTryRecvError, Options, ReconnectPolicy, Socket, SocketType,
};
#[cfg(target_os = "linux")]
use tokio::io::AsyncWriteExt;
#[cfg(target_os = "linux")]
use tokio::net::TcpStream;

#[cfg(target_os = "linux")]
static RESOURCE_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[cfg(target_os = "linux")]
fn socket_addr(ep: &Endpoint) -> SocketAddr {
    match ep {
        Endpoint::Tcp {
            host: Host::Ip(ip),
            port,
        } => SocketAddr::new(*ip, *port),
        other => panic!("expected TCP endpoint, got {other:?}"),
    }
}

#[cfg(target_os = "linux")]
async fn wait_connections_at_most(sock: &Socket, max: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let len = sock.connections().await.unwrap().len();
        if len <= max {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "connection count stayed above {max}: {len}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn undrained_monitor_lags_without_blocking_socket() {
    let ep = inproc_ep("resource-monitor-lag");
    // 80 dealers emit about 240 events, well past this capacity.
    let router = Socket::new(SocketType::Router, Options::default().monitor_capacity(64));
    let mut mon = router.monitor();
    router.bind(ep.clone()).await.unwrap();

    for i in 0..80 {
        let identity = Bytes::from(format!("d{i}"));
        let dealer = Socket::new(
            SocketType::Dealer,
            Options::default()
                .identity(identity.clone())
                .reconnect(ReconnectPolicy::Disabled),
        );
        dealer.connect(ep.clone()).await.unwrap();
        dealer
            .wait_connected(1, Duration::from_secs(1))
            .await
            .expect("dealer did not connect");

        dealer.send(Message::single("tick")).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), router.recv())
            .await
            .expect("router did not receive")
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), identity);
        assert_eq!(msg.part_bytes(1).unwrap(), &b"tick"[..]);
        dealer.close().await.unwrap();
    }

    assert!(
        matches!(mon.try_recv(), Err(MonitorTryRecvError::Lagged(_))),
        "undrained monitor should lag instead of growing unbounded"
    );

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"final")),
    );
    dealer.connect(ep).await.unwrap();
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("final dealer did not connect");
    dealer.send(Message::single("still-usable")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(1), router.recv())
        .await
        .expect("router did not receive final message")
        .unwrap();
    assert_eq!(msg, Message::multipart(["final", "still-usable"]));
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "local Linux resource watch; can hang on hosted CI"]
async fn tcp_identity_churn_fd_count_returns_near_baseline() {
    let _guard = RESOURCE_TEST_LOCK.lock().await;

    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    let ep = test_support::tcp_loopback(port);
    let baseline = test_support::open_fd_count().expect("fd count available on Linux");

    for _ in 0..30 {
        let dealer = Socket::new(
            SocketType::Dealer,
            Options::default()
                .identity(Bytes::from_static(b"worker"))
                .reconnect(ReconnectPolicy::Disabled),
        );
        dealer.connect(ep.clone()).await.unwrap();
        dealer
            .wait_connected(1, Duration::from_secs(1))
            .await
            .expect("dealer did not connect");
        dealer.send(Message::single("ping")).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), router.recv())
            .await
            .expect("router did not receive")
            .unwrap();
        assert_eq!(msg, Message::multipart(["worker", "ping"]));
        dealer.close().await.unwrap();
    }

    wait_connections_at_most(&router, 0, Duration::from_secs(2)).await;
    router.close().await.unwrap();

    let final_count = test_support::wait_for_fd_count_at_most(baseline + 6, Duration::from_secs(2))
        .await
        .expect("fd count available on Linux");
    assert!(
        final_count <= baseline + 6,
        "fd count did not return near baseline: baseline={baseline}, final={final_count}"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "local Linux resource watch; can hang on hosted CI"]
async fn stream_close_under_remote_write_returns_fd_count_near_baseline() {
    let _guard = RESOURCE_TEST_LOCK.lock().await;

    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream
        .bind(Endpoint::Tcp {
            host: Host::Ip(IpAddr::from([127, 0, 0, 1])),
            port: 0,
        })
        .await
        .unwrap();
    let addr = socket_addr(&ep);
    let baseline = test_support::open_fd_count().expect("fd count available on Linux");

    let mut client = TcpStream::connect(addr).await.unwrap();
    let connect_msg = tokio::time::timeout(Duration::from_secs(1), stream.recv())
        .await
        .expect("stream did not receive connect notification")
        .unwrap();
    assert!(
        !connect_msg.part_bytes(0).unwrap().is_empty(),
        "STREAM peer identity should be non-empty"
    );

    let mut writer = tokio::spawn(async move {
        let payload = [b'x'; 1024];
        loop {
            if client.write_all(&payload).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.close().await.unwrap();
    tokio::select! {
        res = &mut writer => {
            let _ = res;
        }
        () = tokio::time::sleep(Duration::from_secs(2)) => {
            writer.abort();
            panic!("remote writer did not observe close");
        }
    }

    let final_count = test_support::wait_for_fd_count_at_most(baseline + 6, Duration::from_secs(2))
        .await
        .expect("fd count available on Linux");
    assert!(
        final_count <= baseline + 6,
        "fd count did not return near baseline: baseline={baseline}, final={final_count}"
    );
}
