mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

/// Bind a port, then drop the listener so the port is guaranteed unbound.
/// Uses a raw `std::net::TcpListener` so the port is released synchronously
/// on drop (no async actor cleanup race).
fn grab_and_release_port() -> Endpoint {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    tcp_ep(port)
}

#[tokio::test]
async fn reconnect_stop_conn_refused_stops_dial() {
    let ep = grab_and_release_port();

    let opts = Options::default()
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)))
        .reconnect_stop_conn_refused(true);
    let push = Socket::new(SocketType::Push, opts);
    let mut mon = push.monitor();
    push.connect(ep).await.unwrap();

    // With reconnect_stop_conn_refused, the first ECONNREFUSED should
    // stop the dialer. No ConnectDelayed events should fire.
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                return true;
            }
        }
    })
    .await;

    // Timeout means no ConnectDelayed arrived. That's the expected path.
    assert!(result.is_err(), "expected no ConnectDelayed events");
}

#[tokio::test]
async fn reconnect_stop_default_retries() {
    let ep = grab_and_release_port();

    // Default: reconnect_stop_conn_refused is false. Should retry.
    let opts = Options::default().reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)));
    let push = Socket::new(SocketType::Push, opts);
    let mut mon = push.monitor();
    push.connect(ep).await.unwrap();

    let mut count = 0u32;
    // 10 s: Windows CI runners are slower than Linux.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                count += 1;
                if count >= 3 {
                    return;
                }
            }
        }
    })
    .await
    .expect("should see at least 3 ConnectDelayed events");
}

#[tokio::test]
async fn reconnect_stop_after_established_session() {
    let opts = Options::default()
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)))
        .reconnect_stop_conn_refused(true);
    let push = Socket::new(SocketType::Push, opts);

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    push.connect(ep).await.unwrap();

    // Wait for the connection to establish.
    push.send(Message::from("hello")).await.unwrap();
    let msg = pull.recv().await.unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);

    // Drop the listener. The port becomes unbound.
    drop(pull);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The reconnect attempt should hit ECONNREFUSED and stop.
    let mut mon = push.monitor();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                return true;
            }
        }
    })
    .await;

    assert!(
        result.is_err(),
        "expected no ConnectDelayed after ECONNREFUSED stop"
    );
}

#[tokio::test]
async fn reconnect_stop_sub_can_rearm_same_endpoint() {
    let ep = grab_and_release_port();

    let opts = Options::default()
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)))
        .reconnect_stop_conn_refused(true);
    let sub = Socket::new(SocketType::Sub, opts);
    sub.connect(ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    let pub_ = Socket::new(SocketType::Pub, Options::default());
    pub_.bind(ep.clone()).await.unwrap();
    sub.connect(ep).await.unwrap();
    sub.subscribe(bytes::Bytes::new()).await.unwrap();

    pub_.wait_subscribed(1, Duration::from_secs(2))
        .await
        .expect("subscription did not arrive after reconnect was rearmed");
    pub_.send(Message::single("again")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("subscriber did not receive after reconnect was rearmed")
        .unwrap();
    assert_eq!(msg, Message::single("again"));
}
