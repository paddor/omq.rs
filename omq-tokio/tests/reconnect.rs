//! Reconnect/backoff: dialer reconnects when a listener appears late,
//! restarts mid-session, or drops abruptly while sends are in-flight.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[tokio::test]
async fn connect_retries_until_listener_appears() {
    let port = loopback_port();

    let pull_ep = tcp_ep(port);
    let bind_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(pull_ep).await.unwrap();
        pull
    });

    let opts = Options {
        reconnect: ReconnectPolicy::Exponential {
            min: Duration::from_millis(20),
            max: Duration::from_millis(80),
        },
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(tcp_ep(port)).await.unwrap();

    push.send(Message::single("eventually")).await.unwrap();
    let pull = bind_handle.await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"eventually"[..]);
}

#[tokio::test]
async fn reconnect_after_peer_restart() {
    // Peer (listener side) shuts down mid-session; dialer reconnects to the
    // same endpoint when a fresh listener appears.
    let port = loopback_port();

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    pull1.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    push.connect(tcp_ep(port)).await.unwrap();

    // Confirm the session is live before simulating the peer restart.
    push.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"before");

    // Peer restarts: close cleanly so the actor tears down.
    pull1.close().await.unwrap();

    // Spin until the OS port is free and the new listener is up.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(tcp_ep(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    push.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer restart timed out")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"after");
}

#[tokio::test]
async fn peer_drop_mid_send_is_handled_cleanly() {
    // Peer is dropped while the push engine has in-flight sends. The socket
    // must not panic or deadlock; it reconnects and resumes delivery.
    let port = loopback_port();

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    pull1.bind(tcp_ep(port)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    push.connect(tcp_ep(port)).await.unwrap();

    // Confirm handshake before flooding.
    push.send(Message::single("sync")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("sync recv timed out")
        .unwrap();

    // Flood sends from a background task; the peer will be dropped mid-flood.
    let push2 = push.clone();
    let flood = tokio::spawn(async move {
        for _ in 0..100 {
            let _ = tokio::time::timeout(
                Duration::from_millis(20),
                push2.send(Message::single("flood")),
            )
            .await;
        }
    });

    // Drop the peer while the push engine may still have pending writes.
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(pull1);

    // Spin until the actor finishes teardown and the OS port is free again.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(tcp_ep(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 was dropped");

    // Push must have reconnected; this send must reach pull2.
    push.send(Message::single("after")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer drop timed out")
        .unwrap();

    let _ = flood.await;
}
