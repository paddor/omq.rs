//! Reconnect/backoff: dialer reconnects when a listener appears late,
//! restarts mid-session, or drops abruptly while sends are in-flight.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_compio::endpoint::Host;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

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

#[compio::test]
async fn connect_retries_until_listener_appears() {
    let port = loopback_port();

    // Spawn the bind after a short delay; first dials should fail
    // and get backed off until we appear.
    let pull_ep = tcp_ep(port);
    let bind_handle = compio::runtime::spawn(async move {
        compio::time::sleep(Duration::from_millis(150)).await;
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
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"eventually"[..]);
}

#[compio::test]
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
    let m = compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"before");

    // Peer restarts: close cleanly. close() cancels listener tasks
    // immediately so the OS port is freed as the runtime processes
    // the io_uring cancellations.
    pull1.close().await.unwrap();

    // Spin until the runtime has processed the listener cancellation
    // and the OS port is free again.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(tcp_ep(port)).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    push.send(Message::single("after")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer restart timed out")
        .unwrap();
    assert_eq!(&*m.parts()[0].coalesce(), b"after");
}

#[compio::test]
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
    compio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("sync recv timed out")
        .unwrap();

    // Flood sends from a background task; pull1 will be closed mid-flood.
    let push2 = push.clone();
    let flood = compio::runtime::spawn(async move {
        for _ in 0..100 {
            let _ = compio::time::timeout(
                Duration::from_millis(20),
                push2.send(Message::single("flood")),
            )
            .await;
        }
    });

    // Close pull1 while the push engine may still have pending writes.
    // close() cancels listener and dialer tasks immediately.
    compio::time::sleep(Duration::from_millis(10)).await;
    pull1.close().await.unwrap();

    // Spin until the runtime has processed the listener cancellation
    // and the OS port is free again.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(tcp_ep(port)).await.is_ok() {
            bound = true;
            break;
        }
        compio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 was dropped");

    // Push must have reconnected; this send must reach pull2.
    push.send(Message::single("after")).await.unwrap();
    compio::time::timeout(Duration::from_secs(2), pull2.recv())
        .await
        .expect("recv after peer drop timed out")
        .unwrap();

    let _ = flood.await;
}
