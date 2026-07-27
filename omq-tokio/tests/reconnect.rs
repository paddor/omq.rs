//! Reconnect/backoff: dialer reconnects when a listener appears late,
//! restarts mid-session, or drops abruptly while sends are in-flight.

mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, MonitorEvent, OnMute, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[tokio::test]
async fn connect_retries_until_listener_appears() {
    let (ep_tx, ep_rx) = tokio::sync::oneshot::channel();

    let bind_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let pull = Socket::new(SocketType::Pull, Options::default());
        let ep = pull.bind(tcp_ep(0)).await.unwrap();
        ep_tx.send(ep).unwrap();
        pull
    });

    let ep = ep_rx.await.unwrap();

    let opts = Options {
        reconnect: ReconnectPolicy::Exponential {
            min: Duration::from_millis(20),
            max: Duration::from_millis(80),
        },
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();

    push.send(Message::single("eventually")).await.unwrap();
    let pull = bind_handle.await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m, Message::single("eventually"));
}

#[tokio::test]
async fn reconnect_after_peer_restart() {
    // Peer (listener side) shuts down mid-session; dialer reconnects to the
    // same endpoint when a fresh listener appears.
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    let mut mon = push.monitor();
    push.connect(ep.clone()).await.unwrap();
    test_support::wait_for_handshake_on(&mut mon).await;

    // Confirm the session is live before simulating the peer restart.
    push.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), pull1.recv())
        .await
        .expect("initial recv timed out")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"before");

    // Peer restarts: close cleanly so the actor tears down.
    pull1.close().await.unwrap();

    // Spin until the OS port is free and the new listener is up.
    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    test_support::wait_for_handshake_on(&mut mon).await;
    push.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after peer restart timed out")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"after");
}

#[tokio::test]
async fn peer_drop_mid_send_is_handled_cleanly() {
    // Peer is dropped while the push engine has in-flight sends. The socket
    // must not panic or deadlock; it reconnects and resumes delivery.
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Default::default()
        },
    );
    let mut mon = push.monitor();
    push.connect(ep.clone()).await.unwrap();

    // Confirm handshake before flooding.
    push.send(Message::single("sync")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), pull1.recv())
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
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 was dropped");

    // Wait for the second handshake on the push side. Without this, a `send`
    // racing the disconnect can be committed to the dying peer's pipe and
    // lost. ZMQ does not guarantee delivery for messages already accepted by
    // a transport that dies before the peer reads them.
    // Synchronizing on the new HandshakeSucceeded means the next send routes
    // to the live peer, exercising "reconnects and resumes delivery" without
    // depending on in-flight survival.
    let mut handshakes = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handshakes < 2 {
        let evt = tokio::time::timeout_at(deadline, mon.recv())
            .await
            .expect("timed out waiting for second HandshakeSucceeded")
            .unwrap();
        if matches!(evt, MonitorEvent::HandshakeSucceeded { .. }) {
            handshakes += 1;
        }
    }

    // Push has reconnected; this send must reach pull2.
    push.send(Message::single("after")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after peer drop timed out")
        .unwrap();

    let _ = flood.await;
}

#[tokio::test]
async fn exponential_backoff_retry_in_grows() {
    // ConnectDelayed monitor events carry the configured retry_in duration.
    // With ExponentialPolicy{min, max}, successive retry_in values must be
    // non-decreasing and at least as large as `min`.
    use omq_tokio::MonitorEvent;

    // Nothing is listening; every dial attempt will fail immediately.
    // Bind+close to discover a free port that has no listener.
    let free_port = {
        use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
        let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Exponential {
                min: Duration::from_millis(20),
                max: Duration::from_millis(200),
            },
            ..Default::default()
        },
    );
    let mut mon = push.monitor();
    push.connect(tcp_ep(free_port)).await.unwrap();

    // Collect the first 4 ConnectDelayed events.
    let mut delays: Vec<Duration> = Vec::new();
    // 10 s: Windows CI runners are slower than Linux.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while delays.len() < 4 {
        let evt = tokio::time::timeout_at(deadline, mon.recv())
            .await
            .expect("timed out waiting for ConnectDelayed events")
            .unwrap();
        if let MonitorEvent::ConnectDelayed { retry_in, .. } = evt {
            delays.push(retry_in);
        }
    }

    // Each retry_in must be >= min.
    for (i, &d) in delays.iter().enumerate() {
        assert!(
            d >= Duration::from_millis(20),
            "delay[{i}] = {d:?} is below min (20 ms)"
        );
    }
    // Delays must be non-decreasing (exponential growth or plateau at max).
    for i in 1..delays.len() {
        assert!(
            delays[i] >= delays[i - 1],
            "delay[{i}] = {:?} decreased from delay[{}] = {:?}",
            delays[i],
            i - 1,
            delays[i - 1]
        );
    }
}

#[tokio::test]
async fn push_hwm_drains_after_reconnect() {
    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let ep = pull1.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default()
            .send_hwm(4)
            .on_mute(OnMute::DropNewest)
            .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(30))),
    );
    push.connect(ep.clone()).await.unwrap();
    test_support::wait_for_handshake(&push).await;

    push.send(Message::single("warmup")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pull1.recv())
        .await
        .expect("warmup timed out")
        .unwrap();

    pull1.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..10 {
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            push.send(Message::single(format!("q{i}"))),
        )
        .await;
    }

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..40 {
        if pull2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to rebind");

    push.send(Message::single("final")).await.unwrap();

    let mut count = 0;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), pull2.recv()).await {
        count += 1;
        if count > 10 {
            break;
        }
    }
    assert!(count >= 1, "expected at least 1 message, got {count}");
}
