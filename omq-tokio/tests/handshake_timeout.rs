//! Bind a raw TCP listener that accepts then sits silent. The
//! omq-tokio driver should hit its handshake deadline and tear the
//! connection down. Connect-side round-robin sends queue only in the
//! endpoint's pre-ready pipe before READY; after reconnect is disabled
//! and that route is removed, sends mute.
//!
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

#[tokio::test]
async fn null_refusal_stops_only_its_endpoint() {
    use omq_tokio::proto::greeting::{Greeting, MechanismName};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let refused_endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let healthy = Socket::new(SocketType::Pull, Options::default());
    let healthy_endpoint = healthy
        .bind("tcp://127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let push = Socket::new(
        SocketType::Push,
        Options::default().reconnect(ReconnectPolicy::Fixed(Duration::from_millis(10))),
    );
    push.connect(healthy_endpoint).await.unwrap();
    push.wait_connected(1, Duration::from_secs(1))
        .await
        .unwrap();
    let mut monitor = push.monitor();
    push.connect(refused_endpoint.clone()).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    peer.read_exact(&mut [0; 64]).await.unwrap();
    let mut greeting = bytes::BytesMut::new();
    Greeting::current(MechanismName::NULL, false).encode(&mut greeting);
    peer.write_all(&greeting).await.unwrap();
    peer.write_all(b"\x04\x0b\x05ERROR\x04auth").await.unwrap();
    peer.shutdown().await.unwrap();
    drop(peer);

    let (endpoint, refusal) = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let MonitorEvent::ConnectStopped {
                endpoint,
                reason: omq_tokio::DisconnectReason::HandshakeRefused(refusal),
            } = monitor.recv().await.unwrap()
            {
                break (endpoint, refusal);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(endpoint, refused_endpoint);
    assert_eq!(refusal.mechanism, MechanismName::NULL);
    assert_eq!(refusal.reason, "auth");
    assert_eq!(refusal.status_code(), None);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "fatal ERROR must stop automatic retries"
    );
    push.send(Message::single("healthy route")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), healthy.recv())
            .await
            .unwrap()
            .unwrap(),
        Message::single("healthy route")
    );
    push.close().await.unwrap();
    healthy.close().await.unwrap();
}

#[tokio::test]
async fn connect_to_silent_peer_queues_until_pre_ready_pipe_full() {
    let listener = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();

    let accept_handle = std::thread::spawn(move || {
        let (s, _addr) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(800));
        drop(s);
    });

    let hwm: u32 = 4;
    let opts = Options {
        handshake_timeout: Some(Duration::from_millis(300)),
        reconnect: ReconnectPolicy::Disabled,
        send_hwm: hwm,
        ..Default::default()
    };
    let push = Socket::new(SocketType::Push, opts);
    push.connect(Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    })
    .await
    .unwrap();

    let mut accepted = 0usize;
    for _ in 0..=(hwm as usize) {
        match tokio::time::timeout(Duration::from_millis(50), push.send(Message::single("x"))).await
        {
            Ok(Ok(())) => accepted += 1,
            _ => break,
        }
    }
    assert_eq!(
        accepted as u32, hwm,
        "pre-ready pipe should buffer up to send_hwm before blocking"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            push.send(Message::single("after"))
        )
        .await
        .is_err(),
        "reconnect-disabled route should mute after handshake failure"
    );

    let _ = accept_handle.join();
}

#[tokio::test]
async fn failed_driver_handshake_waits_before_reconnecting() {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let push = Socket::new(
        SocketType::Push,
        Options {
            handshake_timeout: Some(Duration::from_millis(80)),
            reconnect: ReconnectPolicy::Exponential {
                min: Duration::from_millis(200),
                max: Duration::from_millis(800),
            },
            ..Options::default()
        },
    );
    let mut monitor = push.monitor();
    push.connect(endpoint).await.unwrap();
    let (mut silent_peer, _) = listener.accept().await.unwrap();
    for (attempt, interval_ms) in [(1, 200), (2, 400)] {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(
                monitor.recv().await.unwrap(),
                MonitorEvent::HandshakeFailed { .. }
            ) {}
        })
        .await
        .unwrap();
        let failed_at = tokio::time::Instant::now();
        let (retry, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert!(
            failed_at.elapsed() >= Duration::from_millis(interval_ms - 50),
            "driver failure bypassed the configured reconnect delay"
        );
        assert!(matches!(
            monitor.recv().await.unwrap(),
            MonitorEvent::ConnectDelayed { retry_in, attempt: observed, .. }
                if retry_in >= Duration::from_millis(interval_ms) && observed == attempt
        ));
        silent_peer = retry;
    }
    drop(silent_peer);
    push.close().await.unwrap();
}
