//! Linger: `close()` with linger > 0 drains all queued messages before
//! returning. Exercises the send-queue drain path that linger=0 (the
//! default) never touches.

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn free_tcp_ep() -> Endpoint {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    tcp_ep(listener.local_addr().unwrap().port())
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn full_actor_receive_keeps_queries_and_close_responsive_tcp() {
    full_actor_receive_keeps_controls("tcp").await;
}

#[tokio::test]
async fn full_actor_receive_keeps_queries_and_close_responsive_inproc() {
    full_actor_receive_keeps_controls("inproc").await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn full_actor_receive_keeps_queries_and_close_responsive_ws() {
    full_actor_receive_keeps_controls("ws").await;
}

async fn full_actor_receive_keeps_controls(scheme: &str) {
    let receiver = Socket::new(
        SocketType::Router,
        Options::default()
            .recv_hwm(1)
            .linger(Duration::from_millis(50)),
    );
    let endpoint = if scheme == "inproc" {
        inproc_ep("full-actor-receive-controls")
    } else {
        let suffix = if scheme == "ws" { "/" } else { "" };
        format!("{scheme}://127.0.0.1:0{suffix}").parse().unwrap()
    };
    let endpoint = receiver.bind(endpoint).await.unwrap();
    let sender = Socket::new(SocketType::Dealer, Options::default());
    sender.connect(endpoint).await.unwrap();
    sender
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    // The shared receive pipe currently rounds HWM up to at least 16 slots.
    for sequence in 0u8..64 {
        sender
            .send(Message::single(vec![sequence; 128]))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let peers = tokio::time::timeout(Duration::from_millis(250), receiver.connections())
        .await
        .expect("full actor receive must not block socket queries")
        .unwrap();
    assert_eq!(peers.len(), 1);
    // Drain some messages, then fill the application queue again. Identity
    // wrapping must happen only once when the staged delivery is retried.
    for sequence in 0u8..8 {
        let message = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.len(), 2);
        assert!(
            message
                .part_slice(1)
                .unwrap()
                .iter()
                .all(|&b| b == sequence)
        );
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_millis(500), receiver.close())
        .await
        .expect("finite close must reach its deadline with a full receive queue")
        .unwrap();
    sender.close().await.unwrap();
}

#[tokio::test]
async fn full_xpub_notifications_keep_queries_and_close_responsive() {
    let publisher = Socket::new(
        SocketType::XPub,
        Options::default()
            .recv_hwm(1)
            .linger(Duration::from_millis(50)),
    );
    let endpoint = publisher.bind(tcp_ep(0)).await.unwrap();
    let subscriber = Socket::new(SocketType::Sub, Options::default());
    for prefix in 0..32 {
        subscriber
            .subscribe(format!("prefix-{prefix}"))
            .await
            .unwrap();
    }
    subscriber.connect(endpoint).await.unwrap();
    subscriber
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_millis(250), publisher.connections())
        .await
        .expect("full XPUB notifications must not block socket queries")
        .unwrap();
    tokio::time::timeout(Duration::from_millis(500), publisher.close())
        .await
        .expect("finite close must interrupt notification backpressure")
        .unwrap();
    subscriber.close().await.unwrap();
}

#[tokio::test]
async fn linger_stops_new_sends_but_preserves_pre_ready_messages() {
    let endpoint = free_tcp_ep();
    let sender = Socket::new(SocketType::Push, Options::default().linger_forever());
    sender.connect(endpoint.clone()).await.unwrap();
    sender.send(Message::single("accepted")).await.unwrap();
    let handle = sender.clone();
    let close = tokio::spawn(sender.close());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), handle.recv())
            .await
            .unwrap(),
        Err(omq_tokio::Error::Closed)
    ));
    assert!(matches!(
        handle.try_send(Message::single("too late")),
        Err(omq_tokio::TrySendError::Closed)
    ));
    let receiver = Socket::new(SocketType::Pull, Options::default());
    receiver.bind(endpoint).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.part_slice(0).unwrap(), b"accepted");
    tokio::time::timeout(Duration::from_secs(2), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn linger_drains_driver_owned_large_partial_writes_tcp() {
    drain_large_partial_writes("tcp").await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn linger_drains_driver_owned_large_partial_writes_ws() {
    drain_large_partial_writes("ws").await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn linger_drains_driver_owned_large_partial_writes_verified_wss() {
    drain_large_partial_writes("wss").await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn linger_drains_fanout_on_two_io_threads() {
    use omq_tokio::{Context, ContextConfig};

    let context = Context::with_config(ContextConfig { io_threads: 2 });
    let publisher = context.socket(
        SocketType::Pub,
        Options {
            xpub_nodrop: true,
            send_buffer_size: Some(64 * 1024),
            ..Options::default()
                .send_hwm(32)
                .linger(Duration::from_secs(4))
        },
    );
    let endpoint = publisher.bind("ws://127.0.0.1:0/").await.unwrap();
    let mut subscribers = Vec::new();
    for _ in 0..4 {
        let subscriber = Socket::new(SocketType::Sub, Options::default().recv_hwm(1));
        subscriber.subscribe("").await.unwrap();
        subscriber.connect(endpoint.clone()).await.unwrap();
        subscribers.push(subscriber);
    }
    publisher
        .wait_subscribed(4, Duration::from_secs(2))
        .await
        .unwrap();
    for sequence in 0u8..16 {
        publisher
            .send(Message::single(vec![sequence; 256 * 1024]))
            .await
            .unwrap();
    }
    let close = tokio::spawn(publisher.close());
    tokio::time::sleep(Duration::from_millis(30)).await;
    for sequence in 0u8..16 {
        for subscriber in &subscribers {
            let message = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
                .await
                .unwrap()
                .unwrap();
            let body = message.part_slice(0).unwrap();
            assert_eq!(body.len(), 256 * 1024);
            assert!(body.iter().all(|&byte| byte == sequence));
        }
    }
    tokio::time::timeout(Duration::from_secs(2), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for subscriber in subscribers {
        subscriber.close().await.unwrap();
    }
    context.term();
}

async fn drain_large_partial_writes(scheme: &str) {
    const COUNT: u32 = 24;
    let receiver_options = Options::default().recv_hwm(1);
    let sender_options = Options {
        send_buffer_size: Some(64 * 1024),
        ..Options::default()
            .send_hwm(32)
            .linger(Duration::from_secs(4))
    };
    #[cfg(feature = "ws")]
    let (receiver_options, sender_options) = if scheme == "wss" {
        let mut receiver_options = receiver_options;
        let mut sender_options = sender_options;
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let cert = certified.cert.pem().into_bytes();
        receiver_options.wss_tls.server_cert_pem = Some(cert.clone());
        receiver_options.wss_tls.server_key_pem =
            Some(certified.signing_key.serialize_pem().into_bytes());
        sender_options.wss_tls.trust_pem = Some(cert);
        sender_options.wss_tls.trust_system = false;
        (receiver_options, sender_options)
    } else {
        (receiver_options, sender_options)
    };
    let receiver = Socket::new(SocketType::Pull, receiver_options);
    let suffix = if scheme == "tcp" { "" } else { "/" };
    let endpoint = receiver
        .bind(format!("{scheme}://127.0.0.1:0{suffix}"))
        .await
        .unwrap();
    let sender = Socket::new(SocketType::Push, sender_options);
    sender.connect(endpoint).await.unwrap();
    sender
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    for sequence in 0..COUNT {
        let mut body = vec![sequence as u8; 1024 * 1024];
        body[..4].copy_from_slice(&sequence.to_be_bytes());
        sender.send(Message::single(body)).await.unwrap();
    }
    let close = tokio::spawn(sender.close());
    // Force driver-owned data to survive the actor's queue-empty decision.
    tokio::time::sleep(Duration::from_millis(30)).await;
    for sequence in 0..COUNT {
        let message = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap_or_else(|_| panic!("{scheme}: linger lost message {sequence}/{COUNT}"))
            .unwrap();
        let body = message.part_bytes(0).unwrap();
        assert_eq!(body.len(), 1024 * 1024);
        assert_eq!(&body[..4], &sequence.to_be_bytes());
        assert!(body[4..].iter().all(|&byte| byte == sequence as u8));
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    tokio::time::timeout(Duration::from_secs(2), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn linger_nonzero_drains_queued_messages_inproc() {
    const N: u32 = 20;

    let ep = inproc_ep("linger-drain-inproc-tok");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // close() with linger blocks until the queue is drained.
    tokio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.part_bytes(0).unwrap().as_ref().try_into().unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes),
            i,
            "message {i} out of order or missing"
        );
    }
}

#[tokio::test]
async fn linger_nonzero_drains_queued_messages_tcp() {
    const N: u32 = 50;

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(3), push.close())
        .await
        .expect("close timed out — linger drain stalled")
        .unwrap();

    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        let bytes: [u8; 4] = m.part_bytes(0).unwrap().as_ref().try_into().unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes),
            i,
            "message {i} out of order or missing"
        );
    }
}

#[tokio::test]
async fn linger_forever_waits_until_drained() {
    // linger_forever (None) means "wait indefinitely until queue drains".
    // The receiver runs concurrently in a spawned task so that close()
    // can block until the queue is empty without deadlocking.
    const N: u32 = 20;

    let ep = inproc_ep("linger-forever-tok");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 0..N {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // Drain concurrently so close() doesn't wait on an idle consumer.
    let recv_task = tokio::spawn(async move {
        let mut received = Vec::with_capacity(N as usize);
        for _ in 0..N {
            let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
                .await
                .expect("recv timed out in linger_forever task")
                .unwrap();
            let bytes: [u8; 4] = m.part_bytes(0).unwrap().as_ref().try_into().unwrap();
            received.push(u32::from_be_bytes(bytes));
        }
        received
    });

    tokio::time::timeout(Duration::from_secs(2), push.close())
        .await
        .expect("close timed out with linger_forever")
        .unwrap();

    let received = recv_task.await.unwrap();
    for (i, v) in received.into_iter().enumerate() {
        assert_eq!(v, i as u32, "message {i} out of order or missing");
    }
}

#[tokio::test]
async fn linger_forever_close_keeps_dialer_until_late_peer_drains() {
    let ep = free_tcp_ep();
    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep.clone()).await.unwrap();

    push.send(Message::single("queued-before-peer"))
        .await
        .unwrap();

    let close_task = tokio::spawn(async move { push.close().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !close_task.is_finished(),
        "close returned before queued message could drain"
    );

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("late peer did not receive queued message")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"queued-before-peer");

    tokio::time::timeout(Duration::from_secs(2), close_task)
        .await
        .expect("close did not finish after late peer drained")
        .unwrap()
        .unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn drop_with_linger_forever_keeps_dialer_until_late_peer_drains() {
    let ep = free_tcp_ep();
    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep.clone()).await.unwrap();

    push.send(Message::single("queued-before-drop"))
        .await
        .unwrap();
    drop(push);

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("late peer did not receive queued message after drop")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), b"queued-before-drop");

    pull.close().await.unwrap();
}

#[tokio::test]
async fn drop_default_linger_zero_drops_peerless_queue() {
    let ep = free_tcp_ep();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep.clone()).await.unwrap();

    push.send(Message::single("drop-me")).await.unwrap();
    drop(push);

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(300), pull.recv()).await;
    assert!(got.is_err(), "default linger=0 delivered a queued message");

    pull.close().await.unwrap();
}

#[tokio::test]
async fn linger_forever_returns_when_connected_idle() {
    let ep = inproc_ep("linger-forever-idle-tok");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    tokio::time::timeout(Duration::from_millis(500), push.close())
        .await
        .expect("idle close with linger_forever hung")
        .unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn linger_forever_bound_no_peer_returns_when_empty() {
    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.bind(tcp_ep(0)).await.unwrap();

    tokio::time::timeout(Duration::from_millis(500), push.close())
        .await
        .expect("bound no-peer close with empty queue hung")
        .unwrap();
}

#[tokio::test]
async fn linger_zero_returns_immediately_on_close() {
    // Default linger (ZERO): close() does not wait for pending queue;
    // messages that have not yet been delivered are silently dropped.
    // We verify close() returns promptly even with a full queue.
    let ep = inproc_ep("linger-zero-fast-tok");

    let push = Socket::new(SocketType::Push, Options::default()); // linger = ZERO by default
    push.bind(ep.clone()).await.unwrap();

    // No pipe: bound no-peer sends mute; linger=0 must still close quickly.
    let _ = tokio::time::timeout(
        Duration::from_millis(10),
        push.send(Message::single("queued")),
    )
    .await;

    let t0 = std::time::Instant::now();
    push.close().await.unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "linger=0 close took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn close_with_linger_zero_overrides_configured_forever() {
    let ep = inproc_ep("linger-override-zero-tok");

    let push = Socket::new(SocketType::Push, Options::default().linger_forever());
    push.bind(ep).await.unwrap();

    let _ = tokio::time::timeout(
        Duration::from_millis(10),
        push.send(Message::single("queued")),
    )
    .await;

    let t0 = std::time::Instant::now();
    push.close_with_linger(Some(Duration::ZERO)).await.unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "close_with_linger(0) took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn close_with_huge_linger_does_not_panic() {
    let push = Socket::new(SocketType::Push, Options::default());
    tokio::time::timeout(
        Duration::from_millis(500),
        push.close_with_linger(Some(Duration::MAX)),
    )
    .await
    .expect("close with huge linger hung")
    .expect("close with huge linger failed");
}

#[tokio::test]
async fn linger_completes_within_timeout_after_peer_disconnect() {
    // Queued messages cannot be delivered after the peer disconnects.
    // close() with a finite linger must return within the linger window
    // rather than hanging indefinitely waiting for a peer that is gone.
    const LINGER: Duration = Duration::from_millis(300);

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default().linger(LINGER));
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Queue up messages; let the peer receive a few then disconnect.
    for i in 0u32..50 {
        push.send(Message::single(i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }

    // Drain a handful so the connection is live, then drop the peer.
    for _ in 0..5 {
        let _ = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    }
    pull.close().await.unwrap();

    // close() must return within linger + generous slack; it must not block
    // until the linger timeout if the underlying queue could be drained sooner
    // (and it must not hang indefinitely if the peer is gone).
    let t0 = std::time::Instant::now();
    tokio::time::timeout(LINGER + Duration::from_millis(500), push.close())
        .await
        .expect("close() hung past linger timeout after peer disconnect")
        .unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed <= LINGER + Duration::from_millis(500),
        "close took {elapsed:?}, expected ≤ linger({LINGER:?}) + 500 ms"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_linger_interrupts_a_saturated_inproc_subscription_handler() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let publisher = Socket::new(SocketType::XPub, Options::default().recv_hwm(1));
    let endpoint = publisher
        .bind(inproc_ep("linger-subscription-flood"))
        .await
        .unwrap();
    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(endpoint).await.unwrap();
    subscriber
        .wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();
    let sent = Arc::new(AtomicUsize::new(0));
    let producer = subscriber.clone();
    let progress = sent.clone();
    let flood = tokio::spawn(async move {
        for n in 0..4000 {
            if producer.subscribe(format!("flood-{n}")).await.is_err() {
                return;
            }
            progress.fetch_add(1, Ordering::Release);
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while sent.load(Ordering::Acquire) <= 1024 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        sent.load(Ordering::Acquire) < 4000,
        "notification backpressure must reach the subscriber"
    );
    let emergency = subscriber.clone();
    let closed = tokio::time::timeout(
        Duration::from_millis(500),
        subscriber.close_with_linger(Some(Duration::from_millis(50))),
    )
    .await;
    // Cleanup remains reachable even when the finite close regression fails.
    emergency
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();
    flood.abort();
    let _ = flood.await;
    publisher.close().await.unwrap();
    closed
        .expect("finite linger must include actor command admission")
        .unwrap();
}
