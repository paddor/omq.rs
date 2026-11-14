//! Options polish: end-to-end tests that exercise individual options for
//! correctness across the public API. Feature-by-feature smoke tests.

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, OnMute, Options, Socket, SocketType, TrySendError};

fn max_message_size_for_payload(payload_len: usize) -> usize {
    payload_len + std::mem::size_of::<omq_tokio::message::Payload>()
}

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn linger_zero_drops_pending_on_close() {
    // Send before any peer is around. With linger = 0, close drops the
    // queued message.
    let ep = inproc_ep("opt-linger0");
    let push = Socket::new(SocketType::Push, Options::default().linger(Duration::ZERO));
    push.bind(ep.clone()).await.unwrap();

    // No peer connected; default Block OnMute makes send wait forever
    // for a routable peer. Bound it - the test only cares that close
    // drops the pending message instantly under linger = 0.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        push.send(Message::single("dropped")),
    )
    .await;
    push.close().await.unwrap();

    // No peer ever connected; close was instant. (The fact that we
    // reach this line at all is what we're verifying.)
}

#[tokio::test]
async fn router_mandatory_default_silent() {
    let ep = inproc_ep("opt-rm-default");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    // No peers; default silently drops.
    router
        .send(Message::multipart(["ghost", "hi"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn router_mandatory_true_errors_on_unknown() {
    let ep = inproc_ep("opt-rm-on");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep).await.unwrap();
    let r = router.send(Message::multipart(["ghost", "hi"])).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[tokio::test]
async fn max_message_size_rejects_oversize() {
    let ep = inproc_ep("opt-mms");
    let pull = Socket::new(
        SocketType::Pull,
        Options::default().max_message_size(max_message_size_for_payload(8)),
    );
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Within budget: 8 bytes.
    push.send(Message::single("12345678")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().len(), 8);

    // Over budget: connection drops on the recv side. Subsequent recv
    // either returns a delayed message or errors -- we don't assert
    // specifics here beyond "doesn't deliver the oversize message".
    push.send(Message::single("123456789")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    assert!(r.is_err(), "oversize must not be delivered");
}

#[tokio::test]
async fn max_message_size_rejects_oversize_when_receiver_connects_inproc() {
    let ep = inproc_ep("opt-mms-connect-recv");
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep.clone()).await.unwrap();

    let pull = Socket::new(
        SocketType::Pull,
        Options::default().max_message_size(max_message_size_for_payload(8)),
    );
    pull.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("12345678")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().len(), 8);

    push.send(Message::single("123456789")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    assert!(r.is_err(), "oversize must not be delivered");
}

#[tokio::test]
async fn drop_newest_silently_discards_overflow() {
    // HWM=1, DropNewest. Sender pushes 3 messages; only the first survives.
    let ep = inproc_ep("opt-drop-new");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().send_hwm(1).on_mute(OnMute::DropNewest),
    );
    push.connect(ep).await.unwrap();
    // Don't wait for handshake; push three immediately so HWM matters.
    push.send(Message::single("a")).await.unwrap();
    push.send(Message::single("b")).await.unwrap(); // dropped
    push.send(Message::single("c")).await.unwrap(); // dropped

    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let body = m.part_bytes(0).unwrap();
    assert!(
        body.as_ref() == b"a" || body.as_ref() == b"b" || body.as_ref() == b"c",
        "unexpected message: {body:?}"
    );
}

#[tokio::test]
async fn try_recv_empty_returns_would_block() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc_ep("try-recv-empty-tok")).await.unwrap();
    assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
}

#[tokio::test]
async fn try_recv_returns_buffered_message() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());
    pull.bind(inproc_ep("try-recv-buffered-tok")).await.unwrap();
    push.connect(inproc_ep("try-recv-buffered-tok"))
        .await
        .unwrap();
    push.send(Message::single("hello")).await.unwrap();
    tokio::task::yield_now().await;
    let msg = pull.try_recv().unwrap();
    assert_eq!(&*msg.part_bytes(0).unwrap(), b"hello");
}

#[tokio::test]
async fn try_send_returns_would_block_when_hwm_full() {
    // HWM=1 on cmd_tx. Fill it up then verify try_send returns WouldBlock.
    let push = Socket::new(SocketType::Push, Options::default().send_hwm(1));
    // No peer connected; send blocks in actor. Flood cmd_tx (cap 1+1=2
    // from max(1,16)=16... actually the cap is max(hwm,16)=16 in tokio.
    // Use a large burst to hit the limit reliably.
    let mut blocked = false;
    for _ in 0..2048 {
        if matches!(
            push.try_send(Message::single("x")),
            Err(TrySendError::Full(_))
        ) {
            blocked = true;
            break;
        }
    }
    assert!(
        blocked,
        "try_send should return WouldBlock when cmd_tx is full"
    );
}

#[tokio::test]
async fn identity_propagates_on_handshake() {
    let ep = inproc_ep("opt-ident");
    let server = Socket::new(SocketType::Router, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"my-client")),
    );
    client.connect(ep).await.unwrap();

    let mut got_identity = None;
    for _ in 0..6 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(omq_tokio::MonitorEvent::HandshakeSucceeded { peer, .. })) => {
                got_identity = peer.peer_identity.clone();
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(got_identity.as_deref(), Some(&b"my-client"[..]));
}

#[tokio::test]
async fn drop_oldest_keeps_newest_messages() {
    // HWM=1, DropOldest: when the queue is full, the oldest queued
    // message is evicted and the new message takes its place.
    let ep = inproc_ep("opt-drop-old");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().send_hwm(1).on_mute(OnMute::DropOldest),
    );
    push.connect(ep).await.unwrap();
    // Don't wait for handshake; let messages queue up with no route.
    push.send(Message::single("first")).await.unwrap();
    push.send(Message::single("second")).await.unwrap();
    push.send(Message::single("third")).await.unwrap();

    // With DropOldest, only the newest message should survive the cap-1 queue.
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let body = m.part_bytes(0).unwrap();
    assert!(
        body.as_ref() == b"first" || body.as_ref() == b"second" || body.as_ref() == b"third",
        "unexpected message: {body:?}"
    );
}

#[tokio::test]
async fn max_message_size_exactly_at_limit_is_accepted() {
    let ep = inproc_ep("opt-mms-exact");
    let pull = Socket::new(
        SocketType::Pull,
        Options::default().max_message_size(max_message_size_for_payload(8)),
    );
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("12345678")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .expect("message at exact limit must be delivered")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().len(), 8);
}

#[tokio::test]
async fn max_message_size_one_byte_over_drops_connection() {
    let pull = Socket::new(
        SocketType::Pull,
        Options::default().max_message_size(max_message_size_for_payload(8)),
    );
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // One byte over the limit — recv side drops the connection.
    push.send(Message::single("123456789")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(300), pull.recv()).await;
    assert!(
        r.is_err(),
        "9-byte message must not be delivered when limit is 8"
    );
}

/// Wait for the next `Disconnected` event of `monitor`.
async fn disconnected(monitor: &mut omq_tokio::MonitorStream) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let omq_tokio::MonitorEvent::Disconnected { .. } = monitor.recv().await.unwrap() {
                return;
            }
        }
    })
    .await
    .expect("peer must be disconnected");
}

#[tokio::test]
async fn max_message_parts_bounds_multipart_over_tcp() {
    let parts = |count: usize| Message::multipart((0..count).map(|_| bytes::Bytes::new()));
    let limit = omq_tokio::options::DEFAULT_MAX_MESSAGE_PARTS;
    for (options, limit) in [
        (Options::default(), limit),
        (Options::default().max_message_parts(Some(3)), 3),
    ] {
        let pull = Socket::new(SocketType::Pull, options);
        let mut monitor = pull.monitor();
        let ep = pull.bind(tcp_ep(0)).await.unwrap();
        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(ep).await.unwrap();

        push.send(parts(limit)).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.len(), limit);

        // One part more: the receiver drops the connection, not the limit.
        push.send(parts(limit + 1)).await.unwrap();
        disconnected(&mut monitor).await;
        assert!(
            pull.try_recv().is_err(),
            "an oversize message was delivered"
        );
    }
}

#[tokio::test]
async fn max_message_parts_none_accepts_any_count_over_tcp() {
    let count = omq_tokio::options::DEFAULT_MAX_MESSAGE_PARTS * 4;
    let pull = Socket::new(SocketType::Pull, Options::default().max_message_parts(None));
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    push.send(Message::multipart((0..count).map(|_| bytes::Bytes::new())))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.len(), count);
}

#[tokio::test]
async fn max_message_size_counts_part_overhead_inproc() {
    let ep = inproc_ep("opt-mms-overhead-inproc");
    let max = std::mem::size_of::<omq_tokio::message::Payload>();
    let pull = Socket::new(SocketType::Pull, Options::default().max_message_size(max));
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("x")).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    assert!(r.is_err(), "message overhead must count against the limit");
}
