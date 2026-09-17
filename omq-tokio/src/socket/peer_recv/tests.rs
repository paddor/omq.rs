use super::*;

fn poll_once<F: std::future::Future>(future: F) -> std::task::Poll<F::Output> {
    let waker = std::task::Waker::noop();
    std::pin::pin!(future).poll(&mut std::task::Context::from_waker(waker))
}

fn register(routes: &mut PeerRecvRoutes, identity: &'static str) -> PeerRecvSink {
    routes
        .register(
            Bytes::from_static(identity.as_bytes()),
            CancellationToken::new(),
        )
        .unwrap()
}

fn put(sink: &mut PeerRecvSink, value: &'static str) {
    assert!(sink.push(Message::single(value)));
    sink.flush();
}

#[test]
fn removing_peer_at_wraparound_does_not_skip_ready_peer() {
    let (mut routes, mut lanes) = PeerRecvRoutes::new(PeerRecvConfig::new(1), 16).unwrap();
    let a = register(&mut routes, "a");
    let mut b = register(&mut routes, "b");
    let _c = register(&mut routes, "c");
    lanes[0].refresh();
    lanes[0].cursor = 2;
    drop(a);
    put(&mut b, "ready");
    assert_eq!(
        lanes[0].try_recv().unwrap().part_slice(0),
        Some(b"b".as_slice())
    );
}

#[test]
fn handover_discards_old_queue_and_waits_for_both_halves_to_release_allocation() {
    let mut config = PeerRecvConfig::new(1);
    config.max_peers_per_lane = 2;
    let (mut routes, mut lanes) = PeerRecvRoutes::new(config, 16).unwrap();
    let mut old = register(&mut routes, "a");
    put(&mut old, "old");
    let mut next = register(&mut routes, "a");
    put(&mut next, "next");
    assert_eq!(
        lanes[0].try_recv().unwrap().part_slice(1),
        Some(b"next".as_slice())
    );
    assert!(
        routes
            .register(Bytes::from_static(b"a"), CancellationToken::new())
            .is_err()
    );
    drop(old);
    let _replacement = register(&mut routes, "a");
}

#[tokio::test]
async fn drains_full_queue_and_wakes_without_a_timer() {
    let (mut routes, mut lanes) = PeerRecvRoutes::new(PeerRecvConfig::new(1), 16).unwrap();
    let mut sink = register(&mut routes, "a");
    for _ in 0..17 {
        put(&mut sink, "value");
    }
    assert!(sink.blocked());
    assert!(poll_once(sink.ready()).is_pending());
    lanes[0].try_recv().unwrap();
    assert!(poll_once(sink.ready()).is_ready());
    assert!(sink.retry_pending());
    let mut out = Vec::with_capacity(16);
    assert_eq!(lanes[0].try_recv_many_into(1024, &mut out).unwrap(), 16);
    assert!(matches!(lanes[0].try_recv(), Err(Error::WouldBlock)));
    put(&mut sink, "after idle");
    assert!(poll_once(lanes[0].recv()).is_ready());
}

#[test]
fn aggregate_count_and_bytes_backpressure_across_peers() {
    for (messages, bytes) in [
        (2, 1024),
        (
            100,
            8 + 2 * std::mem::size_of::<omq_proto::message::Payload>(),
        ),
    ] {
        let mut config = PeerRecvConfig::new(1);
        config.max_messages_per_lane = messages;
        config.max_bytes_per_lane = bytes;
        let (mut routes, mut lanes) = PeerRecvRoutes::new(config, 16).unwrap();
        let mut a = register(&mut routes, "a");
        let mut b = register(&mut routes, "b");
        put(&mut a, "1234");
        put(&mut b, "5678");
        put(&mut b, "next");
        assert!(b.blocked());
        lanes[0].try_recv().unwrap();
        assert!(b.retry_pending());
        assert!(!b.blocked());
    }
}

#[test]
fn empty_frames_cannot_bypass_receive_byte_budget() {
    let mut config = PeerRecvConfig::new(1);
    config.max_bytes_per_lane = 256;
    let (mut routes, mut lanes) = PeerRecvRoutes::new(config, 16).unwrap();
    let mut sink = register(&mut routes, "a");
    assert!(sink.push(Message::multipart(["", "", ""])));
    sink.flush();
    assert!(sink.push(Message::multipart(["", ""])));
    assert!(sink.blocked());
    lanes[0].try_recv().unwrap();
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert!(!sink.push(Message::multipart(["", "", "", "", ""])));
}

#[test]
fn dropping_lane_cancels_idle_and_full_peers_and_releases_budgets() {
    let (mut routes, lanes) = PeerRecvRoutes::new(PeerRecvConfig::new(1), 16).unwrap();
    let mut busy = register(&mut routes, "busy");
    let idle = register(&mut routes, "idle");
    let busy_state = routes.previous[b"busy".as_slice()].upgrade().unwrap();
    let idle_state = routes.previous[b"idle".as_slice()].upgrade().unwrap();
    for _ in 0..17 {
        put(&mut busy, "busy");
    }
    let budget = lanes[0].shared.budget.clone();
    drop(lanes);
    assert!(busy_state.cancel.is_cancelled());
    assert!(idle_state.cancel.is_cancelled());
    assert!(!busy.retry_pending());
    drop((busy, idle));
    assert!(budget.room(64 * 1024 * 1024));
}

#[tokio::test]
async fn socket_shutdown_wakes_idle_lane_and_disconnect_keeps_queued_messages() {
    let (mut routes, mut lanes) = PeerRecvRoutes::new(PeerRecvConfig::new(1), 16).unwrap();
    let mut sink = register(&mut routes, "a");
    put(&mut sink, "last");
    drop(sink);
    assert_eq!(
        lanes[0].recv().await.unwrap().part_slice(1),
        Some(b"last".as_slice())
    );
    assert!(poll_once(lanes[0].recv()).is_pending());
    drop(routes);
    assert!(matches!(lanes[0].recv().await, Err(Error::Closed)));
}

#[tokio::test]
async fn closing_receive_wakes_lane_without_canceling_outbound_linger() {
    let (mut routes, mut lanes) = PeerRecvRoutes::new(PeerRecvConfig::new(1), 16).unwrap();
    let mut sink = register(&mut routes, "a");
    let state = routes.previous[b"a".as_slice()].upgrade().unwrap();
    assert!(poll_once(lanes[0].recv()).is_pending());
    routes.close_receive();
    assert!(matches!(lanes[0].recv().await, Err(Error::Closed)));
    assert!(!state.cancel.is_cancelled());
    assert!(sink.retry_pending());
    put(&mut sink, "closed admission");
    assert!(matches!(lanes[0].try_recv(), Err(Error::Closed)));
}
