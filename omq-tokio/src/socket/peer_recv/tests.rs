use super::*;
use crate::socket::recv::{BlockingRecvWaker, SpscAwareRecv, SpscHandles, recv_pipe};

fn receive_pair(limits: RecvLimits, hwm: usize) -> (PeerRecvRoutes, PeerReceiver) {
    let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
    PeerRecvRoutes::with_limits(hwm, &handles, limits)
}

fn ordinary_receive(
    hwm: usize,
) -> (
    PeerRecvRoutes,
    SpscAwareRecv,
    Arc<crate::socket::recv::SharedRecvPipe>,
) {
    let blocking = BlockingRecvWaker::new();
    let mut handles = SpscHandles::new(blocking.clone(), false);
    let routes = handles.init_peer_recv(hwm, None);
    let (pipe, consumer, notify, space) = recv_pipe(hwm, blocking);
    let receiver = SpscAwareRecv::new(consumer, notify, space, handles, false, false);
    (routes, receiver, pipe)
}

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

struct WakeCount(std::sync::atomic::AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn canceled_waiter_hands_off_batch_without_consuming() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    for cancel_first in [true, false] {
        let (mut routes, receiver, _pipe) = ordinary_receive(16);
        let mut sink = register(&mut routes, "a");
        let counts: [_; 3] = std::array::from_fn(|_| Arc::new(WakeCount(0.into())));
        let wakers = counts
            .each_ref()
            .map(|counter| Waker::from(counter.clone()));
        let mut waiting = std::array::from_fn::<_, 3, _>(|_| Some(Box::pin(receiver.recv())));
        for (future, waker) in waiting.iter_mut().zip(&wakers) {
            assert!(
                future
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_pending()
            );
        }
        for value in ["first", "second", "third"] {
            assert!(sink.push(Message::single(value)));
        }
        sink.flush();
        let mut messages = Vec::new();
        if cancel_first {
            drop(waiting[0].take());
        } else {
            let Poll::Ready(Ok(message)) = waiting[0]
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[0]))
            else {
                panic!("first receive not ready");
            };
            messages.push(message);
            drop(waiting[1].take());
        }
        for index in if cancel_first { 1..3 } else { 2..3 } {
            assert!(
                counts[index].0.load(Ordering::Relaxed) > 0,
                "receiver {index} stranded after cancellation"
            );
            let Poll::Ready(Ok(message)) = waiting[index]
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[index]))
            else {
                panic!("receive not ready");
            };
            messages.push(message);
        }
        messages.push(receiver.try_recv().unwrap());
        for (message, expected) in messages.iter().zip(["first", "second", "third"]) {
            assert_eq!(message.part_slice(1), Some(expected.as_bytes()));
        }
        assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    }
}

#[test]
fn repeated_receives_coalesce_handoffs_to_a_parked_waiter() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let (mut routes, receiver, _pipe) = ordinary_receive(16);
    let mut sink = register(&mut routes, "a");
    let counter = Arc::new(WakeCount(0.into()));
    let waker = Waker::from(counter.clone());
    let mut waiting = Box::pin(receiver.recv());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    for _ in 0..16 {
        assert!(sink.push(Message::single("value")));
    }
    sink.flush();
    receiver.try_recv().unwrap();
    let counts = counter.0.load(Ordering::Relaxed);
    for _ in 0..14 {
        receiver.try_recv().unwrap();
    }
    assert_eq!(counter.0.load(Ordering::Relaxed), counts);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn one_batch_wakes_two_parked_socket_receivers() {
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (mut routes, receiver, _pipe) = ordinary_receive(16);
    let mut sink = register(&mut routes, "a");
    let counts = [
        Arc::new(Wakes(AtomicUsize::new(0))),
        Arc::new(Wakes(AtomicUsize::new(0))),
    ];
    let wakers = counts
        .each_ref()
        .map(|counter| Waker::from(counter.clone()));
    let mut first = std::pin::pin!(receiver.recv());
    let mut second = std::pin::pin!(receiver.recv());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&wakers[0]))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&wakers[1]))
            .is_pending()
    );
    assert!(sink.push(Message::single("first")));
    assert!(sink.push(Message::single("second")));
    sink.flush();
    assert!(counts[0].0.load(Ordering::Relaxed) > 0);
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&wakers[0]))
            .is_ready()
    );
    assert!(
        counts[1].0.load(Ordering::Relaxed) > 0,
        "second parked receiver needs a wake"
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&wakers[1]))
            .is_ready()
    );
}

#[test]
#[ignore = "local sparse-peer receive profiling"]
fn profile_sparse_peer_receive() {
    let idle: usize =
        std::env::var("OMQ_PEER_PROFILE_IDLE").map_or(1024, |value| value.parse().unwrap());
    let messages: usize =
        std::env::var("OMQ_PEER_PROFILE_MESSAGES").map_or(200_000, |value| value.parse().unwrap());
    let config = RecvLimits {
        peers: idle + 1,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 16);
    let quiet: Vec<_> = (0..idle)
        .map(|id| {
            routes
                .register(Bytes::from(id.to_string()), CancellationToken::new())
                .unwrap()
        })
        .collect();
    let mut active = register(&mut routes, "active");
    let start = std::time::Instant::now();
    for _ in 0..messages {
        put(&mut active, "payload");
        let message = receiver.try_recv().unwrap();
        assert_eq!(message.part_slice(1), Some(b"payload".as_slice()));
        std::hint::black_box(message);
    }
    println!(
        "idle={idle} messages={messages} msg_s={:.0}",
        messages as f64 / start.elapsed().as_secs_f64()
    );
    drop(quiet);
}

#[test]
fn removing_peer_at_wraparound_does_not_skip_ready_peer() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let a = register(&mut routes, "a");
    let mut b = register(&mut routes, "b");
    let mut c = register(&mut routes, "c");
    put(&mut c, "first");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(0),
        Some(b"c".as_slice())
    );
    drop(a);
    put(&mut b, "ready");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(0),
        Some(b"b".as_slice())
    );
}

#[test]
fn handover_discards_old_queue_and_waits_for_both_halves_to_release_allocation() {
    let config = RecvLimits {
        peers: 2,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 16);
    let mut old = register(&mut routes, "a");
    put(&mut old, "old");
    let mut next = register(&mut routes, "a");
    put(&mut next, "next");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"next".as_slice())
    );
    assert!(
        routes
            .register(Bytes::from_static(b"a"), CancellationToken::new())
            .is_err()
    );
    drop(old);
    assert!(
        routes
            .register(Bytes::from_static(b"a"), CancellationToken::new())
            .is_err()
    );
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    let _replacement = register(&mut routes, "a");
}

#[tokio::test]
async fn drains_full_queue_and_wakes_without_a_timer() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let mut sink = register(&mut routes, "a");
    for _ in 0..17 {
        put(&mut sink, "value");
    }
    assert!(sink.blocked());
    assert!(poll_once(sink.ready()).is_pending());
    for _ in 0..16 {
        assert!(poll_once(sink.ready()).is_pending());
        receiver.try_recv().unwrap();
    }
    assert!(poll_once(sink.ready()).is_ready());
    assert!(sink.retry_pending());
    let mut out = Vec::with_capacity(16);
    assert_eq!(receiver.try_recv_many_into(1024, &mut out).unwrap(), 1);
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    put(&mut sink, "after idle");
    assert!(receiver.try_recv().is_ok());
}

#[test]
fn full_producer_waits_for_half_ring_credits() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 256);
    let mut sink = register(&mut routes, "a");
    for _ in 0..257 {
        put(&mut sink, "value");
    }
    assert!(sink.blocked());
    for _ in 0..128 {
        assert!(poll_once(sink.ready()).is_pending());
        receiver.try_recv().unwrap();
    }
    assert!(poll_once(sink.ready()).is_ready());
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
}

#[test]
fn idle_peers_do_not_interfere_with_hot_quiet_fairness_or_fifo() {
    let config = RecvLimits {
        peers: 1026,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 256);
    let _idle: Vec<_> = (0..1024)
        .map(|id| {
            routes
                .register(Bytes::from(id.to_string()), CancellationToken::new())
                .unwrap()
        })
        .collect();
    let mut hot = register(&mut routes, "hot");
    let mut quiet = register(&mut routes, "quiet");
    for sequence in 0u32..256 {
        assert!(hot.push(Message::single(Bytes::copy_from_slice(
            &sequence.to_le_bytes()
        ))));
    }
    hot.flush();
    for sequence in 0u32..256 {
        put(&mut quiet, "quiet");
        let pair = [receiver.try_recv().unwrap(), receiver.try_recv().unwrap()];
        assert_eq!(
            pair.iter()
                .filter(|message| message.part_slice(0) == Some(b"quiet".as_slice()))
                .count(),
            1
        );
        let message = pair
            .iter()
            .find(|message| message.part_slice(0) == Some(b"hot".as_slice()))
            .unwrap();
        assert_eq!(
            message.part_slice(1),
            Some(sequence.to_le_bytes().as_slice())
        );
    }
    assert!(receiver.is_empty());
}

#[test]
fn rejected_handover_preserves_the_current_connection() {
    let config = RecvLimits {
        peers: 1,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 16);
    let mut current = register(&mut routes, "a");
    let state = routes.previous[b"a".as_slice()].upgrade().unwrap();
    assert!(
        routes
            .register(Bytes::from_static(b"a"), CancellationToken::new())
            .is_err()
    );
    assert!(state.current.load(Ordering::Acquire));
    assert!(!state.cancel.is_cancelled());
    put(&mut current, "still current");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"still current".as_slice())
    );
}

#[test]
fn idle_disconnect_wakes_receiver_and_releases_registration_slot() {
    let config = RecvLimits {
        peers: 1,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 16);
    let idle = register(&mut routes, "a");
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    let data = receiver.shared.data.clone();
    let mut notified = std::pin::pin!(data.ready());
    assert!(poll_once(notified.as_mut()).is_pending());
    drop(idle);
    assert!(poll_once(notified.as_mut()).is_ready());
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    let _replacement = register(&mut routes, "a");
}

#[test]
fn registration_racing_receiver_drop_always_cancels_admitted_connections() {
    for _ in 0..128 {
        let (mut routes, receiver) = receive_pair(RecvLimits::default(), 16);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let other = barrier.clone();
        let cancel = CancellationToken::new();
        let observer = cancel.clone();
        let register = std::thread::spawn(move || {
            other.wait();
            let sink = routes.register(Bytes::from_static(b"a"), cancel);
            // Keep both alive until after the assertion; route drop itself cancels.
            (routes, sink)
        });
        barrier.wait();
        drop(receiver);
        let (_routes, sink) = register.join().unwrap();
        if let Ok(mut sink) = sink {
            assert!(observer.is_cancelled());
            assert!(!sink.push(Message::single("closed")));
        }
    }
}

#[test]
fn ordinary_async_receive_yields_during_stale_generation_cleanup() {
    use crate::socket::recv::{BlockingRecvWaker, SpscAwareRecv, SpscHandles, recv_pipe};

    let blocking = BlockingRecvWaker::new();
    let mut handles = SpscHandles::new(blocking.clone(), false);
    let mut routes = handles.init_peer_recv(512, None);
    let lane = handles.peer_recv.clone().unwrap();
    let (_pipe, consumer, notify, space) = recv_pipe(512, blocking);
    let receiver = SpscAwareRecv::new(consumer, notify, space, handles, false, false);
    let mut old = register(&mut routes, "a");
    for _ in 0..512 {
        put(&mut old, "stale");
    }
    let mut next = register(&mut routes, "a");
    assert!(poll_once(receiver.recv()).is_pending());
    assert!(!lane.lock().unwrap().is_empty());
    put(&mut next, "current");
    let std::task::Poll::Ready(Ok(message)) = poll_once(receiver.recv()) else {
        panic!("current peer must not wait for the stale backlog");
    };
    assert_eq!(message.part_slice(1), Some(b"current".as_slice()));
    assert!(!lane.lock().unwrap().is_empty());
}

#[test]
fn ordinary_bulk_after_first_preserves_budget_and_releases_partial_credits() {
    use crate::socket::recv::{BlockingRecvWaker, SpscAwareRecv, SpscHandles, recv_pipe};

    for (max, size, expected) in [(1, 8, 1), (1024, 8, 256), (1024, 70_000, 2)] {
        let blocking = BlockingRecvWaker::new();
        let mut handles = SpscHandles::new(blocking.clone(), false);
        let mut routes = handles.init_peer_recv(512, None);
        let (_pipe, consumer, notify, space) = recv_pipe(512, blocking);
        let receiver = SpscAwareRecv::new(consumer, notify, space, handles, false, false);
        let mut sink = register(&mut routes, "a");
        let payload = Bytes::from(vec![0x55; size]);
        for _ in 0..513 {
            assert!(sink.push(Message::single(payload.clone())));
        }
        sink.flush();
        assert!(poll_once(sink.ready()).is_pending());
        let mut out = vec![receiver.try_recv().unwrap()];
        receiver.try_recv_many_after_first(max, &mut out).unwrap();
        assert_eq!(out.len(), expected);
        assert!(poll_once(sink.ready()).is_ready());
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
    }
}

#[test]
fn reconnect_discards_respect_message_and_byte_drain_limits() {
    for (count, size) in [(512, 8), (4, 1024 * 1024)] {
        let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 512);
        let mut old = register(&mut routes, "a");
        let payload = Bytes::from(vec![0x55; size]);
        for _ in 0..count {
            assert!(old.push(Message::single(payload.clone())));
        }
        old.flush();
        let mut next = register(&mut routes, "a");
        assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
        assert!(receiver.take_yield_pending());
        // The first drain yielded rather than discarding the entire old ring.
        assert!(!receiver.is_empty());
        put(&mut next, "current");
        let result = receiver.try_recv();
        let Ok(message) = result else {
            panic!("current peer must not wait for the stale backlog: {result:?}");
        };
        assert_eq!(message.part_slice(1), Some(b"current".as_slice()));
        assert!(!receiver.is_empty());
    }
}

#[test]
fn credit_boundary_and_partial_bulk_return_wake_the_registered_producer() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for bulk in [false, true] {
        let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 256);
        let mut sink = register(&mut routes, "a");
        for _ in 0..257 {
            put(&mut sink, "value");
        }
        let counter = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut ready = std::pin::pin!(sink.ready());
            assert!(ready.as_mut().poll(&mut cx).is_pending());
            if bulk {
                let mut out = Vec::new();
                assert_eq!(receiver.try_recv_many_into(1, &mut out).unwrap(), 1);
            } else {
                for _ in 0..128 {
                    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
                    receiver.try_recv().unwrap();
                }
            }
            assert!(counter.0.load(Ordering::Relaxed) > 0);
            assert!(ready.as_mut().poll(&mut cx).is_ready());
        }
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
    }
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
        let config = RecvLimits {
            messages,
            bytes,
            ..RecvLimits::default()
        };
        let (mut routes, mut receiver) = receive_pair(config, 16);
        let mut a = register(&mut routes, "a");
        let mut b = register(&mut routes, "b");
        put(&mut a, "1234");
        put(&mut b, "5678");
        put(&mut b, "next");
        assert!(b.blocked());
        receiver.try_recv().unwrap();
        assert!(b.retry_pending());
        assert!(!b.blocked());
    }
}

#[test]
fn empty_frames_cannot_bypass_receive_byte_budget() {
    let config = RecvLimits {
        bytes: 256,
        ..RecvLimits::default()
    };
    let (mut routes, mut receiver) = receive_pair(config, 16);
    let mut sink = register(&mut routes, "a");
    assert!(sink.push(Message::multipart(["", "", ""])));
    sink.flush();
    assert!(sink.push(Message::multipart(["", ""])));
    assert!(sink.blocked());
    receiver.try_recv().unwrap();
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert!(!sink.push(Message::multipart(["", "", "", "", ""])));
}

#[test]
fn dropping_receiver_cancels_idle_and_full_peers_and_releases_budgets() {
    let (mut routes, receiver) = receive_pair(RecvLimits::default(), 16);
    let mut busy = register(&mut routes, "busy");
    let idle = register(&mut routes, "idle");
    let busy_state = routes.previous[b"busy".as_slice()].upgrade().unwrap();
    let idle_state = routes.previous[b"idle".as_slice()].upgrade().unwrap();
    for _ in 0..17 {
        put(&mut busy, "busy");
    }
    let budget = receiver.shared.budget.clone();
    drop(receiver);
    assert!(budget.room(64 * 1024 * 1024));
    assert!(busy_state.cancel.is_cancelled());
    assert!(idle_state.cancel.is_cancelled());
    assert!(!busy.retry_pending());
    drop((busy, idle));
    assert!(budget.room(64 * 1024 * 1024));
}

#[tokio::test]
async fn socket_shutdown_wakes_idle_receiver_and_disconnect_keeps_queued_messages() {
    let (mut routes, receiver, _pipe) = ordinary_receive(16);
    let mut sink = register(&mut routes, "a");
    put(&mut sink, "last");
    drop(sink);
    assert_eq!(
        receiver.recv().await.unwrap().part_slice(1),
        Some(b"last".as_slice())
    );
    assert!(poll_once(receiver.recv()).is_pending());
    drop(routes);
    assert!(matches!(receiver.recv().await, Err(Error::Closed)));
}

#[tokio::test]
async fn closing_receive_wakes_receiver_without_canceling_outbound_linger() {
    let (mut routes, receiver, _pipe) = ordinary_receive(16);
    let mut sink = register(&mut routes, "a");
    let state = routes.previous[b"a".as_slice()].upgrade().unwrap();
    assert!(poll_once(receiver.recv()).is_pending());
    routes.close_receive();
    assert!(matches!(receiver.recv().await, Err(Error::Closed)));
    assert!(!state.cancel.is_cancelled());
    assert!(sink.retry_pending());
    put(&mut sink, "closed admission");
    assert!(matches!(receiver.try_recv(), Err(Error::Closed)));
}
