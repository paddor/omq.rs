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
    let receiver = SpscAwareRecv::new(
        consumer,
        notify,
        space,
        handles,
        false,
        false,
        std::time::Duration::ZERO,
    );
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

#[test]
fn held_message_and_live_receipt_exclude_source_from_ordinary_receives() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let mut a = register(&mut routes, "a");
    let mut b = register(&mut routes, "b");
    put(&mut a, "first");
    put(&mut a, "second");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    assert_eq!(receipt.identity(), Some(b"a".as_slice()));
    let source = receipt.source().unwrap().clone();
    receiver.unshift(receipt, message).unwrap();
    put(&mut b, "healthy");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"healthy".as_slice())
    );
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    for _ in 0..3 {
        let (receipt, message) = receiver.try_recv_from(Some(&source)).unwrap();
        assert_eq!(message.part_slice(0), Some(b"first".as_slice()));
        assert!(matches!(
            receiver.try_recv_from(Some(&source)),
            Err(Error::WouldBlock)
        ));
        assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
        receiver.unshift(receipt, message).unwrap();
    }
    let (receipt, _) = receiver.try_recv_from(Some(&source)).unwrap();
    drop(receipt);
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"second".as_slice())
    );
}

#[test]
fn unshift_transfers_charge_without_readmitting_or_overtaking() {
    let (mut routes, mut receiver) = receive_pair(
        RecvLimits {
            messages: 1,
            bytes: 256,
            ..RecvLimits::default()
        },
        16,
    );
    let mut sink = register(&mut routes, "a");
    put(&mut sink, "one");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    assert!(sink.push(Message::single("two")));
    assert!(sink.blocked());
    receiver.unshift(receipt, message).unwrap();
    assert!(sink.retry_pending());
    assert!(sink.blocked());
    let (receipt, message) = receiver.try_recv_from(Some(&source)).unwrap();
    assert_eq!(message.part_slice(0), Some(b"one".as_slice()));
    assert!(sink.retry_pending());
    assert!(sink.blocked());
    drop(receipt);
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"two".as_slice())
    );
}

#[test]
fn receipt_drain_preserves_batched_producer_space_wakes() {
    use std::future::Future;

    for (capacity, release_at) in [(16, 16), (256, 128)] {
        let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), capacity);
        let mut sink = register(&mut routes, "a");
        for _ in 0..=capacity {
            put(&mut sink, "queued");
        }
        assert!(sink.blocked());
        let count = Arc::new(WakeCount(0.into()));
        let waker = std::task::Waker::from(count.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut ready = Box::pin(sink.ready());
        assert!(ready.as_mut().poll(&mut cx).is_pending());
        for consumed in 1..=release_at {
            let (receipt, body) = receiver.try_recv_from(None).unwrap();
            assert_eq!(body.part_slice(0), Some(b"queued".as_slice()));
            drop((receipt, body));
            if consumed < release_at {
                assert_eq!(count.0.load(Ordering::Relaxed), 0);
                assert!(ready.as_mut().poll(&mut cx).is_pending());
            }
        }
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert!(ready.as_mut().poll(&mut cx).is_ready());
        drop(ready);
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
    }
}

#[test]
fn paused_source_retains_one_large_frame_without_blocking_other_sources() {
    use std::future::Future;

    let mut message = Message::from_slice(&vec![7; 8 * 1024 * 1024]);
    message.bound_storage();
    let charge = message.retained_size().unwrap();
    let (mut routes, mut receiver) = receive_pair(
        RecvLimits {
            bytes: 2 * charge,
            ..RecvLimits::default()
        },
        16,
    );
    let mut a = register(&mut routes, "a");
    let mut b = register(&mut routes, "b");
    let budget = routes.previous[b"a".as_slice()]
        .upgrade()
        .unwrap()
        .budget
        .clone();
    assert!(a.push(message.clone()));
    a.flush();
    let (receipt, held) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    receiver.unshift(receipt, held).unwrap();
    assert!(a.push(message.clone()));
    a.flush();
    assert!(!a.blocked());
    assert!(a.push(message.clone()));
    assert!(a.blocked());
    let count = Arc::new(WakeCount(0.into()));
    let waker = std::task::Waker::from(count.clone());
    let mut ready = Box::pin(a.ready());
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(ready.as_mut().poll(&mut cx).is_pending());

    for _ in 0..3 {
        let (receipt, held) = receiver.try_recv_from(Some(&source)).unwrap();
        receiver.unshift(receipt, held).unwrap();
        assert!(b.push(message.clone()));
        b.flush();
        assert!(!b.blocked());
        let (receipt, body) = receiver.try_recv_from(None).unwrap();
        assert_eq!(receipt.identity(), Some(b"b".as_slice()));
        assert_eq!(body.part_slice(0).unwrap().len(), 8 * 1024 * 1024);
        drop((receipt, body));
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
    }
    let (receipt, held) = receiver.try_recv_from(Some(&source)).unwrap();
    drop((receipt, held));
    assert!(count.0.load(Ordering::Relaxed) > 0);
    assert!(ready.as_mut().poll(&mut cx).is_ready());
    drop(ready);
    assert!(a.retry_pending());
    assert!(!a.blocked());
    while receiver.try_recv().is_ok() {}
    assert!(receiver.is_empty());
    assert!(budget.room(2 * charge));
}

#[test]
fn stale_unshift_returns_body_and_does_not_pause_replacement() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let mut old = register(&mut routes, "a");
    put(&mut old, "old");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    let mut new = register(&mut routes, "a");
    let error = receiver.unshift(receipt, message).unwrap_err();
    assert!(matches!(error.error, Error::Closed));
    assert_eq!(error.message.part_slice(0), Some(b"old".as_slice()));
    assert!(matches!(
        receiver.try_recv_from(Some(&source)),
        Err(Error::Closed)
    ));
    put(&mut new, "new");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"new".as_slice())
    );
}

#[test]
fn disconnected_held_source_releases_held_and_unread_charges() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let mut sink = register(&mut routes, "a");
    put(&mut sink, "one");
    put(&mut sink, "two");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    receiver.unshift(receipt, message).unwrap();
    drop(sink);
    assert!(matches!(receiver.try_recv(), Err(Error::WouldBlock)));
    assert!(receiver.is_empty());
    assert!(receiver.paused.is_empty());
    assert!(matches!(
        receiver.try_recv_from(Some(&source)),
        Err(Error::Closed)
    ));
}

#[test]
fn foreign_and_oversized_returns_preserve_ownership_and_release_claim() {
    let (mut routes, mut receiver) = receive_pair(RecvLimits::default(), 16);
    let (_foreign_routes, mut foreign) = receive_pair(RecvLimits::default(), 16);
    let mut sink = register(&mut routes, "a");
    put(&mut sink, "one");
    put(&mut sink, "two");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    assert!(matches!(
        foreign.try_recv_from(Some(&source)),
        Err(Error::Protocol(_))
    ));
    let error = foreign.unshift(receipt, message).unwrap_err();
    assert_eq!(error.message.part_slice(0), Some(b"one".as_slice()));
    let (receipt, _) = receiver.try_recv_from(None).unwrap();
    let large = Message::from_slice(&[7; 1024]);
    let error = receiver.unshift(receipt, large).unwrap_err();
    assert!(matches!(error.error, Error::Protocol(_)));
    assert_eq!(error.message.part_slice(0), Some([7; 1024].as_slice()));
    put(&mut sink, "three");
    assert_eq!(
        receiver.try_recv().unwrap().part_slice(1),
        Some(b"three".as_slice())
    );
}

#[test]
fn targeted_wait_ignores_other_sources_and_wakes_on_its_source() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let (mut routes, receiver) = receive_pair(RecvLimits::default(), 16);
    let receiver = ReceiveCell::new(receiver, Weak::new());
    let mut a = register(&mut routes, "a");
    let mut b = register(&mut routes, "b");
    put(&mut a, "first");
    let (receipt, _) = receiver.lock().try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    drop(receipt);
    let count = Arc::new(WakeCount(0.into()));
    let waker = Waker::from(count.clone());
    let mut waiting = Box::pin(PeerReceiver::recv_from(&receiver, Some(&source)));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    for _ in 0..4 {
        put(&mut b, "unrelated");
    }
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    put(&mut a, "second");
    assert!(count.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Ok((receipt, body))) = waiting.as_mut().poll(&mut Context::from_waker(&waker))
    else {
        panic!("target not ready")
    };
    assert_eq!(body.part_slice(0), Some(b"second".as_slice()));
    drop(receipt);
}

#[test]
fn targeted_wait_ends_on_disconnect_or_socket_close() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    for close_socket in [false, true] {
        let (mut routes, receiver) = receive_pair(RecvLimits::default(), 16);
        let receiver = ReceiveCell::new(receiver, Weak::new());
        let mut a = register(&mut routes, "a");
        put(&mut a, "first");
        let (receipt, _) = receiver.lock().try_recv_from(None).unwrap();
        let source = receipt.source().unwrap().clone();
        drop(receipt);
        let count = Arc::new(WakeCount(0.into()));
        let waker = Waker::from(count.clone());
        let mut waiting = Box::pin(PeerReceiver::recv_from(&receiver, Some(&source)));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if close_socket {
            routes.close_receive();
        } else {
            drop(a);
        }
        assert!(count.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            waiting.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Err(Error::Closed))
        ));
    }
}

#[test]
fn held_only_queue_does_not_reschedule_fair_waiters_in_a_loop() {
    use std::future::Future;
    use std::task::{Context, Waker};
    let (mut routes, receiver, _pipe) = ordinary_receive(16);
    let mut a = register(&mut routes, "a");
    put(&mut a, "held");
    let (receipt, message) = receiver.try_recv_from(None).unwrap();
    let source = receipt.source().unwrap().clone();
    receiver.unshift(receipt, message).unwrap();
    let count = Arc::new(WakeCount(0.into()));
    let waker = Waker::from(count.clone());
    let mut waiting = Box::pin(receiver.recv());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let seen = count.0.load(Ordering::Relaxed);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(count.0.load(Ordering::Relaxed), seen);
    let (receipt, _) = receiver.try_recv_from(Some(&source)).unwrap();
    drop(receipt);
    put(&mut a, "resumed");
    assert!(count.0.load(Ordering::Relaxed) > seen);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
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
    assert!(state.current());
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
    let receiver = SpscAwareRecv::new(
        consumer,
        notify,
        space,
        handles,
        false,
        false,
        std::time::Duration::ZERO,
    );
    let mut old = register(&mut routes, "a");
    for _ in 0..512 {
        put(&mut old, "stale");
    }
    let mut next = register(&mut routes, "a");
    assert!(poll_once(receiver.recv()).is_pending());
    assert!(!lane.lock().is_empty());
    put(&mut next, "current");
    let std::task::Poll::Ready(Ok(message)) = poll_once(receiver.recv()) else {
        panic!("current peer must not wait for the stale backlog");
    };
    assert_eq!(message.part_slice(1), Some(b"current".as_slice()));
    assert!(!lane.lock().is_empty());
}

#[test]
fn ordinary_bulk_after_first_preserves_budget_and_releases_partial_credits() {
    use crate::socket::recv::{BlockingRecvWaker, SpscAwareRecv, SpscHandles, recv_pipe};

    for (max, size, expected) in [(1, 8, 1), (1024, 8, 256), (1024, 70_000, 2)] {
        let blocking = BlockingRecvWaker::new();
        let mut handles = SpscHandles::new(blocking.clone(), false);
        let mut routes = handles.init_peer_recv(512, None);
        let (_pipe, consumer, notify, space) = recv_pipe(512, blocking);
        let receiver = SpscAwareRecv::new(
            consumer,
            notify,
            space,
            handles,
            false,
            false,
            std::time::Duration::ZERO,
        );
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
fn per_source_count_and_byte_bounds_leave_other_sources_usable() {
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
        put(&mut a, "5678");
        put(&mut a, "next");
        assert!(a.blocked());
        let (held, _) = receiver.try_recv_from(None).unwrap();
        put(&mut b, "healthy");
        assert!(!b.blocked());
        let (receipt, body) = receiver.try_recv_from(None).unwrap();
        assert_eq!(receipt.identity(), Some(b"b".as_slice()));
        assert_eq!(body.part_slice(0), Some(b"healthy".as_slice()));
        drop(receipt);
        assert!(a.retry_pending());
        assert!(a.blocked());
        drop(held);
        assert!(a.retry_pending());
        assert!(!a.blocked());
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
    let budget = busy_state.budget.clone();
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
