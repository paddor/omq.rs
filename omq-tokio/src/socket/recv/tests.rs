use super::drain::{
    RecvSource, drain_yring, drain_yring_one, drain_yring_one_into_batch, recv_source_at,
};
use super::{
    BlockingRecvWaker, RECV_BATCH_BYTES, RECV_BATCH_MESSAGES, SpscAwareRecv, SpscHandles,
    TcpYringConsumer, recv_pipe,
};
use omq_proto::Message;
use omq_proto::flow::DrainBudget;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn tcp_consumer(peer_id: u64) -> (yring::Producer<Message>, Arc<TcpYringConsumer>) {
    let (producer, consumer) = yring::spsc(4);
    (
        producer,
        Arc::new(TcpYringConsumer {
            consumer: crate::engine::receive_cell::ReceiveCell::new(
                consumer,
                std::sync::Weak::new(),
            ),
            batch_remaining: AtomicUsize::new(0),
            batch_popped: AtomicUsize::new(0),
            capacity: 4,
            spin_probe: false,
            space: Arc::new(crate::engine::signal::StateSignal::new()),
            peer_id,
        }),
    )
}

fn bulk_receiver(
    peers: usize,
    capacity: usize,
    latency: bool,
) -> (
    SpscAwareRecv,
    Vec<yring::Producer<Message>>,
    Arc<super::SharedRecvPipe>,
) {
    let waker = BlockingRecvWaker::new();
    let handles = SpscHandles::new(waker.clone(), false);
    let mut producers = Vec::new();
    for id in 0..peers {
        let (producer, consumer) = yring::spsc(capacity);
        producers.push(producer);
        handles
            .tcp_consumers
            .write()
            .unwrap()
            .push(Arc::new(TcpYringConsumer {
                consumer: crate::engine::receive_cell::ReceiveCell::new(
                    consumer,
                    std::sync::Weak::new(),
                ),
                batch_remaining: AtomicUsize::new(0),
                batch_popped: AtomicUsize::new(0),
                capacity,
                spin_probe: false,
                space: Arc::new(crate::engine::signal::StateSignal::new()),
                peer_id: id as u64,
            }));
    }
    let (pipe, consumer, notify, space) = recv_pipe(capacity, waker);
    handles.consumer_generation.store(1, Ordering::Release);
    let tuning = super::RecvTuning {
        latency_profile: latency,
        ..super::RecvTuning::default()
    };
    let ends = super::RecvPipeEnds {
        consumer,
        notify,
        space,
    };
    (SpscAwareRecv::new(ends, handles, tuning), producers, pipe)
}

type WaiterResult = omq_proto::Result<Option<Message>>;

/// Block a thread in a receive that ends by `mode`: cancel, a short
/// timeout, or (for "close") a long timeout that close cuts short.
fn spawn_first_waiter(
    recv: &Arc<SpscAwareRecv>,
    cancel: &Arc<super::BlockingRecvCancel>,
    mode: &'static str,
    results: std::sync::mpsc::Sender<WaiterResult>,
) -> std::thread::JoinHandle<()> {
    let recv = recv.clone();
    let cancel = cancel.clone();
    std::thread::spawn(move || {
        let result = if mode == "cancel" {
            recv.blocking_recv_cancelable(&cancel)
        } else {
            let timeout = if mode == "timeout" {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(2)
            };
            recv.blocking_recv_timeout(timeout).map(Some)
        };
        results.send(result).unwrap();
    })
}

fn wait_until_parked(recv: &SpscAwareRecv, thread: &std::thread::Thread) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !recv.blocking_recv_waker.has_waiter(thread.id()) {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}

fn assert_waiter_results(mode: &str, first: &WaiterResult, second: omq_proto::Result<Message>) {
    match mode {
        "cancel" => assert!(matches!(first, Ok(None))),
        "timeout" => assert!(matches!(first, Err(omq_proto::Error::Timeout))),
        _ => assert!(matches!(first, Err(omq_proto::Error::Closed))),
    }

    if mode == "close" {
        assert!(matches!(second, Err(omq_proto::Error::Closed)));
    } else {
        assert_eq!(second.unwrap(), Message::single("remaining"));
    }
}

#[test]
fn concurrent_blocking_waiters_preserve_cancel_timeout_and_close() {
    for mode in ["cancel", "timeout", "close"] {
        let (recv, _, pipe) = bulk_receiver(0, 16, false);
        let recv = Arc::new(recv);
        let cancel = Arc::new(super::BlockingRecvCancel::new());
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let (second_tx, second_rx) = std::sync::mpsc::channel();

        // Park two waiters; the first one ends by `mode`, and the second
        // must still see the next message (or the close).
        let first = spawn_first_waiter(&recv, &cancel, mode, first_tx);
        wait_until_parked(&recv, first.thread());
        let second = {
            let recv = recv.clone();
            std::thread::spawn(move || {
                second_tx
                    .send(recv.blocking_recv_timeout(Duration::from_secs(2)))
                    .unwrap();
            })
        };
        wait_until_parked(&recv, second.thread());

        if mode == "cancel" {
            cancel.cancel();
        } else if mode == "close" {
            pipe.close();
        }
        let first_result = first_rx.recv_timeout(Duration::from_millis(500));
        if mode != "close" {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime
                .block_on(pipe.send(Message::single("remaining")))
                .unwrap();
        }
        let second_result = second_rx.recv_timeout(Duration::from_millis(500));

        // Release both threads before checking results, so a failure does
        // not leave a parked thread behind.
        cancel.cancel();
        pipe.close();
        first.thread().unpark();
        second.thread().unpark();
        first.join().unwrap();
        second.join().unwrap();

        assert_waiter_results(mode, &first_result.unwrap(), second_result.unwrap());
    }
}

#[test]
fn concurrent_blocking_receivers_each_receive_from_one_batch() {
    let (recv, _, pipe) = bulk_receiver(0, 16, false);
    let recv = Arc::new(recv);
    let waker = recv.blocking_recv_waker.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let recv = recv.clone();
        let done_tx = done_tx.clone();
        let worker = std::thread::spawn(move || {
            done_tx
                .send(recv.blocking_recv_timeout(Duration::from_secs(2)))
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if waker.has_waiter(worker.thread().id()) {
                break;
            }
            assert!(Instant::now() < deadline, "receiver did not register");
            std::thread::yield_now();
        }
        workers.push(worker);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        pipe.send(Message::single("first")).await.unwrap();
        pipe.send(Message::single("second")).await.unwrap();
    });
    let first = done_rx.recv_timeout(Duration::from_millis(250));
    let second = done_rx.recv_timeout(Duration::from_millis(250));
    // Always release parked workers before assertions, including on failure.
    pipe.close();
    for worker in workers {
        worker.thread().unpark();
        worker.join().unwrap();
    }
    let mut messages = [first.unwrap().unwrap(), second.unwrap().unwrap()];
    messages.sort_by(|left, right| left.part_slice(0).cmp(&right.part_slice(0)));
    assert_eq!(
        messages,
        [Message::single("first"), Message::single("second")]
    );
}

#[test]
fn overflowing_spin_duration_respects_receive_deadline() {
    for latency in [false, true] {
        let (mut recv, _, _pipe) = bulk_receiver(0, 16, latency);
        recv.recv_spin = Duration::from_secs(u64::MAX);
        assert!(matches!(
            recv.blocking_recv_timeout(Duration::ZERO),
            Err(omq_proto::Error::Timeout)
        ));
    }
}

#[test]
fn blocking_recv_spin_observes_delivery_and_close_before_and_after_parking() {
    for spin in [Duration::from_micros(50), Duration::from_secs(5)] {
        for close in [false, true] {
            let (mut recv, _, pipe) = bulk_receiver(0, 16, false);
            recv.recv_spin = spin;
            let waker = recv.blocking_recv_waker.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                done_tx.send(recv.blocking_recv()).unwrap();
            });
            started_rx.recv().unwrap();
            if spin < Duration::from_secs(1) {
                let until = Instant::now() + Duration::from_secs(1);
                while !waker.has_waiter(worker.thread().id()) {
                    assert!(Instant::now() < until, "receiver never reached its wait");
                    std::thread::yield_now();
                }
            } else {
                std::thread::sleep(Duration::from_millis(5));
                assert!(!waker.has_waiter(worker.thread().id()));
            }
            if close {
                pipe.close();
            } else {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                runtime
                    .block_on(pipe.send(Message::single("ready")))
                    .unwrap();
            }
            let result = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            if close {
                assert!(matches!(result, Err(omq_proto::Error::Closed)));
            } else {
                assert_eq!(result.unwrap(), Message::single("ready"));
            }
            worker.join().unwrap();
        }
    }
}

#[test]
fn single_ring_spinning_observes_delivery_new_sources_and_close_in_both_profiles() {
    for latency in [false, true] {
        let (mut recv, mut producers, pipe) = bulk_receiver(1, 8, latency);
        recv.recv_spin = Duration::MAX;
        Arc::get_mut(&mut recv.tcp_consumers.write().unwrap()[0])
            .unwrap()
            .spin_probe = true;
        let peers = recv.tcp_consumers.clone();
        let generation = recv.consumer_generation.clone();
        let signal = recv.recv_signal.clone();
        let (done, received) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            for _ in 0..3 {
                done.send(recv.blocking_recv_timeout(Duration::from_secs(1)))
                    .unwrap();
            }
        });
        std::thread::sleep(Duration::from_millis(5));
        producers[0].push(Message::single("first")).unwrap();
        producers[0].flush();
        signal.mark();
        assert_eq!(received.recv().unwrap().unwrap(), Message::single("first"));
        std::thread::sleep(Duration::from_millis(5));
        let (mut producer, peer) = tcp_consumer(99);
        producer.push(Message::single("new peer")).unwrap();
        producer.flush();
        peers.write().unwrap().push(peer);
        // Source registration must invalidate a cached idle probe even
        // before this source has marked the socket's data signal.
        generation.fetch_add(1, Ordering::Release);
        assert_eq!(
            received.recv().unwrap().unwrap(),
            Message::single("new peer")
        );
        drop(producer);
        drop(producers);
        pipe.close();
        assert!(matches!(
            received.recv().unwrap(),
            Err(omq_proto::Error::Closed)
        ));
        worker.join().unwrap();
    }
}

fn preload(producers: &mut [yring::Producer<Message>], count: usize, size: usize) {
    for (id, producer) in producers.iter_mut().enumerate() {
        for seq in 0..count {
            let mut payload = vec![id as u8; size];
            payload[1] = seq as u8;
            producer.push(Message::from_slice(&payload)).unwrap();
        }
        producer.flush();
    }
}

#[test]
fn bulk_preloaded_fanin_drains_multiple_fair_rounds() {
    for latency in [false, true] {
        let (recv, mut producers, _pipe) = bulk_receiver(4, 128, latency);
        preload(&mut producers, 100, 53);
        let mut out = Vec::with_capacity(300);
        let allocation = out.as_ptr();
        assert_eq!(recv.try_recv_many_into(259, &mut out).unwrap(), 256);
        for (index, msg) in out.iter().enumerate() {
            let payload = msg.part_slice(0).unwrap();
            assert_eq!(
                (payload[0], payload[1]),
                ((index % 4) as u8, (index / 4) as u8)
            );
            let address = std::ptr::from_ref(msg) as usize;
            assert!(
                (address..address + size_of::<Message>()).contains(&(payload.as_ptr() as usize))
            );
        }
        assert_eq!(allocation, out.as_ptr());
        out.clear();
        assert_eq!(recv.try_recv_many_into(3, &mut out).unwrap(), 3);
        out.clear();
        assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
        assert_eq!(out[0].part_slice(0).unwrap()[0], 3);
        assert_eq!(out[1].part_slice(0).unwrap()[0], 0);
    }
}

#[test]
fn remove_empty_tcp_consumer_drops_empty_peer_ring() {
    let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
    let (_producer, consumer) = tcp_consumer(7);
    handles.tcp_consumers.write().unwrap().push(consumer);

    handles.remove_empty_tcp_consumer(7);

    assert!(handles.tcp_consumers.read().unwrap().is_empty());
    assert_eq!(handles.consumer_generation.load(Ordering::Acquire), 1);
}

#[test]
fn bulk_releases_partial_windows_and_notifies_space() {
    for latency in [false, true] {
        let (recv, mut producers, _pipe) = bulk_receiver(4, 4, latency);
        preload(&mut producers, 4, 16);
        let peers = recv.tcp_consumers.read().unwrap().clone();
        let generations: Vec<_> = peers.iter().map(|p| p.space.generation()).collect();
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
        for (id, producer) in producers.iter_mut().enumerate() {
            assert!(peers[id].space.generation() > generations[id]);
            for _ in 0..if id == 0 { 2 } else { 1 } {
                producer.push(Message::from_slice(b"new")).unwrap();
            }
            assert!(producer.push(Message::from_slice(b"full")).is_err());
            producer.flush();
        }
        out.clear();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 16);
    }
}

#[test]
fn bulk_byte_budget_spans_rounds_and_cached_messages() {
    for latency in [false, true] {
        let (recv, mut producers, _pipe) = bulk_receiver(2, 128, latency);
        preload(&mut producers[..1], 100, 53);
        preload(&mut producers[1..], 100, 32_768);
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 64);
        assert_eq!(
            out.iter().map(Message::byte_len).sum::<usize>(),
            32 * (53 + 32_768)
        );
        out.clear();
        out.push(recv.try_recv().unwrap());
        assert_eq!(recv.try_recv_many_after_first(256, &mut out).unwrap(), 63);
        assert_eq!(out.len(), 64);
    }
}

#[test]
fn bulk_first_message_exhausting_limit_still_releases_its_slot() {
    let (recv, mut producers, _pipe) = bulk_receiver(2, 4, true);
    preload(&mut producers, 4, 16);
    let mut out = vec![recv.try_recv().unwrap()];
    assert_eq!(recv.try_recv_many_after_first(1, &mut out).unwrap(), 0);
    producers[0].push(Message::from_slice(b"released")).unwrap();
    assert_eq!(out.len(), 1);
}

#[test]
fn bulk_oversized_and_multipart_messages_stay_atomic() {
    for batching in [false, true] {
        let (mut recv, mut producers, _pipe) = bulk_receiver(2, 4, false);
        recv.recv_batching = batching;
        let huge = Message::from_slice(&vec![7; RECV_BATCH_BYTES + 1]);
        let multipart = Message::multipart([vec![1; 40_000], vec![2; 40_000]]);
        producers[0].push(huge.clone()).unwrap();
        producers[0].push(Message::from_slice(b"next")).unwrap();
        producers[0].flush();
        producers[1].push(multipart.clone()).unwrap();
        producers[1].flush();
        let mut out = Vec::new();
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
        assert_eq!(out.pop().unwrap(), huge);
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
        assert_eq!(out.pop().unwrap(), multipart);
        assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
        assert_eq!(out.pop().unwrap().part_slice(0).unwrap(), b"next");
    }
}

#[tokio::test]
async fn bulk_hot_peer_does_not_starve_newly_ready_peer_or_shared_pipe() {
    let (recv, mut producers, pipe) = bulk_receiver(2, 512, false);
    preload(&mut producers[..1], 400, 16);
    let mut out = Vec::new();
    assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 256);
    producers[1].push(Message::from_slice(b"quiet")).unwrap();
    producers[1].flush();
    pipe.send(Message::from_slice(b"shared")).await.unwrap();
    out.clear();
    assert_eq!(recv.try_recv_many_into(2, &mut out).unwrap(), 2);
    assert_eq!(out[0].part_slice(0).unwrap(), b"quiet");
    assert_eq!(out[1].part_slice(0).unwrap(), b"shared");
}

#[test]
fn bulk_disconnect_drains_unread_data_then_accepts_new_peer() {
    let (recv, mut producers, pipe) = bulk_receiver(4, 4, false);
    preload(&mut producers, 4, 16);
    drop(producers);
    let mut out = vec![Message::from_slice(b"existing")];
    assert_eq!(recv.try_recv_many_into(3, &mut out).unwrap(), 3);
    assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 13);
    assert_eq!(recv.try_recv_many_into(0, &mut out).unwrap(), 0);
    assert!(matches!(
        recv.try_recv_many_into(1, &mut out),
        Err(omq_proto::Error::WouldBlock)
    ));
    let (mut producer, peer) = tcp_consumer(99);
    producer.push(Message::from_slice(b"reconnected")).unwrap();
    producer.flush();
    recv.tcp_consumers.write().unwrap().push(peer);
    recv.consumer_generation.fetch_add(1, Ordering::Release);
    drop(producer);
    pipe.close();
    assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 1);
    assert_eq!(out.last().unwrap().part_slice(0).unwrap(), b"reconnected");
    assert!(matches!(
        recv.try_recv_many_into(1, &mut out),
        Err(omq_proto::Error::Closed)
    ));
    assert_eq!(out.len(), 18);
}

#[tokio::test]
async fn bulk_shared_pipe_releases_partial_batch_and_preserves_partial_success() {
    let (recv, _producers, pipe) = bulk_receiver(0, 4, false);
    for _ in 0..4 {
        pipe.send(Message::from_slice(b"queued")).await.unwrap();
    }
    let mut out = Vec::new();
    assert_eq!(recv.try_recv_many_into(1, &mut out).unwrap(), 1);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pipe.send(Message::from_slice(b"released")),
    )
    .await
    .unwrap()
    .unwrap();
    pipe.close();
    assert_eq!(recv.try_recv_many_into(256, &mut out).unwrap(), 4);
    assert!(matches!(
        recv.try_recv_many_into(1, &mut out),
        Err(omq_proto::Error::Closed)
    ));
}

#[tokio::test]
async fn bulk_fair_rounds_include_peer_and_shared_sources() {
    let (recv, mut producers, pipe) = bulk_receiver(2, 4, false);
    let first = recv.tcp_consumers.read().unwrap()[0].clone();
    let generation = first.space.generation();
    preload(&mut producers, 4, 16);
    for seq in 0..4 {
        pipe.send(Message::from_slice(&[2, seq])).await.unwrap();
    }
    let mut out = Vec::new();
    assert_eq!(recv.try_recv_many_into(5, &mut out).unwrap(), 5);
    assert!(first.space.generation() > generation);
    // Both peer rings released two consumed slots from partial windows.
    for producer in &mut producers {
        for _ in 0..2 {
            producer.push(Message::from_slice(b"new")).unwrap();
        }
    }
    assert_eq!(recv.try_recv_many_into(7, &mut out).unwrap(), 7);
    for (index, msg) in out.iter().enumerate() {
        assert_eq!(
            &msg.part_slice(0).unwrap()[..2],
            &[(index % 3) as u8, (index / 3) as u8]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_empty_drain_racing_arrival_keeps_receive_wakeup() {
    for batching in [false, true] {
        check_bulk_wakeup(batching).await;
    }
}

async fn check_bulk_wakeup(batching: bool) {
    let (mut recv, mut producers, _pipe) = bulk_receiver(2, 4, false);
    recv.recv_batching = batching;
    let signal = recv.recv_signal.clone();
    let sender = tokio::spawn(async move {
        for seq in 0_u32..2000 {
            let peer = seq as usize % 2;
            let mut item = Message::from_slice(&seq.to_le_bytes());
            loop {
                match producers[peer].push(item) {
                    Ok(()) => break,
                    Err(returned) => item = returned,
                }
                tokio::task::yield_now().await;
            }
            producers[peer].flush();
            signal.mark();
            tokio::task::yield_now().await;
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut seen = vec![false; 2000];
        let mut count = 0;
        let mut out = Vec::with_capacity(7);
        while count < 2000 {
            out.clear();
            if matches!(
                recv.try_recv_many_into(7, &mut out),
                Err(omq_proto::Error::WouldBlock)
            ) {
                out.push(recv.recv().await.unwrap());
                recv.try_recv_many_after_first(7, &mut out).ok();
            }
            for message in &out {
                let seq =
                    u32::from_le_bytes(message.part_slice(0).unwrap().try_into().unwrap()) as usize;
                assert!(!seen[seq]);
                seen[seq] = true;
                count += 1;
            }
            tokio::task::yield_now().await;
        }
        sender.await.unwrap();
    })
    .await
    .expect("lost receive or space wakeup");
}

#[test]
fn remove_empty_tcp_consumer_keeps_unread_messages() {
    let handles = SpscHandles::new(BlockingRecvWaker::new(), false);
    let (mut producer, consumer) = tcp_consumer(7);
    producer.push(Message::from_slice(b"queued")).unwrap();
    producer.flush();
    handles.tcp_consumers.write().unwrap().push(consumer);

    handles.remove_empty_tcp_consumer(7);

    assert_eq!(handles.tcp_consumers.read().unwrap().len(), 1);
    assert_eq!(handles.consumer_generation.load(Ordering::Acquire), 0);
}

#[test]
fn native_lwm_wakes_producer_while_the_other_half_is_still_draining() {
    let (mut producer, mut consumer) = yring::spsc(16);
    assert!(!consumer.release_with_full());
    for sequence in 0_u8..16 {
        producer.push(Message::from_slice(&[sequence])).unwrap();
    }
    producer.flush();
    assert!(producer.is_full());
    let mut progress = super::RingDrain::default();
    let mut out = Vec::new();
    let mut budget = DrainBudget::new(16, RECV_BATCH_BYTES);
    let mut wakes = 0;
    assert_eq!(
        drain_yring(&mut consumer, &mut out, &mut progress, &mut budget, || {
            wakes += 1;
            assert!(!producer.is_full(), "capacity must precede the wake");
            if wakes == 1 {
                // This executes at half capacity, before the original other
                // half has been popped. Its slots remain occupied.
                for sequence in 16_u8..24 {
                    producer.push(Message::from_slice(&[sequence])).unwrap();
                }
                producer.flush();
                assert!(producer.push(Message::single("still full")).is_err());
            }
        }),
        16
    );
    assert_eq!(wakes, 2);
    for (sequence, message) in out.iter().enumerate() {
        assert_eq!(message.part_slice(0).unwrap(), &[sequence as u8]);
    }
    out.clear();
    let mut budget = DrainBudget::new(16, RECV_BATCH_BYTES);
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut out,
            &mut progress,
            &mut budget,
            || panic!("ready producer needs no wake")
        ),
        8
    );
    for (sequence, message) in out.iter().enumerate() {
        assert_eq!(message.part_slice(0).unwrap(), &[(sequence + 16) as u8]);
    }
}

#[test]
fn native_lwm_counts_single_pops_and_preserves_partial_window_release() {
    for capacity in [1, 2, 8, 16, 2048, 4096] {
        let (mut producer, mut consumer) = yring::spsc(capacity);
        assert!(!consumer.release_with_full());
        for _ in 0..capacity {
            producer.push(Message::single("queued")).unwrap();
        }
        producer.flush();
        assert!(producer.is_full());
        let mut progress = super::RingDrain::default();
        let lwm = super::RingDrain::lwm(capacity);
        for popped in 1..=lwm {
            let (message, wake) = drain_yring_one(&mut consumer, &mut progress);
            assert!(message.is_some());
            assert_eq!(wake, popped == lwm);
            assert_eq!(producer.is_full(), popped < lwm);
        }
        assert_eq!(progress.popped, 0);
        if capacity > lwm {
            let (message, wake) = drain_yring_one(&mut consumer, &mut progress);
            assert!(message.is_some());
            assert!(!wake);
            let remaining = progress.remaining;
            assert!(!progress.release(&mut consumer));
            assert_eq!(progress.remaining, remaining);
            assert_eq!(progress.popped, 0);
            // The LWM batch plus this partial release are reusable slots.
            for _ in 0..=lwm {
                producer.push(Message::single("reused")).unwrap();
            }
            assert!(producer.is_full());
        }
    }
}

#[test]
fn latency_drain_keeps_prefetched_batch_open() {
    let (mut producer, mut consumer) = yring::spsc(8);
    producer.push(Message::from_slice(b"a")).unwrap();
    producer.push(Message::from_slice(b"b")).unwrap();
    producer.flush();

    let mut remaining = super::RingDrain::default();
    let (first, released) = drain_yring_one(&mut consumer, &mut remaining);
    assert_eq!(first.unwrap().part_bytes(0).unwrap(), &b"a"[..]);
    assert!(!released);
    let (second, released) = drain_yring_one(&mut consumer, &mut remaining);
    assert_eq!(second.unwrap().part_bytes(0).unwrap(), &b"b"[..]);
    assert!(released);
    let (third, released) = drain_yring_one(&mut consumer, &mut remaining);
    assert!(third.is_none());
    assert!(!released);

    producer.push(Message::from_slice(b"c")).unwrap();
    producer.flush();
    let (next, released) = drain_yring_one(&mut consumer, &mut remaining);
    assert_eq!(next.unwrap().part_bytes(0).unwrap(), &b"c"[..]);
    assert!(released);
}

#[test]
fn recv_source_cursor_rotates_across_all_source_kinds() {
    let sources = (0..3)
        .map(|index| recv_source_at(index, 2))
        .collect::<Vec<_>>();
    assert_eq!(
        sources,
        vec![
            RecvSource::Stream(0),
            RecvSource::Stream(1),
            RecvSource::Shared,
        ]
    );
    assert_eq!(recv_source_at(3 % 3, 2), RecvSource::Stream(0));
}

#[test]
fn throughput_drain_honors_conservative_byte_budget() {
    let (mut producer, mut consumer) = yring::spsc(8);
    for _ in 0..5 {
        producer.push(Message::from_slice(b"tiny")).unwrap();
    }
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(256, 4096);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        4
    );
    assert_eq!(batch.len(), 4);

    let mut next_budget = DrainBudget::new(256, 4096);
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut next_budget,
            || {}
        ),
        1
    );
    assert_eq!(batch.len(), 5);
}

/// Drain with a fresh, roomy budget.
fn drain_unbounded(
    consumer: &mut yring::Consumer<Message>,
    out: &mut Vec<Message>,
    remaining: &mut super::RingDrain,
) -> usize {
    let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
    drain_yring(consumer, out, remaining, &mut budget, || {})
}

#[test]
fn predicate_drain_preserves_rejected_message_across_wraparound() {
    let (mut producer, mut consumer) = yring::spsc(4);
    let mut out = Vec::new();
    let mut remaining = super::RingDrain::default();

    // Advance the ring cursor so the next batch wraps around the end.
    for _ in 0..3 {
        producer.push(Message::from_slice(b"warmup")).unwrap();
    }
    producer.flush();
    drain_yring(
        &mut consumer,
        &mut out,
        &mut remaining,
        &mut DrainBudget::new(3, RECV_BATCH_BYTES),
        || {},
    );
    out.clear();

    let messages = [
        Message::from_slice(b"tiny"),
        Message::from_slice(&vec![1; 1025]),
        Message::multipart([vec![2; 40_000], vec![3; 40_000]]),
        Message::from_slice(b"last"),
    ];
    for message in &messages {
        producer.push(message.clone()).unwrap();
    }
    producer.flush();

    // The byte budget rejects the 80 KB message; it must stay queued.
    let prefix = Message::from_slice(b"existing");
    out.push(prefix.clone());
    let mut budget = DrainBudget::new(256, 5120);
    assert_eq!(
        drain_yring(&mut consumer, &mut out, &mut remaining, &mut budget, || {}),
        2
    );
    assert_eq!(
        out,
        [prefix.clone(), messages[0].clone(), messages[1].clone()]
    );
    assert_eq!(remaining.remaining, 2);
    assert_eq!(budget.bytes(), 5120);
    assert_eq!(
        drain_yring(&mut consumer, &mut out, &mut remaining, &mut budget, || {}),
        0
    );

    // Released slots are reusable while the rejected message waits.
    assert!(producer.push(Message::from_slice(b"reused")).is_ok());
    producer.flush();
    assert_eq!(drain_unbounded(&mut consumer, &mut out, &mut remaining), 1);
    assert_eq!(out[3], messages[2]);
    assert_eq!(drain_unbounded(&mut consumer, &mut out, &mut remaining), 2);
    assert_eq!(out[4], messages[3]);
    assert_eq!(out[5].part_slice(0).unwrap(), b"reused");
}

#[test]
fn throughput_drain_tracks_prefetched_remainder() {
    let (mut producer, mut consumer) = yring::spsc(RECV_BATCH_MESSAGES + 1);
    for _ in 0..RECV_BATCH_MESSAGES {
        producer.push(Message::from_slice(b"batch")).unwrap();
    }
    producer.push(Message::from_slice(b"next")).unwrap();
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, usize::MAX);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        RECV_BATCH_MESSAGES
    );
    assert_eq!(batch.len(), RECV_BATCH_MESSAGES);
    assert_eq!(remaining.remaining, 1);
    assert!(!consumer.is_empty());

    let next = consumer.prefetch_and_pop().unwrap();
    assert_eq!(next.part_bytes(0).unwrap(), &b"next"[..]);
}

#[test]
fn throughput_single_item_batches_do_not_skip_next_slot() {
    let (mut producer, mut consumer) = yring::spsc(4);
    let mut batch = Vec::new();
    let mut remaining = super::RingDrain::default();

    producer.push(Message::from_slice(b"first")).unwrap();
    producer.flush();
    let mut budget = DrainBudget::new(256, usize::MAX);
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        1
    );

    producer.push(Message::from_slice(b"second")).unwrap();
    producer.flush();
    let mut budget = DrainBudget::new(256, usize::MAX);
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        1
    );
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.remove(0).part_bytes(0).unwrap(), &b"first"[..]);
    assert_eq!(batch.remove(0).part_bytes(0).unwrap(), &b"second"[..]);
}

#[test]
fn one_item_drain_keeps_remainder_for_next_fair_round() {
    let (mut producer, mut consumer) = yring::spsc(8);
    producer.push(Message::from_slice(b"first")).unwrap();
    producer.push(Message::from_slice(b"second")).unwrap();
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(256, usize::MAX);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring_one_into_batch(&mut consumer, &mut batch, &mut remaining, &mut budget),
        (1, false)
    );
    assert_eq!(batch.remove(0).part_bytes(0).unwrap().as_ref(), b"first");
    assert_eq!(remaining.remaining, 1);
    assert!(!consumer.is_empty());
}

#[test]
fn large_message_exhausts_throughput_budget() {
    let (mut producer, mut consumer) = yring::spsc(4);
    let large = vec![0; 65_537];
    producer.push(Message::from_slice(&large)).unwrap();
    producer.push(Message::from_slice(&large)).unwrap();
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        1
    );
    assert_eq!(batch.len(), 1);
}

#[test]
fn large_message_batch_survives_ring_slot_reuse() {
    const MSG_SIZE: usize = 1024 * 1024;
    let (mut producer, mut consumer) = yring::spsc(1);
    let first = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
    let second = (0..MSG_SIZE)
        .map(|i| 255u8.wrapping_sub((i & 0xFF) as u8))
        .collect::<Vec<_>>();

    producer.push(Message::single(first.clone())).unwrap();
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        1
    );
    assert_eq!(remaining.remaining, 0);

    producer.push(Message::single(second.clone())).unwrap();
    producer.flush();

    assert_eq!(
        batch.first().unwrap().part_bytes(0).unwrap().as_ref(),
        first.as_slice()
    );

    let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut next_budget,
            || {}
        ),
        1
    );
    assert_eq!(
        batch.remove(0).part_bytes(0).unwrap().as_ref(),
        first.as_slice()
    );
    assert_eq!(
        batch.remove(0).part_bytes(0).unwrap().as_ref(),
        second.as_slice()
    );
}

#[test]
fn held_large_message_survives_many_ring_wraps() {
    const MSG_SIZE: usize = 1024 * 1024;
    let (mut producer, mut consumer) = yring::spsc(1);
    let first = (0..MSG_SIZE)
        .map(|i| ((i as u8).wrapping_mul(31)) ^ ((i >> 8) as u8))
        .collect::<Vec<_>>();

    producer.push(Message::single(first.clone())).unwrap();
    producer.flush();

    let mut batch = Vec::new();
    let mut budget = DrainBudget::new(256, RECV_BATCH_BYTES);
    let mut remaining = super::RingDrain::default();
    assert_eq!(
        drain_yring(
            &mut consumer,
            &mut batch,
            &mut remaining,
            &mut budget,
            || {}
        ),
        1
    );
    let held = batch.remove(0);

    for seq in 0..16u8 {
        let next = (0..MSG_SIZE)
            .map(|i| seq ^ (i as u8).wrapping_mul(17))
            .collect::<Vec<_>>();
        producer.push(Message::single(next.clone())).unwrap();
        producer.flush();

        let mut next_budget = DrainBudget::new(256, RECV_BATCH_BYTES);
        assert_eq!(
            drain_yring(
                &mut consumer,
                &mut batch,
                &mut remaining,
                &mut next_budget,
                || {}
            ),
            1
        );
        assert_eq!(
            batch.remove(0).part_bytes(0).unwrap().as_ref(),
            next.as_slice()
        );
        assert_eq!(held.part_bytes(0).unwrap().as_ref(), first.as_slice());
    }
}
#[test]
fn dropping_a_ring_consumer_wakes_a_producer_waiting_for_space() {
    let (mut producer, consumer) = tcp_consumer(7);
    while producer.push(Message::single("x")).is_ok() {}
    producer.flush();
    let space = consumer.space.clone();
    let seen = space.generation();
    drop(consumer);
    // The wake follows the disconnect, so the producer's recheck sees it.
    assert_ne!(space.generation(), seen);
    assert!(producer.is_consumer_dropped());
}
