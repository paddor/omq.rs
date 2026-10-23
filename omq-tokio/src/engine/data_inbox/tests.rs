use super::*;
use omq_proto::Message;
use std::task::Context;
use std::time::Duration;

fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
}

fn message(n: u8) -> PeerDriverData {
    PeerDriverData::SendMessage(Message::from_slice(&[n]))
}

fn value(data: PeerDriverData) -> u8 {
    let PeerDriverData::SendMessage(message) = data else {
        panic!("raw message")
    };
    message.part_slice(0).unwrap()[0]
}

#[test]
fn clone_lanes_have_exact_capacity_and_fifo() {
    let (root, mut receiver) = channel(3);
    let scope = SenderLanes::default();
    let first = scope.bind(&root);
    let shared = scope.clone_shared().bind(&root);
    let second = scope.clone().bind(&root);
    for n in 0..3 {
        first.try_send(message(n)).unwrap();
    }
    assert!(matches!(
        shared.try_send(message(3)),
        Err(legacy::error::TrySendError::Full(_))
    ));
    for n in 10..13 {
        second.try_send(message(n)).unwrap();
    }
    assert!(matches!(
        second.try_send(message(13)),
        Err(legacy::error::TrySendError::Full(_))
    ));
    let mut first_values = Vec::new();
    let mut second_values = Vec::new();
    for _ in 0..6 {
        let n = value(receiver.try_recv().unwrap());
        if n < 10 {
            first_values.push(n);
        } else {
            second_values.push(n);
        }
    }
    assert_eq!(first_values, [0, 1, 2]);
    assert_eq!(second_values, [10, 11, 12]);
    receiver.release_consumed();
    shared.try_send(message(3)).unwrap();
}

#[test]
fn batch_returns_only_consumed_credits_across_lanes() {
    let (root, mut receiver) = channel(3);
    let scope = SenderLanes::default();
    let first = scope.bind(&root);
    let second = scope.clone().bind(&root);
    for n in 0..3 {
        first.try_send(message(n)).unwrap();
        second.try_send(message(n + 10)).unwrap();
    }
    let mut values = Vec::new();
    {
        let mut batch = receiver.batch();
        for _ in 0..4 {
            values.push(value(batch.try_recv().unwrap()));
        }
    }
    let first_count = values.iter().filter(|&&n| n < 10).count();
    let second_count = values.len() - first_count;
    for (sender, count) in [(&first, first_count), (&second, second_count)] {
        for _ in 0..count {
            sender.try_send(message(99)).unwrap();
        }
        assert!(matches!(
            sender.try_send(message(99)),
            Err(legacy::error::TrySendError::Full(_))
        ));
    }
    let remaining: Vec<_> = (0..6)
        .map(|_| value(receiver.try_recv().unwrap()))
        .filter(|&n| n != 99)
        .collect();
    assert_eq!(remaining.len(), 2);
    assert!(matches!(
        receiver.try_recv(),
        Err(legacy::error::TryRecvError::Empty)
    ));
}

#[test]
fn finished_batch_reports_a_full_lane() {
    let (root, mut receiver) = channel(3);
    let sender = SenderLanes::default().bind(&root);
    sender.try_send(message(0)).unwrap();
    let mut batch = receiver.batch();
    assert_eq!(value(batch.try_recv().unwrap()), 0);
    assert!(!batch.finish());

    for n in 1..4 {
        sender.try_send(message(n)).unwrap();
    }
    let mut batch = receiver.batch();
    assert_eq!(value(batch.try_recv().unwrap()), 1);
    assert!(batch.finish());
}

#[tokio::test]
async fn batch_drop_wakes_capacity_and_finishes_close() {
    let (root, mut receiver) = channel(3);
    let sender = SenderLanes::default().bind(&root);
    for n in 0..3 {
        sender.try_send(message(n)).unwrap();
    }
    let wait = sender.wait_capacity();
    tokio::pin!(wait);
    assert!(poll_once(wait.as_mut()).is_pending());
    {
        let mut batch = receiver.batch();
        for n in 0..3 {
            assert_eq!(value(batch.try_recv().unwrap()), n);
        }
        assert!(poll_once(wait.as_mut()).is_pending());
    }
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    for n in 0..3 {
        sender.try_send(message(n)).unwrap();
    }
    receiver.close();
    {
        let mut batch = receiver.batch();
        for n in 0..3 {
            assert_eq!(value(batch.try_recv().unwrap()), n);
        }
    }
    assert!(receiver.is_empty());
    assert!(receiver.recv().await.is_none());
}

#[test]
fn unwinding_a_batch_returns_credits_without_dropping_queued_data() {
    let (root, mut receiver) = channel(3);
    let sender = SenderLanes::default().bind(&root);
    for n in 0..3 {
        sender.try_send(message(n)).unwrap();
    }
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut batch = receiver.batch();
            assert_eq!(value(batch.try_recv().unwrap()), 0);
            assert_eq!(value(batch.try_recv().unwrap()), 1);
            panic!("encoding failed");
        }))
        .is_err()
    );
    sender.try_send(message(3)).unwrap();
    sender.try_send(message(4)).unwrap();
    assert!(matches!(
        sender.try_send(message(5)),
        Err(legacy::error::TrySendError::Full(_))
    ));
    for n in 2..5 {
        assert_eq!(value(receiver.try_recv().unwrap()), n);
    }
}

#[tokio::test]
async fn registration_wait_reclaims_retired_lanes_even_without_data() {
    let (root, mut receiver) = channel(1);
    let mut scopes = Vec::new();
    for _ in 0..MAX_LANES {
        let scope = SenderLanes::default();
        assert!(scope.bind(&root).send_ready());
        scopes.push(scope);
    }
    let waiting = SenderLanes::default().bind(&root);
    let future = waiting.wait_capacity();
    tokio::pin!(future);
    assert!(poll_once(future.as_mut()).is_pending());
    drop(scopes.pop());
    // The empty receive poll does registration maintenance.
    assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
    tokio::time::timeout(Duration::from_secs(1), future)
        .await
        .unwrap();
    waiting.try_send(message(9)).unwrap();
    assert_eq!(value(receiver.recv().await.unwrap()), 9);
}

#[tokio::test]
async fn canceled_wait_and_ready_probe_preserve_other_waiters() {
    let (root, mut receiver) = channel(1);
    let sender = SenderLanes::default().bind(&root);
    sender.try_send(message(1)).unwrap();
    let wait = sender.wait_capacity();
    tokio::pin!(wait);
    assert!(poll_once(wait.as_mut()).is_pending());
    {
        let canceled = sender.wait_capacity();
        tokio::pin!(canceled);
        assert!(poll_once(canceled.as_mut()).is_pending());
    }
    assert_eq!(value(receiver.recv().await.unwrap()), 1);
    assert!(sender.send_ready());
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
}

#[tokio::test]
async fn graceful_close_drains_reservations_and_wakes_muted_senders() {
    let (root, mut receiver) = channel(1);
    let sender = SenderLanes::default().bind(&root);
    let reserved = sender.try_reserve().unwrap();
    receiver.close();
    assert!(!receiver.is_empty());
    assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
    reserved.send(message(7));
    assert_eq!(value(receiver.recv().await.unwrap()), 7);
    assert!(receiver.recv().await.is_none());
    assert!(matches!(
        sender.try_send(message(8)),
        Err(legacy::error::TrySendError::Closed(_))
    ));

    let (root, mut receiver) = channel(1);
    let sender = SenderLanes::default().bind(&root);
    sender.try_send(message(1)).unwrap();
    let wait = sender.wait_capacity();
    tokio::pin!(wait);
    assert!(poll_once(wait.as_mut()).is_pending());
    receiver.close();
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    assert_eq!(value(receiver.recv().await.unwrap()), 1);
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn abandoning_reservation_completes_graceful_close() {
    let (root, mut receiver) = channel(1);
    let sender = SenderLanes::default().bind(&root);
    let reserved = sender.try_reserve().unwrap();
    receiver.close();
    let wait = receiver.recv();
    tokio::pin!(wait);
    assert!(poll_once(wait.as_mut()).is_pending());
    drop(reserved);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn final_ack_batch_wakes_a_closed_receiver_and_close_progress() {
    #[derive(Debug, Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (root, mut receiver) = dart_channel(3, omq_proto::SocketType::Radio);
    let sender = SenderLanes::default().bind(&root);
    let progress = Arc::new(StateSignal::new());
    sender.watch_close(&progress);
    for n in 0..3 {
        sender.try_send(message(n)).unwrap();
    }
    let claims: Vec<_> = (0..3)
        .map(|_| receiver.dart_try_recv().unwrap().1.unwrap())
        .collect();
    receiver.release_consumed();
    receiver.close();
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut done = Box::pin(receiver.recv());
    assert!(
        done.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let seen = progress.generation();
    Admission::release_batch(claims.into_iter());
    assert_ne!(progress.generation(), seen);
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert!(done.await.is_none());
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn native_progress_observes_ring_release_after_admission_release() {
    let (root, mut receiver) = dart_channel(16, omq_proto::SocketType::Radio);
    let sender = SenderLanes::default().bind(&root);
    let progress = Arc::new(StateSignal::new());
    sender.observe_native_progress(&progress);
    for n in 0..16 {
        sender.try_send(message(n)).unwrap();
    }
    assert!(matches!(
        sender.try_reserve_owned(),
        Err(ReserveError::Full(()))
    ));
    let before_pop = progress.generation();
    assert_eq!(value(receiver.try_recv().unwrap()), 0);
    assert_ne!(progress.generation(), before_pop, "admission capacity edge");
    let before_release = progress.generation();
    assert!(
        matches!(sender.try_reserve_owned(), Err(ReserveError::Full(()))),
        "slot release is batched"
    );
    let wait = progress.changed_after(before_release);
    tokio::pin!(wait);
    assert!(poll_once(wait.as_mut()).is_pending());
    receiver.release_consumed();
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    drop(sender.try_reserve_owned().unwrap());
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn native_progress_observes_registration_reclamation() {
    let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
    let progress = Arc::new(StateSignal::new());
    root.observe_native_progress(&progress);
    let mut scopes = Vec::new();
    for _ in 0..MAX_LANES {
        let scope = SenderLanes::default();
        assert!(scope.bind(&root).send_ready());
        scopes.push(scope);
    }
    let waiting = SenderLanes::default().bind(&root);
    let seen = progress.generation();
    assert!(matches!(
        waiting.try_reserve_owned(),
        Err(ReserveError::Full(()))
    ));
    drop(scopes.pop());
    assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
    tokio::time::timeout(Duration::from_secs(1), progress.changed_after(seen))
        .await
        .unwrap();
    drop(waiting.try_reserve_owned().unwrap());
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn owned_reservation_keeps_capacity_and_survives_graceful_close() {
    let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
    let sender = SenderLanes::default().bind(&root);
    let reserved = sender.try_reserve_owned().unwrap();
    assert!(matches!(
        sender.try_send(message(9)),
        Err(legacy::error::TrySendError::Full(_))
    ));
    receiver.close();
    assert!(poll_once(std::pin::pin!(receiver.recv())).is_pending());
    reserved.send(message(7));
    assert_eq!(value(receiver.recv().await.unwrap()), 7);
    assert!(receiver.recv().await.is_none());

    let (root, mut receiver) = dart_channel(1, omq_proto::SocketType::Radio);
    let sender = SenderLanes::default().bind(&root);
    let reserved = sender.try_reserve_owned().unwrap();
    receiver.close();
    assert!(!receiver.is_empty());
    drop(reserved);
    assert!(receiver.recv().await.is_none());
}

#[cfg(feature = "dart")]
#[test]
fn owned_reservation_is_not_stolen_by_another_publication() {
    let (root, mut receiver) = dart_channel(2, omq_proto::SocketType::Radio);
    let sender = SenderLanes::default().bind(&root);
    let reserved = sender.try_reserve_owned().unwrap();
    sender.try_send(message(9)).unwrap();
    assert!(matches!(
        sender.try_reserve_owned(),
        Err(ReserveError::Full(()))
    ));
    assert_eq!(value(receiver.try_recv().unwrap()), 9);
    receiver.release_consumed();
    reserved.send(message(7));
    assert_eq!(value(receiver.try_recv().unwrap()), 7);
}

#[test]
fn publication_reservations_exclude_concurrent_sends_and_prune_reconnects() {
    let (root, receiver) = channel(1);
    let scope = SenderLanes::default();
    let sender = scope.bind(&root);
    let reserved = sender.try_reserve().unwrap();
    let Sender::Lane(lane) = &sender else {
        panic!("bound lane")
    };
    assert!(lane.producer.try_lock().is_err());
    drop(reserved);
    assert!(lane.producer.try_lock().is_ok());
    drop(receiver);
    for _ in 0..100 {
        let (next, receiver) = channel(1);
        scope.bind(&next).try_send(message(1)).unwrap();
        assert_eq!(scope.0.table.load().len(), 1);
        drop(receiver);
    }
}

#[test]
fn disconnected_send_drops_buffer_after_unlocking_producer() {
    #[derive(Debug)]
    struct Owner {
        bytes: [u8; 128],
        lane: std::sync::Weak<Lane>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            assert!(self.lane.upgrade().unwrap().producer.try_lock().is_ok());
            self.dropped.store(true, Ordering::Release);
        }
    }
    let (root, receiver) = channel(1);
    let sender = SenderLanes::default().bind(&root);
    let Sender::Lane(lane) = &sender else {
        panic!("bound lane")
    };
    let dropped = Arc::new(AtomicBool::new(false));
    let bytes = bytes::Bytes::from_owner(Owner {
        bytes: [0; 128],
        lane: Arc::downgrade(lane),
        dropped: dropped.clone(),
    });
    let permit = sender.try_reserve().unwrap();
    drop(receiver);
    permit.send(PeerDriverData::SendMessage(Message::single(bytes)));
    assert!(dropped.load(Ordering::Acquire));
    assert!(sender.is_empty());
}

#[test]
fn coordinated_teardown_releases_queued_buffers_with_idle_clones_alive() {
    #[derive(Debug)]
    struct Owner([u8; 128], Arc<AtomicUsize>);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::AcqRel);
        }
    }
    let (root, receiver) = channel(4);
    let scope = SenderLanes::default();
    let sender = scope.bind(&root);
    let dropped = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let bytes = bytes::Bytes::from_owner(Owner([0; 128], dropped.clone()));
        sender
            .try_send(PeerDriverData::SendMessage(Message::single(bytes)))
            .unwrap();
    }
    drop(receiver);
    assert_eq!(dropped.load(Ordering::Acquire), 4);
    assert!(sender.is_empty());
    assert_eq!(scope.0.table.load().len(), 1);
}

#[test]
fn immediate_tcp_write_unlocks_reserved_producer_before_buffer_release() {
    use crate::engine::transmit_slot::{PeerTransmitSlot, SlotConfig};
    use crate::routing::peer_outbound::PeerOutbound;
    use crate::socket::dispatch::DirectTcpWriter;
    use std::net::{TcpListener, TcpStream};

    #[derive(Debug)]
    struct Owner([u8; 128], std::sync::Weak<Lane>, Arc<AtomicBool>);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            assert!(self.1.upgrade().unwrap().producer.try_lock().is_ok());
            self.2.store(true, Ordering::Release);
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_remote, _) = listener.accept().unwrap();
    tcp.set_nonblocking(true).unwrap();
    let slot = PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            arena_threshold: 4096,
            arena_cap: 4096,
            cap: 8192,
            msg_cap: 4,
            ..SlotConfig::default()
        },
    );
    slot.handshake_done.store(true, Ordering::Release);
    let writer = Arc::new(DirectTcpWriter::new(tcp));
    writer.publish_idle(|| true);
    let (root, _receiver) = channel(4);
    let sender = SenderLanes::default().bind(&root);
    let Sender::Lane(lane) = &sender else {
        panic!("bound lane")
    };
    let dropped = Arc::new(AtomicBool::new(false));
    let bytes = bytes::Bytes::from_owner(Owner([0; 128], Arc::downgrade(lane), dropped.clone()));
    let target = PeerOutbound::Wire {
        slot,
        inbox: sender,
        direct: Some(writer),
    };
    target.try_send(Message::single(bytes)).unwrap();
    assert!(
        dropped.load(Ordering::Acquire),
        "direct write did not finish"
    );
}
