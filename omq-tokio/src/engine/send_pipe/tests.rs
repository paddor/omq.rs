use tokio::time::{Duration, timeout};

use super::*;

#[test]
fn deferred_sends_publish_before_full_returns() {
    let (mut tx, mut rx) = send_pipe(2);
    tx.try_send_deferred(Message::single("first")).unwrap();
    tx.try_send_deferred(Message::single("second")).unwrap();
    assert!(tx.data_signal.is_idle());
    let mut received = Vec::new();
    assert_eq!(rx.drain_into(&mut received, 2, usize::MAX), 0);
    assert!(matches!(
        tx.try_send_deferred(Message::single("full")),
        Err(SendPipeError::Full(_))
    ));
    assert_eq!(rx.drain_into(&mut received, 2, usize::MAX), 2);
    assert_eq!(
        received,
        [Message::single("first"), Message::single("second")]
    );
}

#[test]
fn dropping_a_deferred_producer_publishes_its_accepted_messages() {
    let (mut tx, mut rx) = send_pipe(2);
    tx.try_send_deferred(Message::single("last")).unwrap();
    drop(tx);
    let mut received = Vec::new();
    assert_eq!(rx.drain_into(&mut received, 2, usize::MAX), 1);
    assert_eq!(received, [Message::single("last")]);
    assert!(rx.is_disconnected());
}

#[test]
fn peer_send_byte_options_honor_defaults_and_explicit_limits() {
    const MIB: usize = 1024 * 1024;
    for (options, expected) in [
        (omq_proto::Options::default(), 16 * MIB),
        (
            omq_proto::Options::default().max_message_size(128 * MIB),
            128 * MIB,
        ),
        (
            omq_proto::Options::default()
                .max_message_size(128 * MIB)
                .peer_send_max_bytes(16 * MIB),
            16 * MIB,
        ),
    ] {
        let (producer, _consumer) = peer_send_pipe(16, &options);
        assert_eq!(producer.peer_max_bytes(), Some(expected));
    }
}

#[tokio::test]
async fn metadata_drain_preserves_limits_order_and_capacity_wakes() {
    for (mut tx, mut rx) in [
        send_pipe(4),
        peer_send_pipe(4, &omq_proto::Options::default()),
    ] {
        for sequence in 0..4 {
            tx.try_send(Message::from(vec![sequence; 4096])).unwrap();
        }
        let mut batch = Vec::new();
        for (count, bytes) in [(0, 128), (4, 0)] {
            assert_eq!(
                rx.drain_into_with_cost(&mut batch, count, bytes, |_, _| 64),
                0
            );
        }
        assert_eq!(rx.drain_into_with_cost(&mut batch, 4, 128, |_, _| 64), 2);
        timeout(Duration::from_millis(10), rx.ready())
            .await
            .expect("bounded drain must rearm the remaining messages");
        for sequence in 4..6 {
            tx.try_send(Message::from(vec![sequence; 4096])).unwrap();
        }
        assert!(matches!(
            tx.try_send(Message::from(vec![6; 4096])),
            Err(SendPipeError::Full(_))
        ));
        assert_eq!(rx.drain_into_with_cost(&mut batch, 1, 128, |_, _| 64), 1);
        assert_eq!(rx.drain_into(&mut batch, 4, 128), 1);
        assert_eq!(rx.drain_into_with_cost(&mut batch, 4, 128, |_, _| 64), 2);
        assert_eq!(batch.len(), 6);
        for (sequence, message) in (0..6).zip(&batch) {
            assert!(
                message
                    .part_slice(0)
                    .unwrap()
                    .iter()
                    .all(|&b| b == sequence)
            );
        }
        assert!(rx.is_empty());
        assert!(
            timeout(Duration::from_millis(10), rx.ready())
                .await
                .is_err()
        );
    }
}

#[cfg(feature = "dart")]
#[test]
fn dart_direct_drain_obeys_budgets_and_preserves_the_suffix() {
    let (mut tx, mut rx) = send_pipe(4);
    for body in ["a", "bb", "ccc", "dddd"] {
        tx.try_send(Message::single(body)).unwrap();
    }
    let mut received = Vec::new();
    assert_eq!(
        rx.drain_queue_with(0, 4, |_| panic!("zero count")),
        Some((0, 0))
    );
    assert_eq!(
        rx.drain_queue_with(4, 0, |_| panic!("zero bytes")),
        Some((0, 0))
    );
    assert_eq!(
        rx.drain_queue_with(1, 10, |message| {
            received.push(message);
            true
        }),
        Some((1, 1))
    );
    assert_eq!(
        rx.drain_queue_with(4, 2, |message| {
            received.push(message);
            true
        }),
        Some((1, 2))
    );
    assert_eq!(
        rx.drain_queue_with(4, 10, |message| {
            received.push(message);
            false
        }),
        Some((1, 3))
    );
    assert!(!rx.is_empty());
    assert_eq!(
        rx.drain_queue_with(4, 10, |message| {
            received.push(message);
            true
        }),
        Some((1, 4))
    );
    assert_eq!(
        received
            .iter()
            .map(|message| message.part_bytes(0).unwrap())
            .collect::<Vec<_>>(),
        ["a", "bb", "ccc", "dddd"]
    );
    assert!(rx.is_empty());
}

#[cfg(feature = "dart")]
#[test]
fn dart_direct_drain_keeps_admission_until_ack_and_observes_close() {
    let (mut tx, mut rx) = send_pipe(2);
    tx.set_dart(omq_proto::SocketType::Scatter);
    tx.try_send(Message::single("first")).unwrap();
    tx.try_send(Message::single("second")).unwrap();
    assert_eq!(rx.drain_queue_with(2, 2048, |_| true), Some((2, 11)));
    assert!(matches!(
        tx.try_send(Message::single("third")),
        Err(SendPipeError::Full(_))
    ));
    rx.dart_acknowledge(1);
    tx.try_send(Message::single("third")).unwrap();
    drop(tx);
    assert!(!rx.is_disconnected());
    assert_eq!(rx.drain_queue_with(2, 2048, |_| true), Some((1, 5)));
    assert!(rx.is_disconnected());

    let (mut tx, mut rx) = send_pipe(1);
    tx.set_dart(omq_proto::SocketType::Scatter);
    tx.try_send(Message::single("retained")).unwrap();
    assert_eq!(rx.drain_queue_with(1, 2048, |_| true), Some((1, 8)));
    drop(rx);
    assert!(matches!(
        tx.try_send(Message::single("closed")),
        Err(SendPipeError::Closed(_))
    ));
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn dart_direct_drain_rearms_and_reactivates_at_low_water() {
    let (mut tx, mut rx) = send_pipe(4);
    for _ in 0..4 {
        tx.try_send(Message::single("x")).unwrap();
    }
    assert!(matches!(
        tx.try_send(Message::single("full")),
        Err(SendPipeError::Full(_))
    ));
    assert_eq!(rx.drain_queue_with(4, 2048, |_| false), Some((1, 1)));
    assert!(tx.above_lwm.load(Ordering::Acquire));
    timeout(Duration::from_millis(10), rx.ready())
        .await
        .expect("suffix must rearm");
    assert_eq!(rx.drain_queue_with(4, 2048, |_| false), Some((1, 1)));
    assert!(!tx.above_lwm.load(Ordering::Acquire));
    assert_eq!(rx.drain_queue_with(4, 2048, |_| true), Some((2, 2)));
    assert!(
        timeout(Duration::from_millis(10), rx.ready())
            .await
            .is_err()
    );
}

#[cfg(feature = "dart")]
#[test]
fn dart_direct_drain_leaves_peer_and_conflate_untouched() {
    for (mut tx, mut rx) in [
        peer_send_pipe(4, &omq_proto::Options::default()),
        send_pipe_with_mode(4, SendPipeMode::Conflate),
    ] {
        tx.try_send(Message::single("body")).unwrap();
        assert_eq!(
            rx.drain_queue_with(4, 2048, |_| panic!("not a Queue")),
            None
        );
        assert!(!rx.data_signal.is_idle());
        let mut staged = Vec::new();
        assert_eq!(rx.drain_into(&mut staged, 4, 2048), 1);
        assert_eq!(staged[0].part_bytes(0).unwrap().as_ref(), b"body");
    }
}

#[cfg(feature = "dart")]
#[test]
fn dart_drain_keeps_send_admission_until_ack() {
    let (mut tx, mut rx) = send_pipe(2);
    tx.set_dart(omq_proto::SocketType::Scatter);
    tx.try_send(Message::single("first")).unwrap();
    tx.try_send(Message::single("second")).unwrap();
    let mut staged = Vec::with_capacity(2);
    assert_eq!(rx.drain_into(&mut staged, 2, 2048), 2);
    assert!(matches!(
        tx.try_send(Message::single("third")),
        Err(SendPipeError::Full(_))
    ));
    rx.dart_acknowledge(1);
    tx.try_send(Message::single("third")).unwrap();
    drop(rx);
    assert!(matches!(
        tx.try_send(Message::single("fourth")),
        Err(SendPipeError::Closed(_))
    ));
}

#[cfg(feature = "dart")]
#[test]
fn dart_full_window_still_rejects_invalid_bodies_before_admission() {
    let (mut tx, _rx) = send_pipe(1);
    tx.set_dart(omq_proto::SocketType::Scatter);
    tx.try_send(Message::single("first")).unwrap();
    assert!(matches!(
        tx.try_send(Message::multipart([
            bytes::Bytes::from_static(b"first"),
            bytes::Bytes::from_static(b"second"),
        ])),
        Err(SendPipeError::Invalid(_))
    ));
}

#[cfg(feature = "dart")]
#[test]
fn dart_bulk_admits_only_the_available_retention_prefix() {
    let (mut tx, mut rx) = send_pipe(4);
    tx.set_dart(omq_proto::SocketType::Scatter);
    let mut messages = VecDeque::from([
        Message::single("a"),
        Message::single("b"),
        Message::single("c"),
    ]);
    assert_eq!(tx.try_send_many(&mut messages, 3).unwrap(), 3);
    let mut staged = Vec::with_capacity(4);
    assert_eq!(rx.drain_into(&mut staged, 4, 4096), 3);
    messages.extend([
        Message::single("d"),
        Message::single("e"),
        Message::single("f"),
    ]);
    assert_eq!(tx.try_send_many(&mut messages, 3).unwrap(), 1);
    assert_eq!(messages.len(), 2);
    rx.dart_acknowledge(2);
    assert_eq!(tx.try_send_many(&mut messages, 3).unwrap(), 2);
    assert!(messages.is_empty());
}

#[tokio::test]
async fn data_ready_rearms_until_pipe_drains() {
    let (mut tx, mut rx) = send_pipe(4);
    tx.try_send(Message::single("a")).unwrap();
    tx.try_send(Message::single("b")).unwrap();

    timeout(Duration::from_secs(1), rx.ready())
        .await
        .expect("first send should notify");

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);

    timeout(Duration::from_secs(1), rx.ready())
        .await
        .expect("partial drain should rearm");

    assert_eq!(rx.drain_into(&mut batch, 4, usize::MAX), 1);
    assert_eq!(batch.len(), 2);

    assert!(
        timeout(Duration::from_millis(10), rx.ready())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ready_observes_nonempty_pipe_when_signal_is_idle() {
    let (mut tx, rx) = send_pipe(4);
    tx.try_send(Message::single("tail")).unwrap();
    rx.data_signal.begin_drain();
    assert!(!rx.data_signal.clear_after(true));

    timeout(Duration::from_millis(10), rx.ready())
        .await
        .expect("nonempty pipe must be ready even without a pending signal");
}

#[tokio::test]
async fn stale_pending_signal_returns_to_drain_path() {
    let (mut tx, mut rx) = send_pipe(4);
    tx.try_send(Message::single("before-drain")).unwrap();
    rx.data_signal.begin_drain();
    tx.try_send(Message::single("during-drain")).unwrap();

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 4, usize::MAX), 2);
    assert!(rx.is_empty());

    timeout(Duration::from_millis(10), rx.ready())
        .await
        .expect("stale pending signal must return to the drain path");
    assert_eq!(rx.drain_into(&mut batch, 4, usize::MAX), 0);
    assert!(
        timeout(Duration::from_millis(10), rx.ready())
            .await
            .is_err()
    );
}

#[test]
fn space_reactivates_at_half_capacity_after_full() {
    let (mut tx, mut rx) = send_pipe(4);
    for _ in 0..4 {
        tx.try_send(Message::single("x")).unwrap();
    }
    assert!(matches!(
        tx.try_send(Message::single("x")),
        Err(SendPipeError::Full(_))
    ));
    assert!(tx.above_lwm.load(Ordering::Acquire));

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
    assert!(tx.above_lwm.load(Ordering::Acquire));
    assert!(!rx.take_freed_full());

    assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
    assert!(!tx.above_lwm.load(Ordering::Acquire));
    assert!(rx.take_freed_full());
    assert!(!rx.take_freed_full());
}

#[test]
fn conflate_pipe_keeps_latest_message() {
    let (mut tx, mut rx) = send_pipe_with_mode(1, SendPipeMode::Conflate);
    tx.try_send(Message::single("a")).unwrap();
    tx.try_send(Message::single("b")).unwrap();
    tx.try_send(Message::single("c")).unwrap();

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 8, usize::MAX), 1);
    assert_eq!(batch[0].part_bytes(0).unwrap().as_ref(), b"c");
    assert_eq!(rx.drain_into(&mut batch, 8, usize::MAX), 0);
}
