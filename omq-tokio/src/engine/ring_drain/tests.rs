use omq_proto::message::Message;

use super::RingDrain;

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
        let mut progress = RingDrain::default();
        let lwm = RingDrain::lwm(capacity);

        for popped in 1..=lwm {
            let (message, wake) = progress.pop(&mut consumer);
            assert!(message.is_some());
            assert_eq!(wake, popped == lwm);
            assert_eq!(producer.is_full(), popped < lwm);
        }

        assert_eq!(progress.popped, 0);
        if capacity > lwm {
            let (message, wake) = progress.pop(&mut consumer);
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

    let mut remaining = RingDrain::default();
    let (first, released) = remaining.pop(&mut consumer);
    assert_eq!(first.unwrap().part_bytes(0).unwrap(), &b"a"[..]);
    assert!(!released);

    let (second, released) = remaining.pop(&mut consumer);
    assert_eq!(second.unwrap().part_bytes(0).unwrap(), &b"b"[..]);
    assert!(released);

    let (third, released) = remaining.pop(&mut consumer);
    assert!(third.is_none());
    assert!(!released);

    producer.push(Message::from_slice(b"c")).unwrap();
    producer.flush();
    let (next, released) = remaining.pop(&mut consumer);
    assert_eq!(next.unwrap().part_bytes(0).unwrap(), &b"c"[..]);
    assert!(released);
}
