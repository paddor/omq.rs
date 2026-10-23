use super::*;
use omq_shm::{BatchKind, Connection, Received, ReceivedBatch};
use std::io::IoSlice;
use std::os::unix::net::UnixStream;

fn batch(count: usize) -> ReceivedBatch {
    let (first, second) = UnixStream::pair().unwrap();
    let mut sender = Connection::new(&first, 256, None).unwrap();
    let mut receiver = Connection::new(&second, 256, None).unwrap();
    let transfer = |sender: &mut Connection,
                    receiver: &mut Connection,
                    first: &UnixStream,
                    second: &UnixStream| {
        assert_eq!(sender.try_write(first, &[IoSlice::new(b"x")]).unwrap(), 1);
        assert_eq!(receiver.try_read(second, &mut [0]).unwrap(), 1);
    };
    transfer(&mut sender, &mut receiver, &first, &second);
    transfer(&mut receiver, &mut sender, &second, &first);
    sender.accept_preface(&receiver.preface()).unwrap();
    receiver.accept_preface(&sender.preface()).unwrap();
    let messages: Vec<_> = (0..count)
        .map(|seq| Message::from_slice(&[seq as u8; 64]))
        .collect();
    let prepared = sender
        .prepare_batch(&messages.iter().collect::<Vec<_>>(), BatchKind::Copied)
        .unwrap()
        .unwrap();
    let mut frames = omq_proto::frame_buffer::FrameBuffer::new();
    assert!(
        sender
            .encode_batch_and_frame(&prepared, BatchKind::Copied, &mut frames)
            .unwrap()
    );
    transfer(&mut sender, &mut receiver, &first, &second);
    let bytes = frames.arena_bytes();
    assert_eq!(bytes[0], 0);
    assert_eq!(bytes.len(), usize::from(bytes[1]) + 2);
    let Received::Batch(batch) = receiver
        .decode_delivery(Message::from_slice(&bytes[2..]))
        .unwrap()
    else {
        panic!("expected batch")
    };
    batch
}

fn queue(capacity: usize, sources: bool) -> Arc<Fanin> {
    Fanin::build(
        capacity,
        Arc::new(DataSignal::new()),
        BlockingRecvWaker::new(),
        sources,
    )
}

#[test]
fn partial_batches_charge_messages_and_resume_without_overtaking() {
    let queue = queue(4, false);
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    assert!(sink.push_batch(batch(8)));
    assert_eq!(
        match sink.pending.as_ref().unwrap() {
            Item::Batch(batch) => batch.len(),
            Item::Message(_) => panic!("expected pending batch"),
        },
        4
    );
    let mut received = Vec::new();
    assert_eq!(
        queue
            .recv_into(&mut received, DrainBudget::new(3, 4096), true)
            .unwrap(),
        3
    );
    assert!(sink.retry_pending());
    assert_eq!(
        match sink.pending.as_ref().unwrap() {
            Item::Batch(batch) => batch.len(),
            Item::Message(_) => panic!("expected pending batch"),
        },
        1
    );
    assert_eq!(
        queue
            .recv_into(&mut received, DrainBudget::new(4, 4096), true)
            .unwrap(),
        4
    );
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert!(sink.push(Message::from_slice(&[99; 64])));
    assert_eq!(
        queue
            .recv_into(&mut received, DrainBudget::new(4, 4096), true)
            .unwrap(),
        2
    );
    for (seq, message) in received[..8].iter().enumerate() {
        assert_eq!(message.part_slice(0).unwrap(), &[seq as u8; 64]);
    }
    assert_eq!(received[8].part_slice(0).unwrap(), &[99; 64]);
}

#[test]
fn scalar_batches_rotate_with_other_peers_and_publish_lwm_credits() {
    let queue = queue(128, false);
    let mut sink = Sink::owned(queue.register().unwrap());
    let mut other = queue.register().unwrap();
    assert!(sink.enable_batches());
    assert!(sink.push_batch(batch(128)));
    let seen = sink.space.generation();
    for seq in 0..128_u8 {
        other.try_send(Message::from_slice(&[200, seq])).unwrap();
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
        assert_eq!(
            queue.try_recv().unwrap().part_slice(0).unwrap(),
            &[200, seq]
        );
        if seq < 63 {
            assert_eq!(sink.space.generation(), seen);
        }
    }
    assert_ne!(sink.space.generation(), seen);
    assert!(queue.try_recv().is_err());
}

#[test]
fn batch_scheduler_discovers_a_new_active_peer_among_idle_lanes() {
    const IDLE: usize = 64;
    let queue = queue(256, false);
    let mut idle: Vec<_> = (0..IDLE).map(|_| queue.register().unwrap()).collect();
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    assert!(sink.push_batch(batch(256)));
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0; 64]);
    idle[IDLE - 1]
        .try_send(Message::single("newly ready"))
        .unwrap();
    let mut discovered = false;
    for _ in 0..=IDLE {
        if queue.try_recv().unwrap() == Message::single("newly ready") {
            discovered = true;
            break;
        }
    }
    assert!(
        discovered,
        "inactive peers must join a continuously busy drain"
    );
}

#[test]
fn batch_scheduler_remaps_active_lanes_after_disconnection() {
    let queue = queue(8, false);
    let mut removed = queue.register().unwrap();
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    removed.try_send(Message::single("old")).unwrap();
    assert!(sink.push_batch(batch(8)));
    assert_eq!(queue.try_recv().unwrap(), Message::single("old"));
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0; 64]);
    drop(removed);
    let mut added = queue.register().unwrap();
    added.try_send(Message::single("new")).unwrap();
    let mut seen = Vec::new();
    for _ in 0..8 {
        seen.push(queue.try_recv().unwrap());
    }
    assert!(seen.contains(&Message::single("new")));
    for seq in 1..8_u8 {
        assert!(seen.contains(&Message::from_slice(&[seq; 64])));
    }
}

#[test]
fn idle_native_peers_cannot_hide_a_ready_shm_batch() {
    let queue = queue(8, false);
    let _idle: Vec<_> = (0..8).map(|_| queue.register().unwrap()).collect();
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    assert!(queue.try_recv().is_err());
    assert!(sink.push_batch(batch(8)));
    for seq in 0..8_u8 {
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
    }
}

#[test]
fn native_readiness_probe_keeps_its_message_across_disconnection_and_registration() {
    let queue = queue(8, false);
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    let mut native = queue.register().unwrap();
    assert!(sink.push_batch(batch(8)));
    for seq in 0..2_u8 {
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
    }
    native.try_send(Message::single("last native")).unwrap();
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[2; 64]);
    drop(native);
    let _new_peer = queue.register().unwrap();
    assert_eq!(queue.try_recv().unwrap(), Message::single("last native"));
    for seq in 3..8_u8 {
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq; 64]);
    }
}

#[test]
fn source_claims_pause_and_return_the_batch_cursor() {
    let queue = queue(8, true);
    let mut sink = Sink::owned(queue.register().unwrap());
    let mut other = queue.register().unwrap();
    assert!(sink.enable_batches());
    assert!(sink.push_batch(batch(8)));
    let (receipt, message) = queue.try_recv_with_receipt(None).unwrap();
    let source = receipt.lane().unwrap().clone();
    other.try_send(Message::single("other")).unwrap();
    assert_eq!(queue.try_recv().unwrap(), Message::single("other"));
    assert!(queue.try_recv().is_err());
    queue.unshift(receipt, message).unwrap();
    let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
    assert_eq!(message.part_slice(0).unwrap(), &[0; 64]);
    drop(receipt);
    for seq in 1..8_u8 {
        let (receipt, message) = queue.try_recv_with_receipt(Some(&source)).unwrap();
        assert_eq!(message.part_slice(0).unwrap(), &[seq; 64]);
        drop(receipt);
    }
}

#[tokio::test]
async fn receiver_close_releases_a_blocked_batch_waiter() {
    let queue = queue(1, false);
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.enable_batches());
    assert!(sink.push_batch(batch(4)));
    assert!(sink.blocked());
    queue.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
        .await
        .unwrap();
    assert!(!sink.retry_pending());
}
#[test]
fn plain_bulk_receive_reuses_output_storage_and_preserves_existing_values() {
    let queue = queue(16, false);
    let mut producer = queue.register().unwrap();
    producer.try_send(Message::single("first")).unwrap();
    producer.try_send(Message::single("second")).unwrap();
    let mut out = Vec::with_capacity(16);
    out.push(Message::single("prefix"));
    let allocation = out.as_ptr();
    assert_eq!(
        queue
            .recv_into(&mut out, DrainBudget::new(4, 4096), true)
            .unwrap(),
        2
    );
    assert_eq!(out.as_ptr(), allocation);
    assert_eq!(
        out,
        [
            Message::single("prefix"),
            Message::single("first"),
            Message::single("second")
        ]
    );
    out.clear();
    producer.try_send(Message::single("reused")).unwrap();
    assert_eq!(
        queue
            .recv_into(&mut out, DrainBudget::new(4, 4096), true)
            .unwrap(),
        1
    );
    assert_eq!(out.as_ptr(), allocation);
}

#[test]
fn disconnected_batches_drain_before_lane_reuse_without_reviving_sources() {
    let queue = queue(2, true);
    for _ in 0..16 {
        let mut sink = Sink::owned(queue.register().unwrap());
        assert!(sink.enable_batches());
        assert!(sink.push_batch(batch(4)));
        let (receipt, held) = queue.try_recv_with_receipt(None).unwrap();
        let source = receipt.lane().unwrap().clone();
        drop(sink);
        drop(receipt);
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[1; 64]);
        assert!(queue.try_recv().is_err());
        assert!(matches!(
            queue.try_recv_with_receipt(Some(&source)),
            Err(Error::Closed)
        ));
        assert_eq!(held.part_slice(0).unwrap(), &[0; 64]);
    }
}
