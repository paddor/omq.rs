use super::*;

fn queue(capacity: usize) -> Arc<Fanin> {
    Fanin::new(
        capacity,
        Arc::new(DataSignal::new()),
        BlockingRecvWaker::new(),
    )
}

#[test]
fn single_receives_coalesce_space_wakes_until_release_boundary() {
    for capacity in [1_usize, 2, 4, 16, 32, 256] {
        let release_batch = capacity.div_ceil(2).max(64).min(capacity);
        let queue = queue(capacity);
        let mut sender = queue.register().unwrap();
        for seq in 0..capacity {
            sender.try_send(Message::from_slice(&[seq as u8])).unwrap();
        }
        assert!(sender.is_full()); // Register the real producer space waker.
        let space = sender.space();
        let seen = space.generation();
        for seq in 0..capacity {
            assert_eq!(
                queue.try_recv().unwrap().part_slice(0).unwrap(),
                &[seq as u8]
            );
            if seq + 1 < release_batch {
                assert_eq!(space.generation(), seen, "no per-message space wake");
            } else if seq + 1 == release_batch {
                assert_ne!(space.generation(), seen, "LWM must wake the sender");
            }
        }
        assert_ne!(
            space.generation(),
            seen,
            "consumed batch must wake the sender"
        );
        assert!(!sender.is_full());
        for seq in 0..capacity {
            sender.try_send(Message::from_slice(&[seq as u8])).unwrap();
        }
        assert!(sender.is_full(), "only consumed slots may be reused");
    }
}

#[test]
fn single_receives_release_partial_credits_before_empty() {
    let queue = queue(32);
    let mut sender = queue.register().unwrap();
    for round in 0..64 {
        for seq in 0..3 {
            sender.try_send(Message::from_slice(&[round, seq])).unwrap();
        }
        for seq in 0..3 {
            assert_eq!(
                queue.try_recv().unwrap().part_slice(0).unwrap(),
                &[round, seq]
            );
        }
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        assert!(queue.signal.is_idle());
    }
    for seq in 0..32 {
        sender.try_send(Message::from_slice(&[seq])).unwrap();
    }
    assert!(sender.is_full());
}

#[test]
fn bulk_after_single_receives_releases_partial_credits_from_every_lane() {
    for batching in [false, true] {
        let queue = queue(8);
        let mut senders: Vec<_> = (0..2).map(|_| queue.register().unwrap()).collect();
        for (peer, sender) in senders.iter_mut().enumerate() {
            for seq in 0..8 {
                sender
                    .try_send(Message::from_slice(&[peer as u8, seq]))
                    .unwrap();
            }
            assert!(sender.is_full());
        }
        for peer in 0..2 {
            assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[peer, 0]);
        }
        let mut out = Vec::new();
        queue
            .recv_into(&mut out, DrainBudget::new(1, 1024), batching)
            .unwrap();
        assert_eq!(out[0].part_slice(0).unwrap(), &[0, 1]);
        for (peer, sender) in senders.iter_mut().enumerate() {
            let freed = if peer == 0 { 2 } else { 1 };
            for seq in 8..8 + freed {
                sender
                    .try_send(Message::from_slice(&[peer as u8, seq]))
                    .unwrap();
            }
            assert!(sender.is_full());
        }
    }
}

#[test]
fn single_receives_notice_new_lanes_and_drain_disconnected_senders() {
    let queue = queue(32);
    let mut first = queue.register().unwrap();
    for seq in 0..32 {
        first.try_send(Message::from_slice(&[0, seq])).unwrap();
    }
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, 0]);
    let mut second = queue.register().unwrap();
    second.try_send(Message::from_slice(&[1, 0])).unwrap();
    drop((first, second));
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, 1]);
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[1, 0]);
    for seq in 2..32 {
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[0, seq]);
    }
    assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
    let mut reconnected = queue.register().unwrap();
    reconnected.try_send(Message::from_slice(b"new")).unwrap();
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"new");
}

#[tokio::test]
async fn single_receive_batch_wakes_pending_sink_and_close_wakes_partial_batch() {
    use futures::FutureExt as _;

    for capacity in [1, 2, 16, 32] {
        let queue = queue(capacity);
        let mut sink = Sink::owned(queue.register().unwrap());
        for seq in 0..=capacity {
            assert!(sink.push(Message::from_slice(&[seq as u8])));
        }
        assert!(sink.blocked());
        let receiver = queue.clone();
        let drain = async move {
            // Let ready() register its waiter before releasing capacity.
            tokio::task::yield_now().await;
            for seq in 0..capacity {
                assert_eq!(
                    receiver.try_recv().unwrap().part_slice(0).unwrap(),
                    &[seq as u8]
                );
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(sink.ready(), drain);
        })
        .await
        .expect("released receive batch must wake the pending sink");
        assert!(sink.retry_pending());
        assert!(!sink.blocked());
        for seq in 0..capacity {
            assert!(sink.push(Message::from_slice(&[seq as u8])));
        }
        assert!(sink.blocked());
        assert!(sink.ready().now_or_never().is_none());
        // Close must wake the sink even without reaching a release boundary.
        assert_eq!(
            queue.try_recv().unwrap().part_slice(0).unwrap(),
            &[capacity as u8]
        );
        queue.close();
        tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
            .await
            .expect("closed receiver must wake a pending sink");
        assert!(!sink.retry_pending());
        assert!(matches!(queue.try_recv(), Err(Error::Closed)));
    }
}

#[test]
fn bulk_policy_changes_rotation_and_releases_slots() {
    for batching in [false, true] {
        let queue = queue(32);
        let mut senders: Vec<_> = (0..4).map(|_| queue.register().unwrap()).collect();
        for (peer, sender) in senders.iter_mut().enumerate() {
            for seq in 0..32 {
                sender
                    .try_send(Message::from_slice(&[peer as u8, seq]))
                    .unwrap();
            }
            assert!(sender.is_full());
        }
        let mut out = Vec::with_capacity(256);
        let allocation = out.as_ptr();
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(8, 2 * 1024 * 1024), batching)
                .unwrap(),
            8
        );
        for (index, message) in out.iter().enumerate() {
            assert_eq!(
                message.part_slice(0).unwrap(),
                &[
                    if batching { 0 } else { index as u8 % 4 },
                    if batching {
                        index as u8
                    } else {
                        index as u8 / 4
                    }
                ]
            );
        }
        assert_eq!(out.as_ptr(), allocation);
        let freed = if batching { 8 } else { 2 };
        for seq in 32..32 + freed {
            senders[0].try_send(Message::from_slice(&[0, seq])).unwrap();
        }
        assert!(senders[0].is_full());
        // Switching to single receives rotates after each delivered message.
        let mut peers = Vec::new();
        for _ in 0..4 {
            let message = queue.try_recv().unwrap();
            let peer = message.part_slice(0).unwrap()[0];
            assert!(!peers.contains(&peer));
            peers.push(peer);
        }
    }
}

#[test]
fn byte_budget_and_multipart_atomicity_survive_disconnected_senders() {
    for batching in [false, true] {
        let queue = queue(4);
        let mut first = queue.register().unwrap();
        let mut second = queue.register().unwrap();
        let huge = Message::from_slice(&vec![7; 2 * 1024 * 1024 + 1]);
        let multipart = Message::multipart([vec![1; 40_000], vec![2; 40_000]]);
        first.try_send(huge.clone()).unwrap();
        second.try_send(multipart.clone()).unwrap();
        drop((first, second));
        let mut out = Vec::new();
        for expected in [huge, multipart] {
            out.clear();
            assert_eq!(
                queue
                    .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), batching)
                    .unwrap(),
                1
            );
            assert_eq!(out[0], expected);
        }
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        let mut reconnected = queue.register().unwrap();
        reconnected.try_send(Message::from_slice(b"new")).unwrap();
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), b"new");
    }
}

#[test]
fn batching_bulk_drain_keeps_per_sender_bursts() {
    let queue = queue(64);
    let mut senders: Vec<_> = (0..2).map(|_| queue.register().unwrap()).collect();
    let mut out = Vec::with_capacity(256);
    for round in 0..2u8 {
        for (peer, sender) in senders.iter_mut().enumerate() {
            for seq in 0..64u8 {
                sender
                    .try_send(Message::from_slice(&[peer as u8, round, seq]))
                    .unwrap();
            }
        }
        out.clear();
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true)
                .unwrap(),
            128
        );
        // One 64-message burst per lane, FIFO within each lane.
        for (index, message) in out.iter().enumerate() {
            let bytes = message.part_slice(0).unwrap();
            assert_eq!(bytes, &[(index / 64) as u8, round, (index % 64) as u8]);
        }
        // Slots were released before returning.
        for sender in &mut senders {
            assert!(!sender.is_full());
        }
    }
}

#[test]
fn byte_predicate_appends_only_admitted_messages_and_keeps_signal_armed() {
    for batching in [false, true] {
        let queue = queue(4);
        let mut sender = queue.register().unwrap();
        let messages = [
            Message::from_slice(b"tiny"),
            Message::from_slice(&vec![1; 1025]),
            Message::multipart([vec![2; 40_000], vec![3; 40_000]]),
            Message::from_slice(b"last"),
        ];
        for message in &messages {
            sender.try_send(message.clone()).unwrap();
        }
        let prefix = Message::from_slice(b"existing");
        let mut out = vec![prefix.clone()];
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::new(256, 5120), batching)
                .unwrap(),
            2
        );
        assert_eq!(out, [prefix, messages[0].clone(), messages[1].clone()]);
        assert!(!queue.signal.is_idle());
        assert!(!sender.is_full());
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::WORKER, batching)
                .unwrap(),
            1
        );
        assert_eq!(out[3], messages[2]);
        assert!(!queue.signal.is_idle());
        assert_eq!(
            queue
                .recv_into(&mut out, DrainBudget::WORKER, batching)
                .unwrap(),
            1
        );
        assert_eq!(out[4], messages[3]);
        assert!(!queue.signal.is_idle());
        assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
        assert!(queue.signal.is_idle());
    }
}

#[test]
fn batching_bulk_drain_clears_signal_only_on_observed_empty() {
    let queue = queue(32);
    let mut sender = queue.register().unwrap();
    for seq in 0..8u8 {
        sender.try_send(Message::from_slice(&[seq])).unwrap();
    }
    assert!(!queue.signal.is_idle());
    let mut out = Vec::new();
    // The budget stops the drain with data left: the signal stays armed.
    assert_eq!(
        queue
            .recv_into(&mut out, DrainBudget::new(4, 2 * 1024 * 1024), true)
            .unwrap(),
        4
    );
    assert!(!queue.signal.is_idle());
    // A successful drain stays coalesced until the next empty probe.
    assert_eq!(
        queue
            .recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true)
            .unwrap(),
        4
    );
    assert!(!queue.signal.is_idle());
    assert!(matches!(
        queue.recv_into(&mut out, DrainBudget::new(256, 2 * 1024 * 1024), true),
        Err(Error::WouldBlock)
    ));
    assert!(queue.signal.is_idle());
    for (seq, message) in out.iter().enumerate() {
        assert_eq!(message.part_slice(0).unwrap(), &[seq as u8]);
    }
}

#[test]
fn deferred_sink_flush_preserves_pending_message_order() {
    let queue = queue(2);
    let mut sink = Sink::owned(queue.register().unwrap());
    assert!(sink.push_deferred(Message::from_slice(&[0])));
    assert!(sink.push_deferred(Message::from_slice(&[1])));
    assert!(matches!(queue.try_recv(), Err(Error::WouldBlock)));
    assert!(sink.push_deferred(Message::from_slice(&[2])));
    assert!(sink.blocked());
    sink.flush();
    for seq in 0..2 {
        assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[seq]);
    }
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert_eq!(queue.try_recv().unwrap().part_slice(0).unwrap(), &[2]);
}

#[tokio::test]
async fn pending_sink_wakes_on_partial_release_and_close() {
    let queue = queue(2);
    let mut sink = Sink::owned(queue.register().unwrap());
    for seq in 0..3 {
        assert!(sink.push(Message::from_slice(&[seq])));
    }
    assert!(sink.blocked());
    let mut out = Vec::new();
    {
        let ready = sink.ready();
        tokio::pin!(ready);
        let mut task = Context::from_waker(Waker::noop());
        assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_pending());
        queue
            .recv_into(&mut out, DrainBudget::new(1, 1024), true)
            .unwrap();
        assert!(std::future::Future::poll(ready.as_mut(), &mut task).is_ready());
    }
    assert!(sink.retry_pending());
    assert!(!sink.blocked());
    assert!(sink.push(Message::from_slice(b"pending")));
    queue.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), sink.ready())
        .await
        .unwrap();
    assert!(!sink.retry_pending());
    assert!(matches!(queue.try_recv(), Err(Error::Closed)));
    assert!(queue.register().is_none());
}
