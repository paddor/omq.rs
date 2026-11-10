use super::*;

fn queue(capacity: usize) -> (Producer, Consumer) {
    queue_with_options(capacity, &Options::default())
}

fn queue_with_options(capacity: usize, options: &Options) -> (Producer, Consumer) {
    channel(
        capacity,
        options,
        Arc::new(DataSignal::new()),
        Arc::new(StateSignal::new()),
    )
}

#[test]
fn configured_handle_queues_fill_independently_and_wake_after_drain() {
    for capacity in [1, 4, 128] {
        let options = Options::default().peer_send_queue_capacity(capacity);
        let (root, mut receiver) = queue_with_options(256, &options);
        let mut busy = root.register().unwrap();
        let mut quiet = root.register().unwrap();

        for _ in 0..capacity {
            busy.try_send(Message::single("busy")).unwrap();
        }

        assert!(matches!(
            busy.try_send(Message::single("retry")),
            Err(SendPipeError::Full(_))
        ));
        assert!(!busy.ready());

        let signal = busy.space();
        let seen = signal.generation();
        quiet.try_send(Message::single("quiet")).unwrap();

        let mut out = Vec::new();
        assert_eq!(receiver.drain_into(&mut out, 256, usize::MAX), capacity + 1);
        assert!(
            out.iter()
                .any(|message| *message == Message::single("quiet"))
        );

        assert_ne!(signal.generation(), seen);
        assert!(busy.ready());
        busy.try_send(Message::single("retry")).unwrap();
    }
}

#[test]
fn configured_registration_limit_counts_retired_senders() {
    for limit in [1, 129] {
        let options = Options::default().peer_max_senders(limit);
        let (root, mut receiver) = queue_with_options(256, &options);
        let mut senders: Vec<_> = (0..limit).map(|_| root.register().unwrap()).collect();

        assert!(!root.registration_ready());
        assert!(root.register().is_none());

        senders[0].try_send(Message::single("retained")).unwrap();
        drop(senders);
        assert!(!root.registration_ready());
        assert!(root.register().is_none());

        let signal = root.registration_space();
        let seen = signal.generation();
        assert_eq!(receiver.drain_into(&mut Vec::new(), 256, usize::MAX), 1);
        assert_ne!(signal.generation(), seen);
        assert!(root.registration_ready());
        assert!(root.register().is_some());
    }
}

#[test]
fn ready_sends_do_not_broadcast_ring_capacity() {
    let (mut sender, mut receiver) = queue(4);
    let signal = sender.space();
    let generation = signal.generation();
    let mut out = Vec::new();

    for _ in 0..16 {
        sender.try_send(Message::single("x")).unwrap();
        assert_eq!(receiver.drain_into(&mut out, 1, 1024), 1);
    }

    assert_eq!(signal.generation(), generation);
}

#[test]
fn delivered_capacity_wake_does_not_need_another_broadcast() {
    let (mut sender, mut receiver) = queue(1);
    sender.try_send(Message::single("first")).unwrap();
    assert!(matches!(
        sender.try_send(Message::single("blocked")),
        Err(SendPipeError::Full(_))
    ));

    let signal = sender.space();
    let generation = signal.generation();
    assert!(!sender.ready());

    assert_eq!(receiver.drain_into(&mut Vec::new(), 1, 1024), 1);
    assert!(sender.ready());
    assert_eq!(signal.generation(), generation + 1);

    sender.try_send(Message::single("next")).unwrap();
    assert_eq!(signal.generation(), generation + 1);
}

#[test]
fn producer_count_and_payload_budget_stay_bounded_through_churn() {
    let (root, mut receiver) = queue(4);
    let mut senders: Vec<_> = (0..Options::default().peer_max_senders)
        .map(|_| root.register().unwrap())
        .collect();
    assert!(root.register().is_none());

    for sender in &mut senders[..4] {
        sender.try_send(Message::single("x")).unwrap();
    }

    assert!(matches!(
        senders[4].try_send(Message::single("x")),
        Err(SendPipeError::Full(_))
    ));
    assert_eq!(root.shared.messages.load(Ordering::Acquire), 4);
    assert_eq!(
        root.shared.bytes.load(Ordering::Acquire),
        4 * (1 + std::mem::size_of::<omq_proto::message::Payload>())
    );

    drop(senders);
    assert!(root.register().is_none(), "retired rings still count");

    let mut out = Vec::new();
    assert_eq!(receiver.drain_into(&mut out, 8, 1024), 4);
    assert!(root.register().is_some());
    assert_eq!(root.shared.messages.load(Ordering::Acquire), 0);
    assert_eq!(root.shared.bytes.load(Ordering::Acquire), 0);
    assert!(root.sender.as_ref().unwrap().registered_lanes() <= 2);
}

#[test]
fn receiver_drop_reclaims_payloads_while_idle_senders_live() {
    let (root, receiver) = queue(4);
    let mut sender = root.register().unwrap();
    sender.try_send(Message::single("held")).unwrap();

    drop(receiver);
    assert_eq!(root.shared.messages.load(Ordering::Acquire), 0);
    assert_eq!(root.shared.bytes.load(Ordering::Acquire), 0);
    assert!(!sender.alive());
    assert!(matches!(
        sender.try_send(Message::single("returned")),
        Err(SendPipeError::Closed(_))
    ));
}

#[test]
fn busy_sender_cannot_starve_an_already_queued_sender() {
    let (root, mut receiver) = queue(256);
    let mut busy = root.register().unwrap();
    let mut quiet = root.register().unwrap();

    for _ in 0..64 {
        busy.try_send(Message::single("busy")).unwrap();
    }

    quiet.try_send(Message::single("quiet")).unwrap();
    let mut out = Vec::new();

    for _ in 0..=64 {
        receiver.drain_into(&mut out, 1, 1024);
        if out
            .last()
            .is_some_and(|message| message.part_slice(0) == Some(b"quiet"))
        {
            return;
        }

        busy.try_send(Message::single("busy")).unwrap();
    }

    panic!("quiet sender starved beyond one fanring burst");
}

#[test]
fn readiness_after_a_budget_rejection_matches_admission() {
    // Each part also costs one 64-byte payload slot.
    let options = Options::default().peer_send_max_bytes(300);
    let (mut sender, _receiver) = queue_with_options(16, &options);

    sender.try_send(Message::single(vec![0; 100])).unwrap();
    let large = Message::single(vec![0; 100]);
    assert!(matches!(
        sender.try_send(large.clone()),
        Err(SendPipeError::Full(_))
    ));

    // Bytes remain below the cap, but not enough for the waiting send.
    assert!(!sender.has_space());
    assert!(!sender.ready());

    sender.try_send(Message::single(vec![0; 8])).unwrap();
    assert!(sender.has_space());
}
