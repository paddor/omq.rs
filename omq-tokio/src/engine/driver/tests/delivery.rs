use super::*;

#[test]
fn yring_sink_signals_every_flush_even_when_nonempty() {
    let (producer, _consumer) = yring::spsc(4);
    let signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let signals_for_sink = signals.clone();
    let mut sink = YringSink {
        producer,
        signal: Box::new(move || {
            signals_for_sink.fetch_add(1, Ordering::Relaxed);
        }),
        space: Arc::new(StateSignal::new()),
    };

    assert!(matches!(sink.producer.push(Message::single("a")), Ok(())));
    sink.flush_and_signal();
    assert!(matches!(sink.producer.push(Message::single("b")), Ok(())));
    sink.flush_and_signal();

    assert_eq!(signals.load(Ordering::Relaxed), 2);
}

#[test]
fn full_rep_sink_returns_original_message_without_internal_route() {
    let (producer, mut consumer) = yring::spsc(1);
    let mut sink = RecvSink::rep(
        RecvSink::Yring(YringSink {
            producer,
            signal: Box::new(|| {}),
            space: Arc::new(StateSignal::new()),
        }),
        7,
    );
    sink.try_deliver(Message::multipart(["", "first"])).unwrap();
    let original = Message::multipart(["", "second"]).with_routing_id(99);
    let Err(omq_proto::TrySendError::Full(returned)) = sink.try_deliver(original.clone()) else {
        panic!("full sink must return the request");
    };
    assert_eq!(returned, original);
    assert_eq!(returned.routing_id(), Some(99));
    let received = consumer.prefetch_and_pop().unwrap();
    assert_eq!(received.routing_id(), Some(8));
    assert_eq!(received.get(1), Some(b"first".as_slice()));
}

#[test]
fn receive_config_recycles_only_its_owner_and_retains_pending_queues() {
    let (producer, mut initial_consumer) = yring::spsc(4);
    let config = RecvSinkConfig::new(
        RecvSink::Yring(YringSink {
            producer,
            signal: Box::new(|| {}),
            space: Arc::new(StateSignal::new()),
        }),
        Arc::new(|| {}),
        Arc::new(StateSignal::new()),
        4,
    );
    let mut first = config.take_sink_for_peer(0).unwrap();
    config.peer_disconnected(1);
    assert!(config.take_sink_for_peer(2).is_none());
    first.try_deliver(Message::single("first")).unwrap();
    assert_eq!(
        initial_consumer.prefetch_and_pop(),
        Some(Message::single("first"))
    );
    drop(first);
    config.peer_disconnected(0);
    let mut second = config.take_sink_for_peer(2).unwrap();
    second.try_deliver(Message::single("second")).unwrap();
    drop(second);
    config.peer_disconnected(2);
    assert!(
        config.take_sink_for_peer(3).is_none(),
        "pending ring must survive churn"
    );
    let mut pending = config.try_take_pending_consumer().unwrap();
    assert_eq!(pending.prefetch_and_pop(), Some(Message::single("second")));
    let mut third = config.take_sink_for_peer(3).unwrap();
    third.try_deliver(Message::single("third")).unwrap();
    let mut newest = config.try_take_pending_consumer().unwrap();
    assert_eq!(newest.prefetch_and_pop(), Some(Message::single("third")));
}

#[test]
fn yring_sink_deferred_send_signals_once_per_flush() {
    let (producer, mut consumer) = yring::spsc(4);
    let signals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let signals_for_sink = signals.clone();
    let mut sink = YringSink {
        producer,
        signal: Box::new(move || {
            signals_for_sink.fetch_add(1, Ordering::Relaxed);
        }),
        space: Arc::new(StateSignal::new()),
    };
    let mut pending = false;
    // Wake hints stay conservative until the consumer acknowledges their
    // activation with an empty check. Prime that registration first.
    sink.try_send_deferred(Message::single("warmup"), &mut pending)
        .unwrap();
    sink.flush_pending(&mut pending);
    assert_eq!(consumer.prefetch_and_pop(), Some(Message::single("warmup")));
    assert_eq!(consumer.prefetch_and_pop(), None);
    signals.store(0, Ordering::Relaxed);

    sink.try_send_deferred(Message::single("a"), &mut pending)
        .unwrap();
    sink.try_send_deferred(Message::single("b"), &mut pending)
        .unwrap();
    assert_eq!(signals.load(Ordering::Relaxed), 0);
    assert_eq!(consumer.prefetch(), 0);

    sink.flush_pending(&mut pending);
    assert_eq!(signals.load(Ordering::Relaxed), 1);
    assert_eq!(consumer.prefetch(), 2);
    sink.try_send_deferred(Message::single("c"), &mut pending)
        .unwrap();
    sink.flush_pending(&mut pending);
    assert_eq!(
        signals.load(Ordering::Relaxed),
        1,
        "nonempty queue needs no extra wake"
    );
    for expected in ["a", "b", "c"] {
        assert_eq!(consumer.prefetch_and_pop(), Some(Message::single(expected)));
    }
    assert_eq!(consumer.prefetch_and_pop(), None);
    sink.try_send_deferred(Message::single("d"), &mut pending)
        .unwrap();
    sink.flush_pending(&mut pending);
    assert_eq!(
        signals.load(Ordering::Relaxed),
        2,
        "registered empty consumer must wake"
    );
    sink.flush_and_signal();
    assert_eq!(
        signals.load(Ordering::Relaxed),
        2,
        "nothing flushed needs no wake"
    );
}

#[test]
fn receive_frame_validation_drops_bad_envelopes_without_using_queue_credit() {
    let (events, mut receiver) = mpsc::channel(1);
    let mut events = PeerOutput::from(events);
    let mut pending = None;
    let mut completion = CompletionProgress::default();
    let mut sink = None;
    let mut delivery = MessageDelivery {
        sink: &mut sink,
        peer_out: &mut events,
        peer_id: 0,
        completion: &mut completion,
        pending: &mut pending,
        reserved_admission: false,
        expected_parts: Some(2),
    };
    for message in [
        Message::from("body"),
        Message::multipart(["group", "body", "extra"]),
    ] {
        assert!(delivery.send(message, false, &mut false));
    }
    assert!(receiver.try_recv().is_err());
    assert!(delivery.send(Message::with_group("group", "body"), false, &mut false));
    let (_, PeerEvent::Event(Event::Message(message))) = receiver.try_recv().unwrap() else {
        panic!("expected valid grouped message");
    };
    assert_eq!(message, Message::with_group("group", "body"));
    assert!(pending.is_none());
}

#[tokio::test]
async fn full_peer_queue_bounds_pending_delivery_and_leaves_decoder_remainder_in_place() {
    let (mut sender, mut connection) = ready_push_pull_connections();
    for _ in 0..100 {
        sender
            .send_message(&Message::single(Bytes::from(vec![0; 512])))
            .unwrap();
    }
    let wire = drain_transmit(&mut sender);
    assert!(wire.len() < READ_BUF_MAX);
    connection.handle_input(Bytes::from(wire)).unwrap();
    let handles =
        crate::socket::recv::SpscHandles::new(crate::socket::recv::BlockingRecvWaker::new(), false);
    let (mut routes, mut receiver) =
        crate::socket::peer_recv::PeerRecvRoutes::new(16, &handles, &omq_proto::Options::default());
    let sink = routes
        .register(Bytes::from_static(b"peer"), CancellationToken::new())
        .unwrap();
    let mut sink = Some(RecvSink::Peer(sink));
    let (events, _rx) = mpsc::channel(1);
    let mut events = PeerOutput::from(events);
    let mut pending = None;
    for _ in 0..8 {
        assert_eq!(
            drain_decoded_messages(
                &mut connection,
                &mut None,
                ReceiveProfile::Throughput,
                MessageDelivery {
                    sink: &mut sink,
                    peer_out: &mut events,
                    peer_id: 0,
                    completion: &mut CompletionProgress::default(),
                    pending: &mut pending,
                    reserved_admission: false,
                    expected_parts: None,
                },
                ReceiveRateLimiters {
                    connection: &mut None,
                    ip: None
                },
                None,
                #[cfg(target_os = "linux")]
                None,
            )
            .unwrap(),
            DriverStep::Continue
        );
        assert!(sink.as_ref().unwrap().peer_blocked());
    }
    let mut queued = Vec::new();
    assert_eq!(receiver.try_recv_many_into(100, &mut queued).unwrap(), 16);
    // Only one additional message left the decoder for pending admission.
    // Repeated driver turns under backpressure consume no further input.
    let mut remaining = 0;
    let mut bytes = 0;
    while let Some(message) = connection.poll_message() {
        remaining += 1;
        bytes += message.max_message_size_len();
        assert!(remaining <= 100 && bytes < READ_BUF_MAX);
    }
    assert_eq!(remaining, 83);
}
