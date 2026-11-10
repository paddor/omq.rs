use super::*;
use omq_proto::WorkloadProfile;

#[test]
fn dealer_uses_latency_route_only_when_explicitly_requested() {
    let io_pool = crate::context::IoPoolHandle::none();
    let latency = Options::default().workload_profile(WorkloadProfile::Latency);
    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Dealer, &latency, &io_pool),
        SendStrategy::Latency(_)
    ));

    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Dealer, &Options::default(), &io_pool),
        SendStrategy::RoundRobin(_)
    ));
}

#[test]
fn latency_profile_extends_client_pair_and_channel() {
    let io_pool = crate::context::IoPoolHandle::none();
    let latency = Options::default().workload_profile(WorkloadProfile::Latency);

    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Client, &latency, &io_pool),
        SendStrategy::Latency(_)
    ));
    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Client, &Options::default(), &io_pool),
        SendStrategy::RoundRobin(_)
    ));
    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Pair, &latency, &io_pool),
        SendStrategy::Latency(_)
    ));
    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Channel, &latency, &io_pool),
        SendStrategy::Latency(_)
    ));
    assert!(matches!(
        SendStrategy::for_socket_type(SocketType::Pair, &Options::default(), &io_pool),
        SendStrategy::Exclusive(_)
    ));
}

#[test]
fn rep_reply_wraps_and_returns_body() {
    let envelopes = [
        RepEnvelope::new(),
        RepEnvelope::from_iter([Bytes::from_static(b"id")]),
        RepEnvelope::from_iter([Bytes::from_static(b"hop"), Bytes::from_static(b"id")]),
    ];
    let bodies = [
        Message::single(vec![7u8; 256]),
        Message::single("small"),
        Message::multipart(["a", "b"]),
    ];

    for envelope in &envelopes {
        for body in &bodies {
            let reply = rep_reply_with_envelope(envelope, body.clone());
            let expected: Vec<Bytes> = envelope
                .iter()
                .cloned()
                .chain([Bytes::new()])
                .chain(body.iter())
                .collect();
            assert_eq!(reply.iter().collect::<Vec<_>>(), expected);
            assert_eq!(&rep_reply_body(envelope, reply), body);
        }
    }
}

#[test]
fn identity_types_use_transmit_slots_for_latency_profile() {
    let io_pool = crate::context::IoPoolHandle::none();
    let latency = Options::default().workload_profile(WorkloadProfile::Latency);

    for socket_type in [SocketType::Router, SocketType::Server] {
        let latency_strategy = SendStrategy::for_socket_type(socket_type, &latency, &io_pool);
        assert!(matches!(latency_strategy, SendStrategy::Identity(_)));
        assert!(latency_strategy.needs_transmit_slot());

        let throughput_strategy =
            SendStrategy::for_socket_type(socket_type, &Options::default(), &io_pool);
        assert!(matches!(throughput_strategy, SendStrategy::Identity(_)));
        assert!(!throughput_strategy.needs_transmit_slot());
    }
}
