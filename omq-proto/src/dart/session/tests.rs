use super::*;

#[test]
fn paced_credit_separates_initial_and_released_storage_from_admission() {
    let mut s = session(2);
    s.pace_receive_credit();
    assert_eq!(s.receive_right_edge(), 0);
    assert_eq!(s.pending_receive_credit(), 2);
    assert_eq!(s.classify(1, 0), super::super::Admission::OutsideWindow);
    s.grant_receive_credit(1);
    assert_eq!(s.receive_right_edge(), 1);
    assert!(s.commit_receive_with_delivery(
        Receipt::new(0, Ecn::NotEct, Duration::ZERO),
        Message::single("small"),
        |_| Ok(())
    ));
    assert_eq!(s.received_units(), 1);
    assert_eq!(
        s.receive_right_edge(),
        1,
        "inline storage release cannot grant unpaced credit"
    );
    assert_eq!(s.pending_receive_credit(), 2);
    s.grant_receive_credit(2);
    assert_eq!(s.receive_right_edge(), 3);
}

#[test]
fn out_of_order_firsts_share_a_bounded_reservation() {
    let mut s = session(32);
    let mib: u64 = 1024 * 1024;
    let chunk = Message::single(vec![1; 16]);
    assert_eq!(s.classify(1, 1), Admission::Accept);
    // The first waiting body is admitted at any size.
    assert!(s.may_reserve_ahead(48 * mib));
    assert!(s.commit_fragment(
        Receipt::new(1, Ecn::Unavailable, Duration::ZERO),
        Some(48 * mib),
        chunk
    ));
    // Further bodies must fit the shared budget.
    assert!(s.may_reserve_ahead(16 * mib));
    assert!(!s.may_reserve_ahead(17 * mib));
}

fn session(window: usize) -> Session {
    Session::new(
        1,
        2,
        SessionConfig {
            window,
            congestion: DartCongestion::Lan,
            ecn: false,
            max_send_rate: None,
        },
    )
}

#[test]
fn pacing_cache_preserves_delays_across_sizes_and_rates() {
    let mut session = session(32);
    assert!(session.handle_control(
        Packet::Status(super::Status {
            session: 2,
            serial: 1,
            ack: 0,
            credit: 32,
            counts: [0; 5],
        }),
        Duration::ZERO,
    ));
    let sizes = [16, 16, 32, 512, 16];
    let mut output = [0; MAX_DATAGRAM];
    for rate in [10_000_000, 3_000_000, u64::MAX, 1] {
        session.config.max_send_rate = Some(rate);
        session.update_pacing_rate();
        assert_eq!(session.pacing.bytes, 0);
        let now = session.pacing.next_at + Duration::from_secs(1);
        let mut tokens = Vec::new();
        for size in sizes {
            let sequence = session.submit(Message::from_slice(&vec![7; size])).unwrap();
            let (token, _) = session
                .prepare_data(sequence, now, false, &mut output)
                .unwrap();
            tokens.push(token);
        }
        let mut expected = now;
        for (token, size) in tokens.into_iter().zip(sizes) {
            expected +=
                Duration::from_nanos(((DATA_HEADER + size) as u64 * 1_000_000_000 / rate).max(1));
            session.commit_transmit(token, now);
            assert_eq!(session.pacing.next_at, expected);
            assert_eq!(session.pacing.bytes, DATA_HEADER + size);
        }
    }
}

#[test]
fn send_capacity_stops_a_batch_before_sequence_exhaustion() {
    let mut session = session(2);
    let near_limit = u64::MAX - 3;
    session.send.base = near_limit;
    session.peer.ack = near_limit;
    session.send.cursor = near_limit;
    session.send.next = near_limit;
    assert_eq!(session.send_capacity(), 1);
    assert_eq!(session.submit(Message::single("last")).unwrap(), near_limit);
    assert_eq!(session.send_capacity(), 0);
    assert!(session.is_exhausted());
    assert!(session.submit(Message::single("overflow")).is_err());
}

#[test]
fn reordered_receipt_prefix_resumes_under_a_drain_budget() {
    let mut session = session(128);
    for sequence in 1..128 {
        session.commit_receive(
            Receipt::new(sequence, Ecn::NotEct, Duration::ZERO),
            Message::single("data"),
        );
    }
    session.commit_receive(
        Receipt::new(0, Ecn::NotEct, Duration::ZERO),
        Message::single("data"),
    );
    assert_eq!(session.next_receive(), 64);
    assert!(session.poll_progress());
    assert_eq!(session.next_receive(), 128);
}

#[test]
fn ack_retirement_and_nak_marking_have_message_and_byte_budgets() {
    for size in [8, 1024] {
        let mut session = session(256);
        let initial = Status {
            session: 2,
            serial: 1,
            ack: 0,
            credit: 256,
            counts: [0; 5],
        };
        assert!(session.handle_control(Packet::Status(initial), Duration::ZERO));
        let mut bytes = [0; MAX_DATAGRAM];
        for sequence in 0..256 {
            session.submit(Message::from_slice(&vec![7; size])).unwrap();
            let (token, _) = session
                .prepare_data(sequence, Duration::ZERO, false, &mut bytes)
                .unwrap();
            session.commit_transmit(token, Duration::ZERO);
        }
        session.handle_control(
            Packet::Nak {
                session: 2,
                first: 0,
                count: 256,
            },
            Duration::ZERO,
        );
        assert!(session.send.repairs <= 64);
        let ack = Status {
            serial: 2,
            ack: 256,
            counts: [0, 0, 0, 256, 0],
            ..initial
        };
        session.handle_control(Packet::Status(ack), Duration::from_micros(100));
        assert!(session.outstanding() >= 128);
        if size == 1024 {
            assert!(session.outstanding() > 128);
        }
        while session.poll_progress() {}
        assert_eq!(session.outstanding(), 0);
        assert_eq!(session.send.repairs, 0);
    }
}
