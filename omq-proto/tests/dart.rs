use std::time::Duration;

use omq_proto::dart::{
    self, Admission, Ecn, Handshake, Packet, Phase, Ready, Session, SessionConfig, Status,
};
use omq_proto::{DartCongestion, Message, SocketType};

fn session(local: u64, remote: u64, window: usize) -> Session {
    Session::new(
        local,
        remote,
        SessionConfig {
            window,
            congestion: DartCongestion::Lan,
            ecn: false,
            max_send_rate: None,
        },
    )
}

fn status(session: u64, serial: u64, ack: u64, credit: u64) -> Packet<'static> {
    Packet::Status(Status {
        session,
        serial,
        ack,
        credit,
        counts: [0, 0, 0, ack, 0],
    })
}

fn message(value: u64) -> Message {
    Message::from_slice(&value.to_le_bytes())
}

fn transfer(sender: &mut Session, receiver: &mut Session, now: Duration, drop: bool) -> bool {
    let mut packet = [0; dart::MAX_DATAGRAM];
    let sequence = sender.next_repair().unwrap_or_else(|| sender.next_send());
    let Some((token, length)) = sender.prepare_data(sequence, now, false, &mut packet) else {
        return false;
    };
    sender.commit_transmit(token, now);
    if !drop {
        let Packet::Data {
            session,
            sequence,
            payload,
        } = dart::decode_packet(&packet[..length]).unwrap()
        else {
            panic!()
        };
        if receiver.classify(session, sequence) == Admission::Accept {
            receiver.commit_receive(sequence, Message::from_slice(payload), Ecn::NotEct, now);
        }
    }
    true
}

fn feedback(receiver: &mut Session, sender: &mut Session, now: Duration, drop: bool) {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    for _ in 0..3 {
        let Some((token, length)) = receiver.prepare_control(now, &mut bytes) else {
            break;
        };
        receiver.commit_transmit(token, now);
        if !drop {
            assert!(sender.handle_control(dart::decode_packet(&bytes[..length]).unwrap(), now));
        }
    }
}

fn receive_payload(receiver: &mut Session, id: u64, sequence: u64, payload: &[u8], now: Duration) {
    if receiver.classify(id, sequence) == Admission::Accept {
        receiver.commit_receive(sequence, Message::from_slice(payload), Ecn::NotEct, now);
    }
}

fn receive_packet(receiver: &mut Session, packet: Packet<'_>, now: Duration) -> bool {
    match packet {
        Packet::First {
            session,
            sequence,
            length,
            payload,
        } => {
            if receiver.classify(session, sequence) == Admission::Accept {
                assert!(receiver.commit_fragment(
                    sequence,
                    Some(length),
                    Message::from_slice(payload),
                    Ecn::NotEct,
                    now
                ));
            }
        }
        Packet::Continuation {
            session,
            sequence,
            payload,
        } => {
            if receiver.classify(session, sequence) == Admission::Accept {
                assert!(receiver.commit_fragment(
                    sequence,
                    None,
                    Message::from_slice(payload),
                    Ecn::NotEct,
                    now
                ));
            }
        }
        Packet::Data {
            session,
            sequence,
            payload,
        } => receive_payload(receiver, session, sequence, payload, now),
        Packet::Packed {
            session,
            first,
            messages,
        } => {
            for (offset, payload) in messages.iter().enumerate() {
                receive_payload(receiver, session, first + offset as u64, payload, now);
            }
        }
        _ => return false,
    }
    true
}

#[test]
fn status_message_threshold_tracks_window_and_resets_after_feedback() {
    for window in [1, 2, 4, 16, 64, 256, 512, 4096] {
        let mut receiver = session(2, 1, window);
        let threshold = (window / 4).clamp(1, 128);
        let mut bytes = [0; dart::MAX_DATAGRAM];
        let (token, _) = receiver
            .prepare_control(Duration::ZERO, &mut bytes)
            .unwrap();
        receiver.commit_transmit(token, Duration::ZERO);
        for sequence in 0..threshold as u64 {
            receiver.commit_receive(sequence, message(sequence), Ecn::NotEct, Duration::ZERO);
            if sequence + 1 < threshold as u64 {
                assert!(
                    receiver
                        .prepare_control(Duration::ZERO, &mut bytes)
                        .is_none()
                );
            }
        }
        let (token, length) = receiver
            .prepare_control(Duration::ZERO, &mut bytes)
            .unwrap();
        let Packet::Status(status) = dart::decode_packet(&bytes[..length]).unwrap() else {
            panic!("receipt threshold must solicit status");
        };
        assert_eq!(status.ack, threshold as u64);
        assert_eq!(status.credit, window as u64);
        assert_eq!(status.counts, [0, 0, 0, threshold as u64, 0]);
        receiver.commit_transmit(token, Duration::ZERO);
        assert!(
            receiver
                .prepare_control(Duration::ZERO, &mut bytes)
                .is_none()
        );
        if threshold < window && threshold > 1 {
            receiver.commit_receive(threshold as u64, message(7), Ecn::NotEct, Duration::ZERO);
            assert!(
                receiver
                    .prepare_control(Duration::ZERO, &mut bytes)
                    .is_none()
            );
        }
    }
}

#[test]
fn pending_receipt_still_bounds_status_delay_to_fifty_microseconds() {
    let mut receiver = session(2, 1, 512);
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let (token, _) = receiver
        .prepare_control(Duration::ZERO, &mut bytes)
        .unwrap();
    receiver.commit_transmit(token, Duration::ZERO);
    receiver.commit_receive(0, message(7), Ecn::Ect0, Duration::from_micros(10));
    assert!(
        receiver
            .prepare_control(Duration::from_micros(59), &mut bytes)
            .is_none()
    );
    let (_, length) = receiver
        .prepare_control(Duration::from_micros(60), &mut bytes)
        .unwrap();
    let Packet::Status(status) = dart::decode_packet(&bytes[..length]).unwrap() else {
        panic!("pending receipt must solicit timed status");
    };
    assert_eq!(status.ack, 1);
    assert_eq!(status.counts, [1, 0, 0, 0, 0]);
}

#[test]
fn duplicates_and_gaps_solicit_feedback_below_status_threshold() {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    for duplicate in [false, true] {
        let mut receiver = session(2, 1, 512);
        let (token, _) = receiver
            .prepare_control(Duration::ZERO, &mut bytes)
            .unwrap();
        receiver.commit_transmit(token, Duration::ZERO);
        if duplicate {
            receiver.commit_receive(0, message(7), Ecn::Ect1, Duration::ZERO);
            assert!(
                receiver
                    .prepare_control(Duration::ZERO, &mut bytes)
                    .is_none()
            );
            assert_eq!(receiver.classify(2, 0), Admission::Duplicate);
            let (_, length) = receiver
                .prepare_control(Duration::ZERO, &mut bytes)
                .unwrap();
            let Packet::Status(status) = dart::decode_packet(&bytes[..length]).unwrap() else {
                panic!("duplicate must solicit cumulative status");
            };
            assert_eq!(status.ack, 1);
            assert_eq!(status.counts, [0, 1, 0, 0, 0]);
        } else {
            receiver.commit_receive(1, message(7), Ecn::NotEct, Duration::ZERO);
            let (_, length) = receiver
                .prepare_control(Duration::ZERO, &mut bytes)
                .unwrap();
            assert!(matches!(
                dart::decode_packet(&bytes[..length]).unwrap(),
                Packet::Nak {
                    session: 2,
                    first: 0,
                    count: 1
                }
            ));
        }
    }
}

#[test]
fn direct_delivery_preserves_inline_storage_forms_and_routing() {
    for original in [
        Message::new(),
        message(7),
        Message::new().with_routing_id(7),
        message(7).with_routing_id(7),
        Message::from(omq_proto::message::Payload::new()),
        Message::from(omq_proto::message::Payload::from_slice(b"inline payload")),
    ] {
        let mut receiver = session(2, 1, 2);
        assert_eq!(
            original.retained_size(),
            Some(std::mem::size_of::<Message>())
        );
        assert!(receiver.commit_receive_with_delivery(
            0,
            original.clone(),
            Ecn::NotEct,
            Duration::ZERO,
            |delivered| {
                assert_eq!(delivered.routing_id(), original.routing_id());
                assert_eq!(delivered.len(), original.len());
                assert_eq!(delivered.part_slice(0), original.part_slice(0));
                Ok(())
            }
        ));
        assert_eq!(receiver.next_receive(), 1);
        assert_eq!(receiver.receive_right_edge(), 3);
        assert!(receiver.take_received().is_none());
    }
}

#[test]
fn direct_inline_delivery_matches_retained_receipt_feedback_across_wraps() {
    for window in [1, 2, 16] {
        for size in [0, 8, omq_proto::message::MAX_INLINE_MESSAGE] {
            let mut direct = session(2, 1, window);
            let mut retained = session(2, 1, window);
            for sequence in 0..(window * 3) as u64 {
                let now = Duration::from_micros(sequence * 100);
                let body = vec![sequence as u8; size];
                let ecn = match sequence % 5 {
                    0 => Ecn::Ce,
                    1 => Ecn::Ect0,
                    2 => Ecn::Ect1,
                    3 => Ecn::NotEct,
                    _ => Ecn::Unavailable,
                };
                assert_eq!(direct.classify(2, sequence), Admission::Accept);
                assert!(direct.commit_receive_with_delivery(
                    sequence,
                    Message::from_slice(&body),
                    ecn,
                    now,
                    |message| {
                        assert_eq!(message.part_slice(0).unwrap(), body);
                        Ok(())
                    }
                ));
                assert!(direct.take_received().is_none());
                retained.commit_receive(sequence, Message::from_slice(&body), ecn, now);
                assert_eq!(
                    retained.take_received().unwrap().part_slice(0).unwrap(),
                    body
                );
                retained.release_receive(1);
                assert_eq!(direct.next_receive(), retained.next_receive());
                assert_eq!(direct.receive_right_edge(), retained.receive_right_edge());
                assert_eq!(direct.classify(2, sequence), Admission::Duplicate);
                assert_eq!(retained.classify(2, sequence), Admission::Duplicate);
                assert_eq!(direct.stats(), retained.stats());
                let mut direct_bytes = [0; dart::MAX_DATAGRAM];
                let mut retained_bytes = [0; dart::MAX_DATAGRAM];
                while let Some((token, length)) = direct.prepare_control(now, &mut direct_bytes) {
                    let retained_packet = retained.prepare_control(now, &mut retained_bytes);
                    assert_eq!(retained_packet, Some((token, length)));
                    assert_eq!(direct_bytes[..length], retained_bytes[..length]);
                    direct.commit_transmit(token, now);
                    retained.commit_transmit(token, now);
                }
                assert!(retained.prepare_control(now, &mut retained_bytes).is_none());
            }
        }
    }
}

#[test]
fn direct_delivery_rejection_and_gaps_preserve_order_and_credit() {
    let mut receiver = session(2, 1, 4);
    assert!(!receiver.commit_receive_with_delivery(0, message(0), Ecn::Ce, Duration::ZERO, Err));
    assert_eq!(receiver.next_receive(), 1);
    assert_eq!(receiver.receive_right_edge(), 4);
    assert!(!receiver.commit_receive_with_delivery(
        1,
        message(1),
        Ecn::Ect0,
        Duration::ZERO,
        |_| panic!("retained predecessor must be delivered first")
    ));
    for sequence in 0u64..2 {
        assert_eq!(
            receiver.take_received().unwrap().part_slice(0).unwrap(),
            sequence.to_le_bytes()
        );
        receiver.release_receive(1);
    }
    receiver.commit_receive(3, message(3), Ecn::NotEct, Duration::ZERO);
    assert!(receiver.commit_receive_with_delivery(
        2,
        message(2),
        Ecn::NotEct,
        Duration::ZERO,
        |m| {
            assert_eq!(m.part_slice(0).unwrap(), 2u64.to_le_bytes());
            Ok(())
        }
    ));
    assert_eq!(receiver.next_receive(), 4);
    assert_eq!(receiver.receive_right_edge(), 7);
    assert_eq!(
        receiver.take_received().unwrap().part_slice(0).unwrap(),
        3u64.to_le_bytes()
    );
    receiver.release_receive(1);
    assert_eq!(receiver.receive_right_edge(), 8);
    assert!(receiver.take_received().is_none());
}

#[test]
fn direct_delivery_retains_shared_bodies_and_waits_for_fragment_assembly() {
    let mut receiver = session(2, 1, 4);
    let body = vec![7; omq_proto::message::MAX_INLINE_MESSAGE + 1];
    assert!(!receiver.commit_receive_with_delivery(
        0,
        Message::single(body.clone()),
        Ecn::NotEct,
        Duration::ZERO,
        |_| panic!("shared body must retain its receive credit")
    ));
    assert_eq!(receiver.receive_right_edge(), 4);
    assert_eq!(
        receiver.take_received().unwrap().part_slice(0).unwrap(),
        body
    );
    receiver.release_receive(1);
    assert!(receiver.commit_fragment(
        1,
        Some(2048),
        Message::single(vec![1; 1024]),
        Ecn::NotEct,
        Duration::ZERO
    ));
    assert!(receiver.take_received().is_none());
    assert!(receiver.commit_fragment(
        2,
        None,
        Message::single(vec![2; 1024]),
        Ecn::NotEct,
        Duration::ZERO
    ));
    assert!(!receiver.commit_receive_with_delivery(
        3,
        message(3),
        Ecn::NotEct,
        Duration::ZERO,
        |_| panic!("unfinished assembly must be delivered first")
    ));
    let assembled = receiver.take_received().unwrap();
    assert_eq!(&assembled.part_slice(0).unwrap()[..1024], &[1; 1024]);
    assert_eq!(&assembled.part_slice(0).unwrap()[1024..], &[2; 1024]);
    receiver.release_receive(1);
    assert_eq!(
        receiver.take_received().unwrap().part_slice(0).unwrap(),
        3u64.to_le_bytes()
    );
    receiver.release_receive(1);
    assert_eq!(receiver.receive_right_edge(), 8);
}

#[test]
fn fragments_stream_across_windows_and_repair_every_kind() {
    for window in [1, 2, 16, 256] {
        for size in [1025, 4096, 16384, 70_001] {
            let body: Vec<_> = (0..size).map(|index| index as u8).collect();
            let mut sender = session(1, 2, window);
            let mut receiver = session(2, 1, window);
            let last = sender.submit(Message::single(body.clone())).unwrap();
            let mut output = [0; dart::MAX_DATAGRAM];
            let mut drops = [false; 3];
            let mut result = None;
            for turn in 0..10_000 {
                let now = Duration::from_micros(turn * 100);
                feedback(&mut receiver, &mut sender, now, false);
                sender.poll_progress();
                receiver.poll_progress();
                sender.handle_timeout(now);
                let sequence = sender.next_repair().unwrap_or_else(|| sender.next_send());
                if let Some((token, length)) =
                    sender.prepare_data(sequence, now, false, &mut output)
                {
                    sender.commit_transmit(token, now);
                    let drop = [0, 1, last].iter().enumerate().any(|(index, position)| {
                        if sequence == *position && !drops[index] {
                            drops[index] = true;
                            true
                        } else {
                            false
                        }
                    });
                    if !drop {
                        receive_packet(
                            &mut receiver,
                            dart::decode_packet(&output[..length]).unwrap(),
                            now,
                        );
                    }
                }
                if let Some(message) = receiver.take_received() {
                    assert!(result.is_none());
                    assert_eq!(message.part_slice(0), Some(body.as_slice()));
                    receiver.release_receive(1);
                    result = Some(message);
                }
                assert!(!receiver.receive_failed());
                assert!(sender.outstanding() <= window);
                if result.is_some() && sender.acknowledged_position() == last + 1 {
                    break;
                }
            }
            assert!(result.is_some(), "window={window} size={size}");
            assert!(drops.into_iter().all(|dropped| dropped));
            assert_eq!(sender.stats().acknowledged, 1);
            assert_eq!(sender.acknowledged_position(), last + 1);
        }
    }
}

#[test]
fn fragment_length_is_u64_and_bad_allocations_do_not_commit_receipt() {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let length = dart::encode_fragment(2, 0, Some(u64::MAX), b"x", None, &mut bytes).unwrap();
    assert!(matches!(
        dart::decode_packet(&bytes[..length]),
        Some(Packet::First {
            length: u64::MAX,
            ..
        })
    ));
    let mut receiver = session(2, 1, 2);
    assert!(!receiver.commit_fragment(
        0,
        Some(u64::MAX),
        Message::single("x"),
        Ecn::NotEct,
        Duration::ZERO
    ));
    assert_eq!(receiver.next_receive(), 0);
    assert_eq!(receiver.classify(2, 0), Admission::Accept);
}

#[test]
fn retention_waits_for_receipt_ack_not_udp_submission() {
    let mut sender = session(1, 2, 2);
    assert!(sender.handle_control(status(2, 1, 0, 2), Duration::ZERO));
    sender.submit(message(0)).unwrap();
    sender.submit(message(1)).unwrap();
    assert!(sender.submit(message(2)).is_err());
    let mut receiver = session(2, 1, 2);
    assert!(transfer(&mut sender, &mut receiver, Duration::ZERO, false));
    assert_eq!(sender.outstanding(), 2);
    assert!(sender.handle_control(status(2, 2, 1, 2), Duration::from_micros(10)));
    assert_eq!(sender.outstanding(), 1);
    assert!(sender.submit(message(2)).is_ok());
}

#[test]
fn window_reserves_space_for_a_missing_first_message() {
    let mut receiver = session(2, 1, 4);
    for sequence in [3, 2, 1] {
        assert_eq!(receiver.classify(2, sequence), Admission::Accept);
        receiver.commit_receive(sequence, message(sequence), Ecn::NotEct, Duration::ZERO);
    }
    assert!(receiver.take_received().is_none());
    assert_eq!(receiver.classify(2, 4), Admission::OutsideWindow);
    receiver.commit_receive(0, message(0), Ecn::NotEct, Duration::ZERO);
    for sequence in 0u64..4 {
        assert_eq!(
            receiver.take_received().unwrap().part_slice(0).unwrap(),
            sequence.to_le_bytes()
        );
    }
    assert_eq!(receiver.receive_right_edge(), 4);
    receiver.release_receive(4);
    assert_eq!(receiver.receive_right_edge(), 8);
}

#[test]
fn duplicates_do_not_create_receipt_or_capacity() {
    let mut receiver = session(2, 1, 2);
    receiver.commit_receive(0, message(0), Ecn::Ce, Duration::ZERO);
    assert_eq!(receiver.classify(2, 0), Admission::Duplicate);
    receiver.take_received().unwrap();
    receiver.release_receive(1);
    assert_eq!(receiver.classify(2, 0), Admission::Duplicate);
    assert_eq!(receiver.receive_right_edge(), 3);
    assert_eq!(receiver.stats().duplicates, 2);
}

#[test]
fn stale_and_impossible_feedback_leave_retention_and_credit_unchanged() {
    let mut sender = session(1, 2, 2);
    assert!(sender.handle_control(status(2, 2, 0, 2), Duration::ZERO));
    sender.submit(message(0)).unwrap();
    assert!(sender.handle_control(status(2, 1, 0, 1), Duration::ZERO));
    assert!(!sender.handle_control(status(2, 3, 1, 3), Duration::ZERO));
    assert!(!sender.handle_control(status(99, 3, 0, 3), Duration::ZERO));
    assert_eq!(sender.outstanding(), 1);
}

#[test]
fn failed_send_is_not_in_flight_and_can_be_prepared_again() {
    let mut sender = session(1, 2, 2);
    sender.handle_control(status(2, 1, 0, 2), Duration::ZERO);
    sender.submit(message(0)).unwrap();
    let mut bytes = [0; dart::MAX_DATAGRAM];
    assert!(
        sender
            .prepare_data(0, Duration::ZERO, false, &mut bytes)
            .is_some()
    );
    assert_eq!(sender.next_send(), 0);
    sender.handle_timeout(Duration::from_secs(5));
    assert!(sender.next_repair().is_none());
    assert!(
        sender
            .prepare_data(0, Duration::ZERO, false, &mut bytes)
            .is_some()
    );
}

#[test]
fn malformed_control_counters_cannot_overflow() {
    let mut sender = session(1, 2, 2);
    let packet = Packet::Status(Status {
        session: 2,
        serial: 1,
        ack: 0,
        credit: 2,
        counts: [u64::MAX; 5],
    });
    assert!(!sender.handle_control(packet, Duration::ZERO));
}

#[test]
fn held_receiver_storage_stops_new_data_but_not_control() {
    let mut sender = session(1, 2, 1);
    let mut receiver = session(2, 1, 1);
    feedback(&mut receiver, &mut sender, Duration::ZERO, false);
    sender.submit(message(0)).unwrap();
    transfer(&mut sender, &mut receiver, Duration::ZERO, false);
    let held = receiver.take_received().unwrap();
    feedback(
        &mut receiver,
        &mut sender,
        Duration::from_micros(100),
        false,
    );
    sender.submit(message(1)).unwrap();
    assert!(!transfer(
        &mut sender,
        &mut receiver,
        Duration::from_micros(100),
        false
    ));
    feedback(&mut receiver, &mut sender, Duration::from_secs(1), false);
    drop(held);
    receiver.release_receive(1);
    feedback(&mut receiver, &mut sender, Duration::from_secs(1), false);
    assert!(transfer(
        &mut sender,
        &mut receiver,
        Duration::from_secs(1),
        false
    ));
}

#[test]
fn retransmissions_keep_message_sequence_and_are_paced() {
    let mut sender = session(1, 2, 2);
    let mut receiver = session(2, 1, 2);
    feedback(&mut receiver, &mut sender, Duration::ZERO, false);
    sender.submit(message(7)).unwrap();
    transfer(&mut sender, &mut receiver, Duration::ZERO, true);
    sender.handle_timeout(Duration::from_millis(5));
    assert_eq!(sender.next_repair(), Some(0));
    transfer(&mut sender, &mut receiver, Duration::from_millis(5), false);
    assert_eq!(
        receiver.take_received().unwrap().part_slice(0).unwrap(),
        7u64.to_le_bytes()
    );
    assert_eq!(sender.stats().retransmitted, 1);
}

#[test]
fn lost_naks_status_and_tail_data_eventually_recover() {
    for fault in 0..4 {
        let mut sender = session(1, 2, 4);
        let mut receiver = session(2, 1, 4);
        let mut submitted = 0u64;
        let mut delivered = 0u64;
        for tick in 0..200_000u64 {
            let now = Duration::from_micros(tick * 10);
            sender.handle_timeout(now);
            receiver.handle_timeout(now);
            feedback(&mut receiver, &mut sender, now, tick < 100 && fault == 1);
            feedback(&mut sender, &mut receiver, now, tick < 100 && fault == 2);
            if submitted < 32 && sender.submit(message(submitted)).is_ok() {
                submitted += 1;
            }
            transfer(
                &mut sender,
                &mut receiver,
                now,
                tick < 100 && (fault == 0 || fault == 3 && submitted == 4),
            );
            while let Some(message) = receiver.take_received() {
                assert_eq!(message.part_slice(0).unwrap(), delivered.to_le_bytes());
                delivered += 1;
                receiver.release_receive(1);
            }
            if delivered == 32 && sender.outstanding() == 0 {
                break;
            }
        }
        assert_eq!(
            (submitted, delivered, sender.outstanding()),
            (32, 32, 0),
            "fault {fault}"
        );
    }
}

#[test]
fn decoder_checks_control_lengths_session_and_tags() {
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let packets = [
        status(2, 1, 0, 4),
        Packet::Nak {
            session: 2,
            first: 0,
            count: 1,
        },
        Packet::Probe {
            session: 2,
            next: 0,
        },
    ];
    for packet in packets {
        let length = dart::encode_packet(packet, &mut bytes).unwrap();
        assert_eq!(dart::decode_packet(&bytes[..length]), Some(packet));
        for prefix in 0..length {
            assert!(dart::decode_packet(&bytes[..prefix]).is_none());
        }
        bytes[length] = 0;
        assert!(dart::decode_packet(&bytes[..=length]).is_none());
    }
    assert!(dart::decode_packet(&[0; 32]).is_none());
}

fn ready(session: u64, echo: u64, phase: Phase) -> Ready<'static> {
    Ready {
        socket_type: SocketType::Channel,
        identity: None,
        reply_requested: phase == Phase::Hello,
        session,
        echo,
        phase,
    }
}

#[test]
fn confirmation_requires_both_fresh_session_challenges() {
    let mut connector = Handshake::connector(10);
    let mut listener = Handshake::listener(20, 10);
    assert!(!listener.receive(ready(10, 99, Phase::Confirm)));
    assert!(!connector.receive(ready(20, 99, Phase::Welcome)));
    assert!(!listener.confirmed());
    assert!(connector.receive(ready(20, 10, Phase::Welcome)));
    assert!(connector.confirmed());
    assert!(listener.receive(ready(10, 20, Phase::Confirm)));
    assert!(listener.confirmed());
    assert!(!connector.receive(ready(21, 10, Phase::Welcome)));
}

#[test]
fn adaptive_feedback_reduces_window_on_ce_and_disables_unusable_ecn() {
    for received_ecn in [Ecn::Ce, Ecn::NotEct, Ecn::Unavailable] {
        let mut sender = Session::new(
            1,
            2,
            SessionConfig {
                window: 32,
                congestion: DartCongestion::Adaptive,
                ecn: true,
                max_send_rate: None,
            },
        );
        let mut receiver = session(2, 1, 32);
        feedback(&mut receiver, &mut sender, Duration::ZERO, false);
        sender.submit(message(0)).unwrap();
        let mut bytes = [0; dart::MAX_DATAGRAM];
        let (token, _) = sender
            .prepare_data(0, Duration::ZERO, false, &mut bytes)
            .unwrap();
        sender.commit_transmit(token, Duration::ZERO);
        let before = sender.congestion_window();
        receiver.commit_receive(0, message(0), received_ecn, Duration::ZERO);
        feedback(
            &mut receiver,
            &mut sender,
            Duration::from_micros(100),
            false,
        );
        assert_eq!(sender.outstanding(), 0);
        assert_eq!(sender.bytes_in_flight(), 0);
        if received_ecn == Ecn::Ce {
            assert!(sender.congestion_window() < before);
            assert!(sender.ecn_enabled());
        } else {
            assert!(!sender.ecn_enabled());
            assert_eq!(sender.stats().ecn_failures, 1);
        }
    }
}

#[test]
fn adaptive_batch_reserves_the_complete_congestion_window() {
    let mut sender = Session::new(
        1,
        2,
        SessionConfig {
            window: 64,
            congestion: DartCongestion::Adaptive,
            ecn: false,
            max_send_rate: None,
        },
    );
    assert!(sender.handle_control(status(2, 1, 0, 64), Duration::ZERO));
    for _ in 0..64 {
        sender
            .submit(Message::from_slice(&[7; dart::MAX_BODY]))
            .unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut tokens = Vec::new();
    let mut reserved = 0;
    for sequence in 0..64 {
        let Some((token, length)) =
            sender.prepare_batch_data(sequence, Duration::ZERO, false, reserved, &mut bytes)
        else {
            break;
        };
        reserved += length;
        tokens.push(token);
    }
    let length = dart::DATA_HEADER + dart::MAX_BODY;
    assert_eq!(tokens.len(), sender.congestion_window() / length);
    assert!(reserved <= sender.congestion_window());
    assert!(reserved + length > sender.congestion_window());
    assert_eq!(sender.bytes_in_flight(), 0);
    assert_eq!(sender.next_send(), 0);
    // A partial carrier submission consumes only its successful prefix.
    for token in tokens.iter().take(5) {
        sender.commit_transmit(*token, Duration::ZERO);
    }
    assert_eq!(sender.bytes_in_flight(), 5 * length);
    assert_eq!(sender.next_send(), 5);
    assert_eq!(sender.outstanding(), 64);
    assert!(
        sender
            .prepare_batch_data(5, Duration::from_secs(1), false, usize::MAX, &mut bytes,)
            .is_none()
    );
}

#[test]
fn batch_pacing_charges_only_the_successful_prefix() {
    let mut sender = Session::new(
        1,
        2,
        SessionConfig {
            window: 4,
            congestion: DartCongestion::Adaptive,
            ecn: false,
            max_send_rate: Some(1_000_000),
        },
    );
    assert!(sender.handle_control(status(2, 1, 0, 4), Duration::ZERO));
    for _ in 0..3 {
        sender.submit(Message::from_slice(&[7; 16])).unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut tokens = Vec::new();
    let mut reserved = 0;
    for sequence in 0..3 {
        let (token, length) = sender
            .prepare_batch_data(sequence, Duration::ZERO, false, reserved, &mut bytes)
            .unwrap();
        reserved += length;
        tokens.push(token);
    }
    for token in tokens.iter().take(2) {
        sender.commit_transmit(*token, Duration::ZERO);
    }
    assert_eq!(sender.next_send(), 2);
    assert_eq!(sender.bytes_in_flight(), 66);
    assert_eq!(sender.next_deadline(Duration::ZERO), Duration::ZERO);
    let (status, _) = sender.prepare_control(Duration::ZERO, &mut bytes).unwrap();
    sender.commit_transmit(status, Duration::ZERO);
    assert_eq!(
        sender.next_deadline(Duration::ZERO),
        Duration::from_micros(66)
    );
    assert!(
        sender
            .prepare_data(2, Duration::from_micros(65), false, &mut bytes)
            .is_none()
    );
    assert!(
        sender
            .prepare_data(2, Duration::from_micros(66), false, &mut bytes)
            .is_some()
    );
}

#[test]
fn lan_disables_ecn_and_explicit_rate_limit_paces_repairs_too() {
    let mut sender = Session::new(
        1,
        2,
        SessionConfig {
            window: 2,
            congestion: DartCongestion::Lan,
            ecn: true,
            max_send_rate: Some(25_000),
        },
    );
    assert!(!sender.ecn_enabled());
    sender.handle_control(status(2, 1, 0, 2), Duration::ZERO);
    sender.submit(message(0)).unwrap();
    sender.submit(message(1)).unwrap();
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let (token, _) = sender
        .prepare_data(0, Duration::ZERO, false, &mut bytes)
        .unwrap();
    sender.commit_transmit(token, Duration::ZERO);
    assert!(
        sender
            .prepare_data(1, Duration::ZERO, false, &mut bytes)
            .is_none()
    );
    assert!(
        sender
            .prepare_data(1, Duration::from_millis(1), false, &mut bytes)
            .is_some()
    );
    sender.handle_control(
        Packet::Nak {
            session: 2,
            first: 0,
            count: 1,
        },
        Duration::ZERO,
    );
    assert!(
        sender
            .prepare_data(0, Duration::ZERO, false, &mut bytes)
            .is_none()
    );
}

#[test]
fn delayed_nak_after_ack_is_harmless() {
    let mut sender = session(1, 2, 2);
    let mut receiver = session(2, 1, 2);
    feedback(&mut receiver, &mut sender, Duration::ZERO, false);
    sender.submit(message(0)).unwrap();
    transfer(&mut sender, &mut receiver, Duration::ZERO, false);
    feedback(
        &mut receiver,
        &mut sender,
        Duration::from_micros(100),
        false,
    );
    assert!(sender.handle_control(
        Packet::Nak {
            session: 2,
            first: 0,
            count: 1
        },
        Duration::from_micros(200)
    ));
    assert_eq!(sender.acknowledged_position(), 1);
    assert!(sender.next_repair().is_none());
}

#[test]
fn invalid_message_shapes_never_enter_retention() {
    let mut sender = session(1, 2, 2);
    for invalid in [
        Message::default(),
        Message::multipart([b"".as_slice(), b"x".as_slice()]),
    ] {
        assert!(sender.submit(invalid).is_err());
        assert_eq!(sender.outstanding(), 0);
    }
}

fn retained_packing_messages(size: usize, grouped: bool) -> Session {
    let mut sender = session(1, 2, 256);
    assert!(sender.handle_control(status(2, 1, 0, 256), Duration::ZERO));
    for _ in 0..128 {
        let body = bytes::Bytes::from(vec![7; size]);
        let message = if grouped {
            Message::multipart([bytes::Bytes::from_static(b"group"), body])
        } else {
            Message::single(body)
        };
        // Fragmentation can fill the entire retained window.
        if sender.submit(message).is_err() {
            break;
        }
    }
    sender
}

#[test]
fn packed_prefix_matches_tokens_and_discards_uncommitted_preparation() {
    for grouped in [false, true] {
        for size in [0, 16, 255, 256, 1024, 4096] {
            for capacity in [0, 1, 2, 128] {
                let mut indexed = retained_packing_messages(size, grouped);
                let mut prefix = retained_packing_messages(size, grouped);
                let mut indexed_bytes = [0; dart::MAX_PACKED_DATAGRAM];
                let mut prefix_bytes = [0; dart::MAX_PACKED_DATAGRAM];
                let mut tokens = Vec::with_capacity(capacity);
                let mut indexed_reserved = 0;
                let mut prefix_reserved = 0;
                let expected = indexed.prepare_packed_data(
                    0,
                    Duration::ZERO,
                    grouped,
                    &mut indexed_reserved,
                    &mut indexed_bytes,
                    &mut tokens,
                );
                let prepared = prefix.prepare_packed_prefix(
                    0,
                    Duration::ZERO,
                    grouped,
                    &mut prefix_reserved,
                    &mut prefix_bytes,
                    tokens.capacity(),
                );
                assert_eq!(expected, prepared.map(|(_, length, _)| length));
                assert_eq!(indexed_reserved, prefix_reserved);
                assert_eq!(prefix.next_send(), 0);
                assert_eq!(prefix.bytes_in_flight(), 0);
                if let Some((first, length, count)) = prepared {
                    assert_eq!(count, tokens.len());
                    assert_eq!(first, tokens[0]);
                    assert_eq!(&indexed_bytes[..length], &prefix_bytes[..length]);
                    // A rejected datagram can be prepared again unchanged.
                    let mut retry = [0; dart::MAX_PACKED_DATAGRAM];
                    assert_eq!(
                        prefix.prepare_packed_prefix(
                            0,
                            Duration::ZERO,
                            grouped,
                            &mut 0,
                            &mut retry,
                            capacity
                        ),
                        prepared
                    );
                    assert_eq!(&retry[..length], &prefix_bytes[..length]);
                    for (offset, expected_token) in tokens.into_iter().enumerate() {
                        let token = if offset == 0 {
                            first
                        } else {
                            dart::Transmit::Data {
                                sequence: offset as u64,
                                repair: false,
                            }
                        };
                        assert_eq!(token, expected_token);
                        indexed.commit_transmit(expected_token, Duration::ZERO);
                        prefix.commit_transmit(token, Duration::ZERO);
                    }
                    assert_eq!(indexed.next_send(), prefix.next_send());
                    assert_eq!(indexed.bytes_in_flight(), prefix.bytes_in_flight());
                    for sender in [&mut indexed, &mut prefix] {
                        assert!(sender.handle_control(
                            Packet::Nak {
                                session: 2,
                                first: 0,
                                count: 1
                            },
                            Duration::ZERO
                        ));
                    }
                    let repaired = prefix
                        .prepare_packed_prefix(
                            0,
                            Duration::ZERO,
                            grouped,
                            &mut 0,
                            &mut prefix_bytes,
                            128,
                        )
                        .unwrap();
                    assert_eq!(
                        repaired.0,
                        dart::Transmit::Data {
                            sequence: 0,
                            repair: true
                        }
                    );
                    assert_eq!(repaired.2, 1);
                }
            }
        }
    }
}

#[test]
fn packing_is_immediate_bounded_and_retained_until_acknowledged() {
    let mut sender = session(1, 2, 128);
    sender.handle_control(status(2, 1, 0, 128), Duration::ZERO);
    sender.submit(message(0)).unwrap();
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut tokens = Vec::with_capacity(64);
    let mut reserved = 0;
    let length = sender
        .prepare_packed_data(
            0,
            Duration::ZERO,
            false,
            &mut reserved,
            &mut bytes,
            &mut tokens,
        )
        .unwrap();
    assert!(matches!(
        dart::decode_packet(&bytes[..length]),
        Some(Packet::Data { sequence: 0, .. })
    ));
    assert_eq!(sender.next_send(), 0);
    tokens.clear();
    for sequence in 1..128 {
        sender.submit(message(sequence)).unwrap();
    }
    reserved = 0;
    let length = sender
        .prepare_packed_data(
            0,
            Duration::ZERO,
            false,
            &mut reserved,
            &mut bytes,
            &mut tokens,
        )
        .unwrap();
    let Packet::Packed {
        session,
        first,
        messages,
    } = dart::decode_packet(&bytes[..length]).unwrap()
    else {
        panic!()
    };
    assert_eq!((session, first, messages.message_count()), (2, 0, 64));
    assert_eq!(length, dart::DATA_HEADER + 64 * (1 + 8));
    assert_eq!(bytes[0], 64);
    assert_eq!(&bytes[1..65], &[8; 64]);
    for (sequence, payload) in messages.iter().enumerate() {
        assert_eq!(payload, (sequence as u64).to_le_bytes());
    }
    assert_eq!(sender.next_send(), 0);
    assert_eq!(sender.outstanding(), 128);
    for token in tokens {
        sender.commit_transmit(token, Duration::ZERO);
    }
    assert_eq!(sender.next_send(), 64);
    assert_eq!(sender.outstanding(), 128);
    sender.handle_control(status(2, 2, 64, 192), Duration::from_micros(10));
    assert_eq!(sender.outstanding(), 64);
}

#[test]
fn packed_decoder_rejects_incomplete_oversized_and_wrapping_records() {
    let mut sender = session(1, 2, 64);
    sender.handle_control(status(2, 1, 0, 64), Duration::ZERO);
    for sequence in 0..64 {
        sender.submit(message(sequence)).unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut tokens = Vec::with_capacity(64);
    let length = sender
        .prepare_packed_data(0, Duration::ZERO, false, &mut 0, &mut bytes, &mut tokens)
        .unwrap();
    for prefix in 0..length {
        assert!(
            dart::decode_packet(&bytes[..prefix]).is_none(),
            "prefix {prefix}"
        );
    }
    assert!(dart::decode_packet(&bytes[..=length]).is_none());
    let original = bytes;
    for tag in [0x83, 0xBF, 0xC4, 0xFF] {
        bytes[0] = tag;
        assert!(dart::decode_packet(&bytes[..length]).is_none());
    }
    bytes = original;
    bytes[0] = 67;
    assert!(dart::decode_packet(&bytes[..length]).is_none());
    bytes = original;
    bytes[73..81].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
    assert!(dart::decode_packet(&bytes[..length]).is_none());
    bytes = original;
    bytes[65..73].fill(0);
    assert!(dart::decode_packet(&bytes[..length]).is_none());
    bytes = original;
    bytes[1] = u8::MAX;
    assert!(dart::decode_packet(&bytes[..length]).is_none());
}

#[test]
fn packing_respects_udp_and_preallocated_token_capacity() {
    for (size, mtu, capacity, count) in [
        (16, dart::MAX_DATAGRAM, 64, 64),
        (64, dart::MAX_DATAGRAM, 64, 18),
        (255, dart::MAX_DATAGRAM, 64, 4),
        (8, dart::MAX_PACKED_DATAGRAM, 128, 128),
        (16, dart::MAX_PACKED_DATAGRAM, 128, 84),
        (32, dart::MAX_PACKED_DATAGRAM, 128, 43),
        (64, dart::MAX_PACKED_DATAGRAM, 128, 22),
        (128, dart::MAX_PACKED_DATAGRAM, 128, 11),
        (255, dart::MAX_PACKED_DATAGRAM, 128, 5),
        (256, dart::MAX_PACKED_DATAGRAM, 128, 1),
        (512, dart::MAX_PACKED_DATAGRAM, 128, 1),
        (1024, dart::MAX_PACKED_DATAGRAM, 128, 1),
    ] {
        let mut sender = session(1, 2, 128);
        sender.handle_control(status(2, 1, 0, 128), Duration::ZERO);
        for _ in 0..128 {
            sender.submit(Message::from_slice(&vec![7; size])).unwrap();
        }
        let mut bytes = vec![0; mtu];
        let mut tokens = Vec::with_capacity(capacity);
        let length = sender
            .prepare_packed_data(0, Duration::ZERO, false, &mut 0, &mut bytes, &mut tokens)
            .unwrap();
        assert_eq!(tokens.len(), count);
        assert!(length <= mtu);
        assert_eq!(
            length,
            dart::DATA_HEADER + count * size + if count > 1 { count } else { 0 }
        );
        assert_eq!(sender.next_send(), 0);
        if count > 1 {
            let Packet::Packed { messages, .. } = dart::decode_packet(&bytes[..length]).unwrap()
            else {
                panic!()
            };
            assert_eq!(messages.message_count(), count);
            assert!(messages.iter().all(|payload| payload == vec![7; size]));
        } else {
            assert_eq!(bytes[0], 1);
        }
        sender.reduce_packing_mtu();
        tokens.clear();
        let length = sender
            .prepare_packed_data(0, Duration::ZERO, false, &mut 0, &mut bytes, &mut tokens)
            .unwrap();
        assert!(length <= dart::MAX_DATAGRAM);
        assert!(dart::decode_packet(&bytes[..length]).is_some());
        assert_eq!(sender.next_send(), 0);
        let mut tokens = Vec::with_capacity(1);
        let length = sender
            .prepare_packed_data(0, Duration::ZERO, false, &mut 0, &mut bytes, &mut tokens)
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(length, dart::DATA_HEADER + size);
        assert_eq!(bytes[0], 1);
    }
}

#[test]
fn packing_never_skips_a_large_message_and_preserves_empty_bodies() {
    let mut sender = session(1, 2, 128);
    sender.handle_control(status(2, 1, 0, 128), Duration::ZERO);
    for size in [0, 16, 256, 0, 255] {
        sender.submit(Message::from_slice(&vec![7; size])).unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    for (first, sizes) in [(0, vec![0, 16]), (2, vec![256]), (3, vec![0, 255])] {
        let mut tokens = Vec::with_capacity(64);
        let length = sender
            .prepare_packed_data(
                first,
                Duration::ZERO,
                false,
                &mut 0,
                &mut bytes,
                &mut tokens,
            )
            .unwrap();
        match dart::decode_packet(&bytes[..length]).unwrap() {
            Packet::Packed {
                first: actual,
                messages,
                ..
            } => {
                assert_eq!(actual, first);
                assert_eq!(messages.iter().map(<[u8]>::len).collect::<Vec<_>>(), sizes);
            }
            Packet::Data {
                sequence, payload, ..
            } => {
                assert_eq!(sequence, first);
                assert_eq!(sizes, [payload.len()]);
            }
            _ => panic!(),
        }
        for token in tokens {
            sender.commit_transmit(token, Duration::ZERO);
        }
    }
    for _ in 0..64 {
        sender.submit(Message::from_slice(b"")).unwrap();
    }
    let length = sender
        .prepare_packed_data(
            5,
            Duration::ZERO,
            false,
            &mut 0,
            &mut bytes,
            &mut Vec::with_capacity(64),
        )
        .unwrap();
    let Packet::Packed { messages, .. } = dart::decode_packet(&bytes[..length]).unwrap() else {
        panic!()
    };
    assert_eq!(length, dart::DATA_HEADER + 64);
    assert_eq!(messages.message_count(), 64);
    assert!(messages.iter().all(<[u8]>::is_empty));
}

#[test]
fn congestion_shortens_the_length_table_without_changing_payloads() {
    let mut sender = Session::new(
        1,
        2,
        SessionConfig {
            window: 64,
            congestion: DartCongestion::Adaptive,
            ecn: false,
            max_send_rate: None,
        },
    );
    sender.handle_control(status(2, 1, 0, 64), Duration::ZERO);
    for sequence in 0..64 {
        sender.submit(message(sequence)).unwrap();
    }
    for count in [0, 1, 2, 5, 64] {
        let mut bytes = [0; dart::MAX_DATAGRAM];
        let mut tokens = Vec::with_capacity(64);
        let initial = sender.congestion_window() - count * (dart::DATA_HEADER + 8);
        let mut reserved = initial;
        let length = sender.prepare_packed_data(
            0,
            Duration::ZERO,
            false,
            &mut reserved,
            &mut bytes,
            &mut tokens,
        );
        assert_eq!(tokens.len(), count);
        assert_eq!(reserved, initial + count * (dart::DATA_HEADER + 8));
        if count == 0 {
            assert!(length.is_none());
            continue;
        }
        match dart::decode_packet(&bytes[..length.unwrap()]).unwrap() {
            Packet::Data {
                sequence, payload, ..
            } => {
                assert_eq!(count, 1);
                assert_eq!(sequence, 0);
                assert_eq!(payload, 0u64.to_le_bytes());
            }
            Packet::Packed { messages, .. } => {
                assert_eq!(messages.message_count(), count);
                for (sequence, payload) in messages.iter().enumerate() {
                    assert_eq!(payload, (sequence as u64).to_le_bytes());
                }
            }
            _ => panic!(),
        }
    }
}

#[test]
fn a_lost_packed_datagram_is_repaired_as_individual_messages() {
    let mut sender = session(1, 2, 32);
    let mut receiver = session(2, 1, 32);
    feedback(&mut receiver, &mut sender, Duration::ZERO, false);
    for sequence in 0..32 {
        sender.submit(message(sequence)).unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let mut delayed = Vec::new();
    for batch in 0..2 {
        let mut tokens = Vec::with_capacity(16);
        let length = sender
            .prepare_packed_data(
                batch * 16,
                Duration::ZERO,
                false,
                &mut 0,
                &mut bytes,
                &mut tokens,
            )
            .unwrap();
        for token in tokens {
            sender.commit_transmit(token, Duration::ZERO);
        }
        if batch == 0 {
            delayed.extend_from_slice(&bytes[..length]);
            continue;
        }
        let Packet::Packed {
            session,
            first,
            messages,
        } = dart::decode_packet(&bytes[..length]).unwrap()
        else {
            panic!()
        };
        for (offset, payload) in messages.iter().enumerate() {
            receiver.commit_receive(
                first + offset as u64,
                Message::from_slice(payload),
                Ecn::NotEct,
                Duration::ZERO,
            );
        }
        assert_eq!(session, 2);
    }
    assert!(receiver.take_received().is_none());
    let now = Duration::from_micros(100);
    feedback(&mut receiver, &mut sender, now, false);
    for _ in 0..16 {
        assert!(transfer(&mut sender, &mut receiver, now, false));
    }
    let Packet::Packed {
        session,
        first,
        messages,
    } = dart::decode_packet(&delayed).unwrap()
    else {
        panic!()
    };
    for (offset, _) in messages.iter().enumerate() {
        assert_eq!(
            receiver.classify(session, first + offset as u64),
            Admission::Duplicate
        );
    }
    feedback(&mut receiver, &mut sender, now, false);
    assert_eq!(sender.outstanding(), 0);
    assert_eq!(receiver.stats().duplicates, 16);
    for sequence in 0u64..32 {
        assert_eq!(
            receiver.take_received().unwrap().part_slice(0).unwrap(),
            sequence.to_le_bytes()
        );
    }
    receiver.release_receive(32);
    assert_eq!(receiver.receive_right_edge(), 64);
}

#[test]
fn packed_group_metadata_and_variable_bodies_preserve_boundaries() {
    let mut sender = session(1, 2, 4);
    sender.handle_control(status(2, 1, 0, 4), Duration::ZERO);
    for (group, body) in [("alpha", ""), ("beta", "variable body")] {
        sender
            .submit(Message::with_prefix(group.into(), Message::single(body)))
            .unwrap();
    }
    let mut bytes = [0; dart::MAX_DATAGRAM];
    let length = sender
        .prepare_packed_data(
            0,
            Duration::ZERO,
            true,
            &mut 0,
            &mut bytes,
            &mut Vec::with_capacity(4),
        )
        .unwrap();
    let Packet::Packed { messages, .. } = dart::decode_packet(&bytes[..length]).unwrap() else {
        panic!()
    };
    let bodies: Vec<_> = messages
        .iter()
        .map(|payload| dart::data_body(payload, true).unwrap())
        .collect();
    assert_eq!(
        bodies,
        [
            (Some(b"alpha".as_slice()), b"".as_slice()),
            (Some(b"beta".as_slice()), b"variable body".as_slice())
        ]
    );
}

#[test]
fn deterministic_fault_link_recovers_reordering_duplication_and_burst_loss() {
    use std::collections::VecDeque;
    for congestion in [DartCongestion::Lan, DartCongestion::Adaptive] {
        for seed in 1..=8u64 {
            let config = SessionConfig {
                window: 8,
                congestion,
                ecn: false,
                max_send_rate: None,
            };
            let mut sender = Session::new(1, 2, config);
            let mut receiver = Session::new(2, 1, config);
            feedback(&mut receiver, &mut sender, Duration::ZERO, false);
            let mut random = seed;
            let mut link: VecDeque<(bool, Vec<u8>)> = VecDeque::new();
            let mut submitted = 0u64;
            let mut delivered = 0u64;
            for tick in 0..400_000u64 {
                let now = Duration::from_micros(tick * 5);
                sender.handle_timeout(now);
                receiver.handle_timeout(now);
                for _ in 0..4 {
                    if submitted < 64 && sender.submit(message(submitted)).is_ok() {
                        submitted += 1;
                    } else {
                        break;
                    }
                }
                let mut bytes = [0; dart::MAX_DATAGRAM];
                for (to_sender, core) in [(false, &mut sender), (true, &mut receiver)] {
                    for _ in 0..3 {
                        let Some((token, length)) = core.prepare_control(now, &mut bytes) else {
                            break;
                        };
                        core.commit_transmit(token, now);
                        link.push_back((to_sender, bytes[..length].to_vec()));
                    }
                }
                let sequence = sender.next_repair().unwrap_or_else(|| sender.next_send());
                let mut tokens = Vec::with_capacity(dart::MAX_PACKED_MESSAGES);
                let mut reserved = 0;
                if let Some(length) = sender.prepare_packed_data(
                    sequence,
                    now,
                    false,
                    &mut reserved,
                    &mut bytes,
                    &mut tokens,
                ) {
                    for token in tokens {
                        sender.commit_transmit(token, now);
                    }
                    link.push_back((false, bytes[..length].to_vec()));
                }
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let packet = if random & 1 == 0 {
                    link.pop_front()
                } else {
                    link.pop_back()
                };
                if let Some((to_sender, bytes)) = packet {
                    let fault = tick < 3000;
                    let dropped = fault && (random % 7 == 0 || (100..200).contains(&tick));
                    if !dropped {
                        let repetitions = if fault && random % 11 == 0 { 2 } else { 1 };
                        for _ in 0..repetitions {
                            let packet = dart::decode_packet(&bytes).unwrap();
                            if !receive_packet(&mut receiver, packet, now) {
                                if to_sender {
                                    sender.handle_control(packet, now);
                                } else {
                                    receiver.handle_control(packet, now);
                                }
                            }
                        }
                    }
                }
                while let Some(message) = receiver.take_received() {
                    assert_eq!(message.part_slice(0).unwrap(), delivered.to_le_bytes());
                    delivered += 1;
                    receiver.release_receive(1);
                }
                assert!(sender.outstanding() <= 8);
                assert!(link.len() <= 64);
                if delivered == 64 && sender.outstanding() == 0 {
                    break;
                }
            }
            assert_eq!(
                (submitted, delivered, sender.outstanding()),
                (64, 64, 0),
                "{congestion:?} seed {seed}"
            );
        }
    }
}
