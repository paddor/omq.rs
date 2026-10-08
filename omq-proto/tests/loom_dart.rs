#![cfg(all(loom, target_pointer_width = "64"))]

//! Interleave carrier and application events against the production core.
//! Session ownership remains exclusive, as in the endpoint task. The shared
//! counters below use the production atomics compiled with Loom instead.

use std::time::Duration;

use loom::sync::{Arc, Mutex};
use loom::thread;
use omq_proto::dart::{
    Admission, AdmissionCounter, CreditCounter, Ecn, Packet, Session, SessionConfig, Status,
};
use omq_proto::{DartCongestion, Message};

fn session(local: u64, remote: u64) -> Session {
    Session::new(
        local,
        remote,
        SessionConfig {
            window: 2,
            congestion: DartCongestion::Lan,
            ecn: false,
            max_send_rate: None,
        },
    )
}

fn status(serial: u64, ack: u64, credit: u64) -> Packet<'static> {
    Packet::Status(Status {
        session: 2,
        serial,
        ack,
        credit,
        counts: [0, 0, 0, ack, 0],
    })
}

fn receive(core: &mut Session, sequence: u64) {
    if core.classify(2, sequence) == Admission::Accept {
        core.commit_receive(
            sequence,
            Message::from_slice(&sequence.to_le_bytes()),
            Ecn::NotEct,
            Duration::ZERO,
        );
    }
}

fn interleave(
    core: Session,
    first: fn(&mut Session),
    second: fn(&mut Session),
    check: fn(&mut Session),
) {
    let local = core.local_session();
    loom::model(move || {
        let core = Arc::new(Mutex::new(if local == 1 {
            session(1, 2)
        } else {
            session(2, 1)
        }));
        let other = core.clone();
        let task = thread::spawn(move || first(&mut other.lock().unwrap()));
        second(&mut core.lock().unwrap());
        task.join().unwrap();
        check(&mut core.lock().unwrap());
    });
}

fn ordered(core: &mut Session) {
    for sequence in 0u64..2 {
        assert_eq!(
            core.take_received().unwrap().part_slice(0).unwrap(),
            sequence.to_le_bytes()
        );
    }
    assert!(core.take_received().is_none());
    assert_eq!(core.next_receive(), 2);
}

#[test]
fn reordered_arrivals_deliver_in_order() {
    interleave(session(2, 1), |s| receive(s, 1), |s| receive(s, 0), ordered);
}

fn receive_fragment(core: &mut Session, sequence: u64) {
    if core.classify(2, sequence) == Admission::Accept {
        assert!(core.commit_fragment(
            sequence,
            (sequence == 0).then_some(2048),
            Message::single(vec![sequence as u8; 1024]),
            Ecn::NotEct,
            Duration::ZERO
        ));
    }
}

fn assembled(core: &mut Session) {
    let body = core.take_received().unwrap();
    assert_eq!(body.byte_len(), 2048);
    assert_eq!(&body.part_slice(0).unwrap()[..1024], &[0; 1024]);
    assert_eq!(&body.part_slice(0).unwrap()[1024..], &[1; 1024]);
    assert!(core.take_received().is_none());
    assert_eq!(core.receive_right_edge(), 3);
    core.release_receive(1);
    assert_eq!(core.receive_right_edge(), 4);
}

#[test]
fn first_and_continuation_reorder_without_partial_delivery() {
    interleave(
        session(2, 1),
        |s| receive_fragment(s, 0),
        |s| receive_fragment(s, 1),
        assembled,
    );
}

#[test]
fn duplicate_first_allocates_and_returns_credit_once() {
    interleave(
        session(2, 1),
        |s| receive_fragment(s, 0),
        |s| receive_fragment(s, 0),
        |s| {
            assert!(s.take_received().is_none());
            assert_eq!(s.receive_right_edge(), 3);
            assert_eq!(s.stats().duplicates, 1);
            receive_fragment(s, 1);
            assembled(s);
        },
    );
}

#[test]
fn duplicate_continuation_cannot_finish_twice() {
    interleave(
        session(2, 1),
        |s| receive_fragment(s, 1),
        |s| receive_fragment(s, 1),
        |s| {
            assert!(s.take_received().is_none());
            receive_fragment(s, 0);
            assembled(s);
            assert_eq!(s.stats().duplicates, 1);
        },
    );
}

#[test]
fn concurrent_duplicate_arrivals_ack_once() {
    interleave(
        session(2, 1),
        |s| receive(s, 0),
        |s| receive(s, 0),
        |s| {
            assert_eq!(s.next_receive(), 1);
            assert_eq!(s.stats().duplicates, 1);
            assert!(s.take_received().is_some());
            assert!(s.take_received().is_none());
        },
    );
}

fn receive_packed(core: &mut Session) {
    let mut sender = session(1, 2);
    sender.handle_control(status(1, 0, 2), Duration::ZERO);
    for sequence in 0u64..2 {
        sender
            .submit(Message::from_slice(&sequence.to_le_bytes()))
            .unwrap();
    }
    let mut bytes = [0; omq_proto::dart::MAX_DATAGRAM];
    let length = sender
        .prepare_packed_data(
            0,
            Duration::ZERO,
            false,
            &mut 0,
            &mut bytes,
            &mut Vec::with_capacity(2),
        )
        .unwrap();
    let Packet::Packed {
        session,
        first,
        messages,
    } = omq_proto::dart::decode_packet(&bytes[..length]).unwrap()
    else {
        panic!()
    };
    for (offset, payload) in messages.iter().enumerate() {
        let sequence = first + offset as u64;
        if core.classify(session, sequence) == Admission::Accept {
            core.commit_receive(
                sequence,
                Message::from_slice(payload),
                Ecn::NotEct,
                Duration::ZERO,
            );
        }
    }
}

#[test]
fn packed_arrival_and_scalar_repair_deduplicate_in_either_order() {
    interleave(
        session(2, 1),
        receive_packed,
        |core| receive(core, 1),
        |core| {
            ordered(core);
            assert_eq!(core.stats().duplicates, 1);
            assert_eq!(core.receive_right_edge(), 2);
        },
    );
}

#[test]
fn duplicate_packed_arrivals_never_create_capacity() {
    interleave(session(2, 1), receive_packed, receive_packed, |core| {
        ordered(core);
        assert_eq!(core.stats().duplicates, 2);
        assert_eq!(core.receive_right_edge(), 2);
        core.release_receive(2);
        assert_eq!(core.receive_right_edge(), 4);
    });
}

#[test]
fn wrong_session_cannot_consume_a_slot() {
    interleave(
        session(2, 1),
        |s| {
            assert_eq!(s.classify(99, 0), Admission::OutsideWindow);
        },
        |s| receive(s, 0),
        |s| {
            assert_eq!(s.next_receive(), 1);
            assert_eq!(s.receive_right_edge(), 2);
        },
    );
}

#[test]
fn outside_window_arrival_cannot_evict_missing_data() {
    interleave(
        session(2, 1),
        |s| {
            assert_eq!(s.classify(2, 2), Admission::OutsideWindow);
        },
        |s| receive(s, 1),
        |s| {
            assert!(s.take_received().is_none());
            receive(s, 0);
            ordered(s);
        },
    );
}

#[test]
fn a_duplicate_after_delivery_cannot_redeliver() {
    interleave(
        session(2, 1),
        |s| {
            receive(s, 0);
            s.take_received();
        },
        |s| receive(s, 0),
        |s| {
            assert!(s.take_received().is_none());
            assert_eq!(s.next_receive(), 1);
        },
    );
}

#[test]
fn application_rejection_keeps_ordered_message() {
    interleave(
        session(2, 1),
        |s| {
            receive(s, 0);
            let m = s.take_received().unwrap();
            s.restore_received(m);
        },
        |s| receive(s, 1),
        ordered,
    );
}

#[test]
fn credit_returns_and_arrivals_keep_capacity_bounded() {
    interleave(
        session(2, 1),
        |s| {
            receive(s, 0);
            s.take_received();
            s.release_receive(1);
        },
        |s| receive(s, 1),
        |s| {
            assert_eq!(s.receive_right_edge(), 3);
            assert_eq!(s.next_receive(), 2);
            assert_eq!(
                s.take_received().unwrap().part_slice(0).unwrap(),
                1u64.to_le_bytes()
            );
        },
    );
}

#[test]
fn delayed_credit_does_not_change_receipt_ack() {
    interleave(
        session(2, 1),
        |s| {
            receive(s, 0);
            s.take_received();
        },
        |s| receive(s, 1),
        |s| {
            assert_eq!(s.next_receive(), 2);
            assert_eq!(s.receive_right_edge(), 2);
            s.release_receive(1);
            assert_eq!(s.receive_right_edge(), 3);
        },
    );
}

#[test]
fn credit_feedback_cannot_go_backwards() {
    interleave(
        session(1, 2),
        |s| {
            assert!(s.handle_control(status(2, 0, 4), Duration::ZERO));
        },
        |s| {
            s.handle_control(status(1, 0, 2), Duration::ZERO);
        },
        |s| {
            assert!(!s.handle_control(status(3, 0, 2), Duration::ZERO));
        },
    );
}

#[test]
fn invalid_ack_cannot_release_untransmitted_storage() {
    interleave(
        session(1, 2),
        |s| {
            s.submit(Message::single("a")).unwrap();
        },
        |s| {
            assert!(!s.handle_control(status(1, 1, 2), Duration::ZERO));
        },
        |s| {
            assert_eq!(s.outstanding(), 1);
        },
    );
}

#[test]
fn competing_submitters_share_a_finite_owner_window() {
    interleave(
        session(1, 2),
        |s| {
            s.submit(Message::single("a")).unwrap();
        },
        |s| {
            s.submit(Message::single("b")).unwrap();
        },
        |s| {
            assert_eq!(s.outstanding(), 2);
            assert!(s.submit(Message::single("c")).is_err());
        },
    );
}

#[test]
fn preparation_and_timeout_do_not_acknowledge() {
    interleave(
        session(1, 2),
        |s| {
            s.handle_control(status(1, 0, 2), Duration::ZERO);
            s.submit(Message::single("a")).unwrap();
            let mut bytes = [0; 1200];
            assert!(
                s.prepare_data(0, Duration::ZERO, false, &mut bytes)
                    .is_some()
            );
        },
        |s| s.handle_timeout(Duration::from_secs(5)),
        |s| {
            assert_eq!(s.outstanding(), 1);
            assert_eq!(s.next_send(), 0);
            assert!(s.next_repair().is_none());
        },
    );
}

#[test]
fn probe_and_gap_arrival_preserve_missing_prefix() {
    interleave(
        session(2, 1),
        |s| {
            assert!(s.handle_control(
                Packet::Probe {
                    session: 2,
                    next: 2
                },
                Duration::ZERO
            ));
        },
        |s| receive(s, 1),
        |s| {
            assert_eq!(s.next_receive(), 0);
            assert!(s.take_received().is_none());
            receive(s, 0);
            ordered(s);
        },
    );
}

#[test]
fn impossible_probe_does_not_extend_receive_window() {
    interleave(
        session(2, 1),
        |s| {
            assert!(!s.handle_control(
                Packet::Probe {
                    session: 2,
                    next: u64::MAX
                },
                Duration::ZERO
            ));
        },
        |s| receive(s, 0),
        |s| {
            assert_eq!(s.receive_right_edge(), 2);
        },
    );
}

#[test]
fn forged_counter_overflow_does_not_replace_valid_feedback() {
    interleave(
        session(1, 2),
        |s| {
            assert!(!s.handle_control(
                Packet::Status(Status {
                    session: 2,
                    serial: 99,
                    ack: 0,
                    credit: 2,
                    counts: [u64::MAX; 5]
                }),
                Duration::ZERO
            ));
        },
        |s| {
            assert!(s.handle_control(status(1, 0, 2), Duration::ZERO));
        },
        |s| {
            assert!(s.handle_control(status(2, 0, 3), Duration::ZERO));
        },
    );
}

#[test]
fn wrong_session_feedback_cannot_replace_valid_feedback() {
    interleave(
        session(1, 2),
        |s| {
            assert!(!s.handle_control(
                Packet::Status(Status {
                    session: 99,
                    serial: 99,
                    ack: 0,
                    credit: 2,
                    counts: [0; 5]
                }),
                Duration::ZERO
            ));
        },
        |s| {
            assert!(s.handle_control(status(1, 0, 2), Duration::ZERO));
        },
        |s| {
            assert!(s.handle_control(status(2, 0, 3), Duration::ZERO));
        },
    );
}

#[test]
fn two_producers_cannot_exceed_admission_capacity() {
    loom::model(|| {
        let counter = Arc::new(AdmissionCounter::new(1));
        let other = counter.clone();
        let task = thread::spawn(move || other.acquire(1));
        let acquired = counter.acquire(1);
        assert_eq!(acquired + task.join().unwrap(), 1);
    });
}

#[test]
fn bulk_admission_and_single_admission_share_capacity() {
    loom::model(|| {
        let counter = Arc::new(AdmissionCounter::new(3));
        let other = counter.clone();
        let task = thread::spawn(move || other.acquire(3));
        let acquired = counter.acquire(1);
        assert_eq!(acquired + task.join().unwrap(), 3);
    });
}

#[test]
fn ack_release_racing_enqueue_preserves_capacity() {
    loom::model(|| {
        let counter = Arc::new(AdmissionCounter::new(1));
        assert_eq!(counter.acquire(1), 1);
        let other = counter.clone();
        let task = thread::spawn(move || other.release(1));
        let acquired = counter.acquire(1);
        task.join().unwrap();
        assert_eq!(counter.available() + acquired, 1);
    });
}

#[test]
fn closing_racing_admission_never_reopens() {
    loom::model(|| {
        let counter = Arc::new(AdmissionCounter::new(1));
        let other = counter.clone();
        let task = thread::spawn(move || {
            let count = other.acquire(1);
            other.release(count);
        });
        counter.close();
        task.join().unwrap();
        assert!(counter.is_closed());
        assert_eq!(counter.acquire(1), 0);
    });
}

#[test]
fn final_ack_after_close_keeps_closed_bit() {
    loom::model(|| {
        let counter = Arc::new(AdmissionCounter::new(2));
        assert_eq!(counter.acquire(2), 2);
        let other = counter.clone();
        let task = thread::spawn(move || other.release(2));
        counter.close();
        task.join().unwrap();
        assert!(counter.is_closed());
        assert_eq!(counter.available(), 0);
    });
}

#[test]
fn credit_drain_racing_return_counts_each_slot_once() {
    loom::model(|| {
        let credit = Arc::new(CreditCounter::default());
        let other = credit.clone();
        let task = thread::spawn(move || other.publish(1));
        let count = credit.take();
        task.join().unwrap();
        assert_eq!(count + credit.take(), 1);
        assert_eq!(credit.take(), 0);
    });
}

#[test]
fn separate_return_batches_accumulate_without_loss() {
    loom::model(|| {
        let credit = Arc::new(CreditCounter::default());
        let other = credit.clone();
        let task = thread::spawn(move || other.publish(2));
        credit.publish(3);
        task.join().unwrap();
        assert_eq!(credit.take(), 5);
    });
}

#[test]
fn credit_observation_acquires_prior_storage_publication() {
    use loom::sync::atomic::{AtomicBool, Ordering};
    loom::model(|| {
        let credit = Arc::new(CreditCounter::default());
        let ready = Arc::new(AtomicBool::new(false));
        let returned = credit.clone();
        let storage = ready.clone();
        let task = thread::spawn(move || {
            storage.store(true, Ordering::Relaxed);
            returned.publish(1);
        });
        if credit.take() == 1 {
            assert!(ready.load(Ordering::Relaxed));
        }
        task.join().unwrap();
    });
}
