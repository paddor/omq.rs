use super::*;

#[test]
fn excludes_warmup_backlog_and_messages_after_deadline() {
    let now = Instant::now();
    let window = Window::new(now, Duration::from_secs(1), Duration::from_secs(2));
    let mut counter = Counter::new(window);
    let warmup = Message::from_slice(&[0]);
    let measured = Message::from_slice(&[MEASURED_TAG]);

    counter.record(&measured, now);
    counter.record(&warmup, window.start);
    counter.record(&measured, window.start);
    counter.record(&measured, window.end);
    counter.record(&measured, window.end + Duration::from_secs(1));

    let received = counter.finish(window.end + Duration::from_secs(1));
    assert_eq!(received.count, 1);
    assert_eq!(received.elapsed, Duration::from_secs(3));
    assert!((received.rate() - 1.0 / 3.0).abs() < f64::EPSILON);
}

#[test]
fn classifies_router_messages_by_body_instead_of_identity() {
    use bytes::Bytes;

    let now = Instant::now();
    let window = Window::new(now, Duration::ZERO, Duration::from_secs(1));
    let mut counter = Counter::new(window);
    let warmup = Message::multipart([
        Bytes::from_static(&[MEASURED_TAG]),
        Bytes::from_static(&[0]),
    ]);
    let measured = Message::multipart([
        Bytes::from_static(&[0]),
        Bytes::from_static(&[MEASURED_TAG]),
    ]);

    counter.record(&warmup, now);
    counter.record(&measured, now);

    assert_eq!(counter.finish(window.end).count, 1);
}

#[test]
fn uses_common_window_for_receivers_finishing_at_different_times() {
    let first = Received {
        count: 100,
        elapsed: Duration::from_secs(1),
    };
    let second = Received {
        count: 200,
        elapsed: Duration::from_secs(2),
    };
    assert!((first.combine(second).rate() - 150.0).abs() < f64::EPSILON);
}

#[test]
fn continuously_ready_receiver_yields_at_both_budget_limits() {
    let now = Instant::now();
    let window = Window::new(now, Duration::ZERO, Duration::from_secs(1));

    for (size, expected) in [(16, 256), (1024 * 1024, 2)] {
        let message = Message::from_slice(&vec![MEASURED_TAG; size]);
        let mut counter = Counter::new(window);
        let mut budget = omq_proto::flow::DrainBudget::WORKER;
        let mut clock_reads = 0;

        let result = drain_ready(
            || Ok(message.clone()),
            &mut counter,
            window,
            &mut budget,
            || {
                clock_reads += 1;
                now
            },
        );

        assert_eq!(result, DrainResult::Budget);
        assert_eq!(counter.count, expected);
        assert_eq!(clock_reads, 2, "clock cost must be per batch");
    }
}

#[test]
fn checks_clock_per_batch_and_excludes_batch_crossing_deadline() {
    let now = Instant::now();
    let window = Window::new(now, Duration::ZERO, Duration::from_secs(1));
    let mut times = [now, window.end].into_iter();
    let mut counter = Counter::new(window);
    let mut budget = omq_proto::flow::DrainBudget::new(3, 1024);
    let mut calls = 0;

    let result = drain_ready(
        || {
            calls += 1;
            Ok(Message::from_slice(&[MEASURED_TAG]))
        },
        &mut counter,
        window,
        &mut budget,
        || times.next().unwrap(),
    );

    assert_eq!(result, DrainResult::Deadline);
    assert_eq!(calls, 3);
    assert_eq!(counter.count, 0);
}

#[test]
fn discards_batch_straddling_warmup_and_keeps_next_complete_batch() {
    let now = Instant::now();
    let window = Window::new(now, Duration::from_secs(1), Duration::from_secs(1));
    let mut counter = Counter::new(window);

    for (start, expected) in [(now, 0), (window.start, 2)] {
        let mut times = [start, window.start + Duration::from_millis(1)].into_iter();
        let mut messages = [0, MEASURED_TAG, MEASURED_TAG].into_iter();

        assert_eq!(
            drain_ready(
                || Ok(Message::from_slice(&[messages.next().unwrap()])),
                &mut counter,
                window,
                &mut omq_proto::flow::DrainBudget::new(3, 1024),
                || times.next().expect("only two clock reads per batch"),
            ),
            DrainResult::Budget,
        );

        assert_eq!(counter.count, expected);
    }
}

#[test]
fn expired_window_does_not_receive_another_batch() {
    let now = Instant::now();
    let window = Window::new(now, Duration::ZERO, Duration::from_secs(1));
    let mut budget = omq_proto::flow::DrainBudget::WORKER;

    assert_eq!(
        drain_ready(
            || panic!("no receive after observed deadline"),
            &mut Counter::new(window),
            window,
            &mut budget,
            || window.end,
        ),
        DrainResult::Deadline,
    );
}
