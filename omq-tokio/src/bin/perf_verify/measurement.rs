use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::flow::DrainBudget;
use omq_tokio::Message;

use super::SETTINGS;

pub(super) const MEASURED_TAG: u8 = 0x5a;

#[derive(Clone, Copy, Debug)]
pub(super) struct Window {
    pub start: Instant,
    pub end: Instant,
}

impl Window {
    pub(super) fn new(now: Instant, warmup: Duration, duration: Duration) -> Self {
        let start = now + warmup;
        Self {
            start,
            end: start + duration,
        }
    }
}

#[derive(Debug)]
pub(super) struct Counter {
    window: Window,
    count: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Received {
    pub count: u64,
    pub elapsed: Duration,
}

impl Received {
    pub(super) fn rate(self) -> f64 {
        self.count as f64 / self.elapsed.as_secs_f64()
    }

    pub(super) fn combine(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            elapsed: self.elapsed.max(other.elapsed),
        }
    }
}

impl Counter {
    pub(super) fn new(window: Window) -> Self {
        Self { window, count: 0 }
    }

    #[cfg(test)]
    pub(super) fn record(&mut self, message: &Message, now: Instant) {
        if now >= self.window.start && now < self.window.end && is_measured(message) {
            self.count += 1;
        }
    }

    pub(super) fn finish(self, now: Instant) -> Received {
        assert!(now >= self.window.end, "receiver stopped before deadline");
        Received {
            count: self.count,
            // Include scheduling delay at shutdown; never inflate a rate by
            // counting an overrun against a shorter configured duration.
            elapsed: now.duration_since(self.window.start),
        }
    }
}

fn is_measured(message: &Message) -> bool {
    // ROUTER adds an identity frame; the body remains the last part.
    message
        .get(message.len().saturating_sub(1))
        .and_then(|p| p.first())
        == Some(&MEASURED_TAG)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DrainResult {
    Empty,
    Budget,
    Deadline,
}

pub(super) fn drain_ready(
    mut recv: impl FnMut() -> omq_tokio::Result<Message>,
    counter: &mut Counter,
    window: Window,
    budget: &mut omq_proto::flow::DrainBudget,
    mut now: impl FnMut() -> Instant,
) -> DrainResult {
    let started = now();
    if started >= window.end {
        return DrainResult::Deadline;
    }
    let mut count = 0;
    let result = loop {
        if budget.exhausted() {
            break DrainResult::Budget;
        }
        match recv() {
            Ok(message) => {
                count += u64::from(is_measured(&message));
                if !budget.account(message.byte_len()) {
                    break DrainResult::Budget;
                }
            }
            Err(omq_tokio::Error::WouldBlock) => break DrainResult::Empty,
            Err(error) => panic!("perf recv failed: {error}"),
        }
    };
    let finished = now();
    // Timestamp the bounded batch, not every message. Commit only batches
    // entirely within the window: preemption can lower the count, never add
    // warmup or late traffic. At most one boundary batch per edge is discarded.
    if started >= window.start && finished < window.end {
        counter.count += count;
    }
    if finished >= window.end {
        DrainResult::Deadline
    } else {
        result
    }
}

pub(super) fn payload(size: usize) -> Message {
    tagged_payload(size, MEASURED_TAG)
}

pub(super) fn tagged_payload(size: usize, tag: u8) -> Message {
    let bytes = vec![tag; size];
    if size <= omq_tokio::message::MAX_INLINE_MESSAGE {
        Message::from_slice(&bytes)
    } else {
        Message::single(Bytes::from(bytes))
    }
}

pub(super) fn send_blocking_batch_until(
    sock: &omq_tokio::blocking::Socket,
    message: &Message,
    deadline: Instant,
) -> bool {
    send_blocking_batch_until_with(sock, || message.clone(), deadline)
}

pub(super) fn send_blocking_batch_until_with(
    sock: &omq_tokio::blocking::Socket,
    mut message: impl FnMut() -> Message,
    deadline: Instant,
) -> bool {
    if Instant::now() >= deadline {
        return false;
    }
    let mut budget = DrainBudget::WORKER;
    loop {
        let mut msg = message();
        let bytes = msg.byte_len();
        loop {
            match sock.try_send(msg) {
                Ok(()) => break,
                Err(omq_tokio::TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    msg = returned;
                    thread::yield_now();
                }
                Err(error) => panic!("perf send failed: {error}"),
            }
        }
        if !budget.account(bytes) {
            return true;
        }
    }
}

fn count_blocking(sock: &omq_tokio::blocking::Socket, window: Window) -> Received {
    let mut counter = Counter::new(window);
    loop {
        let mut budget = DrainBudget::WORKER;
        if drain_ready(
            || sock.try_recv(),
            &mut counter,
            window,
            &mut budget,
            Instant::now,
        ) == DrainResult::Deadline
        {
            break;
        }
        thread::yield_now();
    }
    counter.finish(Instant::now())
}

pub(super) fn transfer_blocking(
    sender: &omq_tokio::blocking::Socket,
    receiver: omq_tokio::blocking::Socket,
    size: usize,
) -> f64 {
    transfer_blocking_many(sender, vec![receiver], size)
}

pub(super) fn count_blocking_many(
    sockets: &[omq_tokio::blocking::Socket],
    window: Window,
) -> Received {
    assert!(!sockets.is_empty(), "at least one receiver is required");
    let mut counters: Vec<_> = sockets.iter().map(|_| Counter::new(window)).collect();
    'receive: loop {
        for (socket, counter) in sockets.iter().zip(&mut counters) {
            let mut budget = DrainBudget::new(64, 64 * 1024);
            if drain_ready(
                || socket.try_recv(),
                counter,
                window,
                &mut budget,
                Instant::now,
            ) == DrainResult::Deadline
            {
                break 'receive;
            }
        }
        thread::yield_now();
    }
    let now = Instant::now();
    counters
        .into_iter()
        .map(|counter| counter.finish(now))
        .fold(
            Received {
                count: 0,
                elapsed: now.duration_since(window.start),
            },
            Received::combine,
        )
}

pub(super) fn transfer_blocking_many(
    sender: &omq_tokio::blocking::Socket,
    receivers: Vec<omq_tokio::blocking::Socket>,
    size: usize,
) -> f64 {
    let (window_tx, window_rx) = mpsc::channel();
    let ready = Arc::new(Barrier::new(2));
    let receiver_ready = ready.clone();
    let handle = thread::spawn(move || {
        SETTINGS.affinity.pin(5);
        receiver_ready.wait();
        let window = window_rx.recv().expect("receive benchmark window");
        if receivers.len() == 1 {
            count_blocking(&receivers[0], window)
        } else {
            count_blocking_many(&receivers, window)
        }
    });
    ready.wait();
    let window = Window::new(Instant::now(), SETTINGS.warmup, SETTINGS.measure);
    window_tx.send(window).expect("send benchmark window");
    // Warm the actual data path while the receiver drains. Tags exclude any
    // warmup backlog still in flight when measurement starts.
    for (message, deadline) in [
        (tagged_payload(size, 0), window.start),
        (payload(size), window.end),
    ] {
        while send_blocking_batch_until(sender, &message, deadline) {}
    }
    handle.join().expect("receiver thread").rate()
}

#[cfg(test)]
mod tests {
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
}
