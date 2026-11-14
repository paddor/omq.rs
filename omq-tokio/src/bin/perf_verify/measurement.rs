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

    counter.commit(started, now(), count, result)
}

/// Drain one batch with a single batch receive. Counting a popped batch
/// loads its payloads back to back, so their cache misses overlap.
pub(super) fn drain_batch(
    recv: impl FnOnce(&mut Vec<Message>) -> omq_tokio::Result<usize>,
    batch: &mut Vec<Message>,
    counter: &mut Counter,
    window: Window,
    mut now: impl FnMut() -> Instant,
) -> DrainResult {
    let started = now();
    if started >= window.end {
        return DrainResult::Deadline;
    }

    batch.clear();
    let result = match recv(batch) {
        Ok(_) => DrainResult::Budget,
        Err(omq_tokio::Error::WouldBlock) => DrainResult::Empty,
        Err(error) => panic!("perf recv failed: {error}"),
    };
    let count = batch.iter().map(|m| u64::from(is_measured(m))).sum();
    batch.clear();

    counter.commit(started, now(), count, result)
}

impl Counter {
    fn commit(
        &mut self,
        started: Instant,
        finished: Instant,
        count: u64,
        result: DrainResult,
    ) -> DrainResult {
        // Timestamp the bounded batch, not every message. Commit only batches
        // entirely within the window: preemption can lower the count, never
        // add warmup or late traffic. At most one boundary batch per edge is
        // discarded.
        if started >= self.window.start && finished < self.window.end {
            self.count += count;
        }

        if finished >= self.window.end {
            DrainResult::Deadline
        } else {
            result
        }
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
    count_many(sockets, window, |socket, counter| {
        let mut budget = DrainBudget::new(VISIT_MESSAGES, 64 * 1024);
        drain_ready(
            || socket.try_recv(),
            counter,
            window,
            &mut budget,
            Instant::now,
        )
    })
}

/// Like [`count_blocking_many`], with one batch receive per visit.
pub(super) fn count_batches_many(
    sockets: &[omq_tokio::blocking::Socket],
    window: Window,
) -> Received {
    let mut batch = Vec::with_capacity(VISIT_MESSAGES);
    count_many(sockets, window, |socket, counter| {
        drain_batch(
            |out| socket.try_recv_many_into(VISIT_MESSAGES, out),
            &mut batch,
            counter,
            window,
            Instant::now,
        )
    })
}

/// Messages taken from one receiver before visiting the next.
const VISIT_MESSAGES: usize = 64;

fn count_many(
    sockets: &[omq_tokio::blocking::Socket],
    window: Window,
    mut visit: impl FnMut(&omq_tokio::blocking::Socket, &mut Counter) -> DrainResult,
) -> Received {
    assert!(!sockets.is_empty(), "at least one receiver is required");
    let mut counters: Vec<_> = sockets.iter().map(|_| Counter::new(window)).collect();

    'receive: loop {
        for (socket, counter) in sockets.iter().zip(&mut counters) {
            if visit(socket, counter) == DrainResult::Deadline {
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
#[path = "measurement/tests.rs"]
mod tests;
