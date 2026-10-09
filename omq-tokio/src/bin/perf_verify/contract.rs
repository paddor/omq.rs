//! Workload profile contract: each profile must not lose to the other on
//! the workload it is meant for. Lockstep round trips belong to the latency
//! profile, one-way streams to the throughput profile. Both run in the same
//! process and session, so the check needs no hardware thresholds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Message, Options, SocketType};

use super::affinity::Side;
use super::measurement::Window;
use super::{SETTINGS, context, payload, tcp_zero, transfer_blocking};

/// The favored profile may be at most 15% slower than the other one.
pub(super) const MIN_RATIO: f64 = 0.85;
const RESPONDER: &[u8] = b"perf-responder";
const TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
pub(super) enum Pattern {
    ReqRep,
    DealerRouter,
    Pair,
    Peer,
    PushPull,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Shape {
    /// `peers` requesters, one message in flight each.
    RoundTrip { peers: usize },
    /// One sender streaming to one receiver.
    Stream,
}

impl Pattern {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::ReqRep => "req_rep",
            Self::DealerRouter => "dealer_router",
            Self::Pair => "pair",
            Self::Peer => "peer",
            Self::PushPull => "push_pull",
        }
    }

    /// (requester or sender, responder or receiver)
    fn types(self) -> (SocketType, SocketType) {
        match self {
            Self::ReqRep => (SocketType::Req, SocketType::Rep),
            Self::DealerRouter => (SocketType::Dealer, SocketType::Router),
            Self::Pair => (SocketType::Pair, SocketType::Pair),
            Self::Peer => (SocketType::Peer, SocketType::Peer),
            Self::PushPull => (SocketType::Push, SocketType::Pull),
        }
    }

    fn options(kind: SocketType, profile: WorkloadProfile, identity: Bytes) -> Options {
        Options::default()
            .workload_profile(profile)
            .identity(identity)
            .router_mandatory(matches!(kind, SocketType::Router | SocketType::Peer))
    }

    fn request(self, size: usize) -> Message {
        if matches!(self, Self::Peer) {
            Message::multipart([Bytes::from_static(RESPONDER), Bytes::from(vec![0; size])])
        } else {
            payload(size)
        }
    }
}

pub(super) fn favored(shape: Shape) -> WorkloadProfile {
    match shape {
        Shape::RoundTrip { .. } => WorkloadProfile::Latency,
        Shape::Stream => WorkloadProfile::Throughput,
    }
}

fn other(profile: WorkloadProfile) -> WorkloadProfile {
    match profile {
        WorkloadProfile::Latency => WorkloadProfile::Throughput,
        WorkloadProfile::Throughput => WorkloadProfile::Latency,
    }
}

/// Favored rate over the other profile's rate. A failing ratio is measured
/// again, keeping each profile's best rate, so one noisy window cannot fail.
pub(super) fn ratio(pattern: Pattern, shape: Shape, size: usize) -> f64 {
    let favored = favored(shape);
    let mut best = (0.0f64, 0.0f64);
    for attempt in 0..2 {
        best.0 = best.0.max(rate(pattern, shape, size, favored));
        best.1 = best.1.max(rate(pattern, shape, size, other(favored)));
        let ratio = best.0 / best.1;
        println!(
            "  {favored:?} {:.0}/s, {:?} {:.0}/s, ratio {ratio:.2}{}",
            best.0,
            other(favored),
            best.1,
            if attempt > 0 { " (best of 2)" } else { "" }
        );
        if ratio >= MIN_RATIO {
            return ratio;
        }
    }
    best.0 / best.1
}

fn rate(pattern: Pattern, shape: Shape, size: usize, profile: WorkloadProfile) -> f64 {
    match shape {
        Shape::RoundTrip { peers } => round_trips(pattern, size, peers, profile),
        Shape::Stream => stream(pattern, size, profile),
    }
}

fn stream(pattern: Pattern, size: usize, profile: WorkloadProfile) -> f64 {
    let (sender_type, receiver_type) = pattern.types();
    let receiver_ctx = context(1, Side::Receiver);
    let sender_ctx = context(1, Side::Sender);
    let receiver = receiver_ctx.blocking_socket(
        receiver_type,
        Pattern::options(receiver_type, profile, Bytes::from_static(RESPONDER)),
    );
    let sender = sender_ctx.blocking_socket(
        sender_type,
        Pattern::options(sender_type, profile, Bytes::from_static(b"perf-sender")),
    );
    let endpoint = receiver.bind(tcp_zero()).expect("receiver bind");
    sender.connect(endpoint).expect("sender connect");
    receiver
        .wait_connected(1, TIMEOUT)
        .expect("receiver connect timeout");
    let rate = transfer_blocking(&sender, receiver, size);
    sender_ctx.term();
    receiver_ctx.term();
    rate
}

fn round_trips(pattern: Pattern, size: usize, peers: usize, profile: WorkloadProfile) -> f64 {
    let (requester_type, responder_type) = pattern.types();
    let responder_ctx = context(1, Side::Receiver);
    let requester_ctx = context(1, Side::Sender);
    let responder = responder_ctx.blocking_socket(
        responder_type,
        Pattern::options(responder_type, profile, Bytes::from_static(RESPONDER)),
    );
    let endpoint = responder.bind(tcp_zero()).expect("responder bind");
    let requesters: Vec<_> = (0..peers)
        .map(|index| {
            let requester = requester_ctx.blocking_socket(
                requester_type,
                Pattern::options(
                    requester_type,
                    profile,
                    Bytes::from(format!("perf-requester-{index}")),
                ),
            );
            requester
                .connect(endpoint.clone())
                .expect("requester connect");
            requester
                .wait_connected(1, TIMEOUT)
                .expect("requester connect timeout");
            requester
        })
        .collect();
    responder
        .wait_connected(peers, TIMEOUT)
        .expect("responder connect timeout");

    let stop = Arc::new(AtomicBool::new(false));
    let serving = {
        let stop = stop.clone();
        thread::spawn(move || {
            SETTINGS.affinity.pin(5);
            while !stop.load(Ordering::Acquire) {
                match responder.recv_timeout(Duration::from_millis(20)) {
                    Ok(request) => responder.send(request).expect("responder echo"),
                    Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => {}
                    Err(error) => panic!("responder recv failed: {error}"),
                }
            }
            responder
        })
    };
    let window = Window::new(Instant::now(), SETTINGS.warmup, SETTINGS.measure);
    let request = pattern.request(size);
    let clients: Vec<_> = requesters
        .into_iter()
        .enumerate()
        .map(|(index, requester)| {
            let request = request.clone();
            thread::spawn(move || {
                SETTINGS.affinity.pin(index);
                let mut count = 0u64;
                loop {
                    let started = Instant::now();
                    if started >= window.end {
                        break;
                    }
                    requester.send(request.clone()).expect("requester send");
                    requester.recv_timeout(TIMEOUT).expect("requester recv");
                    if started >= window.start && Instant::now() < window.end {
                        count += 1;
                    }
                }
                (count, requester)
            })
        })
        .collect();
    let mut total = 0;
    let mut sockets = Vec::with_capacity(clients.len() + 1);
    for client in clients {
        let (count, requester) = client.join().expect("requester thread");
        total += count;
        sockets.push(requester);
    }
    stop.store(true, Ordering::Release);
    sockets.push(serving.join().expect("responder thread"));
    drop(sockets);
    requester_ctx.term();
    responder_ctx.term();
    total as f64 / SETTINGS.measure.as_secs_f64()
}
