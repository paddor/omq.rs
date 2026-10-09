//! Copy budgets for message bytes across socket patterns, workload
//! profiles, transports, peer counts, and sizes.
//!
//! Every cell counts the bytes OMQ copies (`omq_proto::copy_stats`) while it
//! moves messages, then checks them against per-message budgets:
//!
//! - Send: messages below the arena threshold may be framed and staged once
//!   each, plus twice per fan-out target. Larger messages are gathered: only
//!   frame headers may be copied, regardless of peer count.
//! - Receive: one copy per delivery below the large-read threshold, at most
//!   a read buffer of prefix (twice if it was assembled) above it, and none
//!   over inproc.
//!
//! Every exception is listed in [`EXCEPTIONS`] with its reason.
//! Run with `cargo test -p omq-tokio --features copy-stats --test omq_copy_budget`.
//! `OMQ_COPY_BUDGET_VERBOSE=1` prints every cell.
mod test_support;

use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::copy_stats::{self, CopyCounts, Site};
use omq_tokio::blocking::Socket;
use omq_tokio::options::{OnMute, WorkloadProfile};
use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

const TIMEOUT: Duration = Duration::from_secs(5);
/// Frame headers, routing envelopes, and delimiters per message.
const HEADER_SLACK: u64 = 64;
const ARENA_THRESHOLD: u64 = omq_proto::frame_buffer::ARENA_THRESHOLD as u64;
/// Default `large_message_threshold`: larger frames are read in place.
const LARGE_READ: u64 = 128 * 1024;
const SIZES: [usize; 3] = [64, 16 * 1024, 1024 * 1024];
const HUB_IDENTITY: &[u8] = b"hub";

/// Cells allowed to exceed the receive budget, with the reason.
const EXCEPTIONS: &[(Pattern, Transport, &str)] = &[(
    Pattern::Peer,
    Transport::Inproc,
    "PEER bounds payloads whose owner does not report its retained size",
)];

/// Counters are process-wide: cells must not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pattern {
    PushPull,
    PubSub,
    PubSubDropOldest,
    PubSubNoDrop,
    RadioDish,
    ReqRep,
    DealerRouter,
    ClientServer,
    Peer,
    Pair,
}

impl Pattern {
    /// (hub, peer) socket types. The hub binds and serves `peers` peers.
    fn types(self) -> (SocketType, SocketType) {
        match self {
            Self::PushPull => (SocketType::Push, SocketType::Pull),
            Self::PubSub | Self::PubSubDropOldest | Self::PubSubNoDrop => {
                (SocketType::Pub, SocketType::Sub)
            }
            Self::RadioDish => (SocketType::Radio, SocketType::Dish),
            Self::ReqRep => (SocketType::Rep, SocketType::Req),
            Self::DealerRouter => (SocketType::Router, SocketType::Dealer),
            Self::ClientServer => (SocketType::Server, SocketType::Client),
            Self::Peer => (SocketType::Peer, SocketType::Peer),
            Self::Pair => (SocketType::Pair, SocketType::Pair),
        }
    }

    fn fan_out(self) -> bool {
        matches!(
            self,
            Self::PubSub | Self::PubSubDropOldest | Self::PubSubNoDrop | Self::RadioDish
        )
    }

    fn round_trip(self) -> bool {
        matches!(
            self,
            Self::ReqRep | Self::DealerRouter | Self::ClientServer | Self::Peer | Self::Pair
        )
    }

    fn peer_counts(self) -> &'static [usize] {
        if self == Self::Pair { &[1] } else { &[1, 4] }
    }

    fn hub_options(self, profile: WorkloadProfile) -> Options {
        let options = base_options(profile).identity(Bytes::from_static(HUB_IDENTITY));
        match self {
            Self::PubSubDropOldest => options.on_mute(OnMute::DropOldest),
            Self::PubSubNoDrop => Options {
                xpub_nodrop: true,
                ..options
            },
            Self::DealerRouter | Self::Peer => options.router_mandatory(true),
            _ => options,
        }
    }

    fn peer_options(self, profile: WorkloadProfile, index: usize) -> Options {
        let options = base_options(profile).identity(Bytes::from(format!("peer-{index}")));
        if self == Self::Peer {
            options.router_mandatory(true)
        } else {
            options
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Tcp,
    Ipc,
    Inproc,
}

impl Transport {
    fn endpoint(self, name: &str) -> Endpoint {
        match self {
            Self::Tcp => test_support::tcp_loopback(0),
            Self::Ipc => test_support::ipc_endpoint(name),
            Self::Inproc => {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                format!("inproc://copy-budget-{name}-{id}")
                    .parse()
                    .expect("inproc endpoint")
            }
        }
    }
}

const TRANSPORTS: [Transport; 3] = [Transport::Tcp, Transport::Ipc, Transport::Inproc];
const PROFILES: [WorkloadProfile; 2] = [WorkloadProfile::Latency, WorkloadProfile::Throughput];

fn base_options(profile: WorkloadProfile) -> Options {
    Options::default()
        .workload_profile(profile)
        .send_hwm(10_000)
        .recv_hwm(10_000)
}

#[derive(Clone, Copy)]
struct Cell {
    pattern: Pattern,
    transport: Transport,
    profile: WorkloadProfile,
    peers: usize,
    size: usize,
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} {:?} {:?} peers={} size={}",
            self.pattern, self.transport, self.profile, self.peers, self.size
        )
    }
}

impl Cell {
    fn messages(&self) -> usize {
        if self.size >= 1024 * 1024 { 8 } else { 64 }
    }

    fn send_budget(&self) -> u64 {
        let targets = if self.pattern.fan_out() {
            self.peers as u64
        } else {
            0
        };
        let size = self.size as u64;
        if self.transport == Transport::Inproc {
            0
        } else if size < ARENA_THRESHOLD {
            (2 + 2 * targets) * (size + HEADER_SLACK)
        } else {
            (1 + targets) * HEADER_SLACK
        }
    }

    fn recv_budget(&self) -> u64 {
        let size = self.size as u64;
        let excepted = EXCEPTIONS.iter().any(|(pattern, transport, _)| {
            *pattern == self.pattern && *transport == self.transport
        });
        if excepted {
            size + HEADER_SLACK
        } else if self.transport == Transport::Inproc {
            0
        } else if size >= LARGE_READ {
            2 * LARGE_READ + HEADER_SLACK
        } else {
            size + HEADER_SLACK
        }
    }
}

struct Traffic {
    sends: u64,
    deliveries: u64,
}

fn payload(size: usize, round: usize) -> Bytes {
    Bytes::from(vec![(round % 251) as u8; size])
}

fn connect(cell: &Cell, ctx: &Context) -> (Socket, Vec<Socket>) {
    let (hub_type, peer_type) = cell.pattern.types();
    let hub = ctx.blocking_socket(hub_type, cell.pattern.hub_options(cell.profile));
    let name = format!("{:?}", cell.pattern).to_lowercase();
    let endpoint = hub.bind(cell.transport.endpoint(&name)).expect("hub bind");
    let peers: Vec<Socket> = (0..cell.peers)
        .map(|index| {
            let peer =
                ctx.blocking_socket(peer_type, cell.pattern.peer_options(cell.profile, index));
            match peer_type {
                SocketType::Sub => peer.subscribe(Bytes::new()).expect("subscribe"),
                SocketType::Dish => peer.join(Bytes::from_static(b"g")).expect("join"),
                _ => {}
            }
            peer.connect(endpoint.clone()).expect("peer connect");
            peer.wait_connected(1, TIMEOUT).expect("peer connected");
            peer
        })
        .collect();
    hub.wait_connected(cell.peers, TIMEOUT)
        .expect("hub connected");
    if hub_type == SocketType::Pub {
        hub.wait_subscribed(cell.peers as u64, TIMEOUT)
            .expect("subscriptions");
    }
    (hub, peers)
}

fn recv(socket: &Socket) -> Message {
    socket.recv_timeout(TIMEOUT).expect("receive")
}

/// Receive one message from whichever peer the hub routed it to.
fn recv_any(peers: &[Socket]) -> Message {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        for peer in peers {
            if let Ok(message) = peer.recv_timeout(Duration::from_millis(1)) {
                return message;
            }
        }
        assert!(Instant::now() < deadline, "no peer received the message");
    }
}

fn body(message: &Message) -> usize {
    message.get(message.len() - 1).expect("body frame").len()
}

fn request(cell: &Cell, round: usize) -> Message {
    let payload = payload(cell.size, round);
    match cell.pattern {
        Pattern::Peer => Message::multipart([Bytes::from_static(HUB_IDENTITY), payload]),
        Pattern::RadioDish => Message::with_group(Bytes::from_static(b"g"), payload),
        _ => Message::single(payload),
    }
}

fn exchange(cell: &Cell, hub: &Socket, peers: &[Socket], rounds: usize) -> Traffic {
    let mut traffic = Traffic {
        sends: 0,
        deliveries: 0,
    };
    for round in 0..rounds {
        if cell.pattern.round_trip() {
            for peer in peers {
                peer.send(request(cell, round)).expect("request");
                let request = recv(hub);
                assert_eq!(body(&request), cell.size, "{cell}: request");
                hub.send(request).expect("reply");
                assert_eq!(body(&recv(peer)), cell.size, "{cell}: reply");
                traffic.sends += 2;
                traffic.deliveries += 2;
            }
        } else if cell.pattern.fan_out() {
            hub.send(request(cell, round)).expect("publish");
            traffic.sends += 1;
            for peer in peers {
                assert_eq!(body(&recv(peer)), cell.size, "{cell}: delivery");
                traffic.deliveries += 1;
            }
        } else {
            hub.send(request(cell, round)).expect("push");
            assert_eq!(body(&recv_any(peers)), cell.size, "{cell}: delivery");
            traffic.sends += 1;
            traffic.deliveries += 1;
        }
    }
    traffic
}

fn measure(cell: &Cell, ctx: &Context) -> (Traffic, CopyCounts) {
    let (hub, peers) = connect(cell, ctx);
    // Settle handshakes, subscriptions, and buffer growth first.
    exchange(cell, &hub, &peers, 4);
    copy_stats::reset();
    let traffic = exchange(cell, &hub, &peers, cell.messages());
    let counts = copy_stats::snapshot();
    for peer in peers {
        peer.close().expect("close peer");
    }
    hub.close().expect("close hub");
    (traffic, counts)
}

fn check(patterns: &[Pattern]) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let verbose = std::env::var_os("OMQ_COPY_BUDGET_VERBOSE").is_some();
    let ctx = Context::new();
    let mut failures = Vec::new();
    for &pattern in patterns {
        for transport in TRANSPORTS {
            for profile in PROFILES {
                for &peers in pattern.peer_counts() {
                    for size in SIZES {
                        let cell = Cell {
                            pattern,
                            transport,
                            profile,
                            peers,
                            size,
                        };
                        let (traffic, counts) = measure(&cell, &ctx);
                        if verbose {
                            println!(
                                "{cell}: send {} recv {} B per message ({counts})",
                                counts.send() / traffic.sends,
                                counts.recv() / traffic.deliveries,
                            );
                        }
                        let send_limit = traffic.sends * cell.send_budget();
                        let recv_limit = traffic.deliveries * cell.recv_budget();
                        if counts.send() > send_limit || counts.recv() > recv_limit {
                            failures.push(format!(
                                "{cell}: send {}/{} B per message, recv {}/{} B per \
                                 delivery ({counts})",
                                counts.send() / traffic.sends,
                                cell.send_budget(),
                                counts.recv() / traffic.deliveries,
                                cell.recv_budget(),
                            ));
                        }
                    }
                }
            }
        }
    }
    ctx.term();
    assert!(
        failures.is_empty(),
        "copy budgets exceeded:\n{}",
        failures.join("\n")
    );
}

#[test]
fn pipeline_copies_stay_in_budget() {
    check(&[Pattern::PushPull]);
}

#[test]
fn fan_out_copies_stay_in_budget() {
    check(&[
        Pattern::PubSub,
        Pattern::PubSubDropOldest,
        Pattern::PubSubNoDrop,
        Pattern::RadioDish,
    ]);
}

#[test]
fn request_reply_copies_stay_in_budget() {
    check(&[
        Pattern::ReqRep,
        Pattern::DealerRouter,
        Pattern::ClientServer,
    ]);
}

#[test]
fn peer_copies_stay_in_budget() {
    check(&[Pattern::Peer, Pattern::Pair]);
}

#[test]
fn every_site_reports_under_its_direction() {
    let recv: Vec<Site> = Site::ALL
        .into_iter()
        .filter(|site| site.is_recv())
        .collect();
    assert_eq!(
        recv,
        [
            Site::RecvAssemble,
            Site::RecvLargePrefix,
            Site::BoundStorage
        ]
    );
}
