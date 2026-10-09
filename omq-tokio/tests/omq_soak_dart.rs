#![cfg(all(feature = "soak", feature = "dart"))]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::dart::{self, Packet};
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{DartCongestion, DartOptions, Endpoint, Message, Options, Socket, SocketType};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const GROUP: &[u8] = b"soak";
const SIZES: [usize; 10] = [0, 16, 64, 255, 256, 512, 1024, 4096, 16384, 70001];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flow {
    ScatterGather,
    ClientServer,
    Peer,
    RadioDish,
    Channel,
}

impl Flow {
    const ALL: [Self; 5] = [
        Self::ScatterGather,
        Self::ClientServer,
        Self::Peer,
        Self::RadioDish,
        Self::Channel,
    ];

    fn kinds(self) -> (SocketType, SocketType) {
        match self {
            Self::ScatterGather => (SocketType::Gather, SocketType::Scatter),
            Self::ClientServer => (SocketType::Server, SocketType::Client),
            Self::Peer => (SocketType::Peer, SocketType::Peer),
            Self::RadioDish => (SocketType::Dish, SocketType::Radio),
            Self::Channel => (SocketType::Channel, SocketType::Channel),
        }
    }

    fn body_index(self) -> usize {
        usize::from(matches!(self, Self::Peer | Self::RadioDish))
    }

    fn replies(self) -> bool {
        matches!(self, Self::ClientServer | Self::Peer | Self::Channel)
    }

    fn message(self, sequence: u64, size: usize) -> Message {
        let body = Message::single(payload(sequence, size));
        match self {
            Self::Peer => Message::with_prefix(Bytes::from_static(b"bound"), body),
            Self::RadioDish => Message::with_prefix(Bytes::from_static(GROUP), body),
            _ => body,
        }
    }

    fn verify(self, message: &Message, sequence: u64, size: usize, identity: &[u8]) {
        assert_eq!(message.len(), self.body_index() + 1, "{self:?} framing");
        match self {
            Self::Peer => assert_eq!(message.part_slice(0), Some(identity)),
            Self::RadioDish => assert_eq!(message.part_slice(0), Some(GROUP)),
            _ => {}
        }
        let body = message.part_slice(self.body_index()).unwrap();
        assert_eq!(
            body,
            payload(sequence, size),
            "{self:?} sequence {sequence}"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Sustained,
    Backpressure,
    Recovery,
}

fn options(congestion: DartCongestion, backpressure: bool, latency: bool) -> Options {
    Options {
        send_hwm: if backpressure { 2 } else { 64 },
        recv_hwm: if backpressure { 2 } else { 64 },
        xpub_nodrop: true,
        workload_profile: latency.then_some(WorkloadProfile::Latency),
        max_message_size: Some(128 * 1024),
        recv_payload_pool: Some(omq_tokio::PayloadPool::new([(2048, 64)]).unwrap()),
        dart: DartOptions {
            congestion,
            window_messages: if backpressure { 1 } else { 64 },
            io_spin: if latency {
                Duration::from_micros(50)
            } else {
                Duration::ZERO
            },
            ..DartOptions::default()
        },
        ..soak_common::soak_options()
    }
}

fn payload(sequence: u64, size: usize) -> Vec<u8> {
    let mut body = vec![0; size];
    if size != 0 {
        body[..8].copy_from_slice(&sequence.to_le_bytes());
        body[8..16].copy_from_slice(&(!sequence).to_le_bytes());
        for (offset, byte) in body.iter_mut().enumerate().skip(16) {
            *byte = (offset as u8).wrapping_mul(31) ^ sequence as u8;
        }
    }
    body
}

async fn receive(socket: &Socket) -> Message {
    tokio::time::timeout(IO_TIMEOUT, socket.recv())
        .await
        .expect("Dart receive stalled")
        .unwrap()
}

async fn send(socket: &Socket, message: Message) {
    tokio::time::timeout(IO_TIMEOUT, socket.send(message))
        .await
        .expect("Dart send stalled")
        .unwrap();
}

#[derive(Default)]
struct Faults {
    dropped: AtomicU64,
    duplicated: AtomicU64,
    reordered: AtomicU64,
}

struct Relay {
    endpoint: Endpoint,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    faults: Arc<Faults>,
}

impl Relay {
    async fn new(target: &Endpoint) -> Self {
        let target: SocketAddr = target
            .to_string()
            .strip_prefix("dart://")
            .unwrap()
            .parse()
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("dart://{}", socket.local_addr().unwrap())
            .parse()
            .unwrap();
        let stop = CancellationToken::new();
        let faults = Arc::new(Faults::default());
        let task = tokio::spawn(relay_packets(socket, target, stop.clone(), faults.clone()));
        Self {
            endpoint,
            stop,
            task,
            faults,
        }
    }

    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(IO_TIMEOUT, self.task)
            .await
            .unwrap()
            .unwrap();
        for (name, counter) in [
            ("dropped", &self.faults.dropped),
            ("duplicated", &self.faults.duplicated),
            ("reordered", &self.faults.reordered),
        ] {
            let count = counter.load(Ordering::Relaxed);
            assert!(count != 0, "relay never {name} a datagram");
            eprintln!("[dart_recovery] {name}={count}");
        }
    }
}

async fn relay_packets(
    socket: UdpSocket,
    target: SocketAddr,
    stop: CancellationToken,
    faults: Arc<Faults>,
) {
    let mut input = [0; dart::MAX_DATAGRAM + 1];
    let mut remote = None;
    let mut pending: Option<(Vec<u8>, SocketAddr, Instant)> = None;
    let mut count = 0_u64;
    let mut turn_messages = 0;
    let mut turn_bytes = 0;
    loop {
        let deadline = pending.as_ref().map_or_else(
            || Instant::now() + Duration::from_secs(60),
            |(_, _, deadline)| *deadline,
        );
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            () = tokio::time::sleep_until(deadline.into()), if pending.is_some() => {
                let (body, destination, _) = pending.take().unwrap();
                socket.send_to(&body, destination).await.unwrap();
            },
            packet = socket.recv_from(&mut input) => {
                let (length, source) = packet.unwrap();
                let destination = if source == target {
                    let Some(remote) = remote else { continue; };
                    remote
                } else {
                    remote = Some(source);
                    target
                };
                let body = &input[..length];
                let decoded = dart::decode_packet(body);
                let sequenced = matches!(decoded, Some(Packet::Data { .. } | Packet::Packed { .. } | Packet::First { .. } | Packet::Continuation { .. }));
                if decoded.is_some() { count += 1; }
                if decoded.is_some() && count.is_multiple_of(113) {
                    faults.dropped.fetch_add(1, Ordering::Relaxed);
                } else if sequenced && count.is_multiple_of(37) && pending.is_none() {
                    pending = Some((body.to_vec(), destination, Instant::now() + Duration::from_millis(2)));
                    faults.reordered.fetch_add(1, Ordering::Relaxed);
                } else {
                    socket.send_to(body, destination).await.unwrap();
                    if sequenced && count.is_multiple_of(73) {
                        socket.send_to(body, destination).await.unwrap();
                        faults.duplicated.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some((body, destination, _)) = pending.take() {
                        socket.send_to(&body, destination).await.unwrap();
                    }
                }
                turn_messages += 1;
                turn_bytes += length;
                if turn_messages >= 64 || turn_bytes >= 64 * 1024 {
                    tokio::task::yield_now().await;
                    turn_messages = 0;
                    turn_bytes = 0;
                }
            }
        }
    }
}

struct Scenario {
    flow: Flow,
    options: Options,
    bound: Socket,
    source: Socket,
    fast: Option<Socket>,
    endpoint: Endpoint,
    relay: Option<Relay>,
    sequence: u64,
    cycles: u64,
}

impl Scenario {
    async fn new(
        flow: Flow,
        congestion: DartCongestion,
        workload: Workload,
        latency: bool,
    ) -> Self {
        let options = options(congestion, workload == Workload::Backpressure, latency);
        let (bound_kind, source_kind) = flow.kinds();
        let bound = Socket::new(
            bound_kind,
            options.clone().identity(Bytes::from_static(b"bound")),
        );
        if flow == Flow::RadioDish {
            bound.join(Bytes::from_static(GROUP)).await.unwrap();
        }
        let endpoint = bound
            .bind("dart://127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let relay = if workload == Workload::Recovery {
            Some(Relay::new(&endpoint).await)
        } else {
            None
        };
        let source = Socket::new(
            source_kind,
            options.clone().identity(Bytes::from_static(b"source")),
        );
        source
            .connect(
                relay
                    .as_ref()
                    .map_or(&endpoint, |relay| &relay.endpoint)
                    .clone(),
            )
            .await
            .unwrap();
        source.wait_connected(1, IO_TIMEOUT).await.unwrap();
        bound.wait_connected(1, IO_TIMEOUT).await.unwrap();
        let fast = if workload == Workload::Backpressure && flow != Flow::Channel {
            let fast = Socket::new(
                source_kind,
                options.clone().identity(Bytes::from_static(b"fast")),
            );
            fast.connect(endpoint.clone()).await.unwrap();
            fast.wait_connected(1, IO_TIMEOUT).await.unwrap();
            bound.wait_connected(2, IO_TIMEOUT).await.unwrap();
            Some(fast)
        } else {
            None
        };
        Self {
            flow,
            options,
            bound,
            source,
            fast,
            endpoint,
            relay,
            sequence: 0,
            cycles: 0,
        }
    }

    async fn finish_message(&self, source: &Socket, message: Message, sequence: u64, size: usize) {
        if self.flow == Flow::ClientServer {
            assert!(message.routing_id().is_some_and(|id| id != 0));
        }
        if self.flow.replies() {
            send(&self.bound, message).await;
            let response = receive(source).await;
            self.flow.verify(&response, sequence, size, b"bound");
            assert_eq!(response.routing_id(), None);
        }
    }

    async fn burst(&mut self) {
        let size = SIZES[self.cycles as usize % SIZES.len()];
        let count = if size <= 255 { 64 } else { 4 };
        if self.flow == Flow::RadioDish {
            send(
                &self.source,
                Message::with_prefix(
                    Bytes::from_static(b"ignored"),
                    Message::single(payload(self.sequence, size)),
                ),
            )
            .await;
        }
        for sequence in self.sequence..self.sequence + count {
            send(&self.source, self.flow.message(sequence, size)).await;
        }
        for sequence in self.sequence..self.sequence + count {
            let message = receive(&self.bound).await;
            self.flow.verify(&message, sequence, size, b"source");
            self.finish_message(&self.source, message, sequence, size)
                .await;
        }
        self.sequence += count;
        self.cycles += 1;
    }

    async fn backpressure(&mut self) {
        let size = [128, 1024, 4096, 16384, 70001][self.cycles as usize % 5];
        send(&self.source, self.flow.message(self.sequence, size)).await;
        let held = receive(&self.bound).await;
        self.flow.verify(&held, self.sequence, size, b"source");
        let cloned = held.clone();
        let view = held.part_bytes(self.flow.body_index()).unwrap();
        drop(held);
        for sequence in self.sequence + 1..=self.sequence + 2 {
            send(&self.source, self.flow.message(sequence, size)).await;
        }
        assert!(
            matches!(
                self.source
                    .try_send(self.flow.message(self.sequence + 3, size)),
                Err(omq_tokio::TrySendError::Full(_))
            ),
            "{:?}: unacknowledged messages did not fill HWM",
            self.flow
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(2), self.bound.recv())
                .await
                .is_err(),
            "{:?}: clone-held storage returned credit",
            self.flow
        );
        if let Some(fast) = &self.fast {
            send(fast, self.flow.message(self.sequence, 64)).await;
            let message = receive(&self.bound).await;
            self.flow.verify(&message, self.sequence, 64, b"fast");
            self.finish_message(fast, message, self.sequence, 64).await;
        }
        drop(cloned);
        assert!(
            tokio::time::timeout(Duration::from_millis(2), self.bound.recv())
                .await
                .is_err(),
            "{:?}: byte view returned credit early",
            self.flow
        );
        drop(view);
        for sequence in self.sequence + 1..=self.sequence + 2 {
            let message = receive(&self.bound).await;
            self.flow.verify(&message, sequence, size, b"source");
            self.finish_message(&self.source, message, sequence, size)
                .await;
        }
        self.sequence += 3;
        self.cycles += 1;
    }

    async fn churn(&mut self) {
        if self.cycles.is_multiple_of(2) {
            self.bound.unbind(self.endpoint.clone()).await.unwrap();
            assert_eq!(
                self.bound.bind(self.endpoint.clone()).await.unwrap(),
                self.endpoint
            );
            self.bound.wait_connected(1, IO_TIMEOUT).await.unwrap();
            self.source.wait_connected(1, IO_TIMEOUT).await.unwrap();
        } else {
            self.source
                .clone_shared()
                .close_with_linger(Some(Duration::ZERO))
                .await
                .unwrap();
            tokio::time::timeout(IO_TIMEOUT, async {
                while !self.bound.connections().await.unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Dart did not retire closed source");
            self.source = Socket::new(
                self.flow.kinds().1,
                self.options.clone().identity(Bytes::from_static(b"source")),
            );
            self.source
                .connect(self.relay.as_ref().unwrap().endpoint.clone())
                .await
                .unwrap();
            self.source.wait_connected(1, IO_TIMEOUT).await.unwrap();
            self.bound.wait_connected(1, IO_TIMEOUT).await.unwrap();
        }
    }

    async fn close(self) {
        if let Some(fast) = self.fast {
            fast.close_with_linger(Some(Duration::ZERO)).await.unwrap();
        }
        self.source
            .close_with_linger(Some(Duration::ZERO))
            .await
            .unwrap();
        self.bound
            .close_with_linger(Some(Duration::ZERO))
            .await
            .unwrap();
        if let Some(relay) = self.relay {
            relay.close().await;
        }
    }
}

fn resource_snapshot() -> (usize, usize) {
    let fds = std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count);
    let rss = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|text| text.split_whitespace().nth(1)?.parse::<usize>().ok())
        .unwrap_or(0)
        * 4096;
    (fds, rss)
}

fn run(workload: Workload) {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let baseline_fds = resource_snapshot().0;
    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let mut scenarios = Vec::new();
        for congestion in [DartCongestion::Adaptive, DartCongestion::Lan] {
            for (index, flow) in Flow::ALL.into_iter().enumerate() {
                scenarios
                    .push(Scenario::new(flow, congestion, workload, index.is_multiple_of(2)).await);
            }
        }
        let started = Instant::now();
        let mut last_report = started;
        let mut last_churn = started;
        let mut churn_index = 0;
        let mut index = 0;
        let mut tracker = soak_common::ThroughputTracker::new(Duration::from_secs(10));
        while started.elapsed() < duration {
            match workload {
                Workload::Backpressure => scenarios[index].backpressure().await,
                _ => scenarios[index].burst().await,
            }
            index = (index + 1) % scenarios.len();
            if workload == Workload::Recovery && last_churn.elapsed() >= Duration::from_secs(3) {
                scenarios[churn_index].churn().await;
                churn_index = (churn_index + 1) % scenarios.len();
                last_churn = Instant::now();
            }
            let messages = scenarios.iter().map(|scenario| scenario.sequence).sum();
            tracker.record(messages);
            if last_report.elapsed() >= Duration::from_secs(30) {
                let (fds, rss) = resource_snapshot();
                eprintln!(
                    "[dart_{workload:?}] elapsed={:.1}s messages={messages} fds={fds} rss={:.1}MiB",
                    started.elapsed().as_secs_f64(),
                    rss as f64 / 1_048_576.0
                );
                last_report = Instant::now();
            }
        }
        if workload == Workload::Sustained {
            tracker.assert_stable("dart_sustained");
        }
        for scenario in scenarios {
            assert!(
                scenario.cycles >= SIZES.len() as u64,
                "{:?} did not cover all sizes",
                scenario.flow
            );
            eprintln!(
                "[dart_{workload:?}] {:?}/{:?}: cycles={} messages={}",
                scenario.flow, scenario.options.dart.congestion, scenario.cycles, scenario.sequence
            );
            scenario.close().await;
        }
    });
    drop(ctx);
    std::thread::sleep(Duration::from_millis(250));
    let final_fds = resource_snapshot().0;
    assert!(
        final_fds <= baseline_fds + 2,
        "FDs did not return to baseline: {baseline_fds} -> {final_fds}"
    );
    monitor.stop().assert_no_leak(&format!("dart_{workload:?}"));
}

#[test]
fn soak_dart_sustained() {
    run(Workload::Sustained);
}

#[test]
fn soak_dart_backpressure() {
    run(Workload::Backpressure);
}

#[test]
fn soak_dart_recovery() {
    run(Workload::Recovery);
}
