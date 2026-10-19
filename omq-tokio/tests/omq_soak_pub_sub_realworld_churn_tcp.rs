#![cfg(feature = "soak")]
//! Soak: deterministic TCP PUB/SUB churn under sustained load.
//!
//! Exercises a real-world live-subscriber pattern instead of random churn:
//! 0 -> 1 -> 12 -> 3 -> 0 subscribers, repeated for the soak duration.
//! Each phase gates send throughput and in-phase sequence gaps after new
//! subscribers have propagated their subscriptions to the PUB socket.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{Message, MonitorEvent, OnMute, Options, ReconnectPolicy, Socket, SocketType};

const PHASE_TARGETS: &[usize] = &[0, 1, 12, 3, 0];
const TOPICS: &[&[u8]] = &[b"a.", b"b.", b"c.", b"d."];
const PAYLOAD_LEN: usize = 96;
const SEND_BATCH: usize = 256;
const SOAK_HWM: u32 = 8_192;
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
const PHASE_WARMUP: Duration = Duration::from_millis(300);
const DEFAULT_PHASE_DURATION: Duration = Duration::from_secs(3);
const SHORT_PHASE_DURATION: Duration = Duration::from_secs(1);
const MIN_SEND_MSGS_PER_SEC: f64 = 5_000.0;
const MAX_GAP_RATIO: f64 = 0.01;
const MIN_EXPECTED_FOR_GAP_CHECK: u64 = 1_000;
const EXTERNAL_PAUSE_THRESHOLD: Duration = Duration::from_secs(10);

fn active_elapsed(start: Instant, paused: Duration) -> Duration {
    start.elapsed().saturating_sub(paused)
}

fn account_external_pause(
    label: &str,
    last_progress: &mut Instant,
    run_paused: &mut Duration,
    phase_paused: &mut Duration,
) -> bool {
    let now = Instant::now();
    let idle = now.saturating_duration_since(*last_progress);
    *last_progress = now;

    if idle < EXTERNAL_PAUSE_THRESHOLD {
        return false;
    }

    *run_paused += idle;
    *phase_paused += idle;
    eprintln!(
        "[pub_sub_realworld_churn_tcp] ignored external pause during {label}: {:.1}s",
        idle.as_secs_f64()
    );
    true
}

fn pub_options() -> Options {
    soak_common::soak_options()
        .send_hwm(SOAK_HWM)
        .recv_hwm(SOAK_HWM)
        .on_mute(OnMute::DropNewest)
}

fn sub_options() -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options().recv_hwm(SOAK_HWM)
    }
}

fn phase_duration(total: Duration) -> Duration {
    if let Some(ms) = std::env::var("OMQ_SOAK_PUBSUB_PHASE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return Duration::from_millis(ms.max(500));
    }

    if total < Duration::from_secs(30) {
        SHORT_PHASE_DURATION
    } else {
        DEFAULT_PHASE_DURATION
    }
}

fn encode(seq: u64) -> Vec<u8> {
    let topic = TOPICS[seq as usize % TOPICS.len()];
    let mut buf = Vec::with_capacity(topic.len() + 8 + PAYLOAD_LEN);
    buf.extend_from_slice(topic);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.resize(topic.len() + 8 + PAYLOAD_LEN, b'x');
    buf
}

fn decode(part: &[u8]) -> Option<(usize, u64)> {
    for (idx, topic) in TOPICS.iter().enumerate() {
        if part.starts_with(topic) && part.len() >= topic.len() + 8 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&part[topic.len()..topic.len() + 8]);
            return Some((idx, u64::from_le_bytes(bytes)));
        }
    }
    None
}

async fn wait_for_subscribes(mon: &mut omq_tokio::MonitorStream, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut subscribed = 0;
    while subscribed < n {
        let now = Instant::now();
        assert!(now < deadline, "timed out waiting for {n} subscriptions");
        match tokio::time::timeout(deadline - now, mon.recv()).await {
            Ok(Ok(MonitorEvent::SubscribeReceived { .. })) => subscribed += 1,
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("monitor failed while waiting for subscriptions: {e:?}"),
            Err(e) => panic!("timed out waiting for {n} subscriptions: {e}"),
        }
    }
}

async fn wait_for_live_peers(publisher: &Socket, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let live = publisher
            .connections()
            .await
            .expect("publisher connection snapshot")
            .into_iter()
            .filter(|c| c.peer_info.is_some())
            .count();
        if live == n {
            return;
        }
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {n} live peers, still have {live}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Debug)]
struct SubscriberProbe {
    socket: Socket,
    prefix: Bytes,
    last_seq_by_topic: [Option<u64>; 4],
    received: u64,
    gaps: u64,
}

impl SubscriberProbe {
    async fn connect(ep: omq_tokio::Endpoint, id: usize) -> Self {
        let socket = Socket::new(SocketType::Sub, sub_options());
        let prefix = if id.is_multiple_of(2) {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(TOPICS[id % TOPICS.len()])
        };
        socket.connect(ep).await.unwrap();
        socket.subscribe(prefix.clone()).await.unwrap();
        Self {
            socket,
            prefix,
            last_seq_by_topic: [None; 4],
            received: 0,
            gaps: 0,
        }
    }

    fn drain(&mut self) {
        while let Ok(msg) = self.socket.try_recv() {
            let Some(part) = msg.part_bytes(0) else {
                continue;
            };
            let Some((topic_idx, seq)) = decode(&part) else {
                panic!("subscriber received malformed payload");
            };
            if !self.prefix.is_empty() {
                assert!(
                    part.starts_with(&self.prefix),
                    "subscriber received non-matching topic"
                );
            }
            if let Some(prev) = self.last_seq_by_topic[topic_idx] {
                let delta = seq.saturating_sub(prev);
                if delta > TOPICS.len() as u64 {
                    self.gaps += delta / TOPICS.len() as u64 - 1;
                }
            }
            self.last_seq_by_topic[topic_idx] = Some(seq);
            self.received += 1;
        }
    }

    fn reset_gap_window(&mut self) {
        self.last_seq_by_topic = [None; 4];
    }
}

/// Baseline counters captured when a phase's measurement window opens.
struct Measurement {
    start: Instant,
    sent: u64,
    received: u64,
    gaps: u64,
}

impl Measurement {
    /// Opens a window at `seq`. Gap tracking restarts so the first message
    /// after this point starts each subscriber's in-phase gap window.
    fn begin(subs: &mut [SubscriberProbe], seq: u64) -> Self {
        let measurement = Self {
            start: Instant::now(),
            sent: seq,
            received: subs.iter().map(|s| s.received).sum(),
            gaps: subs.iter().map(|s| s.gaps).sum(),
        };
        for sub in subs {
            sub.reset_gap_window();
        }
        measurement
    }

    fn assert_phase(&self, subs: &[SubscriberProbe], seq: u64, target: usize) {
        let elapsed = self.start.elapsed().as_secs_f64().max(0.001);
        let sent_delta = seq.saturating_sub(self.sent);
        let received_now: u64 = subs.iter().map(|s| s.received).sum();
        let gaps_now: u64 = subs.iter().map(|s| s.gaps).sum();
        let recv_delta = received_now.saturating_sub(self.received);
        let gap_delta = gaps_now.saturating_sub(self.gaps);
        let send_rate = sent_delta as f64 / elapsed;

        // Each message a subscriber should have seen was either received
        // or counted as a gap.
        let expected = recv_delta + gap_delta;
        let gap_ratio = if expected == 0 {
            0.0
        } else {
            gap_delta as f64 / expected as f64
        };

        eprintln!(
            "[pub_sub_realworld_churn_tcp] phase target {target}: \
             send_rate {send_rate:.0} msg/s, recv {recv_delta}, \
             gaps {gap_delta}, gap_ratio {gap_ratio:.4}"
        );

        assert!(
            send_rate >= MIN_SEND_MSGS_PER_SEC,
            "phase target {target} send rate too low: {send_rate:.0} msg/s"
        );
        // Small samples make the ratio too noisy to gate on.
        if expected >= MIN_EXPECTED_FOR_GAP_CHECK {
            assert!(
                gap_ratio <= MAX_GAP_RATIO,
                "phase target {target} gap ratio too high: {gap_ratio:.4}"
            );
        }
    }
}

/// Clock and measurement window of one subscriber-count phase.
struct Phase {
    target: usize,
    start: Instant,
    paused: Duration,
    last_progress: Instant,
    /// Opens after `PHASE_WARMUP`, once new subscribers are flowing.
    measurement: Option<Measurement>,
}

impl Phase {
    fn new(target: usize) -> Self {
        let start = Instant::now();
        Self {
            target,
            start,
            paused: Duration::ZERO,
            last_progress: start,
            measurement: None,
        }
    }

    fn active(&self) -> Duration {
        active_elapsed(self.start, self.paused)
    }
}

/// Soak-wide state carried across phases.
struct Soak {
    publisher: Socket,
    mon: omq_tokio::MonitorStream,
    ep: omq_tokio::Endpoint,
    subs: Vec<SubscriberProbe>,
    next_sub_id: usize,
    seq: u64,
    start: Instant,
    /// External pause time excluded from the soak duration.
    paused: Duration,
    last_log: Instant,
    duration: Duration,
    phase_duration: Duration,
}

impl Soak {
    async fn start(duration: Duration) -> Self {
        let phase_duration = phase_duration(duration);
        let publisher = Socket::new(SocketType::Pub, pub_options());
        let mon = publisher.monitor();
        let ep = publisher.bind(soak_common::tcp_ep(0)).await.unwrap();
        let start = Instant::now();

        Self {
            publisher,
            mon,
            ep,
            subs: Vec::new(),
            next_sub_id: 0,
            seq: 0,
            start,
            paused: Duration::ZERO,
            last_log: start,
            duration,
            phase_duration,
        }
    }

    fn active(&self) -> Duration {
        active_elapsed(self.start, self.paused)
    }

    /// Closes the newest subscribers or connects new ones until `target`
    /// are live and subscribed on the PUB side.
    async fn set_target_subscribers(&mut self, target: usize) {
        while self.subs.len() > target {
            let sub = self.subs.pop().unwrap();
            sub.socket.close().await.unwrap();
        }

        if self.subs.len() < target {
            let add = target - self.subs.len();
            for _ in 0..add {
                let sub = SubscriberProbe::connect(self.ep.clone(), self.next_sub_id).await;
                self.next_sub_id += 1;
                self.subs.push(sub);
            }
            wait_for_subscribes(&mut self.mon, add).await;
        }

        wait_for_live_peers(&self.publisher, target).await;
    }

    async fn run_phase(&mut self, target: usize) {
        self.set_target_subscribers(target).await;

        let mut phase = Phase::new(target);
        while phase.active() < self.phase_duration && self.active() < self.duration {
            self.handle_pause(&mut phase, "phase");
            self.send_batch().await;

            if phase.measurement.is_none() && phase.active() >= PHASE_WARMUP {
                phase.measurement = Some(Measurement::begin(&mut self.subs, self.seq));
            }

            self.log_progress(target);
            self.handle_pause(&mut phase, "phase work");
            tokio::task::yield_now().await;
        }

        for sub in &mut self.subs {
            sub.drain();
        }

        if let Some(measurement) = &phase.measurement {
            measurement.assert_phase(&self.subs, self.seq, phase.target);
        }
    }

    /// Excludes an external pause from both clocks and restarts any open
    /// measurement window, since rates across the pause are meaningless.
    fn handle_pause(&mut self, phase: &mut Phase, label: &str) {
        if account_external_pause(
            label,
            &mut phase.last_progress,
            &mut self.paused,
            &mut phase.paused,
        ) {
            self.last_log = Instant::now();
            if phase.measurement.is_some() {
                phase.measurement = Some(Measurement::begin(&mut self.subs, self.seq));
            }
        }
    }

    async fn send_batch(&mut self) {
        for _ in 0..SEND_BATCH {
            let msg = Message::single(encode(self.seq));
            tokio::time::timeout(SEND_TIMEOUT, self.publisher.send(msg))
                .await
                .expect("PUB send stalled")
                .unwrap();
            self.seq += 1;
        }

        for sub in &mut self.subs {
            sub.drain();
        }
    }

    fn log_progress(&mut self, target: usize) {
        if self.last_log.elapsed() < Duration::from_secs(30) {
            return;
        }

        let received: u64 = self.subs.iter().map(|s| s.received).sum();
        let gaps: u64 = self.subs.iter().map(|s| s.gaps).sum();
        eprintln!(
            "[pub_sub_realworld_churn_tcp] {:.0}s, phase_target {target}, \
             sent {}, received {received}, gaps {gaps}",
            self.active().as_secs_f64(),
            self.seq,
        );
        self.last_log = Instant::now();
    }

    async fn finish(self) {
        for sub in self.subs {
            sub.socket.close().await.unwrap();
        }
        wait_for_live_peers(&self.publisher, 0).await;
        self.publisher.close().await.unwrap();
    }
}

#[test]
fn soak_pub_sub_realworld_churn_tcp() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let mut soak = Soak::start(duration).await;

        let mut phase_idx = 0usize;
        while soak.active() < duration {
            let target = PHASE_TARGETS[phase_idx % PHASE_TARGETS.len()];
            phase_idx += 1;
            soak.run_phase(target).await;
        }

        soak.finish().await;
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_realworld_churn_tcp");
}
