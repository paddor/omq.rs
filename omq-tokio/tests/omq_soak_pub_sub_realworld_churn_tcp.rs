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

fn reset_measurement_after_pause(
    subs: &mut [SubscriberProbe],
    seq: u64,
    measure_start: &mut Instant,
    sent_at_measure_start: &mut u64,
    recv_at_measure_start: &mut u64,
    gaps_at_measure_start: &mut u64,
) {
    *measure_start = Instant::now();
    *sent_at_measure_start = seq;
    *recv_at_measure_start = subs.iter().map(|s| s.received).sum();
    *gaps_at_measure_start = subs.iter().map(|s| s.gaps).sum();
    for sub in subs {
        sub.reset_gap_window();
    }
}

fn pub_options() -> Options {
    soak_common::soak_options()
        .send_hwm(65_536)
        .recv_hwm(65_536)
        .on_mute(OnMute::DropNewest)
}

fn sub_options() -> Options {
    Options {
        reconnect: ReconnectPolicy::Disabled,
        ..soak_common::soak_options().recv_hwm(65_536)
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

async fn set_target_subscribers(
    subs: &mut Vec<SubscriberProbe>,
    publisher: &Socket,
    mon: &mut omq_tokio::MonitorStream,
    ep: &omq_tokio::Endpoint,
    target: usize,
    next_id: &mut usize,
) {
    while subs.len() > target {
        let sub = subs.pop().unwrap();
        sub.socket.close().await.unwrap();
    }
    if subs.len() < target {
        let add = target - subs.len();
        for _ in 0..add {
            let sub = SubscriberProbe::connect(ep.clone(), *next_id).await;
            *next_id += 1;
            subs.push(sub);
        }
        wait_for_subscribes(mon, add).await;
    }
    wait_for_live_peers(publisher, target).await;
}

#[test]
#[expect(clippy::too_many_lines)]
fn soak_pub_sub_realworld_churn_tcp() {
    let duration = soak_common::soak_duration();
    let phase_duration = phase_duration(duration);
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let publisher = Socket::new(SocketType::Pub, pub_options());
        let mut mon = publisher.monitor();
        let ep = publisher.bind(soak_common::tcp_ep(0)).await.unwrap();

        let mut subs: Vec<SubscriberProbe> = Vec::new();
        let mut seq = 0u64;
        let mut next_sub_id = 0usize;
        let mut phase_idx = 0usize;
        let start = Instant::now();
        let mut run_paused = Duration::ZERO;
        let mut last_log = start;

        while active_elapsed(start, run_paused) < duration {
            let target = PHASE_TARGETS[phase_idx % PHASE_TARGETS.len()];
            phase_idx += 1;

            set_target_subscribers(
                &mut subs,
                &publisher,
                &mut mon,
                &ep,
                target,
                &mut next_sub_id,
            )
            .await;

            let phase_start = Instant::now();
            let mut measuring = false;
            let mut sent_at_measure_start = seq;
            let mut recv_at_measure_start = 0u64;
            let mut gaps_at_measure_start = 0u64;
            let mut measure_start = phase_start;
            let mut phase_paused = Duration::ZERO;
            let mut last_progress = phase_start;

            while active_elapsed(phase_start, phase_paused) < phase_duration
                && active_elapsed(start, run_paused) < duration
            {
                if account_external_pause(
                    "phase",
                    &mut last_progress,
                    &mut run_paused,
                    &mut phase_paused,
                ) {
                    last_log = Instant::now();
                    if measuring {
                        reset_measurement_after_pause(
                            &mut subs,
                            seq,
                            &mut measure_start,
                            &mut sent_at_measure_start,
                            &mut recv_at_measure_start,
                            &mut gaps_at_measure_start,
                        );
                    }
                }

                for _ in 0..SEND_BATCH {
                    let msg = Message::single(encode(seq));
                    tokio::time::timeout(SEND_TIMEOUT, publisher.send(msg))
                        .await
                        .expect("PUB send stalled")
                        .unwrap();
                    seq += 1;
                }

                for sub in &mut subs {
                    sub.drain();
                }

                if !measuring && active_elapsed(phase_start, phase_paused) >= PHASE_WARMUP {
                    measuring = true;
                    measure_start = Instant::now();
                    sent_at_measure_start = seq;
                    recv_at_measure_start = subs.iter().map(|s| s.received).sum();
                    gaps_at_measure_start = subs.iter().map(|s| s.gaps).sum();
                    // First post-warmup message starts the in-phase gap window.
                    for sub in &mut subs {
                        sub.reset_gap_window();
                    }
                }

                if last_log.elapsed() >= Duration::from_secs(30) {
                    let received: u64 = subs.iter().map(|s| s.received).sum();
                    let gaps: u64 = subs.iter().map(|s| s.gaps).sum();
                    eprintln!(
                        "[pub_sub_realworld_churn_tcp] {:.0}s, phase_target {target}, \
                         sent {seq}, received {received}, gaps {gaps}",
                        active_elapsed(start, run_paused).as_secs_f64(),
                    );
                    last_log = Instant::now();
                }

                if account_external_pause(
                    "phase work",
                    &mut last_progress,
                    &mut run_paused,
                    &mut phase_paused,
                ) {
                    last_log = Instant::now();
                    if measuring {
                        reset_measurement_after_pause(
                            &mut subs,
                            seq,
                            &mut measure_start,
                            &mut sent_at_measure_start,
                            &mut recv_at_measure_start,
                            &mut gaps_at_measure_start,
                        );
                    }
                }

                tokio::task::yield_now().await;
            }

            for sub in &mut subs {
                sub.drain();
            }

            if measuring {
                let elapsed = measure_start.elapsed().as_secs_f64().max(0.001);
                let sent_delta = seq.saturating_sub(sent_at_measure_start);
                let received_now: u64 = subs.iter().map(|s| s.received).sum();
                let gaps_now: u64 = subs.iter().map(|s| s.gaps).sum();
                let recv_delta = received_now.saturating_sub(recv_at_measure_start);
                let gap_delta = gaps_now.saturating_sub(gaps_at_measure_start);
                let send_rate = sent_delta as f64 / elapsed;
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
                if expected >= MIN_EXPECTED_FOR_GAP_CHECK {
                    assert!(
                        gap_ratio <= MAX_GAP_RATIO,
                        "phase target {target} gap ratio too high: {gap_ratio:.4}"
                    );
                }
            }
        }

        for sub in subs {
            sub.socket.close().await.unwrap();
        }
        wait_for_live_peers(&publisher, 0).await;
        publisher.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("pub_sub_realworld_churn_tcp");
}
