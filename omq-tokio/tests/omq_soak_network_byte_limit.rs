#![cfg(feature = "soak")]
//! Shared ingress/egress budgets under mixed transports, churn and shutdown.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

#[path = "soak_network_byte_limit/raw.rs"]
mod raw;
mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::{
    ByteRateLimit, Context, ContextConfig, Endpoint, Message, NetworkRateLimits, Options, Socket,
    SocketType,
};
use rand::Rng;
use tokio::task::JoinSet;

const INGRESS: u64 = 384 * 1024;
const EGRESS: u64 = 256 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(15);
const PHASE: Duration = Duration::from_secs(15);
const SIZES: [usize; 8] = [16, 55, 256, 1024, 1452, 4096, 16384, 65536];
const SCHEMES: &[&str] = &[
    "tcp",
    #[cfg(feature = "lz4")]
    "lz4+tcp",
    #[cfg(feature = "zstd")]
    "zstd+tcp",
    #[cfg(feature = "dart")]
    "dart",
    #[cfg(feature = "quic")]
    "quic",
    #[cfg(feature = "ws")]
    "ws",
];

#[derive(Default)]
struct Counts {
    messages: AtomicU64,
    bytes: AtomicU64,
    connections: AtomicU64,
    cancellations: AtomicU64,
}

impl Counts {
    fn received(&self, bytes: usize) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

/// One epoch: a byte-limited context, its unlimited remote, and the
/// parameters that rotate between epochs.
struct Epoch {
    limited: Context,
    remote: Context,
    lanes: usize,
    burst: u64,
    seed: u64,
}

impl Epoch {
    /// Rotates IO lane count every epoch and burst size every third epoch.
    fn new(index: usize, seed: u64) -> Self {
        let lanes = [1, 2, 4][index % 3];
        // 1500 is about one TCP segment; 65507 is the largest UDP datagram.
        let burst = if (index / 3).is_multiple_of(2) {
            1500
        } else {
            65507
        };

        let limited = Context::with_config(ContextConfig { io_threads: lanes });
        limited
            .set_network_rate_limits(NetworkRateLimits {
                ingress: Some(ByteRateLimit::new(INGRESS, burst)),
                egress: Some(ByteRateLimit::new(EGRESS, burst)),
            })
            .unwrap();
        let remote = Context::with_config(ContextConfig { io_threads: 2 });

        Self {
            limited,
            remote,
            lanes,
            burst,
            seed,
        }
    }

    fn worker(&self, deadline: Instant, seed: u64, counts: &Arc<Counts>) -> Worker {
        Worker {
            limited: self.limited.clone(),
            remote: self.remote.clone(),
            deadline,
            seed,
            counts: counts.clone(),
        }
    }
}

/// Owned inputs of one spawned traffic worker.
struct Worker {
    limited: Context,
    remote: Context,
    deadline: Instant,
    seed: u64,
    counts: Arc<Counts>,
}

fn options() -> Options {
    Options {
        send_hwm: 2,
        recv_hwm: 2,
        linger: Some(Duration::ZERO),
        heartbeat_interval: Some(Duration::from_millis(100)),
        heartbeat_timeout: Some(Duration::from_secs(3)),
        ..Options::default()
    }
}

fn payload(seed: u64, sequence: u64, size: usize) -> Bytes {
    let mut body = vec![0; size];
    body[..8].copy_from_slice(&seed.to_le_bytes());
    body[8..16].copy_from_slice(&sequence.to_le_bytes());
    let mut random = seed ^ sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for byte in &mut body[16..] {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        *byte = if sequence.is_multiple_of(2) {
            sequence as u8
        } else {
            random as u8
        };
    }
    body.into()
}

async fn close(socket: Socket) {
    tokio::time::timeout(IO_TIMEOUT, socket.close_with_linger(Some(Duration::ZERO)))
        .await
        .expect("close stalled behind byte pacing")
        .unwrap();
}

async fn pair(limited: &Context, remote: &Context, scheme: &str) -> (Socket, Socket) {
    let mut server = options();
    let mut client = options();
    #[cfg(feature = "quic")]
    if scheme == "quic" {
        let tls = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        server.quic.server_cert_pem = Some(tls.cert.pem().into_bytes());
        server.quic.server_key_pem = Some(tls.signing_key.serialize_pem().into_bytes());
        client.quic.trust_pem = server.quic.server_cert_pem.clone();
        client.quic.trust_system = false;
    }
    // These options are mutated only when the QUIC feature is enabled.
    let _ = (&mut server, &mut client);
    let bound = limited.socket(SocketType::Channel, server);
    let connected = remote.socket(SocketType::Channel, client);
    let endpoint: Endpoint = format!("{scheme}://127.0.0.1:0").parse().unwrap();
    let endpoint = bound.bind(endpoint).await.unwrap();
    connected.connect(endpoint).await.unwrap();
    bound.wait_connected(1, IO_TIMEOUT).await.unwrap();
    connected.wait_connected(1, IO_TIMEOUT).await.unwrap();
    (bound, connected)
}

/// Received plain TCP payload is a lower bound on charged wire bytes. Start
/// before any application data enters these connections; kernel buffering
/// cannot invalidate this cumulative bound. Compression is tested separately.
struct Envelope {
    started: Instant,
    burst: u64,
    received: [AtomicU64; 2],
}

impl Envelope {
    fn observe(&self, direction: usize, bytes: usize) {
        let total =
            self.received[direction].fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        let rate = [EGRESS, INGRESS][direction];
        let allowed = u128::from(self.burst)
            + self.started.elapsed().as_nanos() * u128::from(rate) / 1_000_000_000;
        assert!(
            u128::from(total) <= allowed,
            "direction {direction} escaped the shared budget: {total} bytes > {allowed}"
        );
    }
}

fn recent_events(monitor: &mut omq_tokio::MonitorStream) -> Vec<String> {
    let mut events = Vec::new();
    let mut budget = omq_tokio::flow::DrainBudget::new(64, 64 * 1024);
    while !budget.exhausted() {
        let event = match monitor.try_recv() {
            Err(omq_tokio::MonitorTryRecvError::Empty) => break,
            event => format!("{event:?}"),
        };
        let _ = budget.account(event.len());
        events.push(event);
    }
    events
}

/// Where one duplex exchange reports its bytes, plus a label for stall panics.
#[derive(Clone, Copy)]
struct ExchangeReport<'a> {
    counts: &'a Counts,
    envelope: Option<&'a Envelope>,
    label: &'a str,
}

async fn exchange(bound: &Socket, remote: &Socket, body: Bytes, report: ExchangeReport<'_>) {
    let ExchangeReport {
        counts,
        envelope,
        label,
    } = report;

    let size = body.len();
    let progress = AtomicU64::new(0);
    let mut bound_events = bound.monitor();
    let mut remote_events = remote.monitor();

    // Bits 2*direction and 2*direction+1 mark send and receive done, so a
    // stall panic shows which half of which direction never completed.
    let transfer = async |sender: &Socket, receiver: &Socket, body: Bytes, direction| {
        sender.send(Message::single(body.clone())).await.unwrap();
        progress.fetch_or(1u64 << (direction * 2), Ordering::Relaxed);
        let message = receiver.recv().await.unwrap();
        progress.fetch_or(2u64 << (direction * 2), Ordering::Relaxed);
        assert_eq!(message.len(), 1, "partial or unexpected multipart delivery");
        assert_eq!(
            message.part_slice(0),
            Some(body.as_ref()),
            "corrupt/reordered payload"
        );
        counts.received(body.len());
        if let Some(envelope) = envelope {
            envelope.observe(direction, body.len());
        }
    };

    tokio::time::timeout(IO_TIMEOUT, async {
        tokio::join!(
            transfer(bound, remote, body.clone(), 0),
            transfer(remote, bound, body, 1)
        );
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{label}: {size}-byte duplex transfer stalled; send/recv bits={:04b}; limited={:?}; remote={:?}",
            progress.load(Ordering::Relaxed),
            recent_events(&mut bound_events),
            recent_events(&mut remote_events),
        )
    });
}

async fn check_envelope(epoch: &Epoch, counts: &Arc<Counts>) {
    let Epoch {
        limited,
        remote,
        lanes,
        burst,
        seed,
    } = epoch;

    let mut pairs = Vec::new();
    for _ in 0..lanes * 2 {
        pairs.push(pair(limited, remote, "tcp").await);
    }

    let envelope = Arc::new(Envelope {
        started: Instant::now(),
        burst: *burst,
        received: [AtomicU64::new(0), AtomicU64::new(0)],
    });

    let mut tasks = JoinSet::new();
    for (index, (bound, remote)) in pairs.into_iter().enumerate() {
        let counts = counts.clone();
        let envelope = envelope.clone();
        let seed = seed ^ index as u64;
        tasks.spawn(async move {
            let report = ExchangeReport {
                counts: &counts,
                envelope: Some(&envelope),
                label: "TCP envelope",
            };
            for sequence in 0..2 {
                exchange(&bound, &remote, payload(seed, sequence, 32768), report).await;
            }

            close(bound).await;
            close(remote).await;
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.expect("aggregate byte envelope failed");
    }

    // 2 * lanes pairs, 2 exchanges each, one body per direction per exchange.
    for direction in &envelope.received {
        assert_eq!(direction.load(Ordering::Relaxed), *lanes as u64 * 4 * 32768);
    }
}

async fn fill_and_cancel(socket: &Socket, seed: u64, counts: &Counts) {
    // Incompressible data larger than the entire burst keeps the driver
    // waiting. A bounded send queue then leaves a caller send pending too.
    let body = payload(seed, 1, 1024 * 1024);
    for _ in 0..16 {
        if tokio::time::timeout(
            Duration::from_millis(2),
            socket.send(Message::single(body.clone())),
        )
        .await
        .is_err()
        {
            counts.cancellations.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
    panic!("failed to create a pending send for cancellation");
}

async fn message_worker(worker: Worker, scheme: &'static str) {
    let Worker {
        limited,
        remote,
        deadline,
        seed,
        counts,
    } = worker;

    let mut sequence = 0;
    while Instant::now() < deadline {
        let (bound, connected) = pair(&limited, &remote, scheme).await;
        counts.connections.fetch_add(1, Ordering::Relaxed);

        for _ in 0..16 {
            let size = SIZES[sequence as usize % SIZES.len()];
            let label = format!("{scheme} sequence={sequence}");
            let report = ExchangeReport {
                counts: &counts,
                envelope: None,
                label: &label,
            };
            exchange(&bound, &connected, payload(seed, sequence, size), report).await;
            sequence += 1;
            if Instant::now() >= deadline {
                break;
            }

            // Brief idle gaps vary demand so buckets refill between bursts.
            if sequence.is_multiple_of(8) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        if scheme == "tcp" {
            fill_and_cancel(&bound, seed, &counts).await;
        }

        close(bound).await;
        close(connected).await;
    }

    assert!(sequence != 0, "{scheme} made no progress");
}

async fn mixed_traffic(epoch: &Epoch, deadline: Instant, counts: &Arc<Counts>) {
    let seed = epoch.seed;

    let mut tasks = JoinSet::new();
    for (index, scheme) in SCHEMES.iter().enumerate() {
        for copy in 0..2 {
            let worker = epoch.worker(deadline, seed ^ (index * 2 + copy) as u64, counts);
            tasks.spawn(message_worker(worker, scheme));
        }
    }

    tasks.spawn(raw::stream_worker(epoch.worker(deadline, seed, counts)));

    // UDP endpoints require a burst that fits a maximum-size datagram.
    if epoch.burst >= 65507 {
        tasks.spawn(raw::udp_worker(epoch.worker(deadline, seed, counts)));
    }

    while let Some(result) = tasks.join_next().await {
        result.expect("mixed transport byte-limit worker failed");
    }
}

#[test]
fn soak_context_network_byte_limits() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let mut rng = soak_common::seeded_rng("network_bytes");

    // With the same OMQ_SOAK_SEED, skipping ahead in the seed stream lets a
    // run resumed at epoch N reuse the original seeds from that epoch on.
    let first_epoch = std::env::var("OMQ_SOAK_START_EPOCH")
        .map_or(1, |value| value.parse::<usize>().expect("positive epoch"));
    assert!(first_epoch != 0);
    for _ in 1..first_epoch {
        let _ = rng.next_u64();
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let started = Instant::now();
        let deadline = started + duration;
        let counts = Arc::new(Counts::default());
        let mut epochs = first_epoch - 1;
        eprintln!("[network_bytes] duration={duration:?}, schemes={SCHEMES:?} + STREAM/UDP");

        while Instant::now() < deadline {
            let epoch = Epoch::new(epochs, rng.next_u64());
            let Epoch { lanes, burst, seed, .. } = epoch;
            eprintln!("[network_bytes] starting epoch={} lanes={lanes} burst={burst} seed={seed}", epochs + 1);

            check_envelope(&epoch, &counts).await;

            // Even a short harness check exercises every enabled transport.
            let phase_end = (Instant::now() + PHASE).min(deadline).max(Instant::now() + Duration::from_secs(1));
            mixed_traffic(&epoch, phase_end, &counts).await;

            let (bound, connected) = pair(&epoch.limited, &epoch.remote, "tcp").await;
            fill_and_cancel(&bound, seed, &counts).await;

            let shutdown = Instant::now();
            // Keep socket handles alive while both runtimes tear down with
            // pending wire writes and cumulative grants still outstanding.
            epoch.limited.term();
            epoch.remote.term();
            assert!(shutdown.elapsed() < Duration::from_secs(3), "context shutdown stalled");
            drop((bound, connected));

            epochs += 1;
            eprintln!(
                "[network_bytes] {:.1}s epoch={epochs} lanes={lanes} burst={burst} messages={} bytes={} connections={} cancellations={}",
                started.elapsed().as_secs_f64(),
                counts.messages.load(Ordering::Relaxed),
                counts.bytes.load(Ordering::Relaxed),
                counts.connections.load(Ordering::Relaxed),
                counts.cancellations.load(Ordering::Relaxed),
            );
        }

        // Every epoch ends with at least one cancelled pending send.
        assert!(counts.cancellations.load(Ordering::Relaxed) >= (epochs + 1 - first_epoch) as u64);
        assert!(started.elapsed() >= duration);
        eprintln!("[network_bytes] completed {} epochs in {:?}", epochs + 1 - first_epoch, started.elapsed());
    });

    drop(runtime);
    monitor.stop().assert_no_leak("network_bytes");
}
