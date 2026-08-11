//! Shared bench scaffolding: same payload sizes, prime + calibrate +
//! best-of-N shape, output formatting, and JSONL schema across every
//! pattern.
//!
//! Each pattern lives in its own bench file (`push_pull.rs`, etc.) that
//! `#[path = "common/mod.rs"] mod common;`s this module.

#![allow(dead_code)]

use std::fs::OpenOptions;
use std::io::Write as _;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
#[cfg(unix)]
use omq_tokio::IpcPath;
use omq_tokio::{Endpoint, Options};

/// Default size sweep: three points that cover the small/medium/large
/// knee of the throughput curve. Pass `--all-sizes` to the bench binary
/// (or set `OMQ_BENCH_SIZES`) for the full geometric sweep.
pub(crate) const DEFAULT_SIZES: &[usize] = &[128, 2_048, 8_192];

/// Full geometric ×4 sweep: 32 B → 128 KiB. Enabled via `--all-sizes`
/// or `OMQ_BENCH_SIZES=32,128,...`.
pub(crate) const ALL_SIZES: &[usize] = &[32, 128, 512, 2_048, 8_192, 32_768, 131_072];

/// Dense ×2 exploratory sweep: 8 B → 256 KiB. Enabled via `--dense-sizes`.
pub(crate) const DENSE_SIZES: &[usize] = &[
    8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072,
    262_144,
];

/// Override with env `OMQ_BENCH_TRANSPORTS=inproc,tcp`.
pub(crate) const DEFAULT_TRANSPORTS: &[&str] = &["inproc", "ipc", "tcp"];

/// One untimed warmup pass; soaks up any first-allocation / first-frame
/// codec setup before the calibration loop starts measuring.
pub(crate) const PRIME_ITERS: usize = 2_000;
pub(crate) const PRIME_BUDGET: Duration = Duration::from_millis(500);

/// Calibration: keep doubling burst size until the timed run lasts at
/// least this long, then extrapolate to the `round_duration()` budget.
pub(crate) const WARMUP_DURATION: Duration = Duration::from_millis(100);

/// Per-cell timed budget. Defaults give `round_duration() × rounds()`
/// ≈ wall time per cell. Each cell reports the **min** wall time
/// across rounds (= peak throughput, closest to the hardware ceiling).
/// Override via env for longer overnight runs (`OMQ_BENCH_ROUND_MS`,
/// `OMQ_BENCH_ROUNDS`).
pub(crate) const DEFAULT_ROUND_DURATION: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_ROUNDS: usize = 3;
pub(crate) const QUICK_ROUND_DURATION: Duration = Duration::from_millis(1500);
pub(crate) const QUICK_ROUNDS: usize = 1;

fn is_quick() -> bool {
    std::env::var("OMQ_BENCH_QUICK").is_ok_and(|v| v == "1")
}

pub(crate) fn round_duration() -> Duration {
    std::env::var("OMQ_BENCH_ROUND_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map_or(
            if is_quick() {
                QUICK_ROUND_DURATION
            } else {
                DEFAULT_ROUND_DURATION
            },
            Duration::from_millis,
        )
}

pub(crate) fn rounds() -> usize {
    std::env::var("OMQ_BENCH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(if is_quick() {
            QUICK_ROUNDS
        } else {
            DEFAULT_ROUNDS
        })
}

/// Hard ceiling per cell — a hang guard, not a tight bound. The 30s
/// base covers TCP setup, ZMTP handshake, subscription propagation,
/// and `wait_connected`'s own 30s deadline. The 2x round budget absorbs
/// calibration overshoot.
pub(crate) fn run_timeout() -> Duration {
    let r = rounds() as u32;
    let per = round_duration();
    per.saturating_mul(r * 2) + Duration::from_secs(30)
}

fn cache_dir() -> PathBuf {
    let base = std::env::var("XDG_CACHE_HOME").map_or_else(
        |_| {
            let home = std::env::var("HOME").expect("HOME not set");
            PathBuf::from(home).join(".cache")
        },
        PathBuf::from,
    );
    base.join("omq")
}

pub(crate) fn results_path() -> PathBuf {
    let mut p = cache_dir();
    let suffix = std::env::var("OMQ_BENCH_RESULTS_SUFFIX").unwrap_or_default();
    if suffix.is_empty() {
        p.push("results_tokio.jsonl");
    } else {
        p.push(format!("results_tokio_{suffix}.jsonl"));
    }
    p
}

pub(crate) fn run_id() -> String {
    static CACHED: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    CACHED
        .get_or_init(|| {
            std::env::var("OMQ_BENCH_RUN_ID").unwrap_or_else(|_| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                format!("ts-{now}")
            })
        })
        .clone()
}

pub(crate) fn compression_results_path() -> PathBuf {
    let mut p = cache_dir();
    p.push("results_compression_tokio.jsonl");
    p
}

pub(crate) fn sizes() -> Vec<usize> {
    if let Ok(s) = std::env::var("OMQ_BENCH_SIZES") {
        return s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
    }
    if std::env::args().any(|a| a == "--dense-sizes") {
        return DENSE_SIZES.to_vec();
    }
    if std::env::args().any(|a| a == "--all-sizes") {
        return ALL_SIZES.to_vec();
    }
    DEFAULT_SIZES.to_vec()
}

pub(crate) fn transports() -> Vec<String> {
    if let Ok(s) = std::env::var("OMQ_BENCH_TRANSPORTS") {
        return s.split(',').map(|t| t.trim().to_string()).collect();
    }
    DEFAULT_TRANSPORTS
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

pub(crate) fn all_transports() -> Vec<String> {
    if let Ok(s) = std::env::var("OMQ_BENCH_TRANSPORTS") {
        return s.split(',').map(|t| t.trim().to_string()).collect();
    }
    DEFAULT_TRANSPORTS
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

pub(crate) fn peers_override() -> Option<Vec<usize>> {
    std::env::var("OMQ_BENCH_PEERS")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
}

pub(crate) fn options(_msg_size: usize) -> Options {
    let mut o = Options::default();
    if let Ok(val) = std::env::var("OMQ_BENCH_ARENA_THRESHOLD") {
        o = o.arena_threshold(val.parse().expect("OMQ_BENCH_ARENA_THRESHOLD"));
    }
    o
}

/// Build a fresh endpoint for cell `seq` on `transport`. For TCP we
/// pre-pick a free port (then drop the placeholder listener) so the
/// bench's bind doesn't have to deal with port-zero discovery.
pub(crate) fn endpoint(transport: &str, seq: usize) -> Endpoint {
    match transport {
        "inproc" => Endpoint::Inproc {
            name: format!("bench-{seq}"),
        },
        #[cfg(unix)]
        "ipc" => Endpoint::Ipc(IpcPath::Abstract(format!(
            "omq-bench-{}-{seq}",
            std::process::id()
        ))),
        "tcp" | "lz4+tcp" | "ws" => {
            let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("bench: failed to allocate a tcp port");
            let port = l.local_addr().unwrap().port();
            drop(l);
            let host = omq_tokio::endpoint::Host::Ip(Ipv4Addr::LOCALHOST.into());
            match transport {
                "tcp" => Endpoint::Tcp { host, port },
                #[cfg(feature = "lz4")]
                "lz4+tcp" => Endpoint::Lz4Tcp { host, port },
                #[cfg(feature = "ws")]
                "ws" => Endpoint::Ws {
                    host,
                    port,
                    path: "/".into(),
                },
                _ => panic!(
                    "bench: transport '{transport}' requires its feature; \
                            rebuild with the required feature flag"
                ),
            }
        }
        other => panic!("bench: unknown transport {other}"),
    }
}

/// Wait until each socket reports at least one connected peer. Polls
/// because the connect handshake is async-driven; ZMTP READY arrives
/// some millis after the TCP/IPC accept.
pub(crate) async fn wait_connected(socks: &[&omq_tokio::Socket]) {
    // Long enough to absorb cumulative pressure from earlier cells in a
    // multi-cell run (TIME_WAIT sockets, kernel scheduling jitter). On a
    // healthy first cell the loop returns within ~10 ms.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut pending = 0usize;
        for s in socks {
            let conns = s.connections().await.unwrap_or_default();
            // Connection is ZMTP-Ready once `peer_info` is populated.
            if !conns.iter().any(|c| c.peer_info.is_some()) {
                pending += 1;
            }
        }
        if pending == 0 {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "bench: {pending}/{} peer(s) never reached peer_info=Some \
             within 30s",
            socks.len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Subscriptions take a moment to propagate from SUB → PUB. Drive a
/// few empty-prefix probes through the publisher; once each subscriber
/// has received one, the routing table is in place.
pub(crate) async fn wait_subscribed(pub_: &omq_tokio::Socket, subs: &[&omq_tokio::Socket]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut pending: Vec<usize> = (0..subs.len()).collect();
    while !pending.is_empty() {
        assert!(
            Instant::now() <= deadline,
            "bench: subscriptions never propagated"
        );
        // Probe.
        let _ = pub_.send(omq_tokio::Message::single("")).await;
        let mut still: Vec<usize> = Vec::new();
        for &i in &pending {
            match tokio::time::timeout(Duration::from_millis(20), subs[i].recv()).await {
                Ok(Ok(_)) => {} // got it; drop from pending
                _ => still.push(i),
            }
        }
        pending = still;
    }
}

/// Run prime + calibration + `rounds()` timed bursts of `burst(n)`.
/// Returns the min-duration round (= peak throughput, the run least
/// perturbed by scheduler/IRQ jitter). `align` rounds n to a multiple
/// of the sender count so per-sender splits stay even.
pub(crate) async fn measure_min_of<F, Fut>(msg_size: usize, align: usize, burst: F) -> Cell
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let prime_start = Instant::now();
    let mut primed = 0usize;
    while primed < PRIME_ITERS && prime_start.elapsed() < PRIME_BUDGET {
        let chunk = (PRIME_ITERS - primed).min(align.max(1).max(10));
        burst(chunk).await;
        primed += chunk;
    }

    let round_dur = round_duration();
    let n_rounds = rounds();
    let mut n = align.max(1).max(10);
    let final_n = loop {
        let t = Instant::now();
        burst(n).await;
        let elapsed = t.elapsed();
        if elapsed >= WARMUP_DURATION {
            let rate = n as f64 / elapsed.as_secs_f64();
            let target = (rate * round_dur.as_secs_f64()) as usize;
            let aligned = (target / align.max(1)) * align.max(1);
            break aligned.max(align.max(1));
        }
        n = n.saturating_mul(4);
    };

    let mut rounds_data = Vec::with_capacity(n_rounds);
    for _ in 0..n_rounds {
        let cpu0 = process_cpu_time();
        let t = Instant::now();
        burst(final_n).await;
        let wall = t.elapsed();
        let cpu = process_cpu_time().saturating_sub(cpu0);
        rounds_data.push((wall, cpu));
    }
    let &(elapsed, cpu_time) = rounds_data
        .iter()
        .min_by_key(|(w, _)| *w)
        .expect("at least one round");
    let mbps = (final_n * msg_size) as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    let msgs_s = final_n as f64 / elapsed.as_secs_f64();
    Cell {
        n: final_n,
        elapsed,
        mbps,
        msgs_s,
        cpu_time,
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Cell {
    pub n: usize,
    pub elapsed: Duration,
    pub mbps: f64,
    pub msgs_s: f64,
    pub cpu_time: Duration,
}

pub(crate) fn process_cpu_time() -> Duration {
    #[cfg(unix)]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, std::ptr::from_mut(&mut ts)) };
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }
    #[cfg(not(unix))]
    {
        Duration::ZERO
    }
}

pub(crate) fn print_header(label: &str) {
    #[cfg(unix)]
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_string();
    #[cfg(not(unix))]
    let kernel = std::env::var("OS").unwrap_or_else(|_| "unknown".into());
    println!(
        "{label} | omq-tokio {} | {} | {kernel}",
        env!("CARGO_PKG_VERSION"),
        rustc_version_runtime(),
    );
    println!();
}

pub(crate) fn print_subheader(transport: &str, peers: usize) {
    let s = if peers > 1 { "s" } else { "" };
    println!("--- {transport} ({peers} peer{s}) ---");
}

pub(crate) fn print_cell(msg_size: usize, c: Cell) {
    let cpu_pct = if c.elapsed.as_nanos() > 0 {
        c.cpu_time.as_secs_f64() / c.elapsed.as_secs_f64() * 100.0
    } else {
        0.0
    };
    println!(
        "  {:>6}  {:>8.1} MB/s  {:>8.0} msg/s  ({:.2}s, cpu {:.0}%, n={})",
        format!("{msg_size}B"),
        c.mbps,
        c.msgs_s,
        c.elapsed.as_secs_f64(),
        cpu_pct,
        c.n,
    );
}

pub(crate) fn append_jsonl(pattern: &str, transport: &str, peers: usize, msg_size: usize, c: Cell) {
    if std::env::var_os("OMQ_BENCH_NO_WRITE").is_some() {
        return;
    }
    let path = results_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let row = format!(
        r#"{{"run_id":"{run}","pattern":"{pattern}","transport":"{transport}","peers":{peers},"msg_size":{msg_size},"msg_count":{n},"elapsed":{el},"cpu_time":{cpu},"mbps":{mbps},"msgs_s":{msgs_s}}}"#,
        run = run_id(),
        pattern = pattern,
        transport = transport,
        peers = peers,
        msg_size = msg_size,
        n = c.n,
        el = c.elapsed.as_secs_f64(),
        cpu = c.cpu_time.as_secs_f64(),
        mbps = c.mbps,
        msgs_s = c.msgs_s,
    );
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{row}");
    }
}

pub(crate) fn payload(target_bytes: usize) -> Bytes {
    const TEMPLATE: &str = r#"{"ts":"2026-04-27T12:34:56.{ID}Z","level":"INFO","service":"api-gateway","trace_id":"{ID}","span_id":"{ID}","user_id":"u-{ID}","method":"POST","path":"/v1/widgets/{ID}","status":200,"latency_ms":42,"region":"us-east-1","host":"api-{ID}.svc.cluster.local","msg":"request handled successfully"}
"#;
    let mut out = String::with_capacity(target_bytes + TEMPLATE.len());
    let mut counter: u32 = 0;
    while out.len() < target_bytes {
        let mut rec = TEMPLATE.to_string();
        let id = format!("{:08x}", counter.wrapping_mul(0x9E37_79B1));
        rec = rec.replace("{ID}", &id);
        out.push_str(&rec);
        counter = counter.wrapping_add(1);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn rustc_version_runtime() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "rustc ?".into())
        .trim()
        .to_string()
}

pub(crate) fn build_context() -> omq_tokio::Context {
    let config = omq_tokio::ContextConfig::from_env();
    let threads = config.io_threads;
    if threads <= 1 {
        println!("runtime: current_thread\n");
    } else {
        println!("runtime: {threads} x current_thread\n");
    }
    omq_tokio::Context::with_config(config)
}

/// Thin wrapper around `tokio::time::timeout` to enforce the per-cell
/// hard ceiling. Panics on timeout with a recognisable message.
pub(crate) async fn with_timeout<T>(label: &str, fut: impl std::future::Future<Output = T>) -> T {
    let to = run_timeout();
    let result = tokio::time::timeout(to, fut)
        .await
        .unwrap_or_else(|_| panic!("BENCH TIMEOUT: {label} exceeded {to:?}"));
    tokio::task::yield_now().await;
    result
}
