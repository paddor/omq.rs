#![allow(dead_code, unreachable_pub)]

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::Endpoint;
use omq_tokio::endpoint::Host;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ---------------------------------------------------------------------------
// Tracking allocator
// ---------------------------------------------------------------------------

pub mod alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    pub static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

    pub struct TrackingAllocator;

    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                LIVE_BYTES.fetch_add(layout.size(), Relaxed);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            LIVE_BYTES.fetch_sub(layout.size(), Relaxed);
            unsafe { System.dealloc(ptr, layout) };
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let old_size = layout.size();
            let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
            if !new_ptr.is_null() {
                if new_size >= old_size {
                    LIVE_BYTES.fetch_add(new_size - old_size, Relaxed);
                } else {
                    LIVE_BYTES.fetch_sub(old_size - new_size, Relaxed);
                }
            }
            new_ptr
        }
    }
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

pub fn soak_options() -> omq_tokio::Options {
    omq_tokio::Options::default().heartbeat_interval(Duration::from_secs(10))
}

pub fn soak_duration() -> Duration {
    let secs: u64 = std::env::var("OMQ_SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    Duration::from_secs(secs.max(5))
}

pub fn build_context() -> omq_tokio::Context {
    omq_tokio::Context::with_config(omq_tokio::ContextConfig::from_env())
}

pub fn seeded_rng(label: &str) -> StdRng {
    let seed: u64 = std::env::var("OMQ_SOAK_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            let mut s = [0u8; 8];
            rand::rng().fill_bytes(&mut s);
            u64::from_le_bytes(s)
        });
    eprintln!("[{label}] OMQ_SOAK_SEED={seed}");
    StdRng::seed_from_u64(seed)
}

// ---------------------------------------------------------------------------
// Endpoint helpers
// ---------------------------------------------------------------------------

pub fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

pub fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

// ---------------------------------------------------------------------------
// Resource monitor (background std::thread)
// ---------------------------------------------------------------------------

type Samples = (
    Vec<(Instant, usize)>,
    Vec<(Instant, usize)>,
    Vec<(Instant, usize)>,
);

const LIVE_GROWTH_WARMUP: Duration = Duration::from_secs(30);
const LIVE_GROWTH_WINDOW: Duration = Duration::from_mins(2);
const LIVE_GROWTH_MIN_SAMPLES: usize = 30;
const LIVE_HEAP_SLOPE_LIMIT_KIB_S: f64 = 512.0;
const LIVE_HEAP_GROWTH_MIN_BYTES: usize = 64 * 1024 * 1024;
const LIVE_RSS_SLOPE_LIMIT_KIB_S: f64 = 1024.0;
const LIVE_RSS_GROWTH_MIN_BYTES: usize = 128 * 1024 * 1024;

pub struct ResourceMonitor {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Samples>>,
    heap_baseline: usize,
}

impl ResourceMonitor {
    pub fn start() -> Self {
        let heap_baseline = alloc::LIVE_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let label = std::thread::current().name().unwrap_or("soak").to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::spawn(move || {
            let started = Instant::now();
            let mut rss_samples = Vec::new();
            let mut fd_samples = Vec::new();
            let mut heap_samples = Vec::new();
            while !stop2.load(Ordering::Relaxed) {
                let now = Instant::now();
                if let Some(rss) = read_rss_bytes() {
                    rss_samples.push((now, rss));
                    assert_live_growth_stable(
                        &label,
                        "RSS",
                        started,
                        &rss_samples,
                        LIVE_RSS_SLOPE_LIMIT_KIB_S,
                        LIVE_RSS_GROWTH_MIN_BYTES,
                    );
                }
                if let Some(fds) = read_fd_count() {
                    fd_samples.push((now, fds));
                }
                heap_samples.push((now, alloc::LIVE_BYTES.load(Ordering::Relaxed)));
                assert_live_growth_stable(
                    &label,
                    "heap",
                    started,
                    &heap_samples,
                    LIVE_HEAP_SLOPE_LIMIT_KIB_S,
                    LIVE_HEAP_GROWTH_MIN_BYTES,
                );
                std::thread::sleep(Duration::from_secs(1));
            }
            (rss_samples, fd_samples, heap_samples)
        });
        Self {
            stop,
            handle: Some(handle),
            heap_baseline,
        }
    }

    pub fn stop(mut self) -> ResourceReport {
        self.stop.store(true, Ordering::Relaxed);
        let (rss_samples, fd_samples, heap_samples) = self.handle.take().unwrap().join().unwrap();
        ResourceReport {
            rss_samples,
            fd_samples,
            heap_samples,
            heap_baseline: self.heap_baseline,
        }
    }
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn read_rss_bytes() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(rss_pages * 4096)
}

fn read_fd_count() -> Option<usize> {
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
}

fn assert_live_growth_stable(
    label: &str,
    metric: &str,
    started: Instant,
    samples: &[(Instant, usize)],
    slope_limit_kib_s: f64,
    min_growth_bytes: usize,
) {
    let Some((now, current)) = samples.last().copied() else {
        return;
    };
    if now.duration_since(started) < LIVE_GROWTH_WARMUP + LIVE_GROWTH_WINDOW {
        return;
    }

    let Some(window_start) = now.checked_sub(LIVE_GROWTH_WINDOW) else {
        return;
    };
    let first = samples
        .iter()
        .position(|(t, _)| *t >= window_start)
        .unwrap_or(0);
    let window = &samples[first..];
    if window.len() < LIVE_GROWTH_MIN_SAMPLES {
        return;
    }

    let window_growth = current.saturating_sub(window.first().unwrap().1);
    if window_growth < min_growth_bytes {
        return;
    }

    let Some(slope_bytes_per_sec) = slope_bytes_per_sec(window) else {
        return;
    };
    let slope_kib_s = slope_bytes_per_sec / 1024.0;
    if slope_kib_s <= slope_limit_kib_s {
        return;
    }

    eprintln!(
        "[{label}] live {metric} growth detected: slope {slope_kib_s:.1} KiB/s over {:.0}s, \
         growth {:.1} MiB, current {:.1} MiB (limit {slope_limit_kib_s:.1} KiB/s and {:.1} MiB); \
         failing before OOM",
        LIVE_GROWTH_WINDOW.as_secs_f64(),
        window_growth as f64 / 1_048_576.0,
        current as f64 / 1_048_576.0,
        min_growth_bytes as f64 / 1_048_576.0,
    );
    std::process::exit(101);
}

fn slope_bytes_per_sec(samples: &[(Instant, usize)]) -> Option<f64> {
    let first_t = samples.first()?.0;
    let elapsed_secs = samples.last()?.0.duration_since(first_t).as_secs_f64();
    if elapsed_secs < 1.0 {
        return None;
    }

    let n_f = samples.len() as f64;
    let sum_x: f64 = samples
        .iter()
        .map(|(t, _)| t.duration_since(first_t).as_secs_f64())
        .sum();
    let sum_y: f64 = samples.iter().map(|(_, v)| *v as f64).sum();
    let dot_xy: f64 = samples
        .iter()
        .map(|(t, v)| t.duration_since(first_t).as_secs_f64() * *v as f64)
        .sum();
    let sum_xx: f64 = samples
        .iter()
        .map(|(t, _)| {
            let x = t.duration_since(first_t).as_secs_f64();
            x * x
        })
        .sum();

    let denom = n_f * sum_xx - sum_x * sum_x;
    if denom == 0.0 {
        None
    } else {
        Some((n_f * dot_xy - sum_x * sum_y) / denom)
    }
}

// ---------------------------------------------------------------------------
// Resource report
// ---------------------------------------------------------------------------

pub struct ResourceReport {
    pub rss_samples: Vec<(Instant, usize)>,
    pub fd_samples: Vec<(Instant, usize)>,
    pub heap_samples: Vec<(Instant, usize)>,
    heap_baseline: usize,
}

impl ResourceReport {
    pub fn assert_no_leak(&self, label: &str) {
        self.report_rss(label);
        self.report_heap_slope(label);
        self.assert_heap_residual(label);
        self.assert_fd_stable(label);
    }

    fn report_rss(&self, label: &str) {
        let n = self.rss_samples.len();
        if n < 10 {
            return;
        }
        let peak_rss = self.rss_samples.iter().map(|(_, v)| *v).max().unwrap_or(0);
        eprintln!(
            "[{label}] RSS peak {:.1} MiB (informational, not gated)",
            peak_rss as f64 / 1_048_576.0,
        );
    }

    fn report_heap_slope(&self, label: &str) {
        let n = self.heap_samples.len();
        if n < 10 {
            return;
        }

        let warmup = n / 5;
        let post_warmup = &self.heap_samples[warmup..];
        if post_warmup.len() < 2 {
            return;
        }

        let Some(slope_bytes_per_sec) = slope_bytes_per_sec(post_warmup) else {
            return;
        };
        let sum_y: f64 = post_warmup.iter().map(|(_, v)| *v as f64).sum();

        let slope_kib_s = slope_bytes_per_sec / 1024.0;
        let avg: f64 = sum_y / post_warmup.len() as f64;

        eprintln!(
            "[{label}] heap: avg {:.1} MiB, slope {slope_kib_s:.1} KiB/s (informational)",
            avg / 1_048_576.0,
        );
    }

    fn assert_heap_residual(&self, label: &str) {
        std::thread::sleep(Duration::from_millis(200));
        let current = alloc::LIVE_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let baseline = self.heap_baseline;

        // Use the peak seen during the run to scale the threshold:
        // a true leak accumulates relative to throughput, so residual
        // after close should be a tiny fraction of the peak.
        let peak = self
            .heap_samples
            .iter()
            .map(|(_, v)| *v)
            .max()
            .unwrap_or(baseline);
        // 5% of peak or 4 MiB, whichever is larger. Covers runtime
        // overhead that persists until the runtime itself is dropped.
        let threshold = (peak / 20).max(8 * 1024 * 1024);

        let growth = current.saturating_sub(baseline);

        eprintln!(
            "[{label}] heap residual: {:.1} KiB (baseline {:.1} MiB, current {:.1} MiB)",
            growth as f64 / 1024.0,
            baseline as f64 / 1_048_576.0,
            current as f64 / 1_048_576.0,
        );
        assert!(
            growth < threshold,
            "[{label}] heap leak: {:.1} KiB residual after close \
             (baseline {:.1} MiB, current {:.1} MiB, threshold {:.1} MiB)",
            growth as f64 / 1024.0,
            baseline as f64 / 1_048_576.0,
            current as f64 / 1_048_576.0,
            threshold as f64 / 1_048_576.0,
        );
    }

    fn assert_fd_stable(&self, label: &str) {
        let n = self.fd_samples.len();
        if n < 10 {
            eprintln!("[{label}] too few FD samples ({n}) to check for leaks");
            return;
        }

        let warmup = n / 5;
        let post_warmup = &self.fd_samples[warmup..];

        if post_warmup.len() < 2 {
            return;
        }

        let first_t = post_warmup.first().unwrap().0;
        let elapsed_secs = post_warmup
            .last()
            .unwrap()
            .0
            .duration_since(first_t)
            .as_secs_f64();
        if elapsed_secs < 1.0 {
            return;
        }

        let n_f = post_warmup.len() as f64;
        let sum_x: f64 = post_warmup
            .iter()
            .map(|(t, _)| t.duration_since(first_t).as_secs_f64())
            .sum();
        let sum_y: f64 = post_warmup.iter().map(|(_, v)| *v as f64).sum();
        let dot_xy: f64 = post_warmup
            .iter()
            .map(|(t, v)| t.duration_since(first_t).as_secs_f64() * *v as f64)
            .sum();
        let sum_xx: f64 = post_warmup
            .iter()
            .map(|(t, _)| {
                let x = t.duration_since(first_t).as_secs_f64();
                x * x
            })
            .sum();

        let slope = (n_f * dot_xy - sum_x * sum_y) / (n_f * sum_xx - sum_x * sum_x);

        let fd_min = post_warmup.iter().map(|(_, v)| *v).min().unwrap_or(0);
        let fd_max = post_warmup.iter().map(|(_, v)| *v).max().unwrap_or(0);

        eprintln!("[{label}] FDs: range {fd_min}..{fd_max}, slope {slope:.4} FDs/s");

        let threshold = if post_warmup.len() >= 120 { 0.05 } else { 1.0 };
        assert!(
            slope < threshold,
            "[{label}] FD leak detected: slope {slope:.4} FDs/s (range {fd_min}..{fd_max})"
        );
    }
}

// ---------------------------------------------------------------------------
// Throughput tracker
// ---------------------------------------------------------------------------

pub struct ThroughputTracker {
    interval: Duration,
    last_record: Instant,
    samples: Vec<(Instant, u64)>,
}

impl ThroughputTracker {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_record: Instant::now(),
            samples: Vec::new(),
        }
    }

    pub fn record(&mut self, cumulative_count: u64) {
        let now = Instant::now();
        if now.duration_since(self.last_record) >= self.interval {
            self.samples.push((now, cumulative_count));
            self.last_record = now;
        }
    }

    pub fn assert_stable(&self, label: &str) {
        if std::env::var_os("OMQ_SOAK_DISABLE_THROUGHPUT_STABILITY").is_some() {
            eprintln!("[{label}] throughput stability check disabled");
            return;
        }

        if self.samples.len() < 4 {
            eprintln!("[{label}] too few throughput samples to check stability");
            return;
        }

        let rates = self.windowed_rates();
        if rates.len() < 4 {
            return;
        }

        let warmup = rates.len() / 5;
        let post_warmup = &rates[warmup..];
        if post_warmup.len() < 2 {
            return;
        }

        let baseline_end = (post_warmup.len() / 5).max(1);
        let baseline: f64 = post_warmup[..baseline_end].iter().sum::<f64>() / baseline_end as f64;

        let tail_start = post_warmup.len() * 4 / 5;
        let tail = &post_warmup[tail_start..];
        let tail_avg: f64 = tail.iter().sum::<f64>() / tail.len() as f64;

        eprintln!(
            "[{label}] throughput: baseline {baseline:.0} msg/s, tail avg {tail_avg:.0} msg/s"
        );

        if baseline > 0.0 {
            assert!(
                tail_avg >= baseline * 0.5,
                "[{label}] throughput degraded: {tail_avg:.0} msg/s < 50% of baseline {baseline:.0} msg/s"
            );
        }
    }

    fn windowed_rates(&self) -> Vec<f64> {
        let mut rates = Vec::new();
        for i in 1..self.samples.len() {
            let dt = self.samples[i]
                .0
                .duration_since(self.samples[i - 1].0)
                .as_secs_f64();
            if dt > 0.0 {
                let dcount = self.samples[i].1.saturating_sub(self.samples[i - 1].1);
                rates.push(dcount as f64 / dt);
            }
        }
        rates
    }
}
