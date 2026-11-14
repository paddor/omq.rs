//! Setup shared by the tokio and blocking bench peers: argument parsing,
//! signals, environment options, start barriers, and CPU time.

use std::net::Ipv4Addr;
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Options};

/// Positional arguments of one mode: `<mode> <addr|name> <size> ...`.
pub(crate) struct ModeArgs<'a>(pub(crate) &'a [String]);

impl ModeArgs<'_> {
    /// The endpoint argument.
    pub(crate) fn ep(&self) -> Endpoint {
        parse_ep(&self.0[2])
    }

    /// The inproc name argument.
    pub(crate) fn name(&self) -> String {
        self.0[2].clone()
    }

    /// The message size argument.
    pub(crate) fn size(&self) -> usize {
        self.parse(3, "msg_size")
    }

    /// A duration in (fractional) seconds at `index`.
    pub(crate) fn secs(&self, index: usize) -> Duration {
        Duration::from_secs_f64(self.parse(index, "duration_secs"))
    }

    /// A required argument at `index`; panics with `what` if invalid.
    pub(crate) fn parse<T: FromStr>(&self, index: usize, what: &str) -> T
    where
        T::Err: std::fmt::Debug,
    {
        self.0[index].parse().expect(what)
    }

    /// An optional argument at `index`; missing or invalid gives `None`.
    pub(crate) fn optional<T: FromStr>(&self, index: usize) -> Option<T> {
        self.0.get(index).and_then(|s| s.parse().ok())
    }
}

/// A port (`4000`), an `ip:port` pair, or a full endpoint URI.
fn parse_ep(s: &str) -> Endpoint {
    if let Ok(port) = s.parse::<u16>() {
        return Endpoint::Tcp {
            host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
            port,
        };
    }

    if let Some((ip, port_str)) = s.split_once(':')
        && let Ok(addr) = ip.parse::<Ipv4Addr>()
        && let Ok(port) = port_str.parse::<u16>()
    {
        return Endpoint::Tcp {
            host: Host::Ip(addr.into()),
            port,
        };
    }

    s.parse()
        .expect("valid endpoint (port, ip:port, or full URI)")
}

#[cfg(unix)]
extern "C" fn exit_on_signal(_sig: libc::c_int) {
    // SAFETY: _exit is async-signal-safe and takes no pointers.
    unsafe { libc::_exit(0) };
}

/// Exit cleanly on SIGTERM/SIGINT, and after 60 s as a hang guard.
pub(crate) fn install_signals() {
    #[cfg(unix)]
    // SAFETY: the handler only calls the async-signal-safe _exit.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGALRM,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::alarm(60);
    }
}

/// Applies the TLS credentials, large-message socket buffers, and the
/// compression and arena knobs both peers read from the environment.
pub(crate) fn env_options(mut o: Options, msg_size: usize) -> Options {
    #[cfg(any(feature = "ws", feature = "quic"))]
    crate::ws_bench_config::configure(&mut o);

    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.recv_buffer_size(buf).send_buffer_size(buf);
    }

    if let Ok(path) = std::env::var("OMQ_BENCH_DICT_FILE") {
        let dict = Bytes::from(std::fs::read(&path).expect("read dict file"));
        o = o.compression_dict(dict);
    }

    if let Ok(val) = std::env::var("OMQ_BENCH_ZSTD_LEVEL") {
        o = o.compression_level(val.parse().expect("OMQ_BENCH_ZSTD_LEVEL"));
    }

    if let Ok(val) = std::env::var("OMQ_BENCH_ZSTD_LEVEL_RANGE") {
        let (min, max) = val
            .split_once(',')
            .and_then(|(min, max)| Some((min.parse().ok()?, max.parse().ok()?)))
            .expect("OMQ_BENCH_ZSTD_LEVEL_RANGE");
        o = o.compression_level_range(min, max);
    }

    if let Ok(val) = std::env::var("OMQ_BENCH_ARENA_THRESHOLD") {
        o = o.arena_threshold(val.parse().expect("OMQ_BENCH_ARENA_THRESHOLD"));
    }

    o
}

/// `OMQ_BENCH_MECHANISM`, lowercased.
pub(crate) fn mechanism_env() -> Option<String> {
    std::env::var("OMQ_BENCH_MECHANISM")
        .ok()
        .map(|s| s.to_ascii_lowercase())
}

/// CURVE server side with the fixed bench server keypair.
#[cfg(feature = "curve")]
pub(crate) fn curve_server(o: Options) -> Options {
    o.curve_server(curve_keypair(0x01))
}

/// CURVE client side with the fixed bench client keypair.
#[cfg(feature = "curve")]
pub(crate) fn curve_client(o: Options) -> Options {
    o.curve_client(curve_keypair(0x02), curve_keypair(0x01).public)
}

#[cfg(feature = "curve")]
fn curve_keypair(byte: u8) -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([byte; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

/// Nearest-rank quantile of an ascending slice.
pub(crate) fn quantile(sorted: &[f64], probability: f64) -> f64 {
    let index = ((sorted.len().saturating_sub(1)) as f64 * probability).round() as usize;
    sorted[index.min(sorted.len().saturating_sub(1))]
}

/// Per-socket receive counts over one measured window.
pub(crate) struct RecvWindow {
    pub(crate) per_socket: Vec<u64>,
    pub(crate) elapsed: f64,
    pub(crate) cpu: f64,
}

impl RecvWindow {
    /// Print totals, per-socket rate spread and quantiles, then every rate.
    pub(crate) fn print_multi_pull(&self, size: usize) {
        let Self { elapsed, cpu, .. } = *self;
        let socket_count = self.per_socket.len();
        let (total, per_min_rate, per_max_rate) = self.spread();
        let mut rates: Vec<f64> = self
            .per_socket
            .iter()
            .map(|&count| count as f64 / elapsed)
            .collect();
        rates.sort_unstable_by(f64::total_cmp);

        println!(
            "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} \
             {per_max_rate:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
            quantile(&rates, 0.10),
            quantile(&rates, 0.25),
            quantile(&rates, 0.50),
            quantile(&rates, 0.75),
            quantile(&rates, 0.90),
        );

        print!(" ");
        for rate in &rates {
            print!("{rate:.1} ");
        }

        println!();
        self.eprint_summary("multi-pull");
    }

    /// Print totals and the per-socket rate spread.
    pub(crate) fn print_multi_sub(&self, size: usize) {
        let Self { elapsed, cpu, .. } = *self;
        let socket_count = self.per_socket.len();
        let (total, per_min_rate, per_max_rate) = self.spread();
        println!(
            "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}"
        );
        self.eprint_summary("multi-sub");
    }

    /// Total count and the slowest and fastest per-socket rates.
    fn spread(&self) -> (u64, f64, f64) {
        let total: u64 = self.per_socket.iter().sum();
        let per_min = self.per_socket.iter().copied().min().unwrap_or(0);
        let per_max = self.per_socket.iter().copied().max().unwrap_or(0);

        (
            total,
            per_min as f64 / self.elapsed,
            per_max as f64 / self.elapsed,
        )
    }

    fn eprint_summary(&self, mode: &str) {
        let socket_count = self.per_socket.len();
        let (total, per_min_rate, per_max_rate) = self.spread();
        eprintln!(
            "{mode}: {socket_count} sockets, {:.0} msg/s total, \
             per-socket [{per_min_rate:.0}, {per_max_rate:.0}] msg/s",
            total as f64 / self.elapsed,
        );
    }
}

/// `OMQ_BENCH_WARMUP_MS`, zero when unset.
pub(crate) fn warmup_duration() -> Duration {
    std::env::var("OMQ_BENCH_WARMUP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::ZERO, Duration::from_millis)
}

/// Time left until the start barrier at `OMQ_BENCH_START_AT`.
pub(crate) fn start_delay() -> Option<Duration> {
    delay_until_start(Duration::ZERO)
}

/// Time left until the warmup barrier, `OMQ_BENCH_WARMUP_MS` before start.
pub(crate) fn warmup_delay() -> Option<Duration> {
    delay_until_start(warmup_duration())
}

/// Time left until `lead` before `OMQ_BENCH_START_AT` (Unix seconds).
/// `None` when unset or already past.
fn delay_until_start(lead: Duration) -> Option<Duration> {
    let start_at = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())?;
    let at = start_at - lead.as_secs_f64();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());

    (at > now).then(|| Duration::from_secs_f64(at - now))
}

#[cfg(unix)]
pub(crate) fn timeval_secs(tv: &libc::timeval) -> f64 {
    let sec = u64::try_from(tv.tv_sec).unwrap_or(0);
    let usec = u64::try_from(tv.tv_usec).unwrap_or(0);
    (Duration::from_secs(sec) + Duration::from_micros(usec)).as_secs_f64()
}

/// User plus system CPU seconds of this process.
#[cfg(unix)]
pub(crate) fn cpu_time_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage fills the writable rusage for RUSAGE_SELF.
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = timeval_secs(&usage.ru_utime);
        let sys = timeval_secs(&usage.ru_stime);

        user + sys
    }
}

#[cfg(windows)]
pub(crate) fn cpu_time_secs() -> f64 {
    // Windows doesn't have rusage; return 0.0 as a placeholder.
    0.0
}
