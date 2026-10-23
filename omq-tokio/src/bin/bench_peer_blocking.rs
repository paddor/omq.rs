//! Blocking bench peer: background IO thread, sync application code.
//!
//! Same interface as `bench_peer_tokio` but uses `blocking::Socket`.
//! The application thread never touches tokio. The Context spawns its
//! own IO thread; send/recv block the calling thread via
//! `Context::block_on`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, SocketType, TrySendError, blocking};
use std::net::Ipv4Addr;

mod blocking_inproc;
mod latency_common;
mod payload_corpus;
mod pushpull_common;
#[cfg(any(feature = "ws", feature = "quic"))]
mod ws_bench_config;

#[path = "bench_peer_blocking/fanin.rs"]
mod fanin;
#[path = "bench_peer_blocking/inproc.rs"]
mod inproc;
#[path = "bench_peer_blocking/payload.rs"]
mod payload;
#[path = "bench_peer_blocking/pubsub.rs"]
mod pubsub;

use fanin::{run_multi_pull, run_multi_push};
use inproc::{run_inproc, run_inproc_2ut, run_inproc_latency};
use payload::bench_payload;
use pubsub::{run_multi_sub, run_pub};

fn multi_pull_drain_batch(size: usize) -> usize {
    let batch = std::env::var("OMQ_BENCH_DRAIN_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(if size <= 1024 { 64 } else { 256 });
    batch.clamp(1, 256).min((64 * 1024 / size.max(1)).max(1))
}

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

static COORD_SOCK: std::sync::OnceLock<blocking::Socket> = std::sync::OnceLock::new();

fn report_bound_port(ctx: &omq_tokio::Context, ep: &Endpoint) {
    let port = match ep {
        Endpoint::Tcp { port, .. } => *port,
        #[cfg(feature = "quic")]
        Endpoint::Quic { port, .. } => *port,
        _ => return,
    };
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        return;
    };
    let coord = COORD_SOCK.get_or_init(|| {
        let s = ctx.blocking_socket(SocketType::Push, Options::default());
        s.connect(coord_ep).unwrap();
        s.wait_connected(1, Duration::from_secs(5)).unwrap();
        s
    });
    let msg = format!("READY {port}");
    coord.send(Message::from_slice(msg.as_bytes())).unwrap();
}

#[cfg(unix)]
extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

fn install_signals() {
    #[cfg(unix)]
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    #[cfg(any(feature = "ws", feature = "quic"))]
    ws_bench_config::set_endpoint(args.get(2).map(String::as_str));
    let ctx = omq_tokio::Context::with_config(omq_tokio::ContextConfig::from_env());
    configure_egress_limit(&ctx);
    let n = ctx.io_threads();
    eprintln!("runtime: blocking, {n} IO thread(s)");
    install_signals();

    let mode = args.get(1).map(String::as_str);
    let handled = run_pipeline_mode(&ctx, mode, &args)
        || run_pattern_mode(&ctx, mode, &args)
        || run_inproc_mode(&ctx, mode, &args);
    if !handled {
        print_usage();
        std::process::exit(1);
    }
}

/// `OMQ_BENCH_EGRESS_BYTE_LIMIT=BYTES_PER_SECOND,BURST_BYTES` caps context egress.
fn configure_egress_limit(ctx: &omq_tokio::Context) {
    let Ok(value) = std::env::var("OMQ_BENCH_EGRESS_BYTE_LIMIT") else {
        return;
    };
    let (rate, burst) = value
        .split_once(',')
        .expect("OMQ_BENCH_EGRESS_BYTE_LIMIT must be BYTES_PER_SECOND,BURST_BYTES");
    ctx.set_network_rate_limits(omq_tokio::NetworkRateLimits {
        ingress: None,
        egress: Some(omq_tokio::ByteRateLimit::new(
            rate.parse().expect("egress bytes per second"),
            burst.parse().expect("egress burst bytes"),
        )),
    })
    .expect("configure context byte limit");
}

/// PUSH/PULL modes over a network endpoint. Returns false for other modes.
fn run_pipeline_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_push(ctx, ep, size, peers);
        }
        Some("pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(ctx, ep, size, Duration::from_secs_f64(duration));
        }
        Some("pull-bind") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(ctx, ep, size, Duration::from_secs_f64(duration));
        }
        Some("multi-pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_pull(ctx, ep, size, Duration::from_secs_f64(duration), count);
        }
        Some("multi-push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let count: usize = args[4].parse().expect("socket_count");
            let duration = args.get(5).and_then(|s| s.parse().ok());
            run_multi_push(ctx, ep, size, count, duration);
        }
        _ => return false,
    }
    true
}

/// PUB/SUB and REQ/REP modes over a network endpoint. Returns false for
/// other modes.
fn run_pattern_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("pub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_pub(ctx, ep, size, peers);
        }
        Some("multi-sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_sub(ctx, ep, size, Duration::from_secs_f64(duration), count);
        }
        Some("rep") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep(ctx, ep, size);
        }
        Some("req") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req(ctx, ep, size, iterations, warmup);
        }
        _ => return false,
    }
    true
}

/// In-process modes. Returns false for other modes.
fn run_inproc_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("inproc") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc(ctx, name, size, Duration::from_secs_f64(duration));
        }
        Some("inproc-2ut") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc_2ut(ctx, name, size, Duration::from_secs_f64(duration));
        }
        Some("inproc-2ut-blocking") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let hwm = args
                .get(5)
                .cloned()
                .or_else(|| std::env::var("OMQ_BENCH_HWM").ok());
            let hwm = hwm.map_or(1000, |value| value.parse().expect("hwm"));
            blocking_inproc::run(ctx, name, size, Duration::from_secs_f64(duration), hwm);
        }
        Some("inproc-latency") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(ctx, name, size, iterations, warmup);
        }
        _ => return false,
    }
    true
}

fn print_usage() {
    eprintln!("usage: bench_peer_blocking push <addr> <size> [peers]");
    eprintln!("       bench_peer_blocking pull <addr> <size> <duration_secs>");
    eprintln!("       bench_peer_blocking pull-bind <addr> <size> <duration_secs>");
    eprintln!("       bench_peer_blocking multi-pull <addr> <size> <duration_secs> <count>");
    eprintln!("       bench_peer_blocking multi-push <addr> <size> <count>");
    eprintln!("       bench_peer_blocking pub <addr> <size>");
    eprintln!("       bench_peer_blocking multi-sub <addr> <size> <duration_secs> <count>");
    eprintln!("       bench_peer_blocking rep <addr> <size>");
    eprintln!("       bench_peer_blocking req <addr> <size> <iterations> <warmup>");
    eprintln!("       bench_peer_blocking inproc <name> <size> <duration_secs>");
    eprintln!("       bench_peer_blocking inproc-2ut <name> <size> <duration_secs>");
    eprintln!("       bench_peer_blocking inproc-2ut-blocking <name> <size> <duration_secs> [hwm]");
    eprintln!("       bench_peer_blocking inproc-latency <name> <size> <iterations> <warmup>");
}

// --- Options -----------------------------------------------------------------

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default().recv_spin(bench_recv_spin());
    #[cfg(any(feature = "ws", feature = "quic"))]
    ws_bench_config::configure(&mut o);
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
    if let Ok(val) = std::env::var("OMQ_BENCH_OFFLOAD_THRESHOLD") {
        let threshold = (val != "off").then(|| val.parse().expect("OMQ_BENCH_OFFLOAD_THRESHOLD"));
        o = o.compression_offload_threshold(threshold);
    }
    if let Ok(val) = std::env::var("OMQ_BENCH_ARENA_THRESHOLD") {
        o = o.arena_threshold(val.parse().expect("OMQ_BENCH_ARENA_THRESHOLD"));
    }
    if let Ok(val) = std::env::var("OMQ_BENCH_RECV_RATE_LIMIT") {
        let (rate, burst) = parse_rate_limit(&val, "OMQ_BENCH_RECV_RATE_LIMIT");
        o = o.recv_rate_limit(rate, burst);
    }
    if let Ok(val) = std::env::var("OMQ_BENCH_RECV_IP_RATE_LIMIT") {
        let (rate, burst) = parse_rate_limit(&val, "OMQ_BENCH_RECV_IP_RATE_LIMIT");
        o = o.recv_ip_rate_limit(rate, burst);
    }
    o
}

fn bench_recv_spin() -> Duration {
    Duration::from_micros(
        std::env::var("OMQ_BENCH_RECV_SPIN_US")
            .map_or(0, |value| value.parse().expect("OMQ_BENCH_RECV_SPIN_US")),
    )
}

fn parse_rate_limit(value: &str, name: &str) -> (u32, u32) {
    let (rate, burst) = value
        .split_once(',')
        .unwrap_or_else(|| panic!("{name} must be RATE,BURST"));
    (
        rate.parse()
            .unwrap_or_else(|_| panic!("invalid {name} rate")),
        burst
            .parse()
            .unwrap_or_else(|_| panic!("invalid {name} burst")),
    )
}

fn mechanism_env() -> Option<String> {
    std::env::var("OMQ_BENCH_MECHANISM")
        .ok()
        .map(|s| s.to_ascii_lowercase())
}

fn bench_options_server(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "curve")]
        Some("curve") => o.curve_server(bench_curve_server_keypair()),
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn bench_options_client(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "curve")]
        Some("curve") => {
            let client_kp = bench_curve_client_keypair();
            let server_pub = bench_curve_server_keypair().public;
            o.curve_client(client_kp, server_pub)
        }
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

#[cfg(feature = "curve")]
fn bench_curve_server_keypair() -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([0x01; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

#[cfg(feature = "curve")]
fn bench_curve_client_keypair() -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([0x02; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

// --- Helpers -----------------------------------------------------------------

fn send_fast(sock: &blocking::Socket, msg: Message) {
    match sock.try_send(msg) {
        Ok(()) => {}
        Err(TrySendError::Full(msg)) => sock.send(msg).unwrap(),
        Err(e) => panic!("try_send failed: {e}"),
    }
}

fn quantile(sorted: &[f64], probability: f64) -> f64 {
    let index = ((sorted.len().saturating_sub(1)) as f64 * probability).round() as usize;
    sorted[index.min(sorted.len().saturating_sub(1))]
}

fn wait_for_start_barrier() {
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let now = wall_secs();
    if start_at > now {
        std::thread::sleep(Duration::from_secs_f64(start_at - now));
    }
}

fn wait_for_warmup_barrier() {
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let warmup_at = start_at - warmup_duration().as_secs_f64();
    let now = wall_secs();
    if warmup_at > now {
        std::thread::sleep(Duration::from_secs_f64(warmup_at - now));
    }
}

fn wall_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

fn warmup_duration() -> Duration {
    std::env::var("OMQ_BENCH_WARMUP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::ZERO, Duration::from_millis)
}

fn recv_loop(sock: &blocking::Socket, duration: Duration, size: usize) -> (u64, f64) {
    std::thread::sleep(Duration::from_millis(500));
    wait_for_start_barrier();
    drain_warmup(sock);

    let batch = pushpull_common::receive_batch(size);
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        let mut received = 0;
        for _ in 0..batch {
            match sock.try_recv() {
                Ok(_) => received += 1,
                Err(omq_tokio::Error::WouldBlock) => break,
                Err(error) => panic!("throughput receive failed: {error}"),
            }
        }
        // Exclude the whole batch that crosses the measurement boundary.
        if Instant::now() >= deadline {
            break;
        }
        count += received;
        if received == 0 {
            std::thread::yield_now();
        }
    }
    (count, t0.elapsed().as_secs_f64())
}

fn drain_warmup(sock: &blocking::Socket) {
    let deadline = Instant::now() + Duration::from_millis(2);
    for _ in 0..256 {
        if Instant::now() >= deadline || sock.try_recv().is_err() {
            break;
        }
    }
}

// --- Subcommands -------------------------------------------------------------

fn run_push(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let corpus = payload_corpus::from_env(size);
    let push = ctx.blocking_socket(SocketType::Push, bench_options_server(size));
    let bound = push.bind(ep).expect("push bind");
    report_bound_port(ctx, &bound);
    if peers > 0 {
        let timeout = if peers > 64 { 20 } else { 10 };
        push.wait_connected(peers, Duration::from_secs(timeout))
            .expect("wait for pull peers");
    }

    if let Some(corpus) = corpus {
        wait_for_warmup_barrier();
        wait_for_start_barrier();
        let mut pacer = Pacer::from_env();
        loop {
            for message in &corpus {
                if let Some(pacer) = &mut pacer {
                    pacer.wait();
                }
                send_fast(&push, message.clone());
            }
        }
    }
    let payload = bench_payload(size);
    wait_for_warmup_barrier();
    let warmup_deadline = Instant::now() + warmup_duration();
    while Instant::now() < warmup_deadline {
        send_fast(&push, Message::from_slice(&payload));
        std::thread::sleep(Duration::from_millis(1));
    }
    wait_for_start_barrier();
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            send_fast(&push, Message::from_slice(&payload));
        }
    }
    let msg = Message::single(payload.clone());
    loop {
        send_fast(&push, msg.clone());
    }
}

/// Spaces corpus sends `OMQ_BENCH_SEND_RATE` messages per second apart. A
/// sender more than 1 ms behind skips ahead, so a stall does not become one
/// large batch.
struct Pacer {
    interval: Duration,
    next: Instant,
}

impl Pacer {
    fn from_env() -> Option<Self> {
        let rate: f64 = std::env::var("OMQ_BENCH_SEND_RATE")
            .ok()?
            .parse()
            .expect("OMQ_BENCH_SEND_RATE");
        assert!(rate > 0.0, "OMQ_BENCH_SEND_RATE must be positive");
        // The default 50 us timer slack would stretch short intervals.
        #[cfg(target_os = "linux")]
        unsafe {
            libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong);
        }
        Some(Self {
            interval: Duration::from_secs_f64(1.0 / rate),
            next: Instant::now(),
        })
    }

    fn wait(&mut self) {
        let now = Instant::now();
        if self.next > now {
            std::thread::sleep(self.next - now);
        } else if now - self.next > Duration::from_millis(1) {
            self.next = now;
        }
        self.next += self.interval;
    }
}

fn run_pull(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let corpus = payload_corpus::from_env(size);
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options_client(size));
    pull.connect(ep).expect("pull connect");

    if std::env::var_os("OMQ_BENCH_COMPRESSION").is_some()
        || std::env::var_os("OMQ_BENCH_VERIFIED_PUSHPULL").is_some()
    {
        run_compression_pull(&pull, size, duration, corpus.as_deref());
        return;
    }

    let cpu_before = cpu_time_secs();
    let (count, elapsed) = recv_loop(&pull, duration, size);
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
}

fn run_compression_pull(
    pull: &blocking::Socket,
    size: usize,
    duration: Duration,
    corpus: Option<&[Message]>,
) {
    pull.wait_connected(1, Duration::from_secs(10))
        .expect("compression sender connected");
    let payload = bench_payload(size);
    let mut index = 0;
    let batch = (64 * 1024 / size).clamp(1, 64);
    // Reads through the first batch at or after `deadline`, so intervals start
    // and end on message arrivals. Slow links deliver few large messages.
    let mut receive_through = |deadline: Instant| {
        let mut count = 0u64;
        loop {
            let mut received = 0u64;
            for _ in 0..batch {
                let Ok(message) = pull.try_recv() else {
                    break;
                };
                let expected = corpus.map_or(payload.as_ref(), |messages| {
                    messages[index].part_slice(0).unwrap()
                });
                assert!(
                    message.len() == 1 && message.part_slice(0) == Some(expected),
                    "corrupt compression payload"
                );
                if let Some(messages) = corpus {
                    index += 1;
                    if index == messages.len() {
                        index = 0;
                    }
                }
                received += 1;
            }
            if received == 0 {
                std::thread::yield_now();
                continue;
            }
            count += received;
            let now = Instant::now();
            if now >= deadline {
                return (count, now);
            }
        }
    };
    let (_, start) = receive_through(Instant::now() + warmup_duration());
    let sender_pid = std::env::var("OMQ_BENCH_SENDER_PID").expect("compression sender PID");
    let sender_before = process_cpu_time(&sender_pid);
    let receiver_before = cpu_time_secs();
    let wire_before = tcp_bytes_received();
    let (count, end) = receive_through(start + duration);
    let wire_bytes = tcp_bytes_received() - wire_before;
    let elapsed = (end - start).as_secs_f64();
    let receiver_cpu = cpu_time_secs() - receiver_before;
    let sender_cpu = process_cpu_time(&sender_pid) - sender_before;
    println!("{count} {elapsed:.9} {size} {receiver_cpu:.9} {sender_cpu:.9} {wire_bytes}");
}

/// TCP payload bytes received on all sockets of this process.
#[cfg(target_os = "linux")]
fn tcp_bytes_received() -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir("/proc/self/fd").expect("list open files") {
        let name = entry.expect("open file entry").file_name();
        let Ok(fd) = name.to_string_lossy().parse::<libc::c_int>() else {
            continue;
        };
        let mut info = std::mem::MaybeUninit::<libc::tcp_info>::zeroed();
        let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
        // Other files and non-TCP sockets fail with ENOTSOCK or EOPNOTSUPP.
        let status = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                info.as_mut_ptr().cast(),
                &raw mut len,
            )
        };
        if status == 0 {
            total += unsafe { info.assume_init() }.tcpi_bytes_received;
        }
    }
    total
}

#[cfg(not(target_os = "linux"))]
fn tcp_bytes_received() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn process_cpu_time(pid: &str) -> f64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .expect("compression sender CPU accounting");
    let (_, counters) = stat.rsplit_once(')').expect("process stat command");
    let fields: Vec<_> = counters.split_whitespace().collect();
    let user: f64 = fields[11].parse().expect("sender user CPU ticks");
    let system: f64 = fields[12].parse().expect("sender system CPU ticks");
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(ticks > 0, "process CPU clock resolution");
    (user + system) / ticks as f64
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_time(_pid: &str) -> f64 {
    0.0
}

fn run_pull_bind(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options_server(size));
    let bound = pull.bind(ep).expect("pull bind");
    report_bound_port(ctx, &bound);

    let cpu_before = cpu_time_secs();
    let (count, elapsed) = recv_loop(&pull, duration, size);
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
}

fn run_rep(ctx: &omq_tokio::Context, ep: Endpoint, size: usize) {
    let pair = latency_common::SocketPair::from_env();
    let rep = ctx.blocking_socket(
        pair.responder(),
        pair.options(bench_options_server(size), false),
    );
    let bound = rep.bind(ep).expect("rep bind");
    report_bound_port(ctx, &bound);
    let first = rep.recv().unwrap();
    pair.validate(&first, size, false);
    rep.send(first).unwrap();
    loop {
        let msg = rep.recv().unwrap();
        rep.send(msg).unwrap();
    }
}

fn run_req(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let pair = latency_common::SocketPair::from_env();
    let req = ctx.blocking_socket(
        pair.requester(),
        pair.options(bench_options_client(size), true),
    );
    req.connect(ep).expect("req connect");
    req.wait_connected(1, Duration::from_secs(5))
        .expect("latency peer ready");

    std::thread::sleep(Duration::from_millis(200));

    let msg = pair.request(size);
    req.send(msg.clone()).unwrap();
    pair.validate(&req.recv().unwrap(), size, true);

    for _ in 0..warmup {
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let cpu = cpu_time_secs() - cpu_before;
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} {cpu:.6} {elapsed:.6}");
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000.0
}

#[cfg(unix)]
fn timeval_secs(tv: &libc::timeval) -> f64 {
    let sec = u64::try_from(tv.tv_sec).unwrap_or(0);
    let usec = u64::try_from(tv.tv_usec).unwrap_or(0);
    (Duration::from_secs(sec) + Duration::from_micros(usec)).as_secs_f64()
}

#[cfg(unix)]
fn cpu_time_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = timeval_secs(&usage.ru_utime);
        let sys = timeval_secs(&usage.ru_stime);
        user + sys
    }
}

#[cfg(windows)]
fn cpu_time_secs() -> f64 {
    0.0
}
