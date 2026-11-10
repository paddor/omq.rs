//! Blocking bench peer: background IO thread, sync application code.
//!
//! Same interface as `bench_peer_tokio` but uses `blocking::Socket`.
//! The application thread never touches tokio. The Context spawns its
//! own IO thread; send/recv block the calling thread via
//! `Context::block_on`.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use omq_tokio::{Endpoint, Message, Options, SocketType, TrySendError, blocking};

mod bench_common;
mod blocking_inproc;
mod json_payload;
mod latency_common;
mod payload_corpus;
mod pushpull_common;
#[cfg(any(feature = "ws", feature = "quic"))]
mod ws_bench_config;

#[path = "bench_peer_blocking/fanin.rs"]
mod fanin;
#[path = "bench_peer_blocking/inproc.rs"]
mod inproc;
#[path = "bench_peer_blocking/pubsub.rs"]
mod pubsub;

use bench_common::{ModeArgs, RecvWindow, cpu_time_secs, quantile, warmup_duration};
use fanin::{run_multi_pull, run_multi_push};
use inproc::{run_inproc, run_inproc_2ut, run_inproc_latency};
use json_payload::bench_payload;
use pubsub::{run_multi_sub, run_pub};

fn multi_pull_drain_batch(size: usize) -> usize {
    let batch = std::env::var("OMQ_BENCH_DRAIN_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(if size <= 1024 { 64 } else { 256 });

    batch.clamp(1, 256).min((64 * 1024 / size.max(1)).max(1))
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    #[cfg(any(feature = "ws", feature = "quic"))]
    ws_bench_config::set_endpoint(args.get(2).map(String::as_str));
    let ctx = omq_tokio::Context::with_config(omq_tokio::ContextConfig::from_env());
    configure_egress_limit(&ctx);
    let n = ctx.io_threads();
    eprintln!("runtime: blocking, {n} IO thread(s)");
    bench_common::install_signals();

    let mode = args.get(1).map(String::as_str);
    let args = ModeArgs(&args);
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
fn run_pipeline_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &ModeArgs<'_>) -> bool {
    match mode {
        Some("push") => {
            let peers = args.optional(4).unwrap_or(0);
            run_push(ctx, args.ep(), args.size(), peers);
        }
        Some("pull") => run_pull(ctx, args.ep(), args.size(), args.secs(4)),
        Some("pull-bind") => run_pull_bind(ctx, args.ep(), args.size(), args.secs(4)),
        Some("multi-pull") => {
            let (ep, size, duration) = (args.ep(), args.size(), args.secs(4));
            run_multi_pull(ctx, ep, size, duration, args.parse(5, "socket_count"));
        }
        Some("multi-push") => {
            let (ep, size) = (args.ep(), args.size());
            let count = args.parse(4, "socket_count");
            run_multi_push(ctx, ep, size, count, args.optional(5));
        }
        _ => return false,
    }

    true
}

/// PUB/SUB and REQ/REP modes over a network endpoint. Returns false for
/// other modes.
fn run_pattern_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &ModeArgs<'_>) -> bool {
    match mode {
        Some("pub") => {
            let peers = args.optional(4).unwrap_or(0);
            run_pub(ctx, args.ep(), args.size(), peers);
        }
        Some("multi-sub") => {
            let (ep, size, duration) = (args.ep(), args.size(), args.secs(4));
            run_multi_sub(ctx, ep, size, duration, args.parse(5, "socket_count"));
        }
        Some("rep") => run_rep(ctx, args.ep(), args.size()),
        Some("req") => {
            let (ep, size) = (args.ep(), args.size());
            let iterations = args.parse(4, "iterations");
            run_req(ctx, ep, size, iterations, args.parse(5, "warmup"));
        }
        _ => return false,
    }

    true
}

/// In-process modes. Returns false for other modes.
fn run_inproc_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &ModeArgs<'_>) -> bool {
    match mode {
        Some("inproc") => run_inproc(ctx, args.name(), args.size(), args.secs(4)),
        Some("inproc-2ut") => run_inproc_2ut(ctx, args.name(), args.size(), args.secs(4)),
        Some("inproc-2ut-blocking") => {
            let (name, size, duration) = (args.name(), args.size(), args.secs(4));
            let hwm = args
                .0
                .get(5)
                .cloned()
                .or_else(|| std::env::var("OMQ_BENCH_HWM").ok());
            let hwm = hwm.map_or(1000, |value| value.parse().expect("hwm"));
            blocking_inproc::run(ctx, name, size, duration, hwm);
        }
        Some("inproc-latency") => {
            let (name, size) = (args.name(), args.size());
            let iterations = args.parse(4, "iterations");
            run_inproc_latency(ctx, name, size, iterations, args.parse(5, "warmup"));
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
    let mut o =
        bench_common::env_options(Options::default().recv_spin(bench_recv_spin()), msg_size);

    if let Ok(val) = std::env::var("OMQ_BENCH_OFFLOAD_THRESHOLD") {
        let threshold = (val != "off").then(|| val.parse().expect("OMQ_BENCH_OFFLOAD_THRESHOLD"));
        o = o.compression_offload_threshold(threshold);
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

fn bench_options_server(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match bench_common::mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "curve")]
        Some("curve") => bench_common::curve_server(o),
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn bench_options_client(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match bench_common::mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "curve")]
        Some("curve") => bench_common::curve_client(o),
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

// --- Helpers -----------------------------------------------------------------

fn send_fast(sock: &blocking::Socket, msg: Message) {
    match sock.try_send(msg) {
        Ok(()) => {}
        Err(TrySendError::Full(msg)) => sock.send(msg).unwrap(),
        Err(e) => panic!("try_send failed: {e}"),
    }
}

/// Start barrier and stop flag shared by the sender and receiver threads of
/// a two-thread inproc run. The receiver sets `stop` when its window ends.
#[derive(Clone)]
struct StartStop {
    barrier: Arc<Barrier>,
    stop: Arc<AtomicBool>,
}

impl StartStop {
    fn new() -> Self {
        Self {
            barrier: Arc::new(Barrier::new(2)),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn wait_for_start_barrier() {
    if let Some(delay) = bench_common::start_delay() {
        std::thread::sleep(delay);
    }
}

fn wait_for_warmup_barrier() {
    if let Some(delay) = bench_common::warmup_delay() {
        std::thread::sleep(delay);
    }
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

    latency_common::print_rtts(rtts, iterations, Some(cpu), elapsed);
}
