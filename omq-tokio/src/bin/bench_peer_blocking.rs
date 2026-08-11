//! Blocking bench peer: background IO thread, sync application code.
//!
//! Same interface as `bench_peer_tokio` but uses `blocking::Socket`.
//! The application thread never touches tokio. The Context spawns its
//! own IO thread; send/recv block the calling thread via
//! `Context::block_on`.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, SocketType, TrySendError, blocking};
use std::net::Ipv4Addr;

fn multi_pull_drain_batch(size: usize) -> usize {
    if let Some(batch) = std::env::var("OMQ_BENCH_DRAIN_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        return batch.max(1);
    }
    if size <= 1024 { 64 } else { 256 }
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
    let Endpoint::Tcp { port, .. } = ep else {
        return;
    };
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        return;
    };
    let coord = COORD_SOCK.get_or_init(|| {
        let s = ctx.blocking_socket(SocketType::Push, Options::default());
        s.connect(coord_ep.parse().expect("valid coord endpoint"))
            .unwrap();
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

#[allow(clippy::too_many_lines)]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ctx = omq_tokio::Context::with_config(omq_tokio::ContextConfig::from_env());
    let n = ctx.io_threads();
    eprintln!("runtime: blocking, {n} IO thread(s)");
    install_signals();

    match args.get(1).map(String::as_str) {
        Some("push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_push(&ctx, ep, size, peers);
        }
        Some("pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(&ctx, ep, size, Duration::from_secs_f64(duration));
        }
        Some("pull-bind") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(&ctx, ep, size, Duration::from_secs_f64(duration));
        }
        Some("multi-pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_pull(&ctx, ep, size, Duration::from_secs_f64(duration), count);
        }
        Some("multi-push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let count: usize = args[4].parse().expect("socket_count");
            let duration = args.get(5).and_then(|s| s.parse().ok());
            run_multi_push(&ctx, ep, size, count, duration);
        }
        Some("pub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_pub(&ctx, ep, size, peers);
        }
        Some("multi-sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_sub(&ctx, ep, size, Duration::from_secs_f64(duration), count);
        }
        Some("rep") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep(&ctx, ep, size);
        }
        Some("req") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req(&ctx, ep, size, iterations, warmup);
        }
        Some("inproc") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc(&ctx, name, size, Duration::from_secs_f64(duration));
        }
        Some("inproc-2ut") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc_2ut(&ctx, name, size, Duration::from_secs_f64(duration));
        }
        Some("inproc-latency") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(&ctx, name, size, iterations, warmup);
        }
        _ => {
            eprintln!("usage: bench_peer_blocking push <addr> <size> [peers]");
            eprintln!("       bench_peer_blocking pull <addr> <size> <duration_secs>");
            eprintln!("       bench_peer_blocking pull-bind <addr> <size> <duration_secs>");
            eprintln!(
                "       bench_peer_blocking multi-pull <addr> <size> <duration_secs> <count>"
            );
            eprintln!("       bench_peer_blocking multi-push <addr> <size> <count>");
            eprintln!("       bench_peer_blocking pub <addr> <size>");
            eprintln!("       bench_peer_blocking multi-sub <addr> <size> <duration_secs> <count>");
            eprintln!("       bench_peer_blocking rep <addr> <size>");
            eprintln!("       bench_peer_blocking req <addr> <size> <iterations> <warmup>");
            eprintln!("       bench_peer_blocking inproc <name> <size> <duration_secs>");
            eprintln!("       bench_peer_blocking inproc-2ut <name> <size> <duration_secs>");
            eprintln!(
                "       bench_peer_blocking inproc-latency <name> <size> <iterations> <warmup>"
            );
            std::process::exit(1);
        }
    }
}

// --- Options -----------------------------------------------------------------

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
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
    if let Ok(val) = std::env::var("OMQ_BENCH_ARENA_THRESHOLD") {
        o = o.arena_threshold(val.parse().expect("OMQ_BENCH_ARENA_THRESHOLD"));
    }
    o
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

fn bench_payload(size: usize) -> Bytes {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("json") {
        json_payload_random(size)
    } else {
        Bytes::from(vec![b'x'; size])
    }
}

fn json_payload_random(target_bytes: usize) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = rand_seed();
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

fn rand_seed() -> u32 {
    let mut buf = [0u8; 4];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| {
            use std::io::Read;
            f.read_exact(&mut buf)?;
            Ok(())
        })
        .expect("/dev/urandom");
    u32::from_ne_bytes(buf)
}

fn xorshift32(state: &mut u32) -> u32 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    x
}

fn json_record(out: &mut String, state: &mut u32) {
    const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR", "TRACE"];
    const SERVICES: &[&str] = &[
        "api-gateway",
        "auth-svc",
        "order-svc",
        "payment-svc",
        "notify-svc",
        "inventory-svc",
        "shipping-svc",
        "billing-svc",
        "search-svc",
        "user-svc",
        "session-svc",
        "analytics-svc",
        "cache-svc",
        "config-svc",
        "audit-svc",
        "rate-limiter",
    ];
    const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];
    const PATHS: &[&str] = &[
        "/v1/widgets",
        "/v1/users",
        "/v1/orders",
        "/v2/events",
        "/v1/health",
        "/v1/sessions",
        "/v1/payments",
        "/v2/search",
        "/v1/inventory",
        "/v1/shipping",
        "/v1/analytics",
        "/v2/config",
    ];
    const REGIONS: &[&str] = &[
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "ap-south-1",
        "eu-central-1",
        "ap-northeast-1",
        "sa-east-1",
        "ca-central-1",
    ];
    const STATUSES: &[u16] = &[
        200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 405, 409, 422, 429, 500, 502, 503,
        504,
    ];
    const MSGS: &[&str] = &[
        "request handled successfully",
        "resource created",
        "cache miss, fetched from origin",
        "rate limit approaching threshold",
        "upstream timeout, retrying",
        "authentication token refreshed",
        "database connection pool exhausted",
        "circuit breaker tripped",
        "message queued for async processing",
        "TLS handshake completed",
        "request routed to fallback backend",
        "payload validation passed",
        "idempotency key matched existing result",
        "graceful shutdown initiated",
        "health check passed all probes",
        "retry attempt succeeded after backoff",
    ];

    let trace_id = xorshift32(state);
    let span_id = xorshift32(state);
    let user_id = xorshift32(state);
    let r = xorshift32(state) as usize;
    let level = LEVELS[r % LEVELS.len()];
    let service = SERVICES[(r >> 4) % SERVICES.len()];
    let method = METHODS[(r >> 8) % METHODS.len()];
    let path = PATHS[(r >> 12) % PATHS.len()];
    let region = REGIONS[(r >> 16) % REGIONS.len()];
    let status = STATUSES[(r >> 20) % STATUSES.len()];
    let latency = (xorshift32(state) % 5000) + 1;
    let r2 = xorshift32(state) as usize;
    let msg = MSGS[r2 % MSGS.len()];
    let host_id = xorshift32(state);
    let _ = write!(
        out,
        r#"{{"ts":"2026-04-27T12:34:56.{trace_id:08x}Z","level":"{level}","service":"{service}","trace_id":"{trace_id:08x}{span_id:08x}","span_id":"{span_id:08x}","user_id":"u-{user_id:08x}","method":"{method}","path":"{path}/{trace_id:08x}","status":{status},"latency_ms":{latency},"region":"{region}","host":"{service}-{host_id:08x}.svc.cluster.local","msg":"{msg}"}}{nl}"#,
        nl = '\n',
    );
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

fn recv_timer_check_interval(size: usize) -> usize {
    if size <= 1024 { 4096 } else { 256 }
}

fn recv_loop(sock: &blocking::Socket, duration: Duration, size: usize) -> (u64, f64) {
    std::thread::sleep(Duration::from_millis(500));
    wait_for_start_barrier();
    drain_warmup(sock);

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    let timer_check_interval = recv_timer_check_interval(size);
    let mut until_timer_check = timer_check_interval;
    loop {
        if sock.try_recv().is_ok() {
            count += 1;
            until_timer_check -= 1;
            if until_timer_check == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                until_timer_check = timer_check_interval;
            }
        } else {
            if Instant::now() >= deadline {
                break;
            }
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
    let push = ctx.blocking_socket(SocketType::Push, bench_options_server(size));
    let bound = push.bind(ep).expect("push bind");
    report_bound_port(ctx, &bound);
    if peers > 0 {
        let timeout = if peers > 64 { 20 } else { 10 };
        push.wait_connected(peers, Duration::from_secs(timeout))
            .expect("wait for pull peers");
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
    } else {
        let msg = Message::single(payload.clone());
        loop {
            send_fast(&push, msg.clone());
        }
    }
}

fn run_pull(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options_client(size));
    pull.connect(ep).expect("pull connect");

    let cpu_before = cpu_time_secs();
    let (count, elapsed) = recv_loop(&pull, duration, size);
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
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

#[expect(clippy::cast_precision_loss)]
#[allow(clippy::needless_pass_by_value)]
fn run_multi_pull(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let drain_batch = multi_pull_drain_batch(size);
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Pull, bench_options_client(size));
        s.connect(ep.clone()).expect("pull connect");
        sockets.push(s);
    }

    std::thread::sleep(Duration::from_millis(500));
    wait_for_start_barrier();
    for s in &sockets {
        drain_warmup(s);
    }

    let counters: Vec<Arc<AtomicU64>> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let started = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let cpu_before = cpu_time_secs();

    let handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(sock, counter)| {
            let started = Arc::clone(&started);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                loop {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    if sock.try_recv().is_err() {
                        std::thread::yield_now();
                        continue;
                    }
                    for _ in 1..drain_batch {
                        if sock.try_recv().is_err() {
                            break;
                        }
                        if started.load(Ordering::Relaxed) {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if started.load(Ordering::Relaxed) {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(Duration::from_millis(500));
    started.store(true, Ordering::Release);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let elapsed = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Release);
    for h in handles {
        h.join().unwrap();
    }
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    let mut rates: Vec<f64> = per_socket
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
    eprintln!(
        "multi-pull: {socket_count} sockets, {:.0} msg/s total, \
         per-socket [{per_min_rate:.0}, {per_max_rate:.0}] msg/s",
        total as f64 / elapsed,
    );
}

#[allow(clippy::needless_pass_by_value)]
fn run_multi_push(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    socket_count: usize,
    duration: Option<f64>,
) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Push, bench_options_client(size));
        s.connect(ep.clone()).expect("push connect");
        sockets.push(s);
    }

    let payload = bench_payload(size);
    let counters: Vec<_> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    wait_for_start_barrier();
    let _handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(sock, counter)| {
            let p = payload.clone();
            std::thread::spawn(move || {
                if p.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
                    loop {
                        send_fast(&sock, Message::from_slice(&p));
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    let msg = Message::single(p);
                    loop {
                        send_fast(&sock, msg.clone());
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    let Some(duration) = duration else {
        std::thread::park();
        return;
    };
    std::thread::sleep(Duration::from_secs_f64(duration));
    let elapsed = duration;
    let rates: Vec<f64> = counters
        .iter()
        .map(|c| c.load(Ordering::Relaxed) as f64 / elapsed)
        .collect();
    let total: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let mut sorted = rates.clone();
    sorted.sort_unstable_by(f64::total_cmp);
    println!(
        "{total} {elapsed:.6} {size} {:.6} {socket_count} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
        cpu_time_secs(),
        sorted[0],
        sorted[socket_count - 1],
        quantile(&sorted, 0.10),
        quantile(&sorted, 0.25),
        quantile(&sorted, 0.50),
        quantile(&sorted, 0.75),
        quantile(&sorted, 0.90),
    );
}

fn run_pub(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let mut opts = bench_options_server(size);
    opts.xpub_nodrop = true;
    let pub_ = ctx.blocking_socket(SocketType::Pub, opts);
    let bound = pub_.bind(ep).expect("pub bind");
    report_bound_port(ctx, &bound);
    if peers > 0 {
        let timeout = if peers > 64 { 20 } else { 10 };
        pub_.wait_connected(peers, Duration::from_secs(timeout))
            .expect("wait for subscribers");
        std::thread::sleep(Duration::from_millis(200));
    }

    let payload = bench_payload(size);
    wait_for_warmup_barrier();
    let warmup_deadline = Instant::now() + warmup_duration();
    while Instant::now() < warmup_deadline {
        send_fast(&pub_, Message::from_slice(&payload));
        std::thread::sleep(Duration::from_millis(1));
    }
    wait_for_start_barrier();
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            send_fast(&pub_, Message::from_slice(&payload));
        }
    } else {
        let msg = Message::single(payload.clone());
        loop {
            send_fast(&pub_, msg.clone());
        }
    }
}

#[expect(clippy::cast_precision_loss)]
#[allow(clippy::needless_pass_by_value)]
fn run_multi_sub(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    let drain_batch = multi_pull_drain_batch(size);
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.blocking_socket(SocketType::Sub, bench_options_client(size));
        s.connect(ep.clone()).expect("sub connect");
        s.subscribe(Bytes::new()).expect("subscribe");
        sockets.push(s);
    }

    wait_for_warmup_barrier();
    let warmup_deadline = Instant::now() + warmup_duration();
    let warmup_handles: Vec<_> = sockets
        .iter()
        .cloned()
        .map(|sock| {
            std::thread::spawn(move || {
                while Instant::now() < warmup_deadline {
                    while sock.try_recv().is_ok() {}
                    std::thread::yield_now();
                }
            })
        })
        .collect();
    for handle in warmup_handles {
        handle.join().unwrap();
    }
    wait_for_start_barrier();

    let counters: Vec<Arc<AtomicU64>> = (0..socket_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;

    let handles: Vec<_> = sockets
        .into_iter()
        .zip(counters.iter().cloned())
        .map(|(sock, counter)| {
            std::thread::spawn(move || {
                let mut n: u64 = 0;
                loop {
                    if Instant::now() >= deadline {
                        break;
                    }
                    if sock.try_recv().is_err() {
                        std::thread::yield_now();
                        continue;
                    }
                    n += 1;
                    for _ in 1..drain_batch {
                        if sock.try_recv().is_err() {
                            break;
                        }
                        n += 1;
                    }
                }
                counter.store(n, Ordering::Relaxed);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: u64 = per_socket.iter().sum();
    let per_min = per_socket.iter().copied().min().unwrap_or(0);
    let per_max = per_socket.iter().copied().max().unwrap_or(0);
    let per_min_rate = per_min as f64 / elapsed;
    let per_max_rate = per_max as f64 / elapsed;
    println!(
        "{total} {elapsed:.6} {size} {cpu:.6} {socket_count} {per_min_rate:.1} {per_max_rate:.1}"
    );
    eprintln!(
        "multi-sub: {socket_count} sockets, {:.0} msg/s total, \
         per-socket [{per_min_rate:.0}, {per_max_rate:.0}] msg/s",
        total as f64 / elapsed,
    );
}

fn run_rep(ctx: &omq_tokio::Context, ep: Endpoint, size: usize) {
    let rep = ctx.blocking_socket(SocketType::Rep, bench_options_server(size));
    let bound = rep.bind(ep).expect("rep bind");
    report_bound_port(ctx, &bound);
    loop {
        let msg = rep.recv().unwrap();
        rep.send(msg).unwrap();
    }
}

fn run_req(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let req = ctx.blocking_socket(SocketType::Req, bench_options_client(size));
    req.connect(ep).expect("req connect");

    std::thread::sleep(Duration::from_millis(200));

    let payload = Bytes::from(vec![b'x'; size]);
    let msg = Message::single(payload);

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

fn run_inproc(ctx: &omq_tokio::Context, name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = ctx.blocking_socket(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).expect("push bind");
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options(size));
    pull.connect(ep).expect("pull connect");

    let payload = bench_payload(size);
    std::thread::spawn(move || {
        if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
            loop {
                send_fast(&push, Message::from_slice(&payload));
            }
        } else {
            let msg = Message::single(payload.clone());
            loop {
                send_fast(&push, msg.clone());
            }
        }
    });

    std::thread::sleep(Duration::from_millis(500));

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if Instant::now() >= deadline {
            break;
        }
        if pull.try_recv().is_ok() {
            count += 1;
            while Instant::now() < deadline && pull.try_recv().is_ok() {
                count += 1;
            }
        } else {
            std::thread::yield_now();
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
}

fn run_inproc_2ut(ctx: &omq_tokio::Context, name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options(size));
    let push = ctx.blocking_socket(SocketType::Push, bench_options(size));
    pull.bind(ep.clone()).expect("pull bind");
    push.connect(ep).expect("push connect");
    push.wait_connected(1, Duration::from_secs(5))
        .expect("wait for inproc connection");

    let payload = bench_payload(size);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let stop = Arc::new(AtomicBool::new(false));

    let sender_barrier = Arc::clone(&barrier);
    let sender_stop = Arc::clone(&stop);
    let sender_push = push.clone();
    let sender = std::thread::spawn(move || {
        sender_barrier.wait();
        if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
            loop {
                if sender_stop.load(Ordering::Acquire) {
                    break;
                }
                match sender_push.try_send(Message::from_slice(&payload)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
        } else {
            let msg = Message::single(payload);
            loop {
                if sender_stop.load(Ordering::Acquire) {
                    break;
                }
                match sender_push.try_send(msg.clone()) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
        }
    });

    let receiver_barrier = barrier;
    let receiver_stop = Arc::clone(&stop);
    let receiver_pull = pull.clone();
    let receiver = std::thread::spawn(move || {
        receiver_barrier.wait();
        let start = Instant::now();
        let deadline = start + duration;
        let mut count = 0u64;
        let mut until_timer_check = 10_000usize;
        loop {
            match receiver_pull.try_recv() {
                Ok(_) => count += 1,
                Err(_) => std::hint::spin_loop(),
            }
            until_timer_check -= 1;
            if until_timer_check == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                until_timer_check = 10_000;
            }
        }
        receiver_stop.store(true, Ordering::Release);
        (count, start.elapsed())
    });

    let (count, elapsed) = receiver.join().expect("receiver thread");
    sender.join().expect("sender thread");
    let _ = pull.close();
    let _ = push.close();
    println!("{count} {:.6} {size}", elapsed.as_secs_f64());
}

fn run_inproc_latency(
    ctx: &omq_tokio::Context,
    name: String,
    size: usize,
    iterations: usize,
    warmup: usize,
) {
    let ep = Endpoint::Inproc { name };
    let rep = ctx.blocking_socket(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).expect("rep bind");

    std::thread::spawn(move || {
        loop {
            let msg = rep.recv().unwrap();
            rep.send(msg).unwrap();
        }
    });

    let req = ctx.blocking_socket(SocketType::Req, Options::default());
    req.connect(ep).expect("req connect");
    std::thread::sleep(Duration::from_millis(200));

    let payload = Bytes::from(vec![b'x'; size]);
    let msg = Message::single(payload);

    for _ in 0..warmup {
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
    }

    let t_wall = Instant::now();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
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
