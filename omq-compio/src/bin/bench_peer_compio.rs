//! Two-process throughput and latency peer for omq-compio.
//!
//! Usage:
//!   `bench_peer_compio` push \<endpoint\> \<`msg_size_bytes`\>
//!   `bench_peer_compio` pull \<endpoint\> \<`msg_size_bytes`\> \<`duration_secs`\>
//!   `bench_peer_compio` rep  \<endpoint\> \<`msg_size_bytes`\>
//!   `bench_peer_compio` req  \<endpoint\> \<`msg_size_bytes`\> \<iterations\> \<warmup\>
//!
//! Endpoint: a port number (`4000`), an `ip:port` pair (`0.0.0.0:4000`),
//! a full URI (`tcp://0.0.0.0:4000`), or an IPC path (`ipc:///tmp/foo.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints raw stats to stdout (for scripts) and a
//!       human-readable summary to stderr.
//! Rep: binds a REP socket, echoes every received message back forever.
//! Req: connects a REQ socket, runs \<warmup\> warm-up round-trips, then
//!      measures \<iterations\> round-trips and prints one line to stdout:
//!        \<`p50_us`\> \<`p99_us`\> \<`p999_us`\> \<`max_us`\> \<iterations\>

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::runtime::ProactorBuilderExt as _;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

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

fn print_bound_port(ep: &Endpoint) {
    match ep {
        Endpoint::Tcp { port, .. } => println!("PORT {port}"),
        #[cfg(feature = "lz4")]
        Endpoint::Lz4Tcp { port, .. } => println!("PORT {port}"),
        _ => {}
    }
}

fn bench_payload(size: usize) -> Bytes {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("json") {
        json_payload(size)
    } else {
        Bytes::from(vec![b'x'; size])
    }
}

fn json_payload(target_bytes: usize) -> Bytes {
    const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR"];
    const SERVICES: &[&str] = &[
        "api-gateway",
        "auth-svc",
        "order-svc",
        "payment-svc",
        "notify-svc",
    ];
    const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
    const PATHS: &[&str] = &[
        "/v1/widgets",
        "/v1/users",
        "/v1/orders",
        "/v2/events",
        "/v1/health",
    ];
    const REGIONS: &[&str] = &[
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "ap-south-1",
        "eu-central-1",
    ];
    const STATUSES: &[u16] = &[200, 201, 204, 400, 404, 500, 502, 503];
    const MSGS: &[&str] = &[
        "request handled successfully",
        "resource created",
        "cache miss, fetched from origin",
        "rate limit approaching threshold",
        "upstream timeout, retrying",
    ];

    use std::fmt::Write as _;
    let mut out = String::with_capacity(target_bytes + 512);
    let mut counter: u32 = 0;
    while out.len() < target_bytes {
        let h = counter.wrapping_mul(0x9E37_79B1) as usize;
        let id = format!("{h:08x}");
        let level = LEVELS[h % LEVELS.len()];
        let service = SERVICES[(h >> 4) % SERVICES.len()];
        let method = METHODS[(h >> 8) % METHODS.len()];
        let path = PATHS[(h >> 12) % PATHS.len()];
        let region = REGIONS[(h >> 16) % REGIONS.len()];
        let status = STATUSES[(h >> 20) % STATUSES.len()];
        let latency = (h % 500) as u32 + 1;
        let msg = MSGS[(h >> 24) % MSGS.len()];
        let _ = write!(
            out,
            r#"{{"ts":"2026-04-27T12:34:56.{id}Z","level":"{level}","service":"{service}","trace_id":"{id}","span_id":"{id}","user_id":"u-{id}","method":"{method}","path":"{path}/{id}","status":{status},"latency_ms":{latency},"region":"{region}","host":"{service}-{id}.svc.cluster.local","msg":"{msg}"}}{nl}"#,
            nl = '\n',
        );
        counter = counter.wrapping_add(1);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
}

extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

#[expect(clippy::too_many_lines)]
fn main() {
    unsafe {
        libc::signal(
            libc::SIGTERM,
            exit_on_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            exit_on_signal as *const () as libc::sighandler_t,
        );
    }
    let args: Vec<String> = std::env::args().collect();
    let msg_size: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let buf_len = (msg_size + 64).next_power_of_two().max(64 * 1024);
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor.with_omq_buffer_pool_sized(std::num::NonZero::new(64).unwrap(), buf_len);
    let rt = compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
        .expect("compio runtime");
    rt.block_on(async {
        match args.get(1).map(String::as_str) {
            Some("push") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_push(ep, size).await;
            }
            Some("pull") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_pull(ep, size, Duration::from_secs_f64(duration)).await;
            }
            Some("inproc") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_inproc(name, size, Duration::from_secs_f64(duration)).await;
            }
            Some("inproc-st") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_inproc_same_thread(name, size, Duration::from_secs_f64(duration)).await;
            }
            Some("rep") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_rep(ep, size).await;
            }
            Some("req") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let iterations: usize = args[4].parse().expect("iterations");
                let warmup: usize = args[5].parse().expect("warmup");
                run_req(ep, size, iterations, warmup).await;
            }
            Some("inproc-latency") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let iterations: usize = args[4].parse().expect("iterations");
                let warmup: usize = args[5].parse().expect("warmup");
                run_inproc_latency(name, size, iterations, warmup).await;
            }
            Some("inproc-st-latency") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let iterations: usize = args[4].parse().expect("iterations");
                let warmup: usize = args[5].parse().expect("warmup");
                run_inproc_st_latency(name, size, iterations, warmup).await;
            }
            Some("pub") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_pub(ep, size).await;
            }
            Some("sub") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_sub(ep, size, Duration::from_secs_f64(duration)).await;
            }
            Some("inproc-pubsub") => {
                let name = args[2].clone();
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                let peers: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
                run_inproc_pubsub(name, size, Duration::from_secs_f64(duration), peers).await;
            }
            Some("push-connect") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                run_push_connect(ep, size).await;
            }
            Some("pull-bind") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let duration: f64 = args[4].parse().expect("duration_secs");
                run_pull_bind(ep, size, Duration::from_secs_f64(duration)).await;
            }
            Some("latency-mt") => {
                let ep = parse_ep(&args[2]);
                let size: usize = args[3].parse().expect("msg_size");
                let iterations: usize = args[4].parse().expect("iterations");
                let warmup: usize = args[5].parse().expect("warmup");
                run_latency_mt(ep, size, iterations, warmup).await;
            }
            _ => {
                eprintln!("usage: bench_peer_compio push <endpoint> <size>");
                eprintln!("       bench_peer_compio pull <endpoint> <size> <duration_secs>");
                eprintln!("       bench_peer_compio inproc <name> <size> <duration_secs>");
                eprintln!("       bench_peer_compio inproc-st <name> <size> <duration_secs>");
                eprintln!("       bench_peer_compio rep <endpoint> <size>");
                eprintln!("       bench_peer_compio req <endpoint> <size> <iterations> <warmup>");
                eprintln!(
                    "       bench_peer_compio inproc-latency <name> <size> <iterations> <warmup>"
                );
                eprintln!(
                    "       bench_peer_compio inproc-st-latency <name> <size> <iterations> <warmup>"
                );
                eprintln!(
                    "       bench_peer_compio latency-mt <endpoint> <size> <iterations> <warmup>"
                );
                std::process::exit(1);
            }
        }
    });
}

async fn run_push(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options_server(size));
    let bound = push.bind(ep).await.expect("push bind");
    print_bound_port(&bound);
    let payload = bench_payload(size);
    if size <= omq_proto::message::MAX_INLINE_MESSAGE {
        loop {
            push.send(Message::from_slice(&payload)).await.unwrap();
        }
    } else {
        let msg = Message::single(payload);
        loop {
            push.send(msg.clone()).await.unwrap();
        }
    }
}

async fn run_push_connect(ep: Endpoint, size: usize) {
    let push = Socket::new(SocketType::Push, bench_options_client(size));
    push.connect(ep).await.expect("push connect");
    let payload = bench_payload(size);
    if size <= omq_proto::message::MAX_INLINE_MESSAGE {
        loop {
            push.send(Message::from_slice(&payload)).await.unwrap();
        }
    } else {
        let msg = Message::single(payload);
        loop {
            push.send(msg.clone()).await.unwrap();
        }
    }
}

async fn run_pull_bind(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options_server(size));
    let bound = pull.bind(ep.clone()).await.expect("pull bind");
    print_bound_port(&bound);

    compio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        pull.recv().await.unwrap();
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
    if std::env::var_os("OMQ_NO_LARGE_MSG").is_some() {
        o = o.disable_large_message_path();
    }
    if msg_size >= 2 * 1024 * 1024 {
        let buf = msg_size * 2;
        o = o.recv_buffer_size(buf).send_buffer_size(buf);
    }
    #[cfg(feature = "lz4")]
    if let Ok(path) = std::env::var("OMQ_BENCH_COMPRESSION_DICT") {
        let dict = std::fs::read(&path).expect("read compression dict file");
        o = o.compression_dict(Bytes::from(dict));
    }
    #[cfg(feature = "lz4")]
    if let Ok(s) = std::env::var("OMQ_BENCH_COMPRESSION_THRESHOLD")
        && let Ok(t) = s.parse::<usize>()
    {
        o = o.compression_threshold(t);
    }
    if let Ok(s) = std::env::var("OMQ_BENCH_SEND_HWM")
        && let Ok(hwm) = s.parse::<u32>()
    {
        o = o.send_hwm(hwm);
    }
    o
}

fn bench_options_server(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "plain")]
        Some("plain") => o.plain_server(|_| true),
        #[cfg(feature = "curve")]
        Some("curve") => o.curve_server(bench_curve_server_keypair()),
        #[cfg(feature = "blake3zmq")]
        Some("blake3zmq") => o.blake3zmq_server(bench_b3_server_keypair()),
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn bench_options_client(msg_size: usize) -> Options {
    let o = bench_options(msg_size);
    match mechanism_env().as_deref() {
        None | Some("null") => o,
        #[cfg(feature = "plain")]
        Some("plain") => o.plain_client("bench", "bench"),
        #[cfg(feature = "curve")]
        Some("curve") => {
            let client_kp = bench_curve_client_keypair();
            let server_pub = bench_curve_server_keypair().public;
            o.curve_client(client_kp, server_pub)
        }
        #[cfg(feature = "blake3zmq")]
        Some("blake3zmq") => {
            let client_kp = bench_b3_client_keypair();
            let server_pub = bench_b3_server_keypair().public;
            o.blake3zmq_client(client_kp, server_pub)
        }
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn mechanism_env() -> Option<String> {
    std::env::var("OMQ_BENCH_MECHANISM")
        .ok()
        .map(|s| s.to_ascii_lowercase())
}

#[cfg(feature = "curve")]
fn bench_curve_server_keypair() -> omq_compio::CurveKeypair {
    let secret = omq_compio::CurveSecretKey::from_bytes([0x01; 32]);
    let public = secret.derive_public();
    omq_compio::CurveKeypair { public, secret }
}

#[cfg(feature = "curve")]
fn bench_curve_client_keypair() -> omq_compio::CurveKeypair {
    let secret = omq_compio::CurveSecretKey::from_bytes([0x02; 32]);
    let public = secret.derive_public();
    omq_compio::CurveKeypair { public, secret }
}

#[cfg(feature = "blake3zmq")]
fn bench_b3_server_keypair() -> omq_compio::Blake3ZmqKeypair {
    omq_compio::Blake3ZmqKeypair::from_secret(omq_compio::Blake3ZmqSecretKey([0x03; 32]))
}

#[cfg(feature = "blake3zmq")]
fn bench_b3_client_keypair() -> omq_compio::Blake3ZmqKeypair {
    omq_compio::Blake3ZmqKeypair::from_secret(omq_compio::Blake3ZmqSecretKey([0x04; 32]))
}

async fn run_inproc(name: String, size: usize, duration: Duration) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let ep = Endpoint::Inproc { name };
    let stop = Arc::new(AtomicBool::new(false));
    let (bound_tx, bound_rx) = flume::bounded::<()>(1);
    let (connected_tx, connected_rx) = flume::bounded::<()>(1);

    let push_ep = ep.clone();
    let push_stop = stop.clone();
    std::thread::spawn(move || {
        let buf_len = (size + 64).next_power_of_two().max(64 * 1024);
        let mut proactor = compio::driver::ProactorBuilder::new();
        proactor.with_omq_buffer_pool_sized(std::num::NonZero::new(64).unwrap(), buf_len);
        let rt = compio::runtime::RuntimeBuilder::new()
            .with_proactor(proactor)
            .build()
            .expect("push runtime");
        rt.block_on(async move {
            let push = Socket::new(SocketType::Push, bench_options(size));
            push.bind(push_ep).await.unwrap();
            let _ = bound_tx.send(());
            let _ = connected_rx.recv_async().await;
            let payload = Bytes::from(vec![b'x'; size]);
            while !push_stop.load(Ordering::Relaxed) {
                if push.send(Message::single(payload.clone())).await.is_err() {
                    break;
                }
            }
        });
    });

    let _ = bound_rx.recv_async().await;
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");
    let _ = connected_tx.send(());
    compio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        pull.recv().await.unwrap();
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    println!("{count} {elapsed:.6} {size}");
}

async fn run_pull(ep: Endpoint, size: usize, duration: Duration) {
    let pull = Socket::new(SocketType::Pull, bench_options_client(size));
    pull.connect(ep.clone()).await.expect("pull connect");

    compio::time::sleep(Duration::from_millis(500)).await;

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        pull.recv().await.unwrap();
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

#[expect(clippy::cast_precision_loss)]
fn eprint_pull_summary(ep: &Endpoint, count: u64, elapsed: f64, size: usize) {
    let total_bytes = u128::from(count) * size as u128;
    let msgs_per_sec = count as f64 / elapsed;
    let bytes_per_sec = total_bytes as f64 / elapsed;
    let mib_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    let mbit_per_sec = bytes_per_sec * 8.0 / 1_000_000.0;
    let total_mib = total_bytes as f64 / (1024.0 * 1024.0);

    eprintln!();
    eprintln!("=== PULL ===");
    eprintln!("  Endpoint    : {ep}");
    eprintln!("  Msg size    : {} B", with_commas(&size.to_string()));
    eprintln!("  Elapsed     : {elapsed:.3} s");
    eprintln!("  Messages    : {}", with_commas(&count.to_string()));
    eprintln!(
        "  Throughput  : {} msg/s",
        with_commas(&format!("{msgs_per_sec:.0}"))
    );
    eprintln!(
        "  Bandwidth   : {} MiB/s  ({} Mbit/s)",
        with_commas(&format!("{mib_per_sec:.2}")),
        with_commas(&format!("{mbit_per_sec:.2}"))
    );
    eprintln!(
        "  Total       : {} MiB",
        with_commas(&format!("{total_mib:.2}"))
    );
    eprintln!();
}

fn with_commas(s: &str) -> String {
    let (int_part, dec_part) = s.find('.').map_or((s, ""), |i| s.split_at(i));
    let (sign, digits) = int_part
        .strip_prefix('-')
        .map_or(("", int_part), |d| ("-", d));
    let mut out = String::with_capacity(s.len() + digits.len() / 3 + 1);
    out.push_str(sign);
    let len = digits.len();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.push_str(dec_part);
    out
}

async fn run_rep(ep: Endpoint, size: usize) {
    let rep = Socket::new(SocketType::Rep, bench_options(size));
    let bound = rep.bind(ep).await.expect("rep bind");
    print_bound_port(&bound);
    loop {
        let msg = rep.recv().await.unwrap();
        rep.send(msg).await.unwrap();
    }
}

async fn run_req(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let req = Socket::new(SocketType::Req, bench_options(size));
    req.connect(ep).await.expect("req connect");
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
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

async fn run_inproc_latency(name: String, size: usize, iterations: usize, warmup: usize) {
    use std::sync::{Arc, Barrier};

    let ep = Endpoint::Inproc { name };
    let ready = Arc::new(Barrier::new(2));

    let rep_ep = ep.clone();
    let rep_ready = ready.clone();
    std::thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new()
            .build()
            .expect("rep runtime");
        rt.block_on(async move {
            let rep = Socket::new(SocketType::Rep, Options::default());
            rep.bind(rep_ep).await.unwrap();
            rep_ready.wait();
            loop {
                let msg = rep.recv().await.unwrap();
                rep.send(msg).await.unwrap();
            }
        });
    });

    ready.wait();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.expect("req connect");
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = wall_start.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
}

async fn run_inproc_st_latency(name: String, size: usize, iterations: usize, warmup: usize) {
    let ep = Endpoint::Inproc { name };

    let rep_ep = ep.clone();
    compio::runtime::spawn(async move {
        let rep = Socket::new(SocketType::Rep, Options::default());
        rep.bind(rep_ep).await.unwrap();
        loop {
            let msg = rep.recv().await.unwrap();
            rep.send(msg).await.unwrap();
        }
    })
    .detach();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.expect("req connect");
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = wall_start.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
    std::process::exit(0);
}

async fn run_latency_mt(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    use std::sync::{Arc, Barrier};

    let ready = Arc::new(Barrier::new(2));

    let rep_ep = ep.clone();
    let rep_ready = ready.clone();
    std::thread::spawn(move || {
        let buf_len = (size + 64).next_power_of_two().max(64 * 1024);
        let mut proactor = compio::driver::ProactorBuilder::new();
        proactor.with_omq_buffer_pool_sized(std::num::NonZero::new(64).unwrap(), buf_len);
        let rt = compio::runtime::RuntimeBuilder::new()
            .with_proactor(proactor)
            .build()
            .expect("rep runtime");
        rt.block_on(async move {
            let rep = Socket::new(SocketType::Rep, bench_options(size));
            rep.bind(rep_ep).await.unwrap();
            rep_ready.wait();
            loop {
                let msg = rep.recv().await.unwrap();
                rep.send(msg).await.unwrap();
            }
        });
    });

    ready.wait();
    let req = Socket::new(SocketType::Req, bench_options(size));
    req.connect(ep).await.expect("req connect");
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..warmup {
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(Message::single(payload.clone())).await.unwrap();
        req.recv().await.unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations}");
    std::process::exit(0);
}

async fn run_pub(ep: Endpoint, size: usize) {
    let pub_ = Socket::new(
        SocketType::Pub,
        bench_options(size).on_mute(omq_compio::OnMute::Block),
    );
    let bound = pub_.bind(ep).await.expect("pub bind");
    print_bound_port(&bound);
    let payload = bench_payload(size);
    loop {
        pub_.send(Message::from_slice(&payload)).await.unwrap();
    }
}

async fn run_sub(ep: Endpoint, size: usize, duration: Duration) {
    let sub = Socket::new(SocketType::Sub, bench_options(size));
    sub.connect(ep.clone()).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    compio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        sub.recv().await.unwrap();
        count += 1;
        while sub.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

async fn run_inproc_pubsub(name: String, size: usize, duration: Duration, peers: usize) {
    let ep = Endpoint::Inproc { name };
    let pub_ = Socket::new(
        SocketType::Pub,
        bench_options(size).on_mute(omq_compio::OnMute::Block),
    );
    pub_.bind(ep.clone()).await.unwrap();

    let mut subs = Vec::with_capacity(peers);
    for _ in 0..peers {
        let s = Socket::new(SocketType::Sub, bench_options(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    compio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);
    compio::runtime::spawn(async move {
        loop {
            if pub_.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    })
    .detach();

    // Drain non-measured subs so PUB doesn't stall on HWM.
    for s in subs.drain(1..) {
        compio::runtime::spawn(async move {
            loop {
                if s.recv().await.is_err() {
                    break;
                }
            }
        })
        .detach();
    }

    compio::time::sleep(Duration::from_millis(500)).await;

    let measured = subs.into_iter().next().unwrap();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        measured.recv().await.unwrap();
        count += 1;
        while measured.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
    std::process::exit(0);
}

#[expect(clippy::cast_precision_loss)]
fn cpu_time_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
        let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
        user + sys
    }
}

async fn run_inproc_same_thread(name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).await.unwrap();
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    let payload = Bytes::from(vec![b'x'; size]);
    compio::runtime::spawn(async move {
        loop {
            if push.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    })
    .detach();

    compio::time::sleep(Duration::from_millis(500)).await;

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        pull.recv().await.unwrap();
        count += 1;
        while pull.try_recv().is_ok() {
            count += 1;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
}
