//! Two-process benchmark peer for omq-tokio.
//!
//! Throughput (PUSH/PULL):
//!   `bench_peer_tokio` push \<addr\> \<`msg_size`\>
//!   `bench_peer_tokio` pull \<addr\> \<`msg_size`\> \<`duration_secs`\>
//!   `bench_peer_tokio` inproc \<name\> \<`msg_size`\> \<`duration_secs`\>
//!
//! Latency (REQ/REP):
//!   `bench_peer_tokio` rep \<addr\> \<`msg_size`\>
//!   `bench_peer_tokio` req \<addr\> \<`msg_size`\> \<iterations\> \<warmup\>
//!   `bench_peer_tokio` rep-exclusive \<addr\> \<`msg_size`\>
//!   `bench_peer_tokio` req-exclusive \<addr\> \<`msg_size`\> \<iterations\> \<warmup\>
//!
//! \<addr\>: a port number (`4000`), an `ip:port` pair (`0.0.0.0:4000`),
//!   a full URI (`tcp://0.0.0.0:4000`), or an IPC path (`ipc:///tmp/foo.sock`).
//!
//! Push: binds, sends \<`msg_size`\> byte messages forever.
//! Pull: connects, warms up for 500 ms, then counts messages for \<duration\>
//!       seconds and prints raw stats to stdout (for scripts) and a
//!       human-readable summary to stderr.
//! Rep: binds, echoes every received message back. Killed by SIGTERM.
//! Req: connects, runs warmup + measured round-trips, prints:
//!         \<`p50_us`\> \<`p99_us`\> \<`p999_us`\> \<`max_us`\> \<iterations\>

use std::time::{Duration, Instant};

const MULTI_PULL_DRAIN_BATCH: usize = 64;
const RECV_BATCH_BYTES: usize = 1024 * 1024;

fn quantile(sorted: &[f64], probability: f64) -> f64 {
    let index = ((sorted.len().saturating_sub(1)) as f64 * probability).round() as usize;
    sorted[index.min(sorted.len().saturating_sub(1))]
}

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};
use std::net::Ipv4Addr;

mod latency_common;
mod payload_corpus;
mod pushpull_common;
#[cfg(any(feature = "ws", feature = "quic"))]
mod ws_bench_config;

#[path = "bench_peer_tokio/fanin.rs"]
mod fanin;
#[path = "bench_peer_tokio/latency.rs"]
mod latency;
#[path = "bench_peer_tokio/payload.rs"]
mod payload;
#[path = "bench_peer_tokio/pubsub.rs"]
mod pubsub;

use fanin::{drain_pending, run_multi_pull, run_multi_push};
use latency::{run_inproc_latency, run_rep, run_rep_exclusive, run_req, run_req_exclusive};
#[cfg(feature = "lz4")]
use payload::train_lz4_json_dict;
#[cfg(feature = "zstd")]
use payload::train_zstd_json_dict;
use payload::{bench_payload, wire_size};
use pubsub::{run_inproc_pubsub, run_multi_sub, run_pub, run_sub};

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

async fn report_bound_port(ctx: &omq_tokio::Context, ep: &Endpoint) {
    let port = match ep {
        Endpoint::Tcp { port, .. } => *port,
        #[cfg(feature = "lz4")]
        Endpoint::Lz4Tcp { port, .. } => *port,
        #[cfg(feature = "zstd")]
        Endpoint::ZstdTcp { port, .. } => *port,
        #[cfg(feature = "ws")]
        Endpoint::Ws { port, .. } | Endpoint::Wss { port, .. } => *port,
        #[cfg(all(feature = "ws", feature = "lz4"))]
        Endpoint::Lz4Ws { port, .. } => *port,
        #[cfg(feature = "quic")]
        Endpoint::Quic { port, .. } => *port,
        _ => return,
    };
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        println!("PORT {port}");
        return;
    };
    let coord = ctx.socket(SocketType::Push, Options::default());
    coord.connect(coord_ep).await.unwrap();
    coord
        .wait_connected(1, Duration::from_secs(5))
        .await
        .unwrap();
    let msg = format!("READY {port}");
    coord
        .send(Message::from_slice(msg.as_bytes()))
        .await
        .unwrap();
    // Keep the socket alive so the driver task flushes the message.
    // The bench process exits via SIGTERM/SIGALRM anyway.
    std::mem::forget(coord);
}

#[cfg(unix)]
extern "C" fn exit_on_signal(_sig: libc::c_int) {
    unsafe { libc::_exit(0) };
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    #[cfg(any(feature = "ws", feature = "quic"))]
    ws_bench_config::set_endpoint(args.get(2).map(String::as_str));

    let config = omq_tokio::ContextConfig::from_env();
    if std::env::var("OMQ_IO_THREADS").is_ok() && config.io_threads != 0 {
        let ctx = omq_tokio::Context::with_config(config);
        let n = ctx.io_threads();
        eprintln!("runtime: {n} x current_thread (dedicated)");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async_main(args, ctx));
    } else if std::env::var("OMQ_BENCH_MT_RUNTIME").is_ok() {
        #[cfg(feature = "bench-mt-runtime")]
        {
            let workers = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
            eprintln!("runtime: multi_thread ({workers} workers)");
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let ctx = omq_tokio::Context::current();
                async_main(args, ctx).await;
            });
        }
        #[cfg(not(feature = "bench-mt-runtime"))]
        panic!("OMQ_BENCH_MT_RUNTIME requires the bench-mt-runtime feature");
    } else {
        eprintln!("runtime: current_thread");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let ctx = omq_tokio::Context::current();
            async_main(args, ctx).await;
        });
    }
}

/// Exit cleanly on SIGTERM/SIGINT, and after 60 s as a hang guard.
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

async fn async_main(args: Vec<String>, ctx: omq_tokio::Context) {
    install_signals();

    let mode = args.get(1).map(String::as_str);
    let handled = run_pipeline_mode(&ctx, mode, &args).await
        || run_pattern_mode(&ctx, mode, &args).await
        || run_inproc_mode(mode, &args).await
        || run_tool_mode(mode, &args);
    if !handled {
        print_usage();
        std::process::exit(1);
    }
}

/// PUSH/PULL modes over a network endpoint. Returns false for other modes.
async fn run_pipeline_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_push(ctx, ep, size, peers).await;
        }
        Some("pub-fanout") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args[4].parse().expect("peers");
            run_push_fanout(ctx, ep, size, peers).await;
        }
        Some("pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(ctx, ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("push-connect") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_push_connect(ctx, ep, size).await;
        }
        Some("pull-bind") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(ctx, ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("multi-pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_pull(ctx, ep, size, Duration::from_secs_f64(duration), count).await;
        }
        Some("multi-push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let count: usize = args[4].parse().expect("socket_count");
            run_multi_push(ctx, ep, size, count).await;
        }
        _ => return false,
    }
    true
}

/// REQ/REP and PUB/SUB modes over a network endpoint. Returns false for
/// other modes.
async fn run_pattern_mode(ctx: &omq_tokio::Context, mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("rep") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep(ctx, ep, size).await;
        }
        Some("rep-exclusive") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep_exclusive(ctx, ep, size).await;
        }
        Some("req") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req(ctx, ep, size, iterations, warmup).await;
        }
        Some("req-exclusive") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req_exclusive(ep, size, iterations, warmup).await;
        }
        Some("pub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_pub(ctx, ep, size, peers).await;
        }
        Some("sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_sub(ctx, ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("multi-sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_sub(ctx, ep, size, Duration::from_secs_f64(duration), count).await;
        }
        _ => return false,
    }
    true
}

/// In-process modes. Returns false for other modes.
async fn run_inproc_mode(mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("inproc") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc(name, size, Duration::from_secs_f64(duration)).await;
        }
        Some("inproc-pubsub") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let peers: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
            run_inproc_pubsub(name, size, Duration::from_secs_f64(duration), peers).await;
        }
        Some("inproc-latency") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(name, size, iterations, warmup).await;
        }
        _ => return false,
    }
    true
}

/// Offline helpers: wire-size estimates and dictionary training. Returns
/// false for other modes.
fn run_tool_mode(mode: Option<&str>, args: &[String]) -> bool {
    match mode {
        Some("wire-size") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            println!("{}", wire_size(&ep, size));
        }
        #[cfg(feature = "lz4")]
        Some("train-dict") => {
            let path = &args[2];
            let capacity: usize = args.get(3).map_or(2048, |s| s.parse().expect("capacity"));
            let dict = train_lz4_json_dict(capacity);
            std::fs::write(path, &dict).expect("write dict file");
            eprintln!("Trained {} byte dict -> {path}", dict.len());
        }
        #[cfg(feature = "zstd")]
        Some("train-zstd-dict") => {
            let path = &args[2];
            let capacity: usize = args.get(3).map_or(2048, |s| s.parse().expect("capacity"));
            let dict = train_zstd_json_dict(capacity);
            std::fs::write(path, &dict).expect("write dict file");
            eprintln!("Trained {} byte zstd dict -> {path}", dict.len());
        }
        _ => return false,
    }
    true
}

fn print_usage() {
    eprintln!("usage: bench_peer_tokio push <addr> <size>");
    eprintln!("       bench_peer_tokio pub-fanout <addr> <size> <peers>");
    eprintln!("       bench_peer_tokio pull <addr> <size> <duration_secs>");
    eprintln!("       bench_peer_tokio multi-pull <addr> <size> <duration_secs> <count>");
    eprintln!("       bench_peer_tokio multi-sub <addr> <size> <duration_secs> <count>");
    eprintln!("       bench_peer_tokio multi-push <addr> <size> <count>");
    eprintln!("       bench_peer_tokio inproc <name> <size> <duration_secs>");
    eprintln!("       bench_peer_tokio rep <addr> <size>");
    eprintln!("       bench_peer_tokio req <addr> <size> <iterations> <warmup>");
    eprintln!("       bench_peer_tokio rep-exclusive <addr> <size>");
    eprintln!("       bench_peer_tokio req-exclusive <addr> <size> <iterations> <warmup>");
    eprintln!("       bench_peer_tokio inproc-latency <name> <size> <iterations> <warmup>");
    eprintln!("<addr>: port number or full endpoint (tcp:// ipc://)");
}

async fn wait_for_start_barrier() {
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    if start_at > now {
        tokio::time::sleep(Duration::from_secs_f64(start_at - now)).await;
    }
}

async fn wait_for_warmup_barrier() {
    let warmup = std::env::var("OMQ_BENCH_WARMUP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis);
    let Some(warmup) = warmup else {
        wait_for_start_barrier().await;
        return;
    };
    let Some(start_at) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    let warmup_at = start_at - warmup.as_secs_f64();
    if warmup_at > now {
        tokio::time::sleep(Duration::from_secs_f64(warmup_at - now)).await;
    }
}

async fn wait_for_handshakes(sock: &Socket, mut monitor: omq_tokio::MonitorStream, peers: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let connected = sock
            .connections()
            .await
            .expect("connection snapshot")
            .into_iter()
            .filter(|c| c.peer_info.is_some())
            .count();
        if connected >= peers {
            return;
        }
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {peers} handshaked peers"
        );
        let _ = tokio::time::timeout(deadline - now, monitor.recv()).await;
    }
}

async fn run_push_fanout(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let push = ctx.socket(SocketType::Push, bench_options(size));
    let monitor = push.monitor();
    let bound = push.bind(ep).await.expect("push bind");
    report_bound_port(ctx, &bound).await;
    wait_for_handshakes(&push, monitor, peers).await;
    let payload = bench_payload(size);
    wait_for_warmup_barrier().await;
    run_push_warmup(&push, &payload).await;
    wait_for_start_barrier().await;
    run_push_loop(&push, &payload).await;
}

#[allow(clippy::verbose_bit_mask)]
async fn send_fast(sock: &Socket, msg: Message) {
    match sock.try_send(msg) {
        Ok(()) => {}
        Err(omq_tokio::TrySendError::Full(msg)) => sock.send(msg).await.unwrap(),
        Err(e) => panic!("try_send failed: {e}"),
    }
}

async fn run_push_loop(sock: &Socket, payload: &Bytes) {
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            send_fast(sock, Message::from_slice(payload)).await;
        }
    }
    let msg = Message::single(payload.clone());
    loop {
        send_fast(sock, msg.clone()).await;
    }
}

async fn run_push_warmup(sock: &Socket, payload: &Bytes) {
    let duration = std::env::var("OMQ_BENCH_WARMUP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::ZERO, Duration::from_millis);
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        send_fast(sock, Message::from_slice(payload)).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Deadline-bounded blocking recv. Returns `true` when a message arrived,
/// `false` if the deadline elapsed first or the socket closed. Bounding the
/// blocking recv keeps a measured loop from hanging forever when a peer
/// delivers zero messages; the hot drain still uses `try_recv` so throughput
/// is unaffected.
async fn recv_before_deadline(sock: &Socket, deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    matches!(
        tokio::time::timeout(deadline - now, sock.recv()).await,
        Ok(Ok(_))
    )
}

async fn recv_loop(sock: &Socket, duration: Duration, size: usize) -> (u64, f64) {
    let batch = pushpull_common::receive_batch(size);
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;

    loop {
        match sock.try_recv() {
            Ok(_) => {}
            Err(Error::WouldBlock) => {
                if !recv_before_deadline(sock, deadline).await {
                    break;
                }
            }
            Err(error) => panic!("throughput receive failed: {error}"),
        }
        let mut received = 1;
        for _ in 1..batch {
            match sock.try_recv() {
                Ok(_) => received += 1,
                Err(Error::WouldBlock) => break,
                Err(error) => panic!("throughput receive failed: {error}"),
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        count += received;
        tokio::task::coop::consume_budget().await;
    }

    (count, t0.elapsed().as_secs_f64())
}

fn bench_options(msg_size: usize) -> Options {
    let mut o = Options::default();
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
    if let Ok(val) = std::env::var("OMQ_BENCH_ARENA_THRESHOLD") {
        o = o.arena_threshold(val.parse().expect("OMQ_BENCH_ARENA_THRESHOLD"));
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
        Some(other) => panic!("unknown OMQ_BENCH_MECHANISM: {other}"),
    }
}

fn mechanism_env() -> Option<String> {
    std::env::var("OMQ_BENCH_MECHANISM")
        .ok()
        .map(|s| s.to_ascii_lowercase())
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

async fn run_push(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let push = ctx.socket(SocketType::Push, bench_options_server(size));
    let monitor = push.monitor();
    let bound = push.bind(ep).await.expect("push bind");
    report_bound_port(ctx, &bound).await;
    if peers > 0 {
        wait_for_handshakes(&push, monitor, peers).await;
    }
    let payload = bench_payload(size);
    wait_for_warmup_barrier().await;
    run_push_warmup(&push, &payload).await;
    wait_for_start_barrier().await;
    run_push_loop(&push, &payload).await;
}

async fn run_push_connect(ctx: &omq_tokio::Context, ep: Endpoint, size: usize) {
    let push = ctx.socket(SocketType::Push, bench_options_client(size));
    push.connect(ep).await.expect("push connect");
    let payload = bench_payload(size);
    run_push_loop(&push, &payload).await;
}

async fn run_pull_bind(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let pull = ctx.socket(SocketType::Pull, bench_options_server(size));
    let bound = pull.bind(ep.clone()).await.expect("pull bind");
    report_bound_port(ctx, &bound).await;

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_pending(&pull);

    let cpu_before = cpu_time_secs();
    let (count, elapsed) = recv_loop(&pull, duration, size).await;
    let cpu = cpu_time_secs() - cpu_before;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    eprint_pull_summary(&ep, count, elapsed, size);
}

async fn run_inproc(name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = Socket::new(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).await.expect("push bind");
    let pull = Socket::new(SocketType::Pull, bench_options(size));
    pull.connect(ep).await.expect("pull connect");

    let payload = Bytes::from(vec![b'x'; size]);
    tokio::spawn(async move {
        loop {
            if push.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let (count, elapsed) = recv_loop(&pull, duration, size).await;
    println!("{count} {elapsed:.6} {size}");
}

async fn run_pull(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let pull = ctx.socket(SocketType::Pull, bench_options_client(size));
    pull.connect(ep.clone()).await.expect("pull connect");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_pending(&pull);

    let cpu_before = cpu_time_secs();
    let (count, elapsed) = recv_loop(&pull, duration, size).await;
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
    // Windows doesn't have rusage; return 0.0 as a placeholder.
    0.0
}
