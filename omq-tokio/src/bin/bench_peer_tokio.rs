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

use std::fmt::Write as _;
use std::time::{Duration, Instant};

const MULTI_PULL_DRAIN_BATCH: usize = 64;

fn quantile(sorted: &[f64], probability: f64) -> f64 {
    let index = ((sorted.len().saturating_sub(1)) as f64 * probability).round() as usize;
    sorted[index.min(sorted.len().saturating_sub(1))]
}

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::exclusive::{Options as ExclusiveOptions, Socket as ExclusiveSocket};
use omq_tokio::{Endpoint, Error, Message, MonitorEvent, Options, Socket, SocketType};
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

async fn report_bound_port(ctx: &omq_tokio::Context, ep: &Endpoint) {
    let Endpoint::Tcp { port, .. } = ep else {
        return;
    };
    let Ok(coord_ep) = std::env::var("OMQ_BENCH_COORD") else {
        println!("PORT {port}");
        return;
    };
    let coord = ctx.socket(SocketType::Push, Options::default());
    coord
        .connect(coord_ep.parse().expect("valid coord endpoint"))
        .await
        .unwrap();
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

#[expect(clippy::too_many_lines)]
async fn async_main(args: Vec<String>, ctx: omq_tokio::Context) {
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
    match args.get(1).map(String::as_str) {
        Some("push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            run_push(&ctx, ep, size, peers).await;
        }
        Some("pub-fanout") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let peers: usize = args[4].parse().expect("peers");
            run_push_fanout(&ctx, ep, size, peers).await;
        }
        Some("pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull(&ctx, ep, size, Duration::from_secs_f64(duration)).await;
        }
        Some("inproc") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_inproc(name, size, Duration::from_secs_f64(duration)).await;
        }
        Some("rep") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep(&ctx, ep, size).await;
        }
        Some("rep-exclusive") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            run_rep_exclusive(&ctx, ep, size).await;
        }
        Some("req") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_req(&ctx, ep, size, iterations, warmup).await;
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
            run_pub(&ctx, ep, size, peers).await;
        }
        Some("sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_sub(&ctx, ep, size, Duration::from_secs_f64(duration)).await;
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
            run_push_connect(&ctx, ep, size).await;
        }
        Some("pull-bind") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            run_pull_bind(&ctx, ep, size, Duration::from_secs_f64(duration)).await;
        }
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
        Some("inproc-latency") => {
            let name = args[2].clone();
            let size: usize = args[3].parse().expect("msg_size");
            let iterations: usize = args[4].parse().expect("iterations");
            let warmup: usize = args[5].parse().expect("warmup");
            run_inproc_latency(name, size, iterations, warmup).await;
        }
        Some("multi-pull") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_pull(&ctx, ep, size, Duration::from_secs_f64(duration), count).await;
        }
        Some("multi-sub") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let duration: f64 = args[4].parse().expect("duration_secs");
            let count: usize = args[5].parse().expect("socket_count");
            run_multi_sub(&ctx, ep, size, Duration::from_secs_f64(duration), count).await;
        }
        Some("multi-push") => {
            let ep = parse_ep(&args[2]);
            let size: usize = args[3].parse().expect("msg_size");
            let count: usize = args[4].parse().expect("socket_count");
            run_multi_push(&ctx, ep, size, count).await;
        }
        _ => {
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
            std::process::exit(1);
        }
    }
}

async fn run_pub(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, peers: usize) {
    let mut opts = bench_options_server(size);
    opts.xpub_nodrop = true;
    let pub_ = ctx.socket(SocketType::Pub, opts);
    let monitor = pub_.monitor();
    let bound = pub_.bind(ep).await.expect("pub bind");
    report_bound_port(ctx, &bound).await;
    if peers > 0 {
        wait_for_subscribes(&pub_, monitor, peers).await;
    }
    let payload = bench_payload(size);
    wait_for_warmup_barrier().await;
    run_push_warmup(&pub_, &payload).await;
    wait_for_start_barrier().await;
    if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
        loop {
            pub_.send(Message::from_slice(&payload)).await.unwrap();
        }
    } else {
        let msg = Message::single(payload.clone());
        loop {
            pub_.send(msg.clone()).await.unwrap();
        }
    }
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

async fn wait_for_subscribes(sock: &Socket, mut monitor: omq_tokio::MonitorStream, peers: usize) {
    let timeout_secs = if peers > 64 { 20 } else { 10 };
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut subscribed = 0;
    while subscribed < peers {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {peers} subscribers ({subscribed} so far)"
        );
        match tokio::time::timeout(deadline - now, monitor.recv()).await {
            Ok(Ok(MonitorEvent::SubscribeReceived { .. })) => subscribed += 1,
            Ok(Ok(_)) => {}
            Ok(Err(omq_tokio::MonitorRecvError::Lagged(n))) => {
                // Monitor buffer overflowed. The skipped events likely
                // include subscribes. Use handshaked connection count as
                // the new floor and keep polling.
                let connected = sock.connections().await.map_or(0, |c| {
                    c.into_iter().filter(|c| c.peer_info.is_some()).count()
                });
                subscribed = subscribed.max(connected);
                eprintln!(
                    "monitor lagged {n}, connections={connected}, \
                     subscribed={subscribed}/{peers}"
                );
            }
            Ok(Err(e)) => panic!("monitor closed while waiting for subscribers: {e:?}"),
            Err(e) => panic!("timed out waiting for {peers} subscribers: {e:?}"),
        }
    }
    // Let any remaining subscribe commands finish processing.
    tokio::time::sleep(Duration::from_millis(100)).await;
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
    } else {
        let msg = Message::single(payload.clone());
        loop {
            send_fast(sock, msg.clone()).await;
        }
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

fn recv_timer_check_interval(size: usize) -> usize {
    if size <= 1024 { 4096 } else { 256 }
}

async fn recv_loop(sock: &Socket, duration: Duration, size: usize) -> (u64, f64) {
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let timer_check_interval = recv_timer_check_interval(size);
    let mut until_timer_check = timer_check_interval;
    let mut count: u64 = 0;

    loop {
        if !recv_before_deadline(sock, deadline).await {
            break;
        }
        count += 1;
        until_timer_check -= 1;
        if until_timer_check == 0 {
            if Instant::now() >= deadline {
                break;
            }
            until_timer_check = timer_check_interval;
        }
        while sock.try_recv().is_ok() {
            count += 1;
            until_timer_check -= 1;
            if until_timer_check == 0 {
                if Instant::now() >= deadline {
                    return (count, t0.elapsed().as_secs_f64());
                }
                until_timer_check = timer_check_interval;
            }
        }
    }

    (count, t0.elapsed().as_secs_f64())
}

async fn run_sub(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, duration: Duration) {
    let sub = ctx.socket(SocketType::Sub, bench_options_client(size));
    sub.connect(ep.clone()).await.expect("sub connect");
    sub.subscribe(Bytes::new()).await.expect("subscribe");

    wait_for_start_barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if !recv_before_deadline(&sub, deadline).await {
            break;
        }
        count += 1;
        while sub.try_recv().is_ok() {
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

async fn run_inproc_pubsub(name: String, size: usize, duration: Duration, peers: usize) {
    let ep = Endpoint::Inproc { name };
    let mut opts = bench_options(size);
    opts.xpub_nodrop = true;
    let pub_ = Socket::new(SocketType::Pub, opts);
    pub_.bind(ep.clone()).await.expect("pub bind");

    let mut subs = Vec::with_capacity(peers);
    for _ in 0..peers {
        let s = Socket::new(SocketType::Sub, bench_options(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        subs.push(s);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let pub_handle = tokio::spawn(async move {
        loop {
            if pub_.send(Message::single(payload.clone())).await.is_err() {
                break;
            }
        }
    });

    // Drain all non-measured subscribers so PUB doesn't block on HWM.
    let mut drain_handles = Vec::new();
    let stop_c = stop.clone();
    for s in subs.drain(1..) {
        let stop_c = stop_c.clone();
        drain_handles.push(tokio::spawn(async move {
            while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                if s.recv().await.is_err() {
                    break;
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

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
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    pub_handle.abort();
    for h in drain_handles {
        h.abort();
    }
    println!("{count} {elapsed:.6} {size}");
}

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
async fn run_multi_pull(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    use std::sync::atomic::{AtomicU64, Ordering as AO};

    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Pull, bench_options_client(size));
        s.connect(ep.clone()).await.expect("pull connect");
        sockets.push(s);
    }

    wait_for_warmup_barrier().await;
    let warmup_deadline = Instant::now()
        + std::env::var("OMQ_BENCH_WARMUP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::ZERO, Duration::from_millis);
    let mut warmup_handles = Vec::with_capacity(socket_count);
    for sock in &sockets {
        let sock = sock.clone();
        warmup_handles.push(tokio::spawn(async move {
            while Instant::now() < warmup_deadline {
                while sock.try_recv().is_ok() {}
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in warmup_handles {
        let _ = h.await;
    }
    wait_for_start_barrier().await;

    let counters: Vec<_> = (0..socket_count)
        .map(|_| std::sync::Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;

    let mut handles = Vec::with_capacity(socket_count);
    for (sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            let mut n: u64 = 0;
            loop {
                if !recv_before_deadline(&sock, deadline).await {
                    break;
                }
                n += 1;
                for _ in 1..MULTI_PULL_DRAIN_BATCH {
                    if sock.try_recv().is_err() {
                        break;
                    }
                    n += 1;
                }
                tokio::task::yield_now().await;
                if Instant::now() >= deadline {
                    break;
                }
            }
            counter.store(n, AO::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(AO::Relaxed)).collect();
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

#[expect(clippy::cast_precision_loss)]
async fn run_multi_sub(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    duration: Duration,
    socket_count: usize,
) {
    use std::sync::atomic::{AtomicU64, Ordering as AO};

    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Sub, bench_options_client(size));
        s.connect(ep.clone()).await.expect("sub connect");
        s.subscribe(Bytes::new()).await.expect("subscribe");
        sockets.push(s);
    }

    wait_for_warmup_barrier().await;
    let warmup_deadline = Instant::now()
        + std::env::var("OMQ_BENCH_WARMUP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::ZERO, Duration::from_millis);
    let mut warmup_handles = Vec::with_capacity(socket_count);
    for sock in &sockets {
        let sock = sock.clone();
        warmup_handles.push(tokio::spawn(async move {
            while Instant::now() < warmup_deadline {
                while sock.try_recv().is_ok() {}
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in warmup_handles {
        let _ = h.await;
    }
    wait_for_start_barrier().await;

    let counters: Vec<_> = (0..socket_count)
        .map(|_| std::sync::Arc::new(AtomicU64::new(0)))
        .collect();
    let cpu_before = cpu_time_secs();
    let t0 = Instant::now();
    let deadline = t0 + duration;

    let mut handles = Vec::with_capacity(socket_count);
    for (sock, counter) in sockets.into_iter().zip(counters.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            let mut n: u64 = 0;
            loop {
                if !recv_before_deadline(&sock, deadline).await {
                    break;
                }
                n += 1;
                while sock.try_recv().is_ok() {
                    n += 1;
                }
                if Instant::now() >= deadline {
                    break;
                }
            }
            counter.store(n, AO::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let cpu = cpu_time_secs() - cpu_before;
    let per_socket: Vec<u64> = counters.iter().map(|c| c.load(AO::Relaxed)).collect();
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

async fn run_multi_push(ctx: &omq_tokio::Context, ep: Endpoint, size: usize, socket_count: usize) {
    let mut sockets = Vec::with_capacity(socket_count);
    for _ in 0..socket_count {
        let s = ctx.socket(SocketType::Push, bench_options_client(size));
        s.connect(ep.clone()).await.expect("push connect");
        sockets.push(s);
    }

    wait_for_start_barrier().await;

    let payload = bench_payload(size);
    let mut handles = Vec::with_capacity(socket_count);
    for sock in sockets {
        let p = payload.clone();
        handles.push(tokio::spawn(async move {
            run_push_loop(&sock, &p).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

fn drain_pending(sock: &Socket) {
    let deadline = Instant::now() + Duration::from_millis(2);
    for _ in 0..256 {
        if Instant::now() >= deadline || sock.try_recv().is_err() {
            break;
        }
    }
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

async fn run_inproc_latency(name: String, size: usize, iterations: usize, warmup: usize) {
    let ep = Endpoint::Inproc { name };

    let rep_ep = ep.clone();
    tokio::spawn(async move {
        let rep = Socket::new(SocketType::Rep, Options::default());
        rep.bind(rep_ep).await.expect("rep bind");
        loop {
            let msg = rep.recv().await.unwrap();
            rep.send(msg).await.unwrap();
        }
    });

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.expect("req connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from(vec![b'x'; size]);
    let msg = Message::single(payload);

    for _ in 0..warmup {
        req.send(msg.clone()).await.unwrap();
        req.recv().await.unwrap();
    }

    let mut rtts = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).await.unwrap();
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

async fn run_rep(ctx: &omq_tokio::Context, ep: Endpoint, size: usize) {
    let rep = ctx.socket(SocketType::Rep, bench_options(size));
    let bound = rep.bind(ep).await.expect("rep bind");
    report_bound_port(ctx, &bound).await;
    loop {
        let msg = rep.recv().await.unwrap();
        rep.send(msg).await.unwrap();
    }
}

async fn run_rep_exclusive(ctx: &omq_tokio::Context, ep: Endpoint, _size: usize) {
    let (mut rep, bound) = ExclusiveSocket::bind(SocketType::Rep, ep, ExclusiveOptions::default())
        .await
        .expect("exclusive rep bind");
    report_bound_port(ctx, &bound).await;
    loop {
        let msg = match rep.recv().await {
            Ok(msg) => msg,
            Err(Error::Closed | Error::Io(_)) => continue,
            Err(error) => panic!("{error}"),
        };
        match rep.send(&msg).await {
            Ok(()) | Err(Error::Closed | Error::Io(_)) => {}
            Err(error) => panic!("{error}"),
        }
    }
}

async fn run_req(
    ctx: &omq_tokio::Context,
    ep: Endpoint,
    size: usize,
    iterations: usize,
    warmup: usize,
) {
    let req = ctx.socket(SocketType::Req, bench_options(size));
    req.connect(ep).await.expect("req connect");

    tokio::time::sleep(Duration::from_millis(200)).await;

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

async fn run_req_exclusive(ep: Endpoint, size: usize, iterations: usize, warmup: usize) {
    let mut req = ExclusiveSocket::connect(SocketType::Req, ep, ExclusiveOptions::default())
        .await
        .expect("exclusive req connect");

    let payload = Bytes::from(vec![b'x'; size]);
    let message = Message::single(payload);

    for _ in 0..warmup {
        req.send(&message).await.unwrap();
        req.recv().await.unwrap();
    }

    let t_wall = Instant::now();
    let cpu_before = cpu_time_secs();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(&message).await.unwrap();
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

fn bench_payload(size: usize) -> Bytes {
    if std::env::var("OMQ_BENCH_PAYLOAD").as_deref() == Ok("json") {
        json_payload_random(size)
    } else {
        Bytes::from(vec![b'x'; size])
    }
}

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn json_payload_seeded(target_bytes: usize, seed: u32) -> Bytes {
    let mut out = String::with_capacity(target_bytes + 512);
    let mut state: u32 = seed;
    while out.len() < target_bytes {
        json_record(&mut out, &mut state);
    }
    out.truncate(target_bytes);
    Bytes::from(out)
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

#[cfg(any(feature = "lz4", feature = "zstd"))]
fn json_dict_samples() -> Vec<Bytes> {
    // Bias toward common message sizes (512B, 1 KiB) with a few at other sizes.
    let sample_sizes: &[(usize, usize)] = &[
        (64, 2),
        (128, 2),
        (256, 4),
        (512, 8),
        (1024, 8),
        (2048, 4),
        (4096, 4),
    ];
    let mut samples = Vec::new();
    for &(size, count) in sample_sizes {
        for i in 1..=count {
            samples.push(json_payload_seeded(size, i as u32));
        }
    }
    samples
}

#[cfg(feature = "lz4")]
fn train_lz4_json_dict(capacity: usize) -> Vec<u8> {
    use omq_proto::proto::transform::lz4::DictTrainer;

    let mut trainer = DictTrainer::new(capacity);
    for payload in json_dict_samples() {
        trainer.add_sample(&payload);
    }
    trainer.train()
}

#[cfg(feature = "zstd")]
fn train_zstd_json_dict(capacity: usize) -> Vec<u8> {
    let samples = json_dict_samples();
    let refs: Vec<&[u8]> = samples.iter().map(Bytes::as_ref).collect();
    omq_proto::proto::transform::train_zdict(&refs, capacity)
        .expect("zstd dictionary training must produce a dict")
        .to_vec()
}

fn wire_size(ep: &Endpoint, size: usize) -> usize {
    use omq_proto::proto::transform::MessageEncoder;
    let options = bench_options(size);
    let Ok(Some((mut enc, _dec))) = MessageEncoder::for_endpoint(ep, &options) else {
        return size;
    };
    let payload = json_payload_random(size);
    let msg = Message::single(payload.clone());
    let frames = enc.encode(&msg).unwrap();
    frames.last().map_or(size, Message::byte_len)
}
