use bytes::Bytes;
use monocoque::rt::LocalRuntime;
use monocoque::zmq::{
    MonocoqueError, Msg, PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, SocketOptions,
    SubSocket,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn cpu() -> f64 {
    let mut r = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes supplied storage.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()) };
    let r = unsafe { r.assume_init() };
    (r.ru_utime.tv_sec + r.ru_stime.tv_sec) as f64
        + (r.ru_utime.tv_usec + r.ru_stime.tv_usec) as f64 / 1e6
}

fn report(port: u16) {
    let Some(ep) = std::env::var_os("OMQ_BENCH_COORD") else {
        return;
    };
    let ctx = zmq::Context::new();
    let s = ctx.socket(zmq::PUSH).expect("coord socket");
    s.connect(ep.to_str().expect("coord endpoint"))
        .expect("coord connect");
    s.send(format!("READY {port}").as_bytes(), 0)
        .expect("coord send");
    std::mem::forget(s);
    std::mem::forget(ctx);
}

async fn barrier() {
    let Some(raw) = std::env::var("OMQ_BENCH_START_AT")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    if raw > now {
        tokio::time::sleep(Duration::from_secs_f64(raw - now)).await;
    }
}

// Write coalescing everywhere except the REQ/REP latency test, which stays
// eager so each request and reply leaves as soon as it is sent. The read
// buffer stays at its default: it starts at 32 KiB and grows by itself, and
// forcing 64 KiB up front halved PULL throughput at 1 KiB.
fn throughput_options() -> SocketOptions {
    SocketOptions::default().with_write_coalescing(true)
}

fn latency_options() -> SocketOptions {
    SocketOptions::default().with_write_coalescing(false)
}

// `bind` returns the bound endpoint with a requested port of 0 resolved.
fn report_endpoint(endpoint: &str) {
    let port = endpoint
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("bound port");
    report(port);
}

// Keep the sender alive until the runner reads its CPU usage and terminates it.
// Receivers normally close first after reporting their measurement.
async fn park_after_disconnect(error: Option<MonocoqueError>) {
    if let Some(error) = error {
        assert!(
            matches!(
                error,
                MonocoqueError::PeerDisconnected(_) | MonocoqueError::Io(_)
            ),
            "peer I/O: {error}"
        );
    }
    std::future::pending::<()>().await;
}

async fn push(s: &str, n: usize) {
    let mut p = PushSocket::with_options(throughput_options());
    report_endpoint(&p.bind(s).await.expect("push bind"));
    while p.peer_count() < 1 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    barrier().await;
    let msg = Msg::from(Bytes::from(vec![b'x'; n]));
    loop {
        if let Err(error) = p.send(msg.clone()).await {
            park_after_disconnect(Some(error)).await;
        }
    }
}

async fn pull(s: &str, n: usize, d: f64) {
    let mut p = PullSocket::with_options(throughput_options());
    p.connect(s).await.expect("pull connect");
    barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let start_cpu = cpu();
    let start = Instant::now();
    let mut count = 0u64;
    let duration = Duration::from_secs_f64(d);
    let _ = tokio::time::timeout(duration, async {
        while start.elapsed() < duration {
            p.recv().await.expect("pull recv");
            count += 1;
        }
    })
    .await;
    println!(
        "{count} {:.6} {n} {:.6}",
        start.elapsed().as_secs_f64(),
        cpu() - start_cpu
    );
}

async fn pub_run(s: &str, n: usize, peers: usize) {
    // Bodies under the vectored-write threshold (16 KiB by default) are copied
    // into the send buffer once per subscriber. At 4 KiB a 4 KiB threshold
    // removes those copies: +23% at 32 subscribers. Lower thresholds cost
    // more at 1 KiB than they save.
    let mut p = PubSocket::with_options(throughput_options().with_vectored_write_threshold(4096));
    report_endpoint(&p.bind(s).await.expect("pub bind"));
    // Subscribers are accepted in the background. Publishing before every
    // subscription has landed would only drop messages, so wait until one
    // message reaches all of them.
    let msg = Msg::from(Bytes::from(vec![b'x'; n]));
    while p.send_ref(&msg).await.expect("pub send") < peers {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    barrier().await;
    // `send_ref` never waits; it skips subscribers at their HWM. Flushing when
    // one was skipped waits for the queued bytes to be written, which lets it
    // catch up and keeps this loop from starving the runtime.
    loop {
        match p.send_ref(&msg).await {
            Ok(delivered) if delivered < p.peer_count() => {
                if let Err(error) = p.flush().await {
                    park_after_disconnect(Some(error)).await;
                }
            }
            Ok(_) => {}
            Err(error) => park_after_disconnect(Some(error)).await,
        }
    }
}

async fn sub(s: &str, n: usize, d: f64, peers: usize) {
    let mut sockets = Vec::with_capacity(peers);
    for _ in 0..peers {
        let mut x = SubSocket::with_options(throughput_options());
        x.connect(s).await.expect("sub connect");
        x.subscribe(b"").await.expect("subscribe");
        sockets.push(x);
    }
    barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let counts = Arc::new(AtomicU64::new(0));
    let start_cpu = cpu();
    let start = Instant::now();
    for mut x in sockets {
        let c = Arc::clone(&counts);
        tokio::task::spawn_local(async move {
            while x.recv().await.is_ok() {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    tokio::time::sleep(Duration::from_secs_f64(d)).await;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{} {:.6} {n} {:.6} {peers} 0 0",
        counts.load(Ordering::Relaxed),
        elapsed,
        cpu() - start_cpu
    );
}

async fn rep(s: &str) {
    let mut r = RepSocket::with_options(latency_options());
    report_endpoint(&r.bind(s).await.expect("rep bind"));
    loop {
        match r.recv_owned().await {
            Ok(msg) => {
                if let Err(error) = r.send(msg).await {
                    park_after_disconnect(Some(error)).await;
                }
            }
            Err(error) => park_after_disconnect(Some(error)).await,
        }
    }
}

async fn req(s: &str, n: usize, iters: usize, warmup: usize) {
    let mut r = ReqSocket::with_options(latency_options());
    r.connect(s).await.expect("req connect");
    let msg = Msg::from(Bytes::from(vec![b'x'; n]));
    r.send(msg.clone()).await.expect("req validation send");
    let reply = r.recv_owned().await.expect("req validation recv");
    assert!(reply == msg, "req reply payload");
    for _ in 0..warmup {
        r.send(msg.clone()).await.expect("req send");
        r.recv().await.expect("req recv");
    }
    let before = cpu();
    let started = Instant::now();
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        r.send(msg.clone()).await.expect("req send");
        r.recv().await.expect("req recv");
        times.push(t.elapsed().as_nanos() as u64);
    }
    times.sort_unstable();
    let q = |p: f64| times[((times.len() - 1) as f64 * p).round() as usize] as f64 / 1000.0;
    println!(
        "{:.3} {:.3} {:.3} {:.3} {iters} {:.6} {:.6}",
        q(0.5),
        q(0.99),
        q(0.999),
        q(1.0),
        cpu() - before,
        started.elapsed().as_secs_f64()
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let rt = LocalRuntime::new().expect("runtime");
    rt.block_on(async move {
        match a.get(1).map(String::as_str) {
            Some("push") => push(&a[2], a[3].parse().unwrap()).await,
            Some("pull") => pull(&a[2], a[3].parse().unwrap(), a[4].parse().unwrap()).await,
            Some("pub") => {
                let peers = a.get(4).map_or(1, |s| s.parse().unwrap());
                pub_run(&a[2], a[3].parse().unwrap(), peers).await;
            }
            Some("multi-sub") => {
                sub(
                    &a[2],
                    a[3].parse().unwrap(),
                    a[4].parse().unwrap(),
                    a[5].parse().unwrap(),
                )
                .await
            }
            Some("rep") => rep(&a[2]).await,
            Some("req") => {
                req(
                    &a[2],
                    a[3].parse().unwrap(),
                    a[4].parse().unwrap(),
                    a[5].parse().unwrap(),
                )
                .await
            }
            _ => panic!("usage: omq_monocoque_bench_peer push|pull|pub|multi-sub|rep|req ..."),
        }
    });
}
