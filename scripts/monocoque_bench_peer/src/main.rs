use bytes::Bytes;
use monocoque::rt::{LocalRuntime, TcpListener, TcpStream};
use monocoque::zmq::{
    PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, SocketOptions, SubSocket,
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

fn addr(s: &str) -> String {
    s.strip_prefix("tcp://").unwrap_or(s).to_owned()
}

// Monocoque clamps the read buffer to its 64 KiB read slab, and its own default
// is 32 KiB. A 16 KiB read buffer is therefore below the untuned default and
// halves the read batch. Bulk transfers want the full slab.
const READ_SLAB: usize = 64 * 1024;

fn throughput_options() -> SocketOptions {
    SocketOptions::default()
        .with_buffer_sizes(READ_SLAB, 64 * 1024)
        .with_write_coalescing(true)
}

fn latency_options() -> SocketOptions {
    SocketOptions::default().with_write_coalescing(false)
}

fn receive_options() -> SocketOptions {
    SocketOptions::default().with_buffer_sizes(READ_SLAB, 8 * 1024)
}

async fn listener(s: &str) -> (TcpListener, u16) {
    let l = TcpListener::bind(addr(s)).await.expect("bind");
    let p = l.local_addr().expect("local addr").port();
    report(p);
    (l, p)
}

// Keep the sender alive until the runner reads its CPU usage and terminates it.
// Receivers normally close first after reporting their measurement.
async fn park_after_disconnect(error: Option<std::io::Error>) {
    if let Some(error) = error {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            ),
            "peer I/O: {error}"
        );
    }
    std::future::pending::<()>().await;
}

async fn push(s: &str, n: usize) {
    let (l, _) = listener(s).await;
    let (stream, _) = l.accept().await.expect("accept");
    let mut p = PushSocket::from_tcp_with_options(stream, throughput_options())
        .await
        .expect("push handshake");
    barrier().await;
    let msg = Bytes::from(vec![b'x'; n]);
    loop {
        if let Err(error) = p.send_one(msg.clone()).await {
            park_after_disconnect(Some(error)).await;
        }
    }
}

async fn pull(s: &str, n: usize, d: f64) {
    let mut p = PullSocket::connect_with_options(addr(s), receive_options())
        .await
        .expect("pull connect");
    barrier().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let start_cpu = cpu();
    let start = Instant::now();
    let mut count = 0u64;
    let mut buf: Vec<Bytes> = Vec::with_capacity(4);
    let duration = Duration::from_secs_f64(d);
    let _ = tokio::time::timeout(duration, async {
        while start.elapsed() < duration {
            assert!(p.recv_into(&mut buf).await.expect("pull recv"), "pull EOF");
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
    // Default to 1 so the chart is reproducible. Set MONOCOQUE_PUB_WORKERS to
    // compare against Monocoque's default, which picks CPU count clamped to
    // [2, 16]. This baseline uses the default PUB socket options.
    let workers: usize = std::env::var("MONOCOQUE_PUB_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut p = PubSocket::bind_with_workers(addr(s), workers)
        .await
        .expect("pub bind");
    report(p.local_addr().expect("local addr").port());
    for _ in 0..peers {
        p.accept_subscriber().await.expect("sub accept");
    }
    barrier().await;
    let msg = Bytes::from(vec![b'x'; n]);
    loop {
        if let Err(error) = p.send_frames(std::slice::from_ref(&msg)).await {
            park_after_disconnect(Some(error)).await;
        }
    }
}

async fn sub(s: &str, n: usize, d: f64, peers: usize) {
    let mut sockets = Vec::with_capacity(peers);
    for _ in 0..peers {
        let stream = TcpStream::connect(addr(s)).await.expect("sub connect");
        let mut x = SubSocket::from_tcp_with_options(stream, receive_options())
            .await
            .expect("sub connect");
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
            let mut buf: Vec<Bytes> = Vec::with_capacity(4);
            while x.recv_into(&mut buf).await.unwrap_or(false) {
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
    let (l, _) = listener(s).await;
    let (stream, _) = l.accept().await.expect("accept");
    let mut r = RepSocket::from_tcp_with_options(stream, latency_options())
        .await
        .expect("rep handshake");
    loop {
        match r.recv().await {
            Ok(Some(msg)) => {
                if let Err(error) = r.send(msg).await {
                    park_after_disconnect(Some(error)).await;
                }
            }
            Ok(None) => park_after_disconnect(None).await,
            Err(error) => park_after_disconnect(Some(error)).await,
        }
    }
}

async fn req(s: &str, n: usize, iters: usize, warmup: usize) {
    let mut r = ReqSocket::connect_with_options(s, latency_options())
        .await
        .expect("req connect");
    let msg = vec![Bytes::from(vec![b'x'; n])];
    let mut reply: Vec<Bytes> = Vec::with_capacity(4);
    r.send(msg.clone()).await.expect("req validation send");
    assert!(
        r.recv_into(&mut reply).await.expect("req validation recv"),
        "req EOF"
    );
    assert_eq!(reply, msg, "req reply payload");
    for _ in 0..warmup {
        r.send(msg.clone()).await.expect("req send");
        assert!(r.recv_into(&mut reply).await.expect("req recv"), "req EOF");
    }
    let before = cpu();
    let started = Instant::now();
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        r.send(msg.clone()).await.expect("req send");
        assert!(r.recv_into(&mut reply).await.expect("req recv"), "req EOF");
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
            _ => panic!("usage: monocoque_bench_peer push|pull|pub|multi-sub|rep|req ..."),
        }
    });
}
