//! Reproducer for comparing the regular and caller-driven DEALER paths.

use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::exclusive::ExclusiveDealer;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn usage() -> ! {
    eprintln!(
        "usage:\n  \
         omq_exclusive_dealer_latency server <bind-uri>\n  \
         omq_exclusive_dealer_latency standard <connect-uri> <size> <iterations> <warmup>\n  \
         omq_exclusive_dealer_latency exclusive <host:port> <size> <iterations> <warmup>"
    );
    std::process::exit(2);
}

fn argument<T: std::str::FromStr>(args: &[String], index: usize, name: &str) -> T {
    args.get(index)
        .unwrap_or_else(|| usage())
        .parse()
        .unwrap_or_else(|_| panic!("invalid {name}"))
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create Tokio runtime");
    runtime.block_on(async_main(&args));
}

async fn async_main(args: &[String]) {
    match args.get(1).map(String::as_str) {
        Some("server") => run_server(argument(args, 2, "bind URI")).await,
        Some(mode @ ("standard" | "exclusive")) => {
            let address = args.get(2).unwrap_or_else(|| usage());
            let size = argument(args, 3, "message size");
            let iterations = argument(args, 4, "iteration count");
            let warmup = argument(args, 5, "warmup count");
            run_client(mode, address, size, iterations, warmup).await;
        }
        _ => usage(),
    }
}

async fn run_server(endpoint: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    let bound = router.bind(endpoint).await.expect("bind ROUTER");
    eprintln!("READY {bound}");
    loop {
        let message = router.recv().await.expect("receive request");
        router.send(message).await.expect("echo response");
    }
}

async fn run_client(mode: &str, address: &str, size: usize, iterations: usize, warmup: usize) {
    assert!(iterations > 0, "iterations must be greater than zero");
    let payload = Bytes::from(vec![b'x'; size]);
    let message = Message::single(payload);
    let identity = Bytes::from_static(b"exclusive-latency-reproducer");

    let mut regular = None;
    let mut exclusive = None;
    match mode {
        "standard" => {
            let endpoint: Endpoint = address.parse().expect("valid connect URI");
            let options = Options::default()
                .identity(identity)
                .workload_profile(WorkloadProfile::Latency);
            let socket = Socket::new(SocketType::Dealer, options);
            socket.connect(endpoint).await.expect("connect DEALER");
            socket
                .wait_connected(1, Duration::from_secs(10))
                .await
                .expect("wait for DEALER handshake");
            regular = Some(socket);
        }
        "exclusive" => {
            exclusive = Some(
                ExclusiveDealer::connect(address, identity)
                    .await
                    .expect("connect exclusive DEALER"),
            );
        }
        _ => unreachable!(),
    }

    for _ in 0..warmup {
        round_trip(regular.as_ref(), exclusive.as_mut(), &message).await;
    }

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        round_trip(regular.as_ref(), exclusive.as_mut(), &message).await;
        samples.push(started.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();

    println!("mode,size,iterations,warmup,p50_us,p95_us,p99_us,max_us");
    println!(
        "{mode},{size},{iterations},{warmup},{:.3},{:.3},{:.3},{:.3}",
        percentile_us(&samples, 50.0),
        percentile_us(&samples, 95.0),
        percentile_us(&samples, 99.0),
        percentile_us(&samples, 100.0),
    );
}

async fn round_trip(
    regular: Option<&Socket>,
    exclusive: Option<&mut ExclusiveDealer>,
    message: &Message,
) {
    if let Some(socket) = regular {
        socket.send(message.clone()).await.expect("standard send");
        socket.recv().await.expect("standard receive");
    } else if let Some(socket) = exclusive {
        socket.send(message).await.expect("exclusive send");
        socket.recv().await.expect("exclusive receive");
    }
}

fn percentile_us(sorted: &[u64], percentile: f64) -> f64 {
    let index = ((sorted.len() as f64 * percentile / 100.0) as usize).min(sorted.len() - 1);
    sorted[index] as f64 / 1_000.0
}
