//! perf-friendly TCP PUSH/PULL driver for `perf record`.
//!
//! Runs a single payload size for a long-enough wall-clock window
//! that flame graphs over it have signal. Tweak via env:
//!   `OMQ_PROFILE_SIZE`   bytes per message (default 128)
//!   `OMQ_PROFILE_SECS`   wall-clock seconds (default 5)
//!
//! Build & profile:
//!   cargo build --release --example `profile_tcp` -p omq-compio
//!   perf record -F 999 --call-graph dwarf,16384 \
//!       `target/release/examples/profile_tcp`
//!   perf report --stdio --no-children -g none -n | head -60

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn loopback_port() -> u16 {
    let l = StdTcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[compio::main]
async fn main() {
    let size: usize = std::env::var("OMQ_PROFILE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let secs: u64 = std::env::var("OMQ_PROFILE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let dur = Duration::from_secs(secs);

    let port = loopback_port();
    let ep = Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    };

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..2_000 {
        push.send(Message::single(payload.clone())).await.unwrap();
    }
    for _ in 0..2_000 {
        pull.recv().await.unwrap();
    }

    let start = Instant::now();
    let mut recv: u64 = 0;
    let send_h = compio::runtime::spawn({
        let push = push.clone();
        let payload = payload.clone();
        async move {
            let mut n = 0u64;
            while start.elapsed() < dur {
                for _ in 0..256 {
                    push.send(Message::single(payload.clone())).await.unwrap();
                    n += 1;
                }
            }
            n
        }
    });
    while start.elapsed() < dur {
        for _ in 0..256 {
            pull.recv().await.unwrap();
            recv += 1;
        }
    }
    let sent = send_h.await.unwrap();
    let elapsed = start.elapsed();
    eprintln!(
        "profile_tcp: size={size}B secs={:.2} sent={sent} recv={recv} rate={:.0} msg/s",
        elapsed.as_secs_f64(),
        recv as f64 / elapsed.as_secs_f64()
    );
}
