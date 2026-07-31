#![cfg(all(feature = "soak", feature = "zstd"))]
//! Soak: zstd compression transport sustained.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

const SIZES: &[usize] = &[64, 1024, 8 * 1024, 64 * 1024, 256 * 1024];

fn zstd_options() -> Options {
    soak_common::soak_options().compression_auto_train(true)
}

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, zstd_options());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("expected ZstdTcp Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::ZstdTcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

fn make_payload(idx: u64, size: usize) -> Vec<u8> {
    let mut v = vec![b'A' + (idx % 16) as u8; size];
    let tag = format!("{{\"seq\":{idx},\"kind\":\"zstd-soak\",\"pad\":\"");
    let tag = tag.as_bytes();
    let n = tag.len().min(size);
    v[..n].copy_from_slice(&tag[..n]);
    v
}

#[test]
fn soak_compression_zstd_sustained() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let (pull, ep) = pull_on_loopback().await;
        let push = Socket::new(
            SocketType::Push,
            zstd_options().linger(Duration::from_secs(5)),
        );
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let send_sent = sent.clone();
        let send_stop = stop.clone();
        let push_clone = push.clone();
        let send_task = tokio::spawn(async move {
            let mut idx: u64 = 0;
            while !send_stop.load(Ordering::Relaxed) {
                let size = SIZES[idx as usize % SIZES.len()];
                let payload = make_payload(idx, size);
                if let Ok(Ok(())) = tokio::time::timeout(
                    Duration::from_secs(2),
                    push_clone.send(Message::single(payload)),
                )
                .await
                {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
                idx += 1;
            }
        });

        let recv_recvd = recvd.clone();
        let recv_stop = stop.clone();
        let pull_clone = pull.clone();
        let recv_task = tokio::spawn(async move {
            while !recv_stop.load(Ordering::Relaxed) {
                if let Ok(Ok(m)) =
                    tokio::time::timeout(Duration::from_secs(2), pull_clone.recv()).await
                {
                    let part = m.part_bytes(0).unwrap();
                    assert!(
                        SIZES.contains(&part.len()),
                        "unexpected message size: {}",
                        part.len()
                    );
                    recv_recvd.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let timer_stop = stop.clone();
        let timer_sent = sent.clone();
        let timer_recvd = recvd.clone();
        let start = Instant::now();
        let mut last_log = start;

        while start.elapsed() < duration {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if last_log.elapsed() >= Duration::from_secs(30) {
                let s = timer_sent.load(Ordering::Relaxed);
                let r = timer_recvd.load(Ordering::Relaxed);
                eprintln!(
                    "[compression_zstd] {:.0}s, sent {s}, recvd {r}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }
        timer_stop.store(true, Ordering::Relaxed);

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[compression_zstd] done: sent {s}, recvd {r} in {:.1}s",
            duration.as_secs_f64(),
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("compression_zstd");
}
