#![cfg(all(feature = "soak", feature = "curve"))]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{CurveKeypair, Message, Socket, SocketType};

/// Sends numbered messages until `stop`. A send that times out is not
/// counted, but its index is still consumed.
fn spawn_sender(
    push: Socket,
    sent: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut idx: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            if let Ok(Ok(())) = tokio::time::timeout(
                Duration::from_secs(2),
                push.send(Message::single(format!("c{idx}"))),
            )
            .await
            {
                sent.fetch_add(1, Ordering::Relaxed);
            }
            idx += 1;
        }
    })
}

/// Receives until `stop`. The recv timeout bounds how long a stop request
/// can go unnoticed.
fn spawn_receiver(
    pull: Socket,
    recvd: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), pull.recv()).await {
                recvd.fetch_add(1, Ordering::Relaxed);
            }
        }
    })
}

/// Waits out the soak duration, logging progress every 30 s.
async fn log_progress(duration: Duration, sent: &AtomicU64, recvd: &AtomicU64) {
    let start = Instant::now();
    let mut last_log = start;

    while start.elapsed() < duration {
        tokio::time::sleep(Duration::from_secs(1)).await;

        if last_log.elapsed() >= Duration::from_secs(30) {
            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            eprintln!(
                "[curve] {:.0}s, sent {s}, recvd {r}",
                start.elapsed().as_secs_f64(),
            );
            last_log = Instant::now();
        }
    }
}

#[test]
fn soak_curve_sustained() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(
            SocketType::Pull,
            soak_common::soak_options().curve_server(server_kp),
        );
        let ep = pull.bind(soak_common::tcp_ep(0)).await.unwrap();

        let push = Socket::new(
            SocketType::Push,
            soak_common::soak_options()
                .curve_client(client_kp, server_pub)
                .linger(Duration::from_secs(5)),
        );
        push.connect(ep).await.unwrap();
        // Give the CURVE handshake a head start before traffic begins.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let send_task = spawn_sender(push.clone(), sent.clone(), stop.clone());
        let recv_task = spawn_receiver(pull.clone(), recvd.clone(), stop.clone());

        log_progress(duration, &sent, &recvd).await;
        stop.store(true, Ordering::Relaxed);

        let _ = send_task.await;
        let _ = recv_task.await;

        let s = sent.load(Ordering::Relaxed);
        let r = recvd.load(Ordering::Relaxed);
        eprintln!(
            "[curve] done: sent {s}, recvd {r} in {:.1}s",
            duration.as_secs_f64(),
        );

        push.close().await.unwrap();
        pull.close().await.unwrap();
    });

    let report = monitor.stop();
    report.assert_no_leak("curve");
}
