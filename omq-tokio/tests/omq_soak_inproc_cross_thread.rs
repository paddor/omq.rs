#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use omq_tokio::{Message, Options, Socket, SocketType};

/// Receives on its own task until `stop`, then drains whatever is left.
fn spawn_receiver(
    pull: Socket,
    recvd: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), pull.recv()).await {
                recvd.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Drain remaining.
        while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), pull.recv()).await {
            recvd.fetch_add(1, Ordering::Relaxed);
        }
    })
}

/// Sends from the calling task for `duration`, logging every 30 s.
async fn send_for(push: &Socket, duration: Duration, sent: &AtomicU64, recvd: &AtomicU64) {
    let start = Instant::now();
    let mut last_log = start;

    while start.elapsed() < duration {
        if let Ok(Ok(())) =
            tokio::time::timeout(Duration::from_millis(100), push.send(Message::single("x"))).await
        {
            sent.fetch_add(1, Ordering::Relaxed);
        }

        if last_log.elapsed() >= Duration::from_secs(30) {
            let s = sent.load(Ordering::Relaxed);
            let r = recvd.load(Ordering::Relaxed);
            eprintln!(
                "[inproc_xthread] {:.0}s, sent {s}, recvd {r}",
                start.elapsed().as_secs_f64(),
            );
            last_log = Instant::now();
        }
    }
}

#[test]
fn soak_inproc_cross_thread() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ep = soak_common::inproc_ep("soak-xthread");
    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let report_sent = sent.clone();
    let report_recvd = recvd.clone();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.bind(ep.clone()).await.unwrap();

        let push = Socket::new(SocketType::Push, Options::default());
        push.connect(ep).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Sender and receiver run on different tasks, so with a multi-thread
        // context the inproc ring is crossed between threads.
        let recv_task = spawn_receiver(pull.clone(), recvd.clone(), stop.clone());
        send_for(&push, duration, &sent, &recvd).await;

        push.close().await.unwrap();

        // Give recv task time to drain, then stop.
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::Relaxed);
        let _ = recv_task.await;
    });

    let s = report_sent.load(Ordering::Relaxed);
    let r = report_recvd.load(Ordering::Relaxed);
    eprintln!(
        "[inproc_xthread] done: sent {s}, recvd {r} in {:.1}s",
        duration.as_secs_f64(),
    );

    assert!(r > 0, "no messages received");
    let loss_pct = if s > 0 {
        (s - r) as f64 / s as f64 * 100.0
    } else {
        0.0
    };
    assert!(
        loss_pct < 1.0,
        "message loss too high: {loss_pct:.2}% ({s} sent, {r} received)"
    );

    let report = monitor.stop();
    report.assert_no_leak("inproc_xthread");
}
