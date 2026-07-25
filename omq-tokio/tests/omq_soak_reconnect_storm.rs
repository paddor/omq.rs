#![cfg(feature = "soak")]

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Message, Socket, SocketType};

#[test]
fn soak_reconnect_storm() {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        // Bind port 0 to discover a free port, then use that endpoint for
        // repeated bind/close cycles so the dialer reconnects to the same address.
        let probe = Socket::new(SocketType::Pull, soak_common::soak_options());
        let ep = probe.bind(soak_common::tcp_ep(0)).await.unwrap();
        probe.close().await.unwrap();

        let push = Socket::new(
            SocketType::Push,
            soak_common::soak_options()
                .send_hwm(16)
                .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(10))),
        );
        push.connect(ep.clone()).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let pull = Socket::new(SocketType::Pull, soak_common::soak_options());

            let mut bound = false;
            for _ in 0..40 {
                if pull.bind(ep.clone()).await.is_ok() {
                    bound = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            if !bound {
                eprintln!("[reconnect_storm] bind failed at cycle {cycles}, retrying");
                continue;
            }

            let tag = format!("c-{cycles}");
            let t0 = Instant::now();
            push.send(Message::single(tag.clone())).await.unwrap();
            let send_us = t0.elapsed().as_micros();

            match tokio::time::timeout(Duration::from_secs(5), pull.recv()).await {
                Ok(Ok(m)) => {
                    assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());
                    delivered += 1;
                }
                other => {
                    let recv_ms = t0.elapsed().as_millis();
                    eprintln!(
                        "[reconnect_storm] MISS cycle {cycles}: \
                         send took {send_us} µs, recv waited {recv_ms} ms, \
                         result={other:?}",
                    );
                }
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[reconnect_storm] {:.0}s, cycles {cycles}, delivered {delivered}",
                    start.elapsed().as_secs_f64(),
                );
                last_log = Instant::now();
            }
        }

        push.close().await.unwrap();

        let pct = if cycles > 0 {
            delivered as f64 / cycles as f64 * 100.0
        } else {
            100.0
        };
        eprintln!(
            "[reconnect_storm] done: {delivered}/{cycles} delivered ({pct:.1}%) in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        // Dying drivers can still write a message to a half-closed TCP socket
        // before reading the FIN. The kernel accepts the write (send buffer)
        // but the peer never reads it. This is inherent to multi-threaded
        // scheduling.
        // Typical delivery is 88-100%; 80% gives headroom for loaded CI.
        assert!(
            pct >= 80.0,
            "reconnect storm delivery rate too low: {pct:.1}%"
        );
    });

    let report = monitor.stop();
    report.assert_no_leak("reconnect_storm");
}
