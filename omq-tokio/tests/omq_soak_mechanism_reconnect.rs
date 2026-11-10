#![cfg(feature = "soak")]
//! Soak: mechanism re-handshake under bind-side restarts.
//!
//! Exercises the full greeting + mechanism state machine reset path
//! hundreds of times per run. The server (PULL) binds with encryption
//! keys, the client (PUSH) connects and sends. The server repeatedly
//! crashes and rebinds with the same keypair; the client reconnects,
//! re-handshakes, and resumes.
//!
//! CURVE (RFC 26) re-handshake test.

mod soak_common;

use std::time::{Duration, Instant};

use omq_tokio::options::ReconnectPolicy;
#[allow(unused_imports)]
use omq_tokio::{Message, Options, Socket, SocketType};

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

fn fast_reconnect() -> Options {
    Options {
        reconnect: ReconnectPolicy::Fixed(Duration::from_millis(10)),
        ..soak_common::soak_options()
    }
}

async fn rebind(ep: &omq_tokio::Endpoint, make: impl Fn() -> Socket) -> Option<Socket> {
    for _ in 0..40 {
        let s = make();
        if s.bind(ep.clone()).await.is_ok() {
            return Some(s);
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    None
}

fn run_mechanism_storm(
    name: &str,
    make_server: impl Fn() -> Socket + Send + Sync + 'static,
    make_client: impl Fn(omq_tokio::Endpoint) -> Socket + Send + Sync + 'static,
) {
    let duration = soak_common::soak_duration();
    let monitor = soak_common::ResourceMonitor::start();
    let name = name.to_owned();
    let report_name = name.clone();

    let ctx = soak_common::build_context();
    ctx.block_on(async move {
        let probe = make_server();
        let ep = probe.bind(soak_common::tcp_ep(0)).await.unwrap();
        probe.close().await.unwrap();

        let push = make_client(ep.clone());
        push.connect(ep.clone()).await.unwrap();

        let start = Instant::now();
        let mut cycles: u64 = 0;
        let mut delivered: u64 = 0;
        let mut last_log = start;

        while start.elapsed() < duration {
            let Some(pull) = rebind(&ep, &make_server).await else {
                eprintln!("[{name}] rebind failed at cycle {cycles}, retrying");
                continue;
            };

            let tag = format!("{name}-{cycles}");
            push.send(Message::single(tag.clone())).await.unwrap();

            match tokio::time::timeout(Duration::from_secs(5), pull.recv()).await {
                Ok(Ok(m)) => {
                    assert_eq!(m.part_bytes(0).unwrap(), tag.as_bytes());
                    delivered += 1;
                }
                other => {
                    eprintln!("[{name}] MISS cycle {cycles}: {other:?}");
                }
            }

            pull.close().await.unwrap();
            cycles += 1;

            if last_log.elapsed() >= Duration::from_secs(30) {
                eprintln!(
                    "[{name}] {:.0}s, cycles {cycles}, delivered {delivered}",
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
            "[{name}] done: {delivered}/{cycles} delivered ({pct:.1}%) in {:.1}s",
            start.elapsed().as_secs_f64(),
        );
        assert!(pct >= 70.0, "[{name}] delivery rate too low: {pct:.1}%");
    });

    let report = monitor.stop();
    report.assert_no_leak(&report_name);
}

#[cfg(feature = "curve")]
#[test]
fn soak_curve_reconnect() {
    use omq_tokio::CurveKeypair;

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    run_mechanism_storm(
        "curve_reconnect",
        move || {
            Socket::new(
                SocketType::Pull,
                soak_common::soak_options().curve_server(server_kp.clone()),
            )
        },
        move |_ep| {
            Socket::new(
                SocketType::Push,
                fast_reconnect()
                    .curve_client(client_kp.clone(), server_pub)
                    .send_hwm(16)
                    .linger(Duration::from_secs(5)),
            )
        },
    );
}
