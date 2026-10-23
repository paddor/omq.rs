//! Inproc throughput and latency modes.

use super::payload::bench_payload;
use super::{bench_options, bench_recv_spin, latency_common, percentile, send_fast};
use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options, SocketType, TrySendError, blocking};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(super) fn run_inproc(ctx: &omq_tokio::Context, name: String, size: usize, duration: Duration) {
    let ep = Endpoint::Inproc { name };
    let push = ctx.blocking_socket(SocketType::Push, bench_options(size));
    push.bind(ep.clone()).expect("push bind");
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options(size));
    pull.connect(ep).expect("pull connect");

    let payload = bench_payload(size);
    std::thread::spawn(move || {
        if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
            loop {
                send_fast(&push, Message::from_slice(&payload));
            }
        }
        let msg = Message::single(payload.clone());
        loop {
            send_fast(&push, msg.clone());
        }
    });

    std::thread::sleep(Duration::from_millis(500));

    let t0 = Instant::now();
    let deadline = t0 + duration;
    let mut count: u64 = 0;
    loop {
        if Instant::now() >= deadline {
            break;
        }
        if pull.try_recv().is_ok() {
            count += 1;
            while Instant::now() < deadline && pull.try_recv().is_ok() {
                count += 1;
            }
        } else {
            std::thread::yield_now();
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{count} {elapsed:.6} {size}");
}

pub(super) fn run_inproc_2ut(
    ctx: &omq_tokio::Context,
    name: String,
    size: usize,
    duration: Duration,
) {
    let ep = Endpoint::Inproc { name };
    let pull = ctx.blocking_socket(SocketType::Pull, bench_options(size));
    let push = ctx.blocking_socket(SocketType::Push, bench_options(size));
    pull.bind(ep.clone()).expect("pull bind");
    push.connect(ep).expect("push connect");
    push.wait_connected(1, Duration::from_secs(5))
        .expect("wait for inproc connection");

    let payload = bench_payload(size);
    // Both threads start together; the receiver stops the sender when its
    // timed window ends.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let stop = Arc::new(AtomicBool::new(false));

    let sender = spawn_2ut_sender(
        push.clone(),
        payload,
        Arc::clone(&barrier),
        Arc::clone(&stop),
    );
    let receiver = spawn_2ut_receiver(pull.clone(), barrier, Arc::clone(&stop), duration);

    let (count, elapsed) = receiver.join().expect("receiver thread");
    sender.join().expect("sender thread");
    let _ = pull.close();
    let _ = push.close();
    println!("{count} {:.6} {size}", elapsed.as_secs_f64());
}

/// Spin on `try_send` until `stop`. Inline-sized payloads build a fresh
/// inline message each time; larger ones clone one shared message.
fn spawn_2ut_sender(
    sender_push: blocking::Socket,
    payload: Bytes,
    sender_barrier: Arc<std::sync::Barrier>,
    sender_stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        sender_barrier.wait();
        if payload.len() <= omq_tokio::message::MAX_INLINE_MESSAGE {
            loop {
                if sender_stop.load(Ordering::Acquire) {
                    break;
                }
                match sender_push.try_send(Message::from_slice(&payload)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
        } else {
            let msg = Message::single(payload);
            loop {
                if sender_stop.load(Ordering::Acquire) {
                    break;
                }
                match sender_push.try_send(msg.clone()) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
        }
    })
}

/// Spin on `try_recv` for `duration`, then set `stop`. Returns the message
/// count and the measured window.
fn spawn_2ut_receiver(
    receiver_pull: blocking::Socket,
    receiver_barrier: Arc<std::sync::Barrier>,
    receiver_stop: Arc<AtomicBool>,
    duration: Duration,
) -> std::thread::JoinHandle<(u64, Duration)> {
    std::thread::spawn(move || {
        receiver_barrier.wait();
        let start = Instant::now();
        let deadline = start + duration;
        let mut count = 0u64;
        // Read the clock only every 10k polls.
        let mut until_timer_check = 10_000usize;
        loop {
            match receiver_pull.try_recv() {
                Ok(_) => count += 1,
                Err(_) => std::hint::spin_loop(),
            }
            until_timer_check -= 1;
            if until_timer_check == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                until_timer_check = 10_000;
            }
        }
        receiver_stop.store(true, Ordering::Release);
        (count, start.elapsed())
    })
}

pub(super) fn run_inproc_latency(
    ctx: &omq_tokio::Context,
    name: String,
    size: usize,
    iterations: usize,
    warmup: usize,
) {
    let ep = Endpoint::Inproc { name };
    let pair = latency_common::SocketPair::from_env();
    let rep = ctx.blocking_socket(
        pair.responder(),
        pair.options(Options::default().recv_spin(bench_recv_spin()), false),
    );
    rep.bind(ep.clone()).expect("rep bind");

    std::thread::spawn(move || {
        let first = rep.recv().unwrap();
        pair.validate(&first, size, false);
        rep.send(first).unwrap();
        loop {
            let msg = rep.recv().unwrap();
            rep.send(msg).unwrap();
        }
    });

    let req = ctx.blocking_socket(
        pair.requester(),
        pair.options(Options::default().recv_spin(bench_recv_spin()), true),
    );
    req.connect(ep).expect("req connect");
    req.wait_connected(1, Duration::from_secs(5))
        .expect("latency peer ready");
    std::thread::sleep(Duration::from_millis(200));

    let msg = pair.request(size);
    req.send(msg.clone()).unwrap();
    pair.validate(&req.recv().unwrap(), size, true);

    for _ in 0..warmup {
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
    }

    let t_wall = Instant::now();
    let mut rtts = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        req.send(msg.clone()).unwrap();
        req.recv().unwrap();
        rtts.push(t0.elapsed().as_nanos() as u64);
    }
    let elapsed = t_wall.elapsed().as_secs_f64();

    rtts.sort_unstable();
    let p50 = percentile(&rtts, 50.0);
    let p99 = percentile(&rtts, 99.0);
    let p999 = percentile(&rtts, 99.9);
    let max = percentile(&rtts, 100.0);
    println!("{p50:.3} {p99:.3} {p999:.3} {max:.3} {iterations} 0 {elapsed:.6}");
}
