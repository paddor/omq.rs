//! C API round-trip latency and single-thread PUSH/PULL cost over inproc.
//!
//! Run: `cargo run --release --example omq_libzmq_bench_latency -p omq-libzmq`

use std::ffi::CString;
use std::time::Instant;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;

fn set_rcvtimeo(sock: *mut libc::c_void, ms: i32) {
    let result = zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&raw const ms).cast(),
        std::mem::size_of::<i32>(),
    );
    assert_eq!(result, 0);
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn bench_req_rep_inproc(iters: usize) {
    let ctx = zmq_ctx_new();
    let req = zmq_socket(ctx, ZMQ_REQ);
    let rep = zmq_socket(ctx, ZMQ_REP);

    let addr = CString::new("inproc://bench-rtt").unwrap();
    assert_eq!(zmq_bind(rep, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(req, addr.as_ptr()), 0);
    std::thread::sleep(std::time::Duration::from_millis(20));
    set_rcvtimeo(req, 5000);
    set_rcvtimeo(rep, 5000);

    let payload = b"ping";
    let reply = b"pong";
    let mut buf = [0u8; 16];

    // REP thread
    let rep_raw = rep as usize;
    let rep_thread = std::thread::spawn(move || {
        let rep = rep_raw as *mut libc::c_void;
        for _ in 0..iters + iters / 10 {
            let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
            assert_eq!(rc, 4);
            assert_eq!(zmq_send(rep, reply.as_ptr().cast(), reply.len(), 0), 4);
        }
    });

    // warmup
    for _ in 0..iters / 10 {
        assert_eq!(zmq_send(req, payload.as_ptr().cast(), payload.len(), 0), 4);
        assert_eq!(zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0), 4);
    }

    let mut latencies = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        assert_eq!(zmq_send(req, payload.as_ptr().cast(), payload.len(), 0), 4);
        assert_eq!(zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0), 4);
        latencies.push(t.elapsed().as_nanos() as u64);
    }

    rep_thread.join().unwrap();

    latencies.sort_unstable();
    println!(
        "REQ/REP inproc round-trip  ({iters} iters)  \
         p50={:6}ns  p95={:6}ns  p99={:6}ns  mean={:.0}ns",
        percentile(&latencies, 50.0),
        percentile(&latencies, 95.0),
        percentile(&latencies, 99.0),
        latencies.iter().sum::<u64>() as f64 / iters as f64,
    );

    assert_eq!(zmq_close(req), 0);
    assert_eq!(zmq_close(rep), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

fn bench_push_pull_throughput(msg_size: usize, iters: usize) {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://bench-tput").unwrap();
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    std::thread::sleep(std::time::Duration::from_millis(20));
    set_rcvtimeo(pull, 5000);

    let expected = i32::try_from(msg_size).expect("message size fits C API");
    let payload: Vec<u8> = (0..msg_size).map(|i| i as u8).collect();
    let mut recv_buf = vec![0u8; msg_size];

    // warmup
    for _ in 0..iters / 10 {
        assert_eq!(
            zmq_send(push, payload.as_ptr().cast(), payload.len(), 0),
            expected
        );
        assert_eq!(
            zmq_recv(pull, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0),
            expected
        );
    }

    let t = Instant::now();
    for _ in 0..iters {
        assert_eq!(
            zmq_send(push, payload.as_ptr().cast(), payload.len(), 0),
            expected
        );
        assert_eq!(
            zmq_recv(pull, recv_buf.as_mut_ptr().cast(), recv_buf.len(), 0),
            expected
        );
    }
    let elapsed = t.elapsed();
    let ns_per = elapsed.as_nanos() as f64 / iters as f64;
    let gbps = (msg_size as f64 * iters as f64) / elapsed.as_secs_f64() / f64::from(1_u32 << 30);
    println!("PUSH/PULL inproc  sz={msg_size:>7}  {ns_per:8.0}ns/msg  {gbps:5.2} GB/s");

    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

fn main() {
    println!("--- round-trip latency ---");
    bench_req_rep_inproc(10_000);

    println!();
    println!("--- push/pull throughput ---");
    for &sz in &[64usize, 1024, 16 * 1024, 256 * 1024] {
        bench_push_pull_throughput(sz, if sz <= 1024 { 20_000 } else { 2_000 });
    }
}
