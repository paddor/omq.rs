//! Blocking C API PUSH/PULL on two application threads.
//!
//! Run: `cargo run --release -p omq-libzmq --example omq_libzmq_bench_streaming -- 64 1000000 1000`
//! Use a separate perf/strace run to measure wake syscalls per message.

use std::ffi::{CString, c_void};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

fn set_i32(socket: *mut c_void, option: i32, value: i32) {
    assert_eq!(
        zmq_setsockopt(socket, option, (&raw const value).cast(), size_of::<i32>()),
        0
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let size: usize = args.get(1).map_or(64, |value| value.parse().expect("size"));
    let messages: u64 = args
        .get(2)
        .map_or(1_000_000, |value| value.parse().expect("messages"));
    let hwm: i32 = args
        .get(3)
        .map_or(1000, |value| value.parse().expect("hwm"));
    assert!(messages > 0 && hwm > 0);
    let expected = i32::try_from(size).expect("message size fits C API");

    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, 8);
    let pull = zmq_socket(ctx, 7);
    assert!(!ctx.is_null() && !push.is_null() && !pull.is_null());
    for socket in [push, pull] {
        set_i32(socket, 23, hwm); // SNDHWM
        set_i32(socket, 24, hwm); // RCVHWM
        set_i32(socket, 27, 5000); // RCVTIMEO
        set_i32(socket, 28, 5000); // SNDTIMEO
        set_i32(socket, 17, 0); // LINGER
    }
    let endpoint = CString::new("inproc://c-streaming-bench").unwrap();
    assert_eq!(zmq_bind(pull, endpoint.as_ptr()), 0);
    assert_eq!(zmq_connect(push, endpoint.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(20));

    let barrier = Arc::new(Barrier::new(2));
    let sender_barrier = barrier.clone();
    let push_address = push as usize;
    let sender = std::thread::spawn(move || {
        let push = push_address as *mut c_void;
        let payload = vec![b'x'; size];
        sender_barrier.wait();
        for _ in 0..messages {
            assert_eq!(zmq_send(push, payload.as_ptr().cast(), size, 0), expected);
        }
    });
    let mut received = vec![0_u8; size];
    barrier.wait();
    let started = Instant::now();
    for _ in 0..messages {
        assert_eq!(
            zmq_recv(pull, received.as_mut_ptr().cast(), size, 0),
            expected
        );
    }
    let elapsed = started.elapsed().as_secs_f64();
    sender.join().expect("sender thread");
    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
    println!("{messages} {elapsed:.9} {size}");
}
