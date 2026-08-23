//! Port of `libzmq/tests/test_pair_tcp.cpp`
//! PAIR socket over TCP: bidirectional, single peer only.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::CString;
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_errno, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PAIR: i32 = 0;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const TIMEOUT_MS: i32 = 2000;

fn set_timeo(sock: *mut std::ffi::c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

fn wait_one_way_ready(sender: *mut std::ffi::c_void, receiver: *mut std::ffi::c_void) {
    let payload = b"READY";
    let rc = zmq_send(sender, payload.as_ptr().cast(), payload.len(), 0);
    assert_eq!(
        rc as usize,
        payload.len(),
        "ready send failed (errno={})",
        zmq_errno(),
    );

    let mut buf = [0u8; 16];
    let rc = zmq_recv(receiver, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc as usize,
        payload.len(),
        "ready recv failed (errno={})",
        zmq_errno(),
    );
    assert_eq!(&buf[..payload.len()], payload);
}

/// from `libzmq/tests/test_pair_tcp.cpp`
#[test]
fn pair_tcp_basic() {
    let ctx = zmq_ctx_new();
    let sb = zmq_socket(ctx, ZMQ_PAIR); // server/bind
    let sc = zmq_socket(ctx, ZMQ_PAIR); // client/connect
    assert!(!sb.is_null() && !sc.is_null());

    let addr = helpers::bind_random_tcp(sb);
    assert_eq!(zmq_connect(sc, addr.as_ptr()), 0, "connect failed");

    set_timeo(sb, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sb, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_SNDTIMEO, TIMEOUT_MS);
    wait_one_way_ready(sc, sb);

    // sc -> sb
    let rc = zmq_send(sc, b"HELLO".as_ptr().cast(), 5, 0);
    assert_eq!(rc, 5);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"HELLO");

    // sb -> sc
    let rc = zmq_send(sb, b"WORLD".as_ptr().cast(), 5, 0);
    assert_eq!(rc, 5);

    let rc = zmq_recv(sc, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"WORLD");

    zmq_close(sc);
    zmq_close(sb);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_tcp_multipart() {
    let ctx = zmq_ctx_new();
    let sb = zmq_socket(ctx, ZMQ_PAIR);
    let sc = zmq_socket(ctx, ZMQ_PAIR);

    let addr = helpers::bind_random_tcp(sb);
    zmq_connect(sc, addr.as_ptr());

    set_timeo(sb, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_SNDTIMEO, TIMEOUT_MS);
    wait_one_way_ready(sc, sb);

    // 3-part message from sc to sb.
    zmq_send(sc, b"A".as_ptr().cast(), 1, ZMQ_SNDMORE);
    zmq_send(sc, b"BB".as_ptr().cast(), 2, ZMQ_SNDMORE);
    zmq_send(sc, b"CCC".as_ptr().cast(), 3, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 1);
    assert_eq!(&buf[..1], b"A");

    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 2);
    assert_eq!(&buf[..2], b"BB");

    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 3);
    assert_eq!(&buf[..3], b"CCC");

    zmq_close(sc);
    zmq_close(sb);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_tcp_many_messages() {
    const N: usize = 100;

    let ctx = zmq_ctx_new();
    let sb = zmq_socket(ctx, ZMQ_PAIR);
    let sc = zmq_socket(ctx, ZMQ_PAIR);

    let addr = helpers::bind_random_tcp(sb);
    zmq_connect(sc, addr.as_ptr());

    set_timeo(sb, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_SNDTIMEO, TIMEOUT_MS);
    wait_one_way_ready(sc, sb);
    let payload = [0xABu8; 64];

    let mut buf = [0u8; 128];
    for i in 0..N {
        let rc = zmq_send(sc, payload.as_ptr().cast(), payload.len(), 0);
        assert_eq!(
            rc as usize,
            payload.len(),
            "send {i} failed (errno={})",
            zmq_errno(),
        );

        let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(
            rc as usize,
            payload.len(),
            "recv {i} failed (errno={})",
            zmq_errno(),
        );
        assert_eq!(&buf[..payload.len()], &payload[..], "recv {i} payload");
    }

    zmq_close(sc);
    zmq_close(sb);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_tcp_truncation() {
    // libzmq behavior: zmq_recv truncates to buf_len but returns actual frame length.
    let ctx = zmq_ctx_new();
    let sb = zmq_socket(ctx, ZMQ_PAIR);
    let sc = zmq_socket(ctx, ZMQ_PAIR);

    let addr = helpers::bind_random_tcp(sb);
    zmq_connect(sc, addr.as_ptr());

    set_timeo(sb, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_SNDTIMEO, TIMEOUT_MS);
    wait_one_way_ready(sc, sb);

    let payload = [0x55u8; 100];
    zmq_send(sc, payload.as_ptr().cast(), 100, 0);

    // Only 10 bytes of buffer.
    let mut buf = [0u8; 10];
    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    // Returns full frame length (100), even though only 10 were copied.
    assert_eq!(rc, 100);
    assert_eq!(&buf[..10], &payload[..10]);

    zmq_close(sc);
    zmq_close(sb);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_tcp_inproc() {
    // Same test over inproc (no TCP involved).
    let ctx = zmq_ctx_new();
    let sb = zmq_socket(ctx, ZMQ_PAIR);
    let sc = zmq_socket(ctx, ZMQ_PAIR);

    let addr = CString::new("inproc://test-pair-inproc").unwrap();
    zmq_bind(sb, addr.as_ptr());
    zmq_connect(sc, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_timeo(sb, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(sc, ZMQ_RCVTIMEO, TIMEOUT_MS);

    let rc = zmq_send(sc, b"inproc-hello".as_ptr().cast(), 12, 0);
    assert_eq!(rc, 12);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(sb, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 12);
    assert_eq!(&buf[..12], b"inproc-hello");

    zmq_close(sc);
    zmq_close(sb);
    zmq_ctx_term(ctx);
}
