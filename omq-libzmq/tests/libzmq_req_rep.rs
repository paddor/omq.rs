//! Port of `libzmq/tests/test_req_rep.cpp` (subset)
//! REQ/REP: strict alternating send/recv state machine.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
#![allow(clippy::similar_names)]

mod helpers;

use std::ffi::CString;
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
#[allow(dead_code)]
const ZMQ_DEALER: i32 = 5;
#[allow(dead_code)]
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_RCVMORE: i32 = 13;
const TIMEOUT_MS: i32 = 2000;

fn set_timeo(sock: *mut std::ffi::c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

fn rcvmore(sock: *mut std::ffi::c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

/// from libzmq: basic REQ/REP request-reply
#[test]
fn req_rep_basic_inproc() {
    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);

    let addr = CString::new("inproc://test-reqrep-basic").unwrap();
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_timeo(rep, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(rep, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_SNDTIMEO, TIMEOUT_MS);

    let rc = zmq_send(req, b"request".as_ptr().cast(), 7, 0);
    assert_eq!(rc, 7);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"request");

    let rc = zmq_send(rep, b"reply".as_ptr().cast(), 5, 0);
    assert_eq!(rc, 5);

    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"reply");

    zmq_close(rep);
    zmq_close(req);
    zmq_ctx_term(ctx);
}

#[test]
fn req_rep_basic_tcp() {
    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);

    let addr = helpers::bind_random_tcp(rep);
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));

    set_timeo(rep, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(rep, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_SNDTIMEO, TIMEOUT_MS);

    let mut buf = [0u8; 64];
    for i in 0u8..5 {
        let req_msg = [i; 8];
        let rc = zmq_send(req, req_msg.as_ptr().cast(), 8, 0);
        assert_eq!(rc, 8, "req send {i}");

        let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 8, "rep recv {i}");
        assert!(buf[..8].iter().all(|&b| b == i));

        let rep_msg = [i + 100; 4];
        let rc = zmq_send(rep, rep_msg.as_ptr().cast(), 4, 0);
        assert_eq!(rc, 4, "rep send {i}");

        let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 4, "req recv {i}");
        assert!(buf[..4].iter().all(|&b| b == i + 100));
    }

    zmq_close(rep);
    zmq_close(req);
    zmq_ctx_term(ctx);
}

/// REQ/REP multipart: REP receives the routing envelope from REQ.
/// From the libzmq perspective, `zmq_recv` on REP returns the payload frame only.
#[test]
fn req_rep_multipart_payload() {
    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);

    let addr = CString::new("inproc://test-reqrep-mp").unwrap();
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_timeo(rep, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_RCVTIMEO, TIMEOUT_MS);

    // REQ sends a single-part request.
    zmq_send(req, b"hello".as_ptr().cast(), 5, 0);

    // REP receives the payload (libzmq strips envelope).
    let mut buf = [0u8; 64];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"hello");
    assert!(!rcvmore(rep), "no more frames after payload");

    zmq_send(rep, b"world".as_ptr().cast(), 5, 0);

    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"world");

    zmq_close(rep);
    zmq_close(req);
    zmq_ctx_term(ctx);
}

/// Multiple clients -> one REP server (serial, one at a time)
#[test]
fn req_rep_multiple_clients() {
    const N: usize = 3;

    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);

    let addr = helpers::bind_random_tcp(rep);

    set_timeo(rep, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(rep, ZMQ_SNDTIMEO, TIMEOUT_MS);
    let mut reqs = Vec::new();
    for _ in 0..N {
        let r = zmq_socket(ctx, ZMQ_REQ);
        zmq_connect(r, addr.as_ptr());
        set_timeo(r, ZMQ_RCVTIMEO, TIMEOUT_MS);
        set_timeo(r, ZMQ_SNDTIMEO, TIMEOUT_MS);
        reqs.push(r);
    }
    std::thread::sleep(Duration::from_millis(100));

    let mut buf = [0u8; 64];
    for (i, req) in reqs.iter().enumerate() {
        let msg = format!("req{i}");
        zmq_send(*req, msg.as_ptr().cast(), msg.len(), 0);

        let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0);
        // Echo it back.
        zmq_send(rep, buf.as_ptr().cast(), rc as usize, 0);

        let rc = zmq_recv(*req, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0);
        let got = std::str::from_utf8(&buf[..rc as usize]).unwrap();
        assert_eq!(got, msg);
    }

    for r in reqs {
        zmq_close(r);
    }
    zmq_close(rep);
    zmq_ctx_term(ctx);
}
