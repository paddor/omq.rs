//! Basic bind/connect/send/recv tests (PUSH/PULL over TCP and inproc).
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
#![allow(clippy::cast_possible_wrap)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_shutdown, zmq_ctx_term, zmq_getsockopt,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_PAIR: i32 = 0;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_ETERM: i32 = 156_384_765;

fn set_rcvtimeo(sock: *mut c_void, ms: i32) {
    let v = ms;
    let rc = zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&v as *const i32).cast(),
        size_of::<i32>(),
    );
    assert_eq!(rc, 0, "setsockopt RCVTIMEO failed");
}

fn set_sndtimeo(sock: *mut c_void, ms: i32) {
    let v = ms;
    let rc = zmq_setsockopt(
        sock,
        ZMQ_SNDTIMEO,
        (&v as *const i32).cast(),
        size_of::<i32>(),
    );
    assert_eq!(rc, 0, "setsockopt SNDTIMEO failed");
}

fn caddr(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[test]
fn send_recv_null_nonzero_buffer_returns_efault() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null() && !pull.is_null());

    let rc = zmq_send(push, std::ptr::null(), 1, 0);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let rc = zmq_recv(pull, std::ptr::null_mut(), 1, ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_shutdown_wakes_blocking_recv() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!pull.is_null());

    let addr = caddr("inproc://test-basic-ctx-shutdown-wakes-recv");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);

    let pull_addr = pull as usize;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut buf = [0u8; 8];
        entered_tx.send(()).expect("entered");
        let rc = zmq_recv(
            pull_addr as *mut c_void,
            buf.as_mut_ptr().cast(),
            buf.len(),
            0,
        );
        done_tx.send((rc, omq_zmq::zmq_errno())).expect("done");
    });

    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("recv thread entered");
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    let (rc, errno) = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("recv should wake on context shutdown");
    assert_eq!(rc, -1);
    assert_eq!(errno, ZMQ_ETERM);
    handle.join().expect("recv thread");

    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn push_pull_inproc() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null() && !pull.is_null());

    let addr = caddr("inproc://test-basic-push-pull");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    // Give the connection a moment.
    std::thread::sleep(Duration::from_millis(20));

    set_rcvtimeo(pull, 1000);
    set_sndtimeo(push, 1000);

    let msg = b"hello";
    let rc = zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
    assert_eq!(rc, msg.len() as i32);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, msg.len() as i32);
    assert_eq!(&buf[..rc as usize], msg);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn push_pull_tcp() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);

    let addr = helpers::bind_random_tcp(pull);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    std::thread::sleep(Duration::from_millis(50));

    set_rcvtimeo(pull, 2000);
    set_sndtimeo(push, 2000);

    for i in 0u8..5 {
        let payload = [i; 32];
        let rc = zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);
        assert_eq!(rc, 32);
    }

    let mut buf = [0u8; 64];
    for i in 0u8..5 {
        let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 32);
        assert!(buf[..32].iter().all(|&b| b == i));
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn dontwait_returns_eagain_when_empty() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = caddr("inproc://test-basic-eagain");
    zmq_bind(pull, addr.as_ptr());

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    // EAGAIN
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn multipart_sndmore_rcvmore() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = caddr("inproc://test-basic-multipart");
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_rcvtimeo(pull, 1000);
    set_sndtimeo(push, 1000);

    // Send a 3-part message.
    zmq_send(push, b"part1".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part2".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part3".as_ptr().cast(), 5, 0);

    let mut buf = [0u8; 64];

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part1");

    // RCVMORE via getsockopt
    let mut more: i32 = 0;
    let mut more_sz = size_of::<i32>();
    zmq_getsockopt(
        pull,
        ZMQ_RCVMORE,
        (&mut more as *mut i32).cast(),
        &mut more_sz,
    );
    assert_ne!(more, 0, "expected RCVMORE after first frame");

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part2");

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part3");

    let mut more: i32 = 0;
    zmq_getsockopt(
        pull,
        ZMQ_RCVMORE,
        (&mut more as *mut i32).cast(),
        &mut more_sz,
    );
    assert_eq!(more, 0, "expected no RCVMORE after last frame");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_inproc_roundtrip() {
    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_PAIR);
    let b = zmq_socket(ctx, ZMQ_PAIR);

    let addr = caddr("inproc://test-basic-pair");
    zmq_bind(a, addr.as_ptr());
    zmq_connect(b, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_rcvtimeo(a, 1000);
    set_rcvtimeo(b, 1000);

    let rc = zmq_send(a, b"ping".as_ptr().cast(), 4, 0);
    assert_eq!(rc, 4);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 4);
    assert_eq!(&buf[..4], b"ping");

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}

#[test]
fn pub_sub_inproc() {
    let ctx = zmq_ctx_new();
    let pub_sock = zmq_socket(ctx, ZMQ_PUB);
    let sub_sock = zmq_socket(ctx, ZMQ_SUB);

    let addr = caddr("inproc://test-basic-pubsub");
    zmq_bind(pub_sock, addr.as_ptr());

    // Subscribe to prefix "hello"
    zmq_setsockopt(sub_sock, ZMQ_SUBSCRIBE, b"hello".as_ptr().cast(), 5);
    zmq_connect(sub_sock, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_rcvtimeo(sub_sock, 1000);

    // Matching message
    zmq_send(pub_sock, b"hello world".as_ptr().cast(), 11, 0);
    // Non-matching message
    zmq_send(pub_sock, b"other stuff".as_ptr().cast(), 11, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub_sock, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc > 0);
    assert_eq!(&buf[..11], b"hello world");

    // The second message should not arrive.
    let rc = zmq_recv(sub_sock, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(pub_sock);
    zmq_close(sub_sock);
    zmq_ctx_term(ctx);
}

#[test]
fn req_rep_inproc() {
    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);

    let addr = caddr("inproc://test-basic-reqrep");
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_rcvtimeo(rep, 1000);
    set_rcvtimeo(req, 1000);

    zmq_send(req, b"request".as_ptr().cast(), 7, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);

    zmq_send(rep, b"reply".as_ptr().cast(), 5, 0);
    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"reply");

    zmq_close(rep);
    zmq_close(req);
    zmq_ctx_term(ctx);
}

#[test]
fn send_empty_message() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = caddr("inproc://test-basic-empty");
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    set_rcvtimeo(pull, 1000);

    // Send zero-length message.
    let rc = zmq_send(push, std::ptr::null(), 0, 0);
    assert_eq!(rc, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 0);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
