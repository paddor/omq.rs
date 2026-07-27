//! Port of `libzmq/tests/test_pub_sub.cpp` (subset)
//! PUB/SUB: topic-filtered fan-out.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_UNSUBSCRIBE: i32 = 7;

fn set_timeo(sock: *mut c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
    zmq_setsockopt(
        sock,
        ZMQ_SNDTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

fn subscribe(sock: *mut c_void, prefix: &[u8]) {
    zmq_setsockopt(sock, ZMQ_SUBSCRIBE, prefix.as_ptr().cast(), prefix.len());
}

#[test]
fn pub_sub_basic_inproc() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = CString::new("inproc://test-pubsub-basic").unwrap();
    zmq_bind(pub_, addr.as_ptr());

    subscribe(sub, b"");
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(sub, 2000);

    zmq_send(pub_, b"hello pubsub".as_ptr().cast(), 12, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 12);
    assert_eq!(&buf[..12], b"hello pubsub");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

#[test]
fn pub_sub_topic_filter_tcp() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = helpers::bind_random_tcp(pub_);
    subscribe(sub, b"topic.A");
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(200));
    set_timeo(sub, 2000);

    zmq_send(pub_, b"topic.B payload".as_ptr().cast(), 15, 0);
    zmq_send(pub_, b"topic.A match".as_ptr().cast(), 13, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 13);
    assert_eq!(&buf[..13], b"topic.A match");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

#[test]
fn pub_sub_multiple_subscribers() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);

    let addr = CString::new("inproc://test-pubsub-multi").unwrap();
    zmq_bind(pub_, addr.as_ptr());

    let sub_a = zmq_socket(ctx, ZMQ_SUB);
    subscribe(sub_a, b"A");
    zmq_connect(sub_a, addr.as_ptr());

    let sub_b = zmq_socket(ctx, ZMQ_SUB);
    subscribe(sub_b, b"B");
    zmq_connect(sub_b, addr.as_ptr());

    std::thread::sleep(Duration::from_millis(50));
    set_timeo(sub_a, 500);
    set_timeo(sub_b, 500);

    zmq_send(pub_, b"A message".as_ptr().cast(), 9, 0);
    zmq_send(pub_, b"B message".as_ptr().cast(), 9, 0);

    let mut buf = [0u8; 64];

    let rc = zmq_recv(sub_a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 9);
    assert_eq!(&buf[..9], b"A message");

    let rc = zmq_recv(sub_b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 9);
    assert_eq!(&buf[..9], b"B message");

    // sub_a should not get "B message".
    let rc = zmq_recv(sub_a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc < 0, "sub_a should not receive B message");

    zmq_close(sub_a);
    zmq_close(sub_b);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

#[test]
fn pub_sub_unsubscribe() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = CString::new("inproc://test-pubsub-unsub").unwrap();
    zmq_bind(pub_, addr.as_ptr());
    subscribe(sub, b"topic");
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(sub, 500);

    zmq_send(pub_, b"topic first".as_ptr().cast(), 11, 0);
    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 11);

    zmq_setsockopt(sub, ZMQ_UNSUBSCRIBE, b"topic".as_ptr().cast(), 5);
    std::thread::sleep(Duration::from_millis(50));

    zmq_send(pub_, b"topic second".as_ptr().cast(), 12, 0);
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc < 0, "should not receive after unsubscribe");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}
