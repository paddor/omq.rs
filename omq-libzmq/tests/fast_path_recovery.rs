//! Verify fast paths recover after peer churn.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_disconnect, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;
fn set_timeo(sock: *mut c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

#[test]
fn inproc_recovers_after_disconnect_reconnect() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://recovery").unwrap();

    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(30));
    set_timeo(pull, 2000);

    let msg = b"hello";
    zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"hello");

    zmq_disconnect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(30));

    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(30));

    let msg2 = b"world";
    zmq_send(push, msg2.as_ptr().cast(), msg2.len(), 0);
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"world");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn inproc_multi_churn() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://multi-churn").unwrap();

    zmq_bind(pull, addr.as_ptr());
    set_timeo(pull, 2000);

    for i in 0u8..5 {
        zmq_connect(push, addr.as_ptr());
        std::thread::sleep(Duration::from_millis(30));

        let msg = [i; 4];
        zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
        let mut buf = [0u8; 64];
        let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 4, "iteration {i}");
        assert_eq!(buf[0], i, "content mismatch at iteration {i}");

        zmq_disconnect(push, addr.as_ptr());
        std::thread::sleep(Duration::from_millis(30));
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
