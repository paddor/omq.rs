//! XPUB/XSUB tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
#![allow(clippy::similar_names)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::{Duration, Instant};

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_XPUB: i32 = 9;
const ZMQ_XSUB: i32 = 10;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_LINGER: i32 = 17;
const ZMQ_DONTWAIT: i32 = 1;
#[allow(dead_code)]
const ZMQ_RCVMORE: i32 = 13;
#[allow(dead_code)]
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_TYPE: i32 = 16;

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

fn set_linger_zero(sock: *mut c_void) {
    let ms: i32 = 0;
    zmq_setsockopt(
        sock,
        ZMQ_LINGER,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

fn get_type(sock: *mut c_void) -> i32 {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_TYPE, (&mut v as *mut i32).cast(), &mut sz);
    v
}

/// Basic XPUB/XSUB: XSUB subscribes via send, XPUB receives subscriptions.
#[test]
fn xpub_xsub_basic() {
    let ctx = zmq_ctx_new();
    let xpub = zmq_socket(ctx, ZMQ_XPUB);
    let xsub = zmq_socket(ctx, ZMQ_XSUB);
    set_linger_zero(xpub);
    set_linger_zero(xsub);
    assert_eq!(get_type(xpub), ZMQ_XPUB);
    assert_eq!(get_type(xsub), ZMQ_XSUB);

    let addr = CString::new("inproc://test-xpub-xsub").unwrap();
    zmq_bind(xpub, addr.as_ptr());
    zmq_connect(xsub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(xpub, 2000);
    set_timeo(xsub, 2000);

    // XSUB subscribes by sending a subscribe frame (0x01 + topic).
    let sub_frame = b"\x01topic";
    zmq_send(xsub, sub_frame.as_ptr().cast(), sub_frame.len(), 0);
    std::thread::sleep(Duration::from_millis(50));

    // XPUB publishes.
    zmq_send(xpub, b"topic hello".as_ptr().cast(), 11, 0);

    // XSUB receives the matching message.
    let mut buf = [0u8; 64];
    let rc = zmq_recv(xsub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 11, "xsub recv failed (errno={})", omq_zmq::zmq_errno());
    assert_eq!(&buf[..11], b"topic hello");

    zmq_close(xsub);
    zmq_close(xpub);
    zmq_ctx_term(ctx);
}

/// XPUB receives subscription notifications from SUB.
#[test]
fn xpub_receives_sub_notifications() {
    let ctx = zmq_ctx_new();
    let xpub = zmq_socket(ctx, ZMQ_XPUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);
    set_linger_zero(xpub);
    set_linger_zero(sub);

    let addr = helpers::bind_random_tcp(xpub);
    set_timeo(xpub, 2000);

    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"news".as_ptr().cast(), 4);
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(200));
    set_timeo(sub, 2000);

    // XPUB should receive a subscription notification: 0x01 + "news".
    let mut buf = [0u8; 64];
    let rc = zmq_recv(xpub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(
        rc >= 5,
        "xpub should receive sub notification (rc={rc}, errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(buf[0], 0x01);
    assert_eq!(&buf[1..rc as usize], b"news");

    // Publish a matching message.
    zmq_send(xpub, b"news flash".as_ptr().cast(), 10, 0);

    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 10);
    assert_eq!(&buf[..10], b"news flash");

    // Publish a non-matching message.
    zmq_send(xpub, b"sports score".as_ptr().cast(), 12, 0);

    // Should not reach SUB. Use nonblocking polls to avoid depending on
    // platform timeout behavior in this filtering test.
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
        assert!(rc < 0, "sub should not receive non-matching msg");
        std::thread::sleep(Duration::from_millis(10));
    }

    zmq_close(sub);
    zmq_close(xpub);
    zmq_ctx_term(ctx);
}

/// XPUB/XSUB proxy pattern: SUB -> XSUB -> XPUB -> SUB.
#[test]
fn xpub_xsub_proxy_pattern() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let xsub = zmq_socket(ctx, ZMQ_XSUB);
    let xpub = zmq_socket(ctx, ZMQ_XPUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);
    set_linger_zero(pub_);
    set_linger_zero(xsub);
    set_linger_zero(xpub);
    set_linger_zero(sub);

    // Backend: PUB -> XSUB
    let addr_be = helpers::bind_random_tcp(xsub);
    zmq_connect(pub_, addr_be.as_ptr());

    // Frontend: XPUB -> SUB
    let addr_fe = helpers::bind_random_tcp(xpub);
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub, addr_fe.as_ptr());

    std::thread::sleep(Duration::from_millis(200));
    set_timeo(xsub, 1000);
    set_timeo(xpub, 1000);
    set_timeo(sub, 2000);

    // Forward subscription from XPUB to XSUB.
    let mut buf = [0u8; 256];
    let rc = zmq_recv(xpub, buf.as_mut_ptr().cast(), buf.len(), 0);
    if rc > 0 {
        zmq_send(xsub, buf[..rc as usize].as_ptr().cast(), rc as usize, 0);
    }
    std::thread::sleep(Duration::from_millis(50));

    // PUB sends.
    zmq_send(pub_, b"proxied msg".as_ptr().cast(), 11, 0);

    // Forward from XSUB to XPUB.
    let rc = zmq_recv(xsub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc,
        11,
        "xsub proxy recv failed (errno={})",
        omq_zmq::zmq_errno()
    );
    zmq_send(xpub, buf[..rc as usize].as_ptr().cast(), rc as usize, 0);

    // SUB receives.
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 11);
    assert_eq!(&buf[..11], b"proxied msg");

    zmq_close(sub);
    zmq_close(xpub);
    zmq_close(xsub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}
