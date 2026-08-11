#![cfg(unix)]
//! `ZMQ_FD` level-triggered behavior tests.
//!
//! omq-libzmq provides an accurate, level-triggered fd (eventfd on Linux,
//! pipe on macOS). It is readable iff `zmq_recv()` would succeed without
//! blocking. No spurious wakeups.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_FD: i32 = 14;
const ZMQ_RCVTIMEO: i32 = 27;

fn set_rcvtimeo(sock: *mut c_void, ms: i32) {
    use omq_zmq::zmq_setsockopt;
    let v = ms;
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&v as *const i32).cast(),
        size_of::<i32>(),
    );
}

fn get_fd(sock: *mut c_void) -> i32 {
    let mut fd: i32 = -1;
    let mut sz = size_of::<i32>();
    let rc = zmq_getsockopt(sock, ZMQ_FD, (&mut fd as *mut i32).cast(), &mut sz);
    assert_eq!(rc, 0, "ZMQ_FD getsockopt failed");
    fd
}

/// Poll the fd for readability with a given timeout (ms). Returns true if readable.
fn fd_readable(fd: i32, timeout_ms: i32) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    ret > 0 && (pfd.revents & libc::POLLIN != 0)
}

#[test]
fn fd_not_readable_when_empty() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://test-fd-empty").unwrap();
    zmq_bind(pull, addr.as_ptr());

    let fd = get_fd(pull);
    assert!(fd >= 0);

    // No messages: fd must not be readable.
    assert!(
        !fd_readable(fd, 0),
        "fd should not be readable when no messages"
    );

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn fd_not_signaled_by_empty_inproc_bypass_install() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-fd-empty-bypass-install").unwrap();
    zmq_bind(pull, addr.as_ptr());

    let fd = get_fd(pull);
    assert!(fd >= 0);
    assert!(
        !fd_readable(fd, 0),
        "fd should not be readable before bypass install"
    );

    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        !fd_readable(fd, 0),
        "empty inproc bypass install must not signal ZMQ_FD"
    );

    set_rcvtimeo(push, 1000);
    zmq_send(push, b"x".as_ptr().cast(), 1, 0);
    assert!(fd_readable(fd, 1000), "fd should be readable after data");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn fd_becomes_readable_after_send() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-fd-arrive").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    let fd = get_fd(pull);
    assert!(!fd_readable(fd, 0), "should not be readable before send");

    set_rcvtimeo(push, 1000);
    zmq_send(push, b"hello".as_ptr().cast(), 5, 0);

    // Wait up to 1s for the message to arrive.
    assert!(fd_readable(fd, 1000), "fd should be readable after send");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn fd_level_triggered_stays_readable() {
    // After a message arrives, the fd should remain readable
    // until all messages are consumed (level-triggered, not edge-triggered).
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-fd-level").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    let fd = get_fd(pull);
    set_rcvtimeo(pull, 1000);

    zmq_send(push, b"msg1".as_ptr().cast(), 4, 0);
    zmq_send(push, b"msg2".as_ptr().cast(), 4, 0);
    zmq_send(push, b"msg3".as_ptr().cast(), 4, 0);

    // Wait for all 3 messages to arrive and be signaled on the fd.
    assert!(fd_readable(fd, 2000), "expected first message");
    // Short wait to let remaining signals propagate.
    std::thread::sleep(Duration::from_millis(100));

    // Consume first message.
    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 4);

    // fd should still be readable (2 more messages pending).
    // Use a small timeout in case signal is slightly delayed.
    assert!(
        fd_readable(fd, 200),
        "fd should remain readable (2 messages left)"
    );

    // Consume second.
    zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(
        fd_readable(fd, 200),
        "fd should remain readable (1 message left)"
    );

    // Consume third.
    zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);

    // Now empty: fd should not be readable.
    assert!(
        !fd_readable(fd, 0),
        "fd should not be readable after draining"
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn fd_not_readable_after_recv() {
    // A single message: fd readable before recv, not readable after.
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-fd-after-recv").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    let fd = get_fd(pull);
    set_rcvtimeo(pull, 1000);

    zmq_send(push, b"one".as_ptr().cast(), 3, 0);
    assert!(fd_readable(fd, 2000));

    let mut buf = [0u8; 64];
    zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);

    assert!(
        !fd_readable(fd, 0),
        "fd should not be readable after consuming the only message"
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
