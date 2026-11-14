#![cfg(unix)]
//! `ZMQ_FD` level-triggered behavior tests.
//!
//! omq-libzmq provides a level-triggered fd (eventfd on Linux, pipe on
//! macOS). It is readable while `zmq_recv()` would succeed without blocking.
//! As in libzmq, it also turns readable once when a socket that reported
//! itself blocked for sending can send again; reading `ZMQ_EVENTS` consumes
//! that wake.
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
const ZMQ_EVENTS: i32 = 15;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_POLLOUT: i32 = 2;

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

fn set_int(sock: *mut c_void, option: i32, value: i32) {
    use omq_zmq::zmq_setsockopt;
    let rc = zmq_setsockopt(
        sock,
        option,
        (&value as *const i32).cast(),
        size_of::<i32>(),
    );
    assert_eq!(rc, 0);
}

fn get_events(sock: *mut c_void) -> i32 {
    let mut events: i32 = 0;
    let mut sz = size_of::<i32>();
    let rc = zmq_getsockopt(sock, ZMQ_EVENTS, (&mut events as *mut i32).cast(), &mut sz);
    assert_eq!(rc, 0, "ZMQ_EVENTS getsockopt failed");
    events
}

/// An event loop that got `EAGAIN` from a send waits on `ZMQ_FD` and then
/// reads `ZMQ_EVENTS`. The descriptor must fire when the send can proceed.
#[test]
fn fd_signals_when_a_blocked_send_can_proceed() {
    for transport in ["tcp", "inproc"] {
        let ctx = zmq_ctx_new();
        let push = zmq_socket(ctx, ZMQ_PUSH);
        let pull = zmq_socket(ctx, ZMQ_PULL);
        set_int(push, ZMQ_SNDHWM, 2);
        set_int(pull, ZMQ_RCVHWM, 2);
        let addr = if transport == "tcp" {
            helpers::bind_random_tcp(pull)
        } else {
            let addr = CString::new("inproc://test-fd-write-edge").unwrap();
            assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
            addr
        };
        assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
        let fd = get_fd(push);

        // Large frames fill the kernel buffers of the TCP case quickly.
        let payload = vec![0x5a_u8; 256 * 1024];
        let mut sent = 0;
        loop {
            while zmq_send(push, payload.as_ptr().cast(), payload.len(), ZMQ_DONTWAIT) >= 0 {
                sent += 1;
                assert!(sent < 10_000, "{transport}: the pipe never filled");
            }
            assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN, "{transport}");
            // Queued frames may still move into kernel buffers. The wake
            // reports that; without one the send is blocked for good.
            if !fd_readable(fd, 200) {
                break;
            }
            assert_ne!(get_events(push) & ZMQ_POLLOUT, 0, "{transport}");
        }
        assert!(sent > 0, "{transport}");

        set_rcvtimeo(pull, 5000);
        let mut buf = vec![0u8; payload.len()];
        for _ in 0..sent {
            let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
            assert_eq!(rc as usize, payload.len(), "{transport}");
        }
        assert!(
            fd_readable(fd, 5000),
            "{transport}: fd must fire when the send can proceed"
        );
        assert_ne!(get_events(push) & ZMQ_POLLOUT, 0, "{transport}");
        assert!(
            !fd_readable(fd, 100),
            "{transport}: reading ZMQ_EVENTS consumes the wake"
        );

        zmq_close(push);
        zmq_close(pull);
        zmq_ctx_term(ctx);
    }
}

/// The wake is also armed by a `ZMQ_EVENTS` read that reports no `POLLOUT`,
/// here on a bound PUSH that has no peer yet.
#[test]
fn fd_signals_after_events_reported_no_pollout() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = helpers::bind_random_tcp(push);
    let fd = get_fd(push);
    assert_eq!(get_events(push) & ZMQ_POLLOUT, 0);
    assert!(!fd_readable(fd, 50), "nothing changed yet");

    assert_eq!(zmq_connect(pull, addr.as_ptr()), 0);
    assert!(fd_readable(fd, 5000), "fd must fire once a peer is ready");
    assert_ne!(get_events(push) & ZMQ_POLLOUT, 0);
    assert!(!fd_readable(fd, 50), "reading ZMQ_EVENTS consumes the wake");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// The descriptor must not leak into programs the process executes.
#[test]
fn fd_is_close_on_exec() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let fd = get_fd(pull);
    // SAFETY: `fd` is the socket's open descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert_ne!(flags & libc::FD_CLOEXEC, 0);

    zmq_close(pull);
    zmq_ctx_term(ctx);
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
fn fd_not_signaled_by_empty_inproc_connect() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-fd-empty-connect").unwrap();
    zmq_bind(pull, addr.as_ptr());

    let fd = get_fd(pull);
    assert!(fd >= 0);
    assert!(
        !fd_readable(fd, 0),
        "fd should not be readable before connect"
    );

    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        !fd_readable(fd, 0),
        "an inproc connect without messages must not signal ZMQ_FD"
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

#[test]
fn fd_and_blocking_receives_rearm_between_concurrent_batches() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    for socket in [push, pull] {
        for (option, value) in [(17, 0_i32), (23, 8), (24, 8), (27, 1000), (28, 1000)] {
            assert_eq!(
                omq_zmq::zmq_setsockopt(
                    socket,
                    option,
                    (&raw const value).cast(),
                    size_of::<i32>()
                ),
                0
            );
        }
    }
    assert_eq!(
        zmq_bind(pull, c"inproc://fd-concurrent-batches".as_ptr()),
        0
    );
    assert_eq!(
        zmq_connect(push, c"inproc://fd-concurrent-batches".as_ptr()),
        0
    );
    let fd = get_fd(pull);
    let (batch_tx, batch_rx) = std::sync::mpsc::channel();
    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    let push_address = push as usize;
    let sender = std::thread::spawn(move || {
        let push = push_address as *mut c_void;
        for round in 0_u32..128 {
            for index in 0_u32..16 {
                let message = (round * 16 + index).to_le_bytes();
                assert_eq!(zmq_send(push, message.as_ptr().cast(), message.len(), 0), 4);
            }
            batch_tx.send(()).unwrap();
            drained_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    });
    for round in 0_u32..128 {
        assert!(fd_readable(fd, 1000), "batch {round} lost its data wake");
        batch_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        for index in 0_u32..16 {
            assert!(fd_readable(fd, 0), "pending batch must remain readable");
            let mut message = [0_u8; 4];
            assert_eq!(
                zmq_recv(pull, message.as_mut_ptr().cast(), message.len(), 0),
                4
            );
            assert_eq!(u32::from_le_bytes(message), round * 16 + index);
        }
        assert!(!fd_readable(fd, 0), "drained batch must reset its FD");
        drained_tx.send(()).unwrap();
    }
    sender.join().unwrap();
    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn fd_and_events_observe_recycled_fast_ring_before_receive() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_rcvtimeo(pull, 1000);
    assert_eq!(
        zmq_bind(pull, c"inproc://fd-recycled-fast-ring".as_ptr()),
        0
    );
    let fd = get_fd(pull);
    for sequence in 0_u32..3 {
        let push = zmq_socket(ctx, ZMQ_PUSH);
        assert_eq!(
            zmq_connect(push, c"inproc://fd-recycled-fast-ring".as_ptr()),
            0
        );
        let payload = sequence.to_le_bytes();
        assert_eq!(zmq_send(push, payload.as_ptr().cast(), payload.len(), 0), 4);
        assert!(fd_readable(fd, 1000));
        let mut events = 0_i32;
        let mut size = size_of::<i32>();
        assert_eq!(
            zmq_getsockopt(pull, 15, (&raw mut events).cast(), &raw mut size),
            0
        );
        assert_ne!(events & 1, 0, "replacement data must appear in ZMQ_EVENTS");
        let mut received = [0_u8; 4];
        assert_eq!(
            zmq_recv(pull, received.as_mut_ptr().cast(), received.len(), 0),
            4
        );
        assert_eq!(received, payload);
        assert!(!fd_readable(fd, 0));
        assert_eq!(zmq_close(push), 0);
        std::thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}
