use std::ffi::CString;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::poll::ZMQ_POLLIN;

const ZMQ_PUSH: c_int = 8;
const ZMQ_PULL: c_int = 7;
const ZMQ_SNDTIMEO: c_int = 28;

static NEXT_ADDR: AtomicUsize = AtomicUsize::new(1);

fn unique_inproc(name: &str) -> CString {
    CString::new(format!(
        "inproc://poller-{name}-{}",
        NEXT_ADDR.fetch_add(1, Ordering::Relaxed)
    ))
    .unwrap()
}

fn set_sndtimeo(sock: *mut c_void, timeout_ms: c_int) {
    assert_eq!(
        crate::zmq_setsockopt(
            sock,
            ZMQ_SNDTIMEO,
            std::ptr::from_ref(&timeout_ms).cast(),
            size_of::<c_int>(),
        ),
        0
    );
}

#[test]
fn poller_socket_reports_user_data() {
    let ctx = crate::zmq_ctx_new();
    let push = crate::zmq_socket(ctx, ZMQ_PUSH);
    let pull = crate::zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null());
    assert!(!pull.is_null());

    let addr = unique_inproc("socket");
    assert_eq!(crate::zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(crate::zmq_connect(push, addr.as_ptr()), 0);
    set_sndtimeo(push, 1000);
    std::thread::sleep(Duration::from_millis(20));

    let poller = zmq_poller_new();
    assert!(!poller.is_null());
    let tag = 0xA11CEusize;
    let user_data = std::ptr::from_ref(&tag).cast::<c_void>().cast_mut();
    assert_eq!(zmq_poller_add(poller, pull, user_data, ZMQ_POLLIN), 0);
    assert_eq!(zmq_poller_size(poller), 1);

    assert_eq!(crate::zmq_send(push, b"x".as_ptr().cast(), 1, 0), 1);
    let mut event = ZmqPollerEvent {
        socket: std::ptr::null_mut(),
        fd: invalid_fd(),
        user_data: std::ptr::null_mut(),
        events: 0,
    };
    assert_eq!(zmq_poller_wait(poller, &raw mut event, 1000), 1);
    assert_eq!(event.socket, pull);
    assert_eq!(event.user_data, user_data);
    assert_ne!(event.events & ZMQ_POLLIN, 0);

    assert_eq!(zmq_poller_remove(poller, pull), 0);
    assert_eq!(zmq_poller_size(poller), 0);
    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);
    assert!(poller_slot.is_null());

    crate::zmq_close(push);
    crate::zmq_close(pull);
    crate::zmq_ctx_term(ctx);
}

#[test]
fn poller_timeout_maps_to_eagain() {
    let ctx = crate::zmq_ctx_new();
    let pull = crate::zmq_socket(ctx, ZMQ_PULL);
    assert!(!pull.is_null());

    let addr = unique_inproc("timeout");
    assert_eq!(crate::zmq_bind(pull, addr.as_ptr()), 0);
    let poller = zmq_poller_new();
    assert_eq!(
        zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
        0
    );

    let mut event = ZmqPollerEvent {
        socket: std::ptr::null_mut(),
        fd: invalid_fd(),
        user_data: std::ptr::null_mut(),
        events: 0,
    };
    assert_eq!(zmq_poller_wait(poller, &raw mut event, 1), -1);
    assert_eq!(crate::zmq_errno(), libc::EAGAIN);
    assert_eq!(zmq_poller_modify(poller, pull, 0), 0);
    assert_eq!(zmq_poller_wait(poller, &raw mut event, 0), -1);
    assert_eq!(crate::zmq_errno(), libc::EAGAIN);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);
    crate::zmq_close(pull);
    crate::zmq_ctx_term(ctx);
}

#[cfg(unix)]
#[test]
fn poller_fd_reports_readable_pipe() {
    let mut fds = [-1; 2];
    // SAFETY: fds points to two valid ints for pipe output.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

    let poller = zmq_poller_new();
    let tag = 0xFDFDusize;
    let user_data = std::ptr::from_ref(&tag).cast::<c_void>().cast_mut();
    assert_eq!(zmq_poller_add_fd(poller, fds[0], user_data, ZMQ_POLLIN), 0);
    assert_eq!(zmq_poller_size(poller), 1);

    let mut poller_fd = -1;
    assert_eq!(zmq_poller_fd(poller, &raw mut poller_fd), -1);
    assert_eq!(crate::zmq_errno(), libc::EINVAL);

    // SAFETY: fds[1] is a valid pipe write fd and buffer has one byte.
    assert_eq!(unsafe { libc::write(fds[1], b"x".as_ptr().cast(), 1) }, 1);

    let mut event = ZmqPollerEvent {
        socket: std::ptr::null_mut(),
        fd: invalid_fd(),
        user_data: std::ptr::null_mut(),
        events: 0,
    };
    assert_eq!(zmq_poller_wait(poller, &raw mut event, 1000), 1);
    assert!(event.socket.is_null());
    assert_eq!(event.fd, fds[0]);
    assert_eq!(event.user_data, user_data);
    assert_ne!(event.events & ZMQ_POLLIN, 0);

    assert_eq!(zmq_poller_remove_fd(poller, fds[0]), 0);
    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);

    // SAFETY: fds were opened by pipe above and are closed once here.
    unsafe {
        libc::close(fds[0]);
        libc::close(fds[1]);
    }
}
