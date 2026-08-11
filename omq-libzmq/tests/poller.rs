//! `zmq_poller_*` C ABI compatibility tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_shutdown, zmq_ctx_term, zmq_poller_add,
    zmq_poller_destroy, zmq_poller_modify, zmq_poller_new, zmq_poller_remove, zmq_poller_size,
    zmq_poller_wait, zmq_poller_wait_all, zmq_send, zmq_setsockopt, zmq_socket,
};
#[cfg(unix)]
use omq_zmq::{zmq_poller_add_fd, zmq_poller_fd, zmq_poller_modify_fd, zmq_poller_remove_fd};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_POLLIN: i16 = 1;

const ZMQ_ENOTSOCK: i32 = 156_384_721;
const ZMQ_ETERM: i32 = 156_384_765;

#[cfg(unix)]
type ZmqFd = i32;
#[cfg(windows)]
type ZmqFd = usize;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PollerEvent {
    socket: *mut c_void,
    fd: ZmqFd,
    user_data: *mut c_void,
    events: i16,
}

fn invalid_fd() -> ZmqFd {
    #[cfg(unix)]
    {
        -1
    }
    #[cfg(windows)]
    {
        ZmqFd::MAX
    }
}

fn set_timeo(sock: *mut c_void, ms: i32) {
    assert_eq!(
        zmq_setsockopt(
            sock,
            ZMQ_RCVTIMEO,
            (&ms as *const i32).cast(),
            size_of::<i32>(),
        ),
        0
    );
    assert_eq!(
        zmq_setsockopt(
            sock,
            ZMQ_SNDTIMEO,
            (&ms as *const i32).cast(),
            size_of::<i32>(),
        ),
        0
    );
}

fn unique_addr(label: &str) -> CString {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    CString::new(format!(
        "inproc://poller-{label}-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
    .unwrap()
}

fn push_pull_pair(label: &str) -> (*mut c_void, *mut c_void, *mut c_void) {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null());
    assert!(!pull.is_null());

    let addr = unique_addr(label);
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    set_timeo(push, 1000);
    set_timeo(pull, 1000);
    std::thread::sleep(Duration::from_millis(20));

    (ctx, push, pull)
}

fn blank_events<const N: usize>() -> [PollerEvent; N] {
    [PollerEvent {
        socket: std::ptr::null_mut(),
        fd: invalid_fd(),
        user_data: std::ptr::null_mut(),
        events: 0,
    }; N]
}

#[test]
fn poller_wait_all_reports_socket_user_data() {
    let (ctx, push1, pull1) = push_pull_pair("wait-all-1");
    let push2 = zmq_socket(ctx, ZMQ_PUSH);
    let pull2 = zmq_socket(ctx, ZMQ_PULL);
    let addr2 = unique_addr("wait-all-2");
    assert_eq!(zmq_bind(pull2, addr2.as_ptr()), 0);
    assert_eq!(zmq_connect(push2, addr2.as_ptr()), 0);
    set_timeo(push2, 1000);
    set_timeo(pull2, 1000);
    std::thread::sleep(Duration::from_millis(20));

    let poller = zmq_poller_new();
    assert!(!poller.is_null());
    let tag1 = 0x1111usize;
    let tag2 = 0x2222usize;
    let user1 = std::ptr::from_ref(&tag1).cast::<c_void>().cast_mut();
    let user2 = std::ptr::from_ref(&tag2).cast::<c_void>().cast_mut();

    assert_eq!(zmq_poller_add(poller, pull1, user1, ZMQ_POLLIN), 0);
    assert_eq!(zmq_poller_add(poller, pull2, user2, ZMQ_POLLIN), 0);
    assert_eq!(zmq_poller_size(poller), 2);

    assert_eq!(zmq_send(push1, b"a".as_ptr().cast(), 1, 0), 1);
    assert_eq!(zmq_send(push2, b"b".as_ptr().cast(), 1, 0), 1);
    std::thread::sleep(Duration::from_millis(20));

    let mut events = blank_events::<2>();
    assert_eq!(
        zmq_poller_wait_all(poller, events.as_mut_ptr().cast(), 2, 1000),
        2
    );

    let seen = events
        .iter()
        .map(|event| (event.socket, event.user_data, event.events & ZMQ_POLLIN))
        .collect::<Vec<_>>();
    assert!(seen.contains(&(pull1, user1, ZMQ_POLLIN)));
    assert!(seen.contains(&(pull2, user2, ZMQ_POLLIN)));

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
    assert!(poller_slot.is_null());
    zmq_close(push1);
    zmq_close(pull1);
    zmq_close(push2);
    zmq_close(pull2);
    zmq_ctx_term(ctx);
}

#[test]
fn poller_modify_zero_suppresses_and_restore_reports_event() {
    let (ctx, push, pull) = push_pull_pair("modify-zero");
    let poller = zmq_poller_new();
    assert_eq!(
        zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
        0
    );
    assert_eq!(zmq_poller_modify(poller, pull, 0), 0);
    assert_eq!(zmq_send(push, b"x".as_ptr().cast(), 1, 0), 1);
    std::thread::sleep(Duration::from_millis(20));

    let mut event = blank_events::<1>()[0];
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    assert_eq!(zmq_poller_modify(poller, pull, ZMQ_POLLIN), 0);
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 1000),
        1
    );
    assert_eq!(event.socket, pull);
    assert_ne!(event.events & ZMQ_POLLIN, 0);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn poller_validation_errors_match_errno_contract() {
    let (ctx, push, pull) = push_pull_pair("validation");
    let poller = zmq_poller_new();

    assert_eq!(
        zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
        0
    );
    assert_eq!(
        zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(
        zmq_poller_add(
            poller,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            ZMQ_POLLIN,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    assert_eq!(zmq_poller_remove(poller, std::ptr::null_mut()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    let mut null_poller_event = blank_events::<1>();
    assert_eq!(
        zmq_poller_wait(
            std::ptr::null_mut(),
            null_poller_event.as_mut_ptr().cast(),
            0,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    assert_eq!(zmq_poller_wait(poller, std::ptr::null_mut(), 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let mut event = blank_events::<1>()[0];
    assert_eq!(
        zmq_poller_wait_all(poller, std::ptr::from_mut(&mut event).cast(), 0, 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
    assert_eq!(zmq_poller_destroy(&mut poller_slot), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
    assert_eq!(zmq_poller_destroy(std::ptr::null_mut()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn empty_poller_wait_maps_timeout_to_eagain() {
    let poller = zmq_poller_new();
    let mut event = blank_events::<1>()[0];
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
}

#[test]
fn poller_wait_after_ctx_shutdown_returns_eterm() {
    let (ctx, push, pull) = push_pull_pair("eterm");
    let poller = zmq_poller_new();
    assert_eq!(
        zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
        0
    );

    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    let mut event = blank_events::<1>()[0];
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ETERM);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[cfg(unix)]
#[test]
fn poller_fd_lifecycle_reports_pipe_readability() {
    let mut fds = [-1; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

    let poller = zmq_poller_new();
    let tag = 0xFDFDusize;
    let user = std::ptr::from_ref(&tag).cast::<c_void>().cast_mut();

    assert_eq!(zmq_poller_add_fd(poller, -1, user, ZMQ_POLLIN), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EBADF);

    assert_eq!(zmq_poller_add_fd(poller, fds[0], user, ZMQ_POLLIN), 0);
    assert_eq!(zmq_poller_add_fd(poller, fds[0], user, ZMQ_POLLIN), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(zmq_poller_modify_fd(poller, fds[0], 0), 0);

    let mut event = blank_events::<1>()[0];
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    assert_eq!(zmq_poller_modify_fd(poller, fds[0], ZMQ_POLLIN), 0);
    assert_eq!(unsafe { libc::write(fds[1], b"x".as_ptr().cast(), 1) }, 1);
    assert_eq!(
        zmq_poller_wait(poller, std::ptr::from_mut(&mut event).cast(), 1000),
        1
    );
    assert!(event.socket.is_null());
    assert_eq!(event.fd, fds[0]);
    assert_eq!(event.user_data, user);
    assert_ne!(event.events & ZMQ_POLLIN, 0);

    let mut fd = -1;
    assert_eq!(zmq_poller_fd(poller, &mut fd), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(zmq_poller_remove_fd(poller, fds[0]), 0);
    assert_eq!(zmq_poller_remove_fd(poller, fds[0]), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let mut poller_slot = poller;
    assert_eq!(zmq_poller_destroy(&mut poller_slot), 0);
    unsafe {
        libc::close(fds[0]);
        libc::close(fds[1]);
    }
}
