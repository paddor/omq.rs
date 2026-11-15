//! `omq_socket_set_ready_callback`: the push form of `ZMQ_FD`. Registration
//! and each `ZMQ_EVENTS` read arm the callback; it then runs once when the
//! descriptor would be signaled.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use omq_zmq::{
    omq_socket_set_ready_callback, zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term,
    zmq_errno, zmq_getsockopt, zmq_recv, zmq_send, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_EVENTS: i32 = 15;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_POLLIN: i32 = 1;
const ZMQ_POLLOUT: i32 = 2;
const WAKE: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(100);

#[derive(Default)]
struct Calls {
    count: Mutex<usize>,
    changed: Condvar,
}

impl Calls {
    fn userdata(&self) -> *mut c_void {
        std::ptr::from_ref(self).cast_mut().cast()
    }

    /// Wait until the callback ran `n` times in total; false on timeout.
    fn wait_for(&self, n: usize, timeout: Duration) -> bool {
        let count = self.count.lock().unwrap();
        let (count, _) = self
            .changed
            .wait_timeout_while(count, timeout, |count| *count < n)
            .unwrap();
        *count >= n
    }

    fn get(&self) -> usize {
        *self.count.lock().unwrap()
    }
}

extern "C" fn on_ready(userdata: *mut c_void) {
    // SAFETY: every test passes a `Calls` that outlives its sockets.
    let calls = unsafe { &*userdata.cast::<Calls>() };
    *calls.count.lock().unwrap() += 1;
    calls.changed.notify_all();
}

fn register(sock: *mut c_void, calls: &Calls) {
    let rc = omq_socket_set_ready_callback(sock, Some(on_ready), calls.userdata());
    assert_eq!(rc, 0, "register the ready callback");
}

fn events(sock: *mut c_void) -> i32 {
    let mut events: i32 = 0;
    let mut sz = size_of::<i32>();
    let rc = zmq_getsockopt(sock, ZMQ_EVENTS, (&mut events as *mut i32).cast(), &mut sz);
    assert_eq!(rc, 0, "ZMQ_EVENTS getsockopt failed");
    events
}

fn send(sock: *mut c_void, data: &[u8]) {
    assert_eq!(
        zmq_send(sock, data.as_ptr().cast(), data.len(), 0),
        i32::try_from(data.len()).unwrap()
    );
}

fn recv(sock: *mut c_void, len: usize) {
    let mut buf = vec![0u8; len];
    assert_eq!(
        zmq_recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0),
        i32::try_from(len).unwrap()
    );
}

#[test]
fn ready_callback_runs_once_per_arm_when_input_arrives() {
    let calls = Calls::default();
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    helpers::set_rcvtimeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);
    register(pull, &calls);
    assert_eq!(events(pull) & ZMQ_POLLIN, 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    send(push, b"one");
    assert!(calls.wait_for(1, WAKE), "input must run the callback");
    send(push, b"two");
    assert!(
        !calls.wait_for(2, QUIET),
        "the callback runs once until ZMQ_EVENTS re-arms it"
    );

    recv(pull, 3);
    recv(pull, 3);
    assert_eq!(events(pull) & ZMQ_POLLIN, 0);
    send(push, b"three");
    assert!(
        calls.wait_for(2, WAKE),
        "a ZMQ_EVENTS read re-arms the callback"
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Mirrors `fd_signals_when_a_blocked_send_can_proceed` in `fd.rs`.
#[test]
fn ready_callback_runs_when_a_blocked_send_can_proceed() {
    for transport in ["tcp", "inproc"] {
        let calls = Calls::default();
        let ctx = zmq_ctx_new();
        let push = zmq_socket(ctx, ZMQ_PUSH);
        let pull = zmq_socket(ctx, ZMQ_PULL);
        helpers::set_int_opt(push, ZMQ_SNDHWM, 2);
        helpers::set_int_opt(pull, ZMQ_RCVHWM, 2);
        let addr = if transport == "tcp" {
            helpers::bind_random_tcp(pull)
        } else {
            let addr = CString::new("inproc://test-ready-callback-write").unwrap();
            assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
            addr
        };
        assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
        register(push, &calls);

        let payload = vec![0x5a_u8; 256 * 1024];
        let mut sent = 0;
        loop {
            while zmq_send(push, payload.as_ptr().cast(), payload.len(), ZMQ_DONTWAIT) >= 0 {
                sent += 1;
                assert!(sent < 10_000, "{transport}: the pipe never filled");
            }
            assert_eq!(zmq_errno(), libc::EAGAIN, "{transport}");
            // Queued frames may still move into kernel buffers; the callback
            // reports that. Without a call the send is blocked for good.
            let seen = calls.get();
            if events(push) & ZMQ_POLLOUT == 0
                && !calls.wait_for(seen + 1, Duration::from_millis(200))
            {
                break;
            }
        }
        assert!(sent > 0, "{transport}");

        let seen = calls.get();
        helpers::set_rcvtimeo(pull, 5000);
        for _ in 0..sent {
            recv(pull, payload.len());
        }
        assert!(
            calls.wait_for(seen + 1, WAKE),
            "{transport}: the callback must run when the send can proceed"
        );
        assert_ne!(events(push) & ZMQ_POLLOUT, 0, "{transport}");

        zmq_close(push);
        zmq_close(pull);
        zmq_ctx_term(ctx);
    }
}

#[test]
fn ready_callback_runs_when_the_context_terminates() {
    let calls = Calls::default();
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    helpers::bind_random_tcp(pull);
    register(pull, &calls);
    assert_eq!(events(pull) & ZMQ_POLLIN, 0);

    // zmq_ctx_term blocks until the socket closes; the callback is the
    // owner's cue to close it.
    let ctx_addr = ctx as usize;
    std::thread::scope(|scope| {
        let term = scope.spawn(move || zmq_ctx_term(ctx_addr as *mut c_void));
        assert!(calls.wait_for(1, WAKE), "termination must run the callback");
        zmq_close(pull);
        assert_eq!(term.join().unwrap(), 0);
    });
}

#[test]
fn cleared_ready_callback_is_not_called() {
    let calls = Calls::default();
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    helpers::set_rcvtimeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);
    register(pull, &calls);
    assert_eq!(
        omq_socket_set_ready_callback(pull, None, std::ptr::null_mut()),
        0
    );
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    send(push, b"one");
    recv(pull, 3);
    assert_eq!(events(pull) & ZMQ_POLLIN, 0);
    send(push, b"two");
    recv(pull, 3);
    assert!(!calls.wait_for(1, QUIET), "a cleared callback never runs");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn ready_callback_rejects_a_null_socket() {
    let rc =
        omq_socket_set_ready_callback(std::ptr::null_mut(), Some(on_ready), std::ptr::null_mut());
    assert_eq!(rc, -1);
    assert_eq!(zmq_errno(), libc::EFAULT);
}
