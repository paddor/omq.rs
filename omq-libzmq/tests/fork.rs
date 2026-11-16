//! A forked child creates a context and uses it after its parent made
//! blocking calls. std's thread parking does not survive a fork on macOS,
//! so no C API call may wait through it.
//!
//! The only test in its binary: no other test may hold a lock at the fork.
#![cfg(unix)]

mod helpers;

use std::ffi::c_void;
use std::time::{Duration, Instant};

use omq_zmq::{
    zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_errno, zmq_recv, zmq_send, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_LINGER: i32 = 17;
const ZMQ_RCVTIMEO: i32 = 27;
const TIMEOUT: Duration = Duration::from_secs(5);

/// One message over TCP. Bind, connect and the receive all block.
fn round_trip(ctx: *mut c_void) -> Result<(), String> {
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    if pull.is_null() || push.is_null() {
        return Err(format!("zmq_socket: errno {}", zmq_errno()));
    }

    helpers::set_int_opt(pull, ZMQ_RCVTIMEO, 5000);
    helpers::set_int_opt(push, ZMQ_LINGER, 0);
    let endpoint = helpers::bind_random_tcp(pull);
    if zmq_connect(push, endpoint.as_ptr()) != 0 {
        return Err(format!("zmq_connect: errno {}", zmq_errno()));
    }

    if zmq_send(push, b"ping".as_ptr().cast(), 4, 0) != 4 {
        return Err(format!("zmq_send: errno {}", zmq_errno()));
    }

    let mut buf = [0u8; 8];
    let received = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    let errno = zmq_errno();
    zmq_close(push);
    zmq_close(pull);

    if received == 4 && &buf[..4] == b"ping" {
        Ok(())
    } else {
        Err(format!("zmq_recv returned {received}, errno {errno}"))
    }
}

fn wait(child: libc::pid_t) -> libc::c_int {
    let deadline = Instant::now() + 4 * TIMEOUT;
    let mut status = 0;
    loop {
        // SAFETY: waits for our own child; `status` outlives the call.
        let done = unsafe { libc::waitpid(child, &raw mut status, libc::WNOHANG) };
        assert_ne!(done, -1, "waitpid failed");
        if done == child {
            return status;
        }

        if Instant::now() >= deadline {
            // SAFETY: kills our own, hung child.
            unsafe { libc::kill(child, libc::SIGKILL) };
            panic!("forked child hung");
        }

        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_forked_child_creates_and_uses_a_context() {
    let parent = zmq_ctx_new();
    round_trip(parent).expect("parent before fork");

    // SAFETY: the child only creates its own context and sockets, then
    // leaves with `_exit` without running destructors.
    let child = unsafe { libc::fork() };
    assert_ne!(child, -1, "fork failed");
    if child == 0 {
        let outcome = std::panic::catch_unwind(|| round_trip(zmq_ctx_new()));
        let code = match outcome {
            Ok(Ok(())) => 0,
            Ok(Err(error)) => {
                eprintln!("forked child: {error}");
                1
            }
            Err(_) => 2,
        };
        // SAFETY: ends the child without the parent's atexit handlers.
        unsafe { libc::_exit(code) };
    }

    let status = wait(child);
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "forked child failed: status {status:#x}"
    );
    round_trip(parent).expect("parent after fork");
    assert_eq!(zmq_ctx_term(parent), 0);
}
