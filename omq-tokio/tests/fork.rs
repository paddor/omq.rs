//! A forked child works with a fresh context after its parent blocked on
//! sockets and resolved names. std's thread parking does not survive a
//! fork on macOS, and the resolver's worker threads never do.
//!
//! The only test in its binary: no other test may hold a lock at the fork.
#![cfg(unix)]

use std::time::{Duration, Instant};

use omq_tokio::{Context, Message, Options, SocketType};

const TIMEOUT: Duration = Duration::from_secs(5);

/// The host the sockets bind to. Linux resolves it through the resolver.
/// macOS's resolver library may need libdispatch, which a forked child of
/// a threaded process must not use.
const HOST: &str = if cfg!(target_vendor = "apple") {
    "127.0.0.1"
} else {
    "localhost"
};

fn round_trip(ctx: &Context) -> Result<(), String> {
    let pull = ctx.blocking_socket(SocketType::Pull, Options::default());
    let endpoint = pull
        .bind(format!("tcp://{HOST}:0").as_str())
        .map_err(|e| format!("bind: {e}"))?;
    let push = ctx.blocking_socket(SocketType::Push, Options::default());
    push.connect(endpoint)
        .map_err(|e| format!("connect: {e}"))?;
    push.send(Message::single("ping"))
        .map_err(|e| format!("send: {e}"))?;
    // The message is in flight, so this thread parks first.
    let message = pull
        .recv_timeout(TIMEOUT)
        .map_err(|e| format!("recv: {e}"))?;

    if message.part_slice(0) == Some(b"ping".as_slice()) {
        Ok(())
    } else {
        Err("wrong message".into())
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
fn a_forked_child_gets_working_sockets_on_a_fresh_context() {
    let parent = Context::new();
    round_trip(&parent).expect("parent before fork");

    // SAFETY: the child only creates its own context and sockets, then
    // leaves with `_exit` without running destructors.
    let child = unsafe { libc::fork() };
    assert_ne!(child, -1, "fork failed");
    if child == 0 {
        let outcome = std::panic::catch_unwind(|| round_trip(&Context::new()));
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
    round_trip(&parent).expect("parent after fork");
}
