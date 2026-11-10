use std::time::{Duration, Instant};

use super::*;

#[test]
fn padded_string_zeroes_the_rest_of_the_buffer() {
    let mut buf = [0xFF_u8; 8];
    let mut len = buf.len();
    let rc = OptOut::new(buf.as_mut_ptr().cast(), &raw mut len).padded_string(b"omq");
    assert_eq!(rc, Ok(()));
    assert_eq!(len, 4);
    assert_eq!(&buf, b"omq\0\0\0\0\0");

    let mut small = 3;
    let rc = OptOut::new(buf.as_mut_ptr().cast(), &raw mut small).padded_string(b"omq");
    assert_eq!(rc, Err(libc::EINVAL));
    assert_eq!(small, 3);
    let rc = OptOut::new(buf.as_mut_ptr().cast(), std::ptr::null_mut()).padded_string(b"omq");
    assert_eq!(rc, Err(libc::EFAULT));
}

#[test]
fn final_close_and_work_wake_a_waiter_between_check_and_wait() {
    for work in [false, true] {
        let context = OmqContext::new(1);
        let mut work_guard = None;

        let counter = if work {
            work_guard = Some(context.begin_work());
            &context.work_count
        } else {
            context.socket_opened();
            &context.socket_count
        };

        let (lock, cvar) = &context.socket_notify;
        let guard = lock.lock().unwrap();
        assert_eq!(counter.load(Ordering::Acquire), 1);

        let closing = context.clone();
        let thread = std::thread::spawn(move || {
            if let Some(work_guard) = work_guard {
                drop(work_guard);
            } else {
                closing.socket_closed();
            }
        });
        // Force the last completion after the predicate check, while
        // this thread still owns the condition variable's mutex.
        let deadline = Instant::now() + Duration::from_secs(5);

        while counter.load(Ordering::Acquire) != 0 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }

        let (guard, waited) = cvar.wait_timeout(guard, Duration::from_secs(5)).unwrap();
        assert!(!waited.timed_out(), "last completion wake was lost");
        drop(guard);
        thread.join().unwrap();
    }
}

fn shared_context(owner: *mut c_void) -> *mut c_void {
    let (mut hi, mut lo) = (0, 0);
    assert_eq!(omq_ctx_share_key(owner, &raw mut hi, &raw mut lo), 0);
    let shared = omq_ctx_from_share_key(hi, lo);
    assert!(!shared.is_null());

    shared
}

#[test]
fn runtime_teardown_under_a_lingering_close_releases_term() {
    let owner = zmq_ctx_new();
    let shared = shared_context(owner);
    let push = crate::zmq_socket(shared, 8);
    assert_eq!(
        crate::zmq_connect(push, c"inproc://linger-runtime-teardown".as_ptr()),
        0
    );
    // Queued for a peer that never binds, so the default infinite linger
    // keeps the close task alive until the owner's runtime drops it.
    assert_eq!(crate::zmq_send(push, b"x".as_ptr().cast(), 1, 0), 1);
    assert_eq!(crate::zmq_close(push), 0);
    assert_eq!(zmq_ctx_term(owner), 0);

    let shared = shared as usize;
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let _ = done_tx.send(zmq_ctx_term(shared as *mut c_void));
    });

    assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)), Ok(0));
}
