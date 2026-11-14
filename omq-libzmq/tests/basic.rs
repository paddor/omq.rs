//! Basic bind/connect/send/recv tests (PUSH/PULL over TCP and inproc).
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
#![allow(clippy::cast_possible_wrap)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_shutdown, zmq_ctx_term, zmq_getsockopt,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_PAIR: i32 = 0;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_ETERM: i32 = 156_384_765;

fn caddr(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[cfg(unix)]
fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts points to writable memory and CLOCK_THREAD_CPUTIME_ID needs no extra invariants.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID)");

    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// A signal handler that returns ends a blocking receive with `EINTR`,
/// as in libzmq. A program can then act on the signal, such as Ctrl-C.
#[cfg(unix)]
#[test]
fn blocking_recv_returns_eintr_when_a_signal_interrupts_it() {
    extern "C" fn on_signal(_signal: libc::c_int) {}
    // SAFETY: the handler does nothing, which is async-signal-safe.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::sigemptyset(&raw mut action.sa_mask);
        assert_eq!(
            libc::sigaction(libc::SIGUSR1, &raw const action, std::ptr::null_mut()),
            0
        );
    }

    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://test-recv-eintr").unwrap();
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    let (started, thread_id) = std::sync::mpsc::channel();
    let socket = pull as usize;
    let receiver = std::thread::spawn(move || {
        // SAFETY: pthread_self has no preconditions.
        started
            .send(unsafe { libc::pthread_self() } as usize)
            .unwrap();
        let mut buf = [0u8; 8];
        let rc = zmq_recv(socket as *mut c_void, buf.as_mut_ptr().cast(), buf.len(), 0);
        (rc, omq_zmq::zmq_errno())
    });
    let thread = thread_id.recv().unwrap() as libc::pthread_t;

    // Early signals can land before the receive blocks, so keep sending.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !receiver.is_finished() && Instant::now() < deadline {
        // SAFETY: the thread is alive until it is joined below.
        unsafe { libc::pthread_kill(thread, libc::SIGUSR1) };
        std::thread::sleep(Duration::from_millis(1));
    }
    let interrupted = receiver.is_finished();
    // Ends the receive in any case, so the test cannot hang.
    zmq_ctx_shutdown(ctx);
    let result = receiver.join().unwrap();
    assert!(interrupted, "the signal did not end the receive");
    assert_eq!(result, (-1, libc::EINTR));

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Install a handler for SIGUSR1 that does nothing but return.
#[cfg(unix)]
fn install_returning_signal_handler() {
    extern "C" fn on_signal(_signal: libc::c_int) {}
    // SAFETY: the handler does nothing, which is async-signal-safe.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as usize;
        libc::sigemptyset(&raw mut action.sa_mask);
        assert_eq!(
            libc::sigaction(libc::SIGUSR1, &raw const action, std::ptr::null_mut()),
            0
        );
    }
}

/// What the sending thread of [`interrupted_send`] reports.
#[cfg(unix)]
struct InterruptedSend {
    /// Result and errno of the blocking send of the final frame.
    result: (i32, i32),
    /// Whether the signal ended that send, not the fallback shutdown.
    interrupted: bool,
    /// Messages that filled the queue first.
    filled: usize,
    /// Tells the thread to retry the final frame.
    retry: std::sync::mpsc::Sender<()>,
    /// Returns the result of the retry.
    thread: std::thread::JoinHandle<i32>,
}

/// On one thread, as the C API requires for a socket: optionally fill the
/// queue, stage every frame but the last, then send the last one blocking.
/// This thread signals the sender until that send returns.
#[cfg(unix)]
fn interrupted_send(
    ctx: *mut c_void,
    sock: *mut c_void,
    fill: bool,
    frames: &'static [&'static [u8]],
) -> InterruptedSend {
    use std::sync::mpsc;
    let (started, blocking) = mpsc::channel();
    let (done, result) = mpsc::channel();
    let (retry, retried) = mpsc::channel::<()>();
    let socket = sock as usize;
    let thread = std::thread::spawn(move || {
        let sock = socket as *mut c_void;
        let send = |frame: &[u8], flags| zmq_send(sock, frame.as_ptr().cast(), frame.len(), flags);
        // A send that times out shows a queue that stays full. One refused
        // without waiting can still find room a moment later.
        let mut filled = 0;
        if fill {
            helpers::set_sndtimeo(sock, 100);
            while send(b"fill", 0) == 4 {
                filled += 1;
                assert!(filled < 10_000, "the queue never filled");
            }
            helpers::set_sndtimeo(sock, -1);
        }
        let (last, staged) = frames.split_last().unwrap();
        for frame in staged {
            assert_eq!(send(frame, ZMQ_SNDMORE), frame.len() as i32);
        }
        // SAFETY: pthread_self has no preconditions.
        let id = unsafe { libc::pthread_self() } as usize;
        started.send((id, filled)).unwrap();
        let rc = send(last, 0);
        done.send((rc, omq_zmq::zmq_errno())).unwrap();
        retried.recv().map_or(-1, |()| send(last, 0))
    });
    let (thread_id, filled) = blocking.recv().unwrap();

    // Early signals can land before the send blocks, so keep sending.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut outcome = None;
    while outcome.is_none() && Instant::now() < deadline {
        // SAFETY: the thread lives until it is joined by the caller.
        unsafe { libc::pthread_kill(thread_id as libc::pthread_t, libc::SIGUSR1) };
        outcome = result.recv_timeout(Duration::from_millis(1)).ok();
    }
    let interrupted = outcome.is_some();
    // Ends the send in any case, so the test cannot hang.
    let result = outcome.unwrap_or_else(|| {
        zmq_ctx_shutdown(ctx);
        result.recv().unwrap()
    });
    InterruptedSend {
        result,
        interrupted,
        filled,
        retry,
        thread,
    }
}

/// A signal handler that returns ends a blocking send with `EINTR`, as in
/// libzmq, while the send waits for a peer.
#[cfg(unix)]
#[test]
fn blocking_send_without_a_peer_returns_eintr_when_a_signal_interrupts_it() {
    install_returning_signal_handler();
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let addr = caddr("inproc://test-send-eintr-no-peer");
    assert_eq!(zmq_bind(push, addr.as_ptr()), 0);

    let send = interrupted_send(ctx, push, false, &[b"once"]);
    assert!(send.interrupted, "the signal did not end the send");
    assert_eq!(send.result, (-1, libc::EINTR));

    // The interrupted message was not queued: a peer gets only the retry.
    let pull = zmq_socket(ctx, ZMQ_PULL);
    helpers::set_rcvtimeo(pull, 2000);
    assert_eq!(zmq_connect(pull, addr.as_ptr()), 0);
    send.retry.send(()).unwrap();
    assert_eq!(send.thread.join().unwrap(), 4);
    let mut buf = [0u8; 8];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 4);
    assert_eq!(&buf[..4], b"once");
    helpers::set_rcvtimeo(pull, 50);
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), -1);

    zmq_close(pull);
    zmq_close(push);
    zmq_ctx_term(ctx);
}

/// The same on a full queue, with a multipart message: the frames staged
/// before the interrupted final frame stay staged, so a retry of that
/// frame sends the whole message once.
#[cfg(unix)]
#[test]
fn blocking_send_on_a_full_queue_returns_eintr_and_keeps_staged_frames() {
    const ZMQ_SNDHWM: i32 = 23;
    const ZMQ_RCVHWM: i32 = 24;
    install_returning_signal_handler();
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let one = 1i32;
    for (sock, option) in [(pull, ZMQ_RCVHWM), (push, ZMQ_SNDHWM)] {
        assert_eq!(
            zmq_setsockopt(sock, option, (&one as *const i32).cast(), size_of::<i32>()),
            0
        );
    }
    let addr = caddr("inproc://test-send-eintr-full");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    let send = interrupted_send(ctx, push, true, &[b"head", b"tail"]);
    assert!(send.interrupted, "the signal did not end the send");
    assert_eq!(send.result, (-1, libc::EINTR));

    // Drain the fill messages; the retry of the final frame then goes out.
    helpers::set_rcvtimeo(pull, 2000);
    let mut buf = [0u8; 8];
    for _ in 0..send.filled {
        assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 4);
        assert_eq!(&buf[..4], b"fill");
    }
    send.retry.send(()).unwrap();
    assert_eq!(send.thread.join().unwrap(), 4);
    for expected in [b"head", b"tail"] {
        assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 4);
        assert_eq!(&buf[..4], expected);
    }
    helpers::set_rcvtimeo(pull, 50);
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), -1);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// A socket without endpoints is a socket without peers, as in libzmq.
/// The context is not terminated, so `ETERM` would mislead callers.
#[test]
fn io_before_bind_or_connect_sees_a_socket_without_peers() {
    let ctx = zmq_ctx_new();
    let mut buf = [0u8; 8];

    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert_eq!(
        zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);
    let mut msg = [0u8; 64];
    assert_eq!(omq_zmq::zmq_msg_init(msg.as_mut_ptr().cast()), 0);
    assert_eq!(
        omq_zmq::zmq_msg_recv(msg.as_mut_ptr().cast(), pull, ZMQ_DONTWAIT),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    // A bound-side PUSH without a peer mutes.
    let push = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(zmq_send(push, b"x".as_ptr().cast(), 1, ZMQ_DONTWAIT), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    // PUB drops for lack of subscribers.
    let publisher = zmq_socket(ctx, ZMQ_PUB);
    assert_eq!(zmq_send(publisher, b"x".as_ptr().cast(), 1, 0), 1);

    for socket in [pull, push, publisher] {
        assert_eq!(zmq_close(socket), 0);
    }
    zmq_ctx_term(ctx);
}

#[test]
fn send_recv_null_nonzero_buffer_returns_efault() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null() && !pull.is_null());

    let rc = zmq_send(push, std::ptr::null(), 1, 0);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let rc = zmq_recv(pull, std::ptr::null_mut(), 1, ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_shutdown_wakes_blocking_recv() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!pull.is_null());

    let addr = caddr("inproc://test-basic-ctx-shutdown-wakes-recv");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);

    let pull_addr = pull as usize;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let mut buf = [0u8; 8];
        entered_tx.send(()).expect("entered");
        let rc = zmq_recv(
            pull_addr as *mut c_void,
            buf.as_mut_ptr().cast(),
            buf.len(),
            0,
        );
        done_tx.send((rc, omq_zmq::zmq_errno())).expect("done");
    });

    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("recv thread entered");
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    let (rc, errno) = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("recv should wake on context shutdown");
    assert_eq!(rc, -1);
    assert_eq!(errno, ZMQ_ETERM);
    handle.join().expect("recv thread");

    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn push_pull_inproc() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null() && !pull.is_null());

    let addr = caddr("inproc://test-basic-push-pull");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    // Give the connection a moment.
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(pull, 1000);
    helpers::set_sndtimeo(push, 1000);

    let msg = b"hello";
    let rc = zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
    assert_eq!(rc, msg.len() as i32);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, msg.len() as i32);
    assert_eq!(&buf[..rc as usize], msg);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[cfg(unix)]
#[test]
fn recv_timeout_after_stale_signal_does_not_spin() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert!(!push.is_null() && !pull.is_null());

    let addr = caddr("inproc://test-basic-recv-timeout-stale-signal");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(pull, 100);
    helpers::set_sndtimeo(push, 1000);

    let payload = b"x";
    assert_eq!(zmq_send(push, payload.as_ptr().cast(), payload.len(), 0), 1);
    let mut buf = [0u8; 8];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 1);

    let wall_start = Instant::now();
    let cpu_start = thread_cpu_time();
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    let wall = wall_start.elapsed();
    let cpu = thread_cpu_time().saturating_sub(cpu_start);

    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);
    assert!(
        wall >= Duration::from_millis(50),
        "recv timeout returned too early: {wall:?}",
    );
    assert!(
        cpu < Duration::from_millis(25),
        "recv timeout spun on CPU: {cpu:?}",
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn push_pull_tcp() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);

    let addr = helpers::bind_random_tcp(pull);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);

    std::thread::sleep(Duration::from_millis(50));

    helpers::set_rcvtimeo(pull, 2000);
    helpers::set_sndtimeo(push, 2000);

    for i in 0u8..5 {
        let payload = [i; 32];
        let rc = zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);
        assert_eq!(rc, 32);
    }

    let mut buf = [0u8; 64];
    for i in 0u8..5 {
        let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 32);
        assert!(buf[..32].iter().all(|&b| b == i));
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// libzmq has no part limit, so a message far above the omq-tokio default
/// of 1024 parts must still arrive.
#[test]
fn multipart_above_the_default_part_limit_arrives_over_tcp() {
    const PARTS: usize = 3000;
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let addr = helpers::bind_random_tcp(pull);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    helpers::set_rcvtimeo(pull, 5000);
    helpers::set_sndtimeo(push, 5000);

    for index in 0..PARTS {
        let flags = if index + 1 < PARTS { ZMQ_SNDMORE } else { 0 };
        assert_eq!(zmq_send(push, b"p".as_ptr().cast(), 1, flags), 1);
    }
    let mut buf = [0u8; 8];
    for _ in 0..PARTS {
        assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 1);
    }
    let mut more: i32 = 1;
    let mut more_sz = size_of::<i32>();
    zmq_getsockopt(
        pull,
        ZMQ_RCVMORE,
        (&mut more as *mut i32).cast(),
        &mut more_sz,
    );
    assert_eq!(more, 0, "the message has exactly {PARTS} parts");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn dontwait_returns_eagain_when_empty() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = caddr("inproc://test-basic-eagain");
    zmq_bind(pull, addr.as_ptr());

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    // EAGAIN
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn multipart_sndmore_rcvmore() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = caddr("inproc://test-basic-multipart");
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(pull, 1000);
    helpers::set_sndtimeo(push, 1000);

    // Send a 3-part message.
    zmq_send(push, b"part1".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part2".as_ptr().cast(), 5, ZMQ_SNDMORE);
    zmq_send(push, b"part3".as_ptr().cast(), 5, 0);

    let mut buf = [0u8; 64];

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part1");

    // RCVMORE via getsockopt
    let mut more: i32 = 0;
    let mut more_sz = size_of::<i32>();
    zmq_getsockopt(
        pull,
        ZMQ_RCVMORE,
        (&mut more as *mut i32).cast(),
        &mut more_sz,
    );
    assert_ne!(more, 0, "expected RCVMORE after first frame");

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part2");

    let n = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], b"part3");

    let mut more: i32 = 0;
    zmq_getsockopt(
        pull,
        ZMQ_RCVMORE,
        (&mut more as *mut i32).cast(),
        &mut more_sz,
    );
    assert_eq!(more, 0, "expected no RCVMORE after last frame");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn pair_inproc_roundtrip() {
    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_PAIR);
    let b = zmq_socket(ctx, ZMQ_PAIR);

    let addr = caddr("inproc://test-basic-pair");
    zmq_bind(a, addr.as_ptr());
    zmq_connect(b, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(a, 1000);
    helpers::set_rcvtimeo(b, 1000);

    let rc = zmq_send(a, b"ping".as_ptr().cast(), 4, 0);
    assert_eq!(rc, 4);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 4);
    assert_eq!(&buf[..4], b"ping");

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}

#[test]
fn pub_sub_inproc() {
    let ctx = zmq_ctx_new();
    let pub_sock = zmq_socket(ctx, ZMQ_PUB);
    let sub_sock = zmq_socket(ctx, ZMQ_SUB);

    let addr = caddr("inproc://test-basic-pubsub");
    zmq_bind(pub_sock, addr.as_ptr());

    // Subscribe to prefix "hello"
    zmq_setsockopt(sub_sock, ZMQ_SUBSCRIBE, b"hello".as_ptr().cast(), 5);
    zmq_connect(sub_sock, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    helpers::set_rcvtimeo(sub_sock, 1000);

    // Matching message
    zmq_send(pub_sock, b"hello world".as_ptr().cast(), 11, 0);
    // Non-matching message
    zmq_send(pub_sock, b"other stuff".as_ptr().cast(), 11, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub_sock, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc > 0);
    assert_eq!(&buf[..11], b"hello world");

    // The second message should not arrive.
    let rc = zmq_recv(sub_sock, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(pub_sock);
    zmq_close(sub_sock);
    zmq_ctx_term(ctx);
}

#[test]
fn req_rep_inproc() {
    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    let req = zmq_socket(ctx, ZMQ_REQ);

    let addr = caddr("inproc://test-basic-reqrep");
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(rep, 1000);
    helpers::set_rcvtimeo(req, 1000);

    zmq_send(req, b"request".as_ptr().cast(), 7, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);

    zmq_send(rep, b"reply".as_ptr().cast(), 5, 0);
    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"reply");

    zmq_close(rep);
    zmq_close(req);
    zmq_ctx_term(ctx);
}

#[test]
fn send_empty_message() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = caddr("inproc://test-basic-empty");
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    helpers::set_rcvtimeo(pull, 1000);

    // Send zero-length message.
    let rc = zmq_send(push, std::ptr::null(), 0, 0);
    assert_eq!(rc, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 0);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// On another thread, queue messages until a send would block, then block
/// in `zmq_send`. Shut the context down and return that send's result.
fn shutdown_during_blocked_send(ctx: *mut c_void, sock: *mut c_void) -> (i32, i32) {
    let sock_addr = sock as usize;
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let sock = sock_addr as *mut c_void;
        // A connect-side pipe moves into the peer's queue once the peer is
        // ready, which frees capacity after the queue looked full. Refill
        // whenever a blocking send still succeeds.
        let mut queued = 0;

        let result = loop {
            while zmq_send(sock, b"x".as_ptr().cast(), 1, ZMQ_DONTWAIT) == 1 {
                queued += 1;
                assert!(queued < 1024, "send never blocked");
            }

            let rc = zmq_send(sock, b"x".as_ptr().cast(), 1, 0);
            if rc != 1 {
                break (rc, omq_zmq::zmq_errno());
            }

            queued += 1;
            assert!(queued < 1024, "send never blocked");
        };

        done_tx.send(result).expect("done");
    });

    assert_eq!(
        done_rx.recv_timeout(Duration::from_millis(200)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "send should block"
    );
    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    let result = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("send should wake on context shutdown");
    handle.join().expect("send thread");

    result
}

#[test]
fn ctx_shutdown_wakes_send_waiting_for_a_peer() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let addr = caddr("inproc://test-basic-shutdown-send-no-peer");
    assert_eq!(zmq_bind(push, addr.as_ptr()), 0);
    assert_eq!(shutdown_during_blocked_send(ctx, push), (-1, ZMQ_ETERM));
    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_shutdown_wakes_send_blocked_on_a_full_pipe() {
    const ZMQ_SNDHWM: i32 = 23;
    const ZMQ_RCVHWM: i32 = 24;
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let one = 1i32;

    for (sock, option) in [(pull, ZMQ_RCVHWM), (push, ZMQ_SNDHWM)] {
        assert_eq!(
            zmq_setsockopt(sock, option, (&one as *const i32).cast(), size_of::<i32>()),
            0
        );
    }

    let addr = caddr("inproc://test-basic-shutdown-send-full");
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(shutdown_during_blocked_send(ctx, push), (-1, ZMQ_ETERM));
    assert_eq!(zmq_close(push), 0);
    assert_eq!(zmq_close(pull), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

/// C callers test `errno` itself after a failed call, as libzmq documents.
#[cfg(unix)]
#[test]
fn a_failed_call_sets_the_c_errno() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let mut buf = [0u8; 8];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), ZMQ_DONTWAIT);
    let c_errno = std::io::Error::last_os_error().raw_os_error();
    assert_eq!(rc, -1);
    assert_eq!(c_errno, Some(omq_zmq::zmq_errno()));
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
