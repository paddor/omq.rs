//! `zmq_proxy` with a muted target waits instead of polling. Own test
//! binary: it measures process CPU time.
#![cfg(target_os = "linux")]
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::{Duration, Instant};

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_proxy_steerable, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PAIR: i32 = 0;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_PULL: i32 = 7;
const ZMQ_PUSH: i32 = 8;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_LINGER: i32 = 17;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_XPUB_NODROP: i32 = 69;

struct ProxyArgs {
    fe: *mut c_void,
    be: *mut c_void,
    ctrl: *mut c_void,
}

// SAFETY: the proxy thread is the only user of these sockets while it runs.
unsafe impl Send for ProxyArgs {}

fn set_i32(sock: *mut c_void, opt: i32, value: i32) {
    assert_eq!(
        zmq_setsockopt(sock, opt, (&value as *const i32).cast(), size_of::<i32>()),
        0
    );
}

/// CPU time and voluntary context switches of this process.
fn usage() -> (Duration, i64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the provided struct.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    // SAFETY: getrusage succeeded.
    let usage = unsafe { usage.assume_init() };
    let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    #[cfg(target_pointer_width = "32")]
    let switches = i64::from(usage.ru_nvcsw);
    #[cfg(target_pointer_width = "64")]
    let switches = usage.ru_nvcsw;
    (
        Duration::from_micros(micros(usage.ru_utime) + micros(usage.ru_stime)),
        switches,
    )
}

#[test]
fn proxy_into_muted_nodrop_publisher_does_not_spin() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUB);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let sub = zmq_socket(ctx, ZMQ_SUB);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);
    for sock in [fe, be, src, sub, ctrl_a, ctrl_b] {
        set_i32(sock, ZMQ_LINGER, 0);
    }
    for sock in [fe, be, src] {
        set_i32(sock, ZMQ_SNDHWM, 16);
        set_i32(sock, ZMQ_RCVHWM, 16);
    }
    set_i32(sub, ZMQ_RCVHWM, 16);
    set_i32(be, ZMQ_XPUB_NODROP, 1);
    assert_eq!(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, std::ptr::null(), 0), 0);

    let addr_fe = CString::new("inproc://proxy-idle-fe").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-idle-ctrl").unwrap();
    assert_eq!(zmq_bind(fe, addr_fe.as_ptr()), 0);
    assert_eq!(zmq_bind(ctrl_a, addr_ctrl.as_ptr()), 0);
    // A wire subscriber that never reads mutes the publisher.
    let publisher = helpers::bind_random_tcp(be);
    assert_eq!(zmq_connect(sub, publisher.as_ptr()), 0);
    assert_eq!(zmq_connect(src, addr_fe.as_ptr()), 0);
    assert_eq!(zmq_connect(ctrl_b, addr_ctrl.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(100));

    let args = ProxyArgs {
        fe,
        be,
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let args = args;
        zmq_proxy_steerable(args.fe, args.be, std::ptr::null_mut(), args.ctrl)
    });

    // Fill until the proxy stops taking input: it then holds a pending send.
    let body = vec![1u8; 64 * 1024];
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut full_since: Option<Instant> = None;
    loop {
        assert!(Instant::now() < deadline, "proxy never backpressured");
        if zmq_send(src, body.as_ptr().cast(), body.len(), ZMQ_DONTWAIT) >= 0 {
            full_since = None;
        } else {
            let since = *full_since.get_or_insert_with(Instant::now);
            if since.elapsed() > Duration::from_millis(200) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    // A 1 ms poll costs little CPU but wakes about 300 times here. The
    // stalled subscriber adds about 30 from its receive space fallback.
    let idle = Duration::from_millis(300);
    let (cpu_before, switches_before) = usage();
    std::thread::sleep(idle);
    let (cpu_after, switches_after) = usage();
    let burned = cpu_after.saturating_sub(cpu_before);
    let wakeups = switches_after - switches_before;
    assert!(
        burned < idle / 10,
        "muted proxy burned {burned:?} CPU in {idle:?}"
    );
    assert!(
        wakeups < 150,
        "muted proxy woke {wakeups} times in {idle:?}"
    );

    // Reading resumes forwarding; control still reaches the proxy.
    set_i32(sub, ZMQ_RCVTIMEO, 5_000);
    let mut buf = vec![0u8; body.len()];
    assert!(zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0) > 0);
    assert_eq!(zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0), 9);
    assert_eq!(proxy.join().unwrap(), 0);

    for sock in [fe, be, src, sub, ctrl_a, ctrl_b] {
        zmq_close(sock);
    }
    zmq_ctx_term(ctx);
}
