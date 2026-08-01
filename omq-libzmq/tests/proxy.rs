//! `zmq_proxy` tests, including large (>64 KB) message forwarding.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr, clippy::similar_names)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_msg_close,
    zmq_msg_data, zmq_msg_init, zmq_msg_recv, zmq_msg_send, zmq_msg_size, zmq_proxy_steerable,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PAIR: i32 = 0;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_XPUB: i32 = 9;
const ZMQ_XSUB: i32 = 10;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_LINGER: i32 = 17;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;

fn set_i32(sock: *mut c_void, opt: i32, value: i32) {
    zmq_setsockopt(sock, opt, (&value as *const i32).cast(), size_of::<i32>());
}

fn set_timeo(sock: *mut c_void, opt: i32, ms: i32) {
    set_i32(sock, opt, ms);
}

fn set_linger(sock: *mut c_void, ms: i32) {
    set_i32(sock, ZMQ_LINGER, ms);
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

#[repr(C)]
struct ZmqMsg([usize; ZMQ_MSG_WORDS]);

impl ZmqMsg {
    fn zeroed() -> Self {
        Self([0; ZMQ_MSG_WORDS])
    }

    fn new() -> Self {
        let mut m = Self::zeroed();
        zmq_msg_init(m.0.as_mut_ptr().cast());
        m
    }
}

struct ProxyArgs {
    fe: *mut c_void,
    be: *mut c_void,
    cap: *mut c_void,
    ctrl: *mut c_void,
}

// SAFETY: ZMQ sockets are thread-safe when used by one thread at a time.
unsafe impl Send for ProxyArgs {}

struct SocketArg(*mut c_void);

// SAFETY: tests move each socket to exactly one thread.
unsafe impl Send for SocketArg {}

impl SocketArg {
    fn into_ptr(self) -> *mut c_void {
        self.0
    }
}

/// Proxy forwards a small message (well under 64 KB).
#[test]
fn proxy_small_message() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_fe = CString::new("inproc://proxy-fe-small").unwrap();
    let addr_be = CString::new("inproc://proxy-be-small").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-small").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);
    set_timeo(src, ZMQ_SNDTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    let payload = b"small proxy test";
    zmq_send(src, payload.as_ptr().cast(), payload.len(), 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(&buf[..payload.len()], payload);

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    proxy.join().ok();

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_retries_pending_after_bypass_backpressure() {
    const N: usize = 64;

    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let hwm = 16i32;
    zmq_setsockopt(
        be,
        ZMQ_SNDHWM,
        (&hwm as *const i32).cast(),
        size_of::<i32>(),
    );
    zmq_setsockopt(
        dst,
        ZMQ_RCVHWM,
        (&hwm as *const i32).cast(),
        size_of::<i32>(),
    );

    let addr_fe = CString::new("inproc://proxy-fe-backpressure").unwrap();
    let addr_be = CString::new("inproc://proxy-be-backpressure").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-backpressure").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    let src_arg = SocketArg(src);
    let sender = std::thread::spawn(move || {
        let src = src_arg.into_ptr();
        let mut sent = 0usize;
        while sent < N {
            let data = (sent as u32).to_le_bytes();
            let rc = zmq_send(src, data.as_ptr().cast(), data.len(), ZMQ_DONTWAIT);
            if rc == i32::try_from(data.len()).expect("test payload len fits i32") {
                sent += 1;
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        SocketArg(src)
    });

    std::thread::sleep(Duration::from_millis(200));

    let mut got = Vec::with_capacity(N);
    let mut buf = [0u8; 4];
    for _ in 0..N {
        let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 4, "dst recv failed (errno={})", omq_zmq::zmq_errno());
        got.push(u32::from_le_bytes(buf));
    }
    assert_eq!(got, (0..N as u32).collect::<Vec<_>>());

    let src = sender.join().unwrap().0;
    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_does_not_starve_reverse_direction_when_frontend_is_hot() {
    const HOT_MESSAGES: u32 = 128;

    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PAIR);
    let be = zmq_socket(ctx, ZMQ_PAIR);
    let left = zmq_socket(ctx, ZMQ_PAIR);
    let right = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    // This test builds frontend-to-backend backlog before asserting reverse
    // progress. Keep teardown independent of any late queued sends.
    for sock in [fe, be, left, right, ctrl_a, ctrl_b] {
        set_linger(sock, 0);
    }

    let hwm = 16i32;
    zmq_setsockopt(
        be,
        ZMQ_SNDHWM,
        (&hwm as *const i32).cast(),
        size_of::<i32>(),
    );
    zmq_setsockopt(
        right,
        ZMQ_RCVHWM,
        (&hwm as *const i32).cast(),
        size_of::<i32>(),
    );

    let addr_fe = CString::new("inproc://proxy-fe-starve").unwrap();
    let addr_be = CString::new("inproc://proxy-be-starve").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-starve").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(left, addr_fe.as_ptr());
    zmq_connect(right, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(left, ZMQ_RCVTIMEO, 5000);
    set_timeo(right, ZMQ_SNDTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    let mut hot_sent = 0usize;
    for i in 0..HOT_MESSAGES {
        let data = i.to_le_bytes();
        let rc = zmq_send(left, data.as_ptr().cast(), data.len(), ZMQ_DONTWAIT);
        if rc == i32::try_from(data.len()).expect("test payload len fits i32") {
            hot_sent += 1;
        }
    }
    assert!(
        hot_sent > hwm as usize,
        "hot path did not build backlog (sent {hot_sent})"
    );

    let payload = b"right-to-left";
    let rc = zmq_send(right, payload.as_ptr().cast(), payload.len(), 0);
    assert_eq!(rc as usize, payload.len());

    let mut buf = [0u8; 64];
    let rc = zmq_recv(left, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc as usize,
        payload.len(),
        "left recv failed (errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(&buf[..payload.len()], payload);

    set_timeo(right, ZMQ_RCVTIMEO, 5000);
    let mut hot_buf = [0u8; 4];
    for n in 0..hot_sent {
        let rc = zmq_recv(right, hot_buf.as_mut_ptr().cast(), hot_buf.len(), 0);
        assert_eq!(
            rc,
            4,
            "right hot recv {n}/{hot_sent} failed (errno={})",
            omq_zmq::zmq_errno()
        );
    }

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(left);
    zmq_close(right);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_multipart_message() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_fe = CString::new("inproc://proxy-fe-multipart").unwrap();
    let addr_be = CString::new("inproc://proxy-be-multipart").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-multipart").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);
    set_timeo(src, ZMQ_SNDTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    zmq_send(src, b"part-a".as_ptr().cast(), 6, ZMQ_SNDMORE);
    zmq_send(src, b"part-b".as_ptr().cast(), 6, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"part-a");
    assert!(rcvmore(dst));
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"part-b");
    assert!(!rcvmore(dst));

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_capture_gets_copy() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let cap = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let cap_dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_fe = CString::new("inproc://proxy-fe-capture").unwrap();
    let addr_be = CString::new("inproc://proxy-be-capture").unwrap();
    let addr_cap = CString::new("inproc://proxy-cap-capture").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-capture").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(cap, addr_cap.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(cap_dst, addr_cap.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);
    set_timeo(cap_dst, ZMQ_RCVTIMEO, 5000);

    let args = ProxyArgs {
        fe,
        be,
        cap,
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    let payload = b"capture copy";
    zmq_send(src, payload.as_ptr().cast(), payload.len(), 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(&buf[..payload.len()], payload);
    let rc = zmq_recv(cap_dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(&buf[..payload.len()], payload);

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(src);
    zmq_close(dst);
    zmq_close(cap_dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(cap);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_pause_resume() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_fe = CString::new("inproc://proxy-fe-pause").unwrap();
    let addr_be = CString::new("inproc://proxy-be-pause").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-ctrl-pause").unwrap();

    zmq_bind(fe, addr_fe.as_ptr());
    zmq_bind(be, addr_be.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(dst, ZMQ_RCVTIMEO, 100);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    zmq_send(ctrl_b, b"PAUSE".as_ptr().cast(), 5, 0);
    std::thread::sleep(Duration::from_millis(100));

    let payload = b"paused payload";
    zmq_send(src, payload.as_ptr().cast(), payload.len(), 0);
    let mut buf = [0u8; 64];
    assert!(zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0) < 0);

    set_timeo(dst, ZMQ_RCVTIMEO, 5000);
    zmq_send(ctrl_b, b"RESUME".as_ptr().cast(), 6, 0);
    let rc = zmq_recv(dst, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(&buf[..payload.len()], payload);

    zmq_send(ctrl_b, b"KILL".as_ptr().cast(), 4, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

/// Proxy correctly forwards messages larger than 64 KB.
///
/// Before the fix, `forward()` used a 64 KB stack buffer with `zmq_recv`,
/// which returned the full frame length even when truncated. Indexing
/// `buf[..len]` with `len > 65536` panicked on the bounds check.
#[test]
fn proxy_large_message() {
    let ctx = zmq_ctx_new();
    let fe = zmq_socket(ctx, ZMQ_PULL);
    let be = zmq_socket(ctx, ZMQ_PUSH);
    let src = zmq_socket(ctx, ZMQ_PUSH);
    let dst = zmq_socket(ctx, ZMQ_PULL);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_ctrl = CString::new("inproc://proxy-ctrl-large").unwrap();

    let addr_fe = helpers::bind_random_tcp(fe);
    let addr_be = helpers::bind_random_tcp(be);
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(src, addr_fe.as_ptr());
    zmq_connect(dst, addr_be.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    std::thread::sleep(Duration::from_millis(200));

    set_timeo(dst, ZMQ_RCVTIMEO, 10000);
    set_timeo(src, ZMQ_SNDTIMEO, 10000);

    let args = ProxyArgs {
        fe,
        be,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    // 128 KB payload: well above the old 64 KB stack buffer limit.
    let size = 128 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let mut msg = ZmqMsg::zeroed();
    omq_zmq::zmq_msg_init_size(msg.0.as_mut_ptr().cast(), size);
    let data = zmq_msg_data(msg.0.as_mut_ptr().cast());
    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), data.cast::<u8>(), size) };
    let rc = zmq_msg_send(msg.0.as_mut_ptr().cast(), src, 0);
    assert_eq!(rc as usize, size);

    let mut recv_msg = ZmqMsg::new();
    let rc = zmq_msg_recv(recv_msg.0.as_mut_ptr().cast(), dst, 0);
    assert_eq!(rc as usize, size);
    assert_eq!(zmq_msg_size(recv_msg.0.as_ptr().cast()), size);
    let got = unsafe {
        std::slice::from_raw_parts(
            zmq_msg_data(recv_msg.0.as_mut_ptr().cast()).cast::<u8>(),
            size,
        )
    };
    assert_eq!(got, &payload[..]);

    zmq_msg_close(recv_msg.0.as_mut_ptr().cast());
    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    proxy.join().ok();

    zmq_close(src);
    zmq_close(dst);
    zmq_close(ctrl_b);
    zmq_close(fe);
    zmq_close(be);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}

#[test]
fn proxy_forwards_xpub_subscriptions_to_xsub() {
    let ctx = zmq_ctx_new();
    let xsub = zmq_socket(ctx, ZMQ_XSUB);
    let xpub = zmq_socket(ctx, ZMQ_XPUB);
    let publisher = zmq_socket(ctx, ZMQ_PUB);
    let subscriber = zmq_socket(ctx, ZMQ_SUB);
    let ctrl_a = zmq_socket(ctx, ZMQ_PAIR);
    let ctrl_b = zmq_socket(ctx, ZMQ_PAIR);

    let addr_xsub = CString::new("inproc://proxy-xsub-side").unwrap();
    let addr_xpub = CString::new("inproc://proxy-xpub-side").unwrap();
    let addr_ctrl = CString::new("inproc://proxy-xpub-ctrl").unwrap();

    zmq_bind(xsub, addr_xsub.as_ptr());
    zmq_bind(xpub, addr_xpub.as_ptr());
    zmq_bind(ctrl_a, addr_ctrl.as_ptr());
    zmq_connect(publisher, addr_xsub.as_ptr());
    zmq_connect(subscriber, addr_xpub.as_ptr());
    zmq_connect(ctrl_b, addr_ctrl.as_ptr());
    set_timeo(subscriber, ZMQ_RCVTIMEO, 5000);

    let args = ProxyArgs {
        fe: xsub,
        be: xpub,
        cap: std::ptr::null_mut(),
        ctrl: ctrl_a,
    };
    let proxy = std::thread::spawn(move || {
        let a = args;
        zmq_proxy_steerable(a.fe, a.be, a.cap, a.ctrl)
    });

    std::thread::sleep(Duration::from_millis(100));
    zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, b"news.".as_ptr().cast(), 5);
    std::thread::sleep(Duration::from_millis(300));

    let payload = b"news.hello";
    zmq_send(publisher, payload.as_ptr().cast(), payload.len(), 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(subscriber, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc as usize,
        payload.len(),
        "subscriber recv failed (errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(&buf[..payload.len()], payload);

    zmq_send(ctrl_b, b"TERMINATE".as_ptr().cast(), 9, 0);
    assert_eq!(proxy.join().unwrap(), 0);

    zmq_close(publisher);
    zmq_close(subscriber);
    zmq_close(ctrl_b);
    zmq_close(xsub);
    zmq_close(xpub);
    zmq_close(ctrl_a);
    zmq_ctx_term(ctx);
}
