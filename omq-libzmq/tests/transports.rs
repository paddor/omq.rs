//! Transport tests: IPC, IPC abstract namespace, lz4+tcp.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_SUBSCRIBE: i32 = 6;

fn set_timeo(sock: *mut c_void, ms: i32) {
    zmq_setsockopt(
        sock,
        ZMQ_RCVTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
    zmq_setsockopt(
        sock,
        ZMQ_SNDTIMEO,
        (&ms as *const i32).cast(),
        size_of::<i32>(),
    );
}

/// Generate platform-specific IPC endpoint for testing.
/// - Windows: named pipe format (`ipc://name`)
/// - Linux: abstract namespace (`ipc://@name`)
/// - Other Unix: filesystem (`ipc:///tmp/name.sock`)
fn ipc_test_endpoint(name: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let suffix = format!("{name}-{pid}-{nanos}");

    #[cfg(target_os = "windows")]
    {
        format!("ipc://omq-libzmq-{suffix}")
    }

    #[cfg(target_os = "linux")]
    {
        format!("ipc://@omq-libzmq-{suffix}")
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        format!("ipc:///tmp/omq-libzmq-{suffix}.sock")
    }
}

// --- IPC (cross-platform) ---

#[test]
fn ipc_push_pull() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new(ipc_test_endpoint("push-pull")).unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(push, 2000);
    set_timeo(pull, 2000);

    let rc = zmq_send(push, b"ipc-msg".as_ptr().cast(), 7, 0);
    assert_eq!(rc, 7);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"ipc-msg");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

// --- IPC PUB/SUB (cross-platform) ---

#[test]
fn ipc_pub_sub() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = CString::new(ipc_test_endpoint("pub-sub")).unwrap();
    zmq_bind(pub_, addr.as_ptr());
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(sub, 2000);

    zmq_send(pub_, b"ipc-pub".as_ptr().cast(), 7, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"ipc-pub");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

// --- IPC (abstract namespace, Linux) ---

#[cfg(target_os = "linux")]
#[test]
fn ipc_abstract_namespace() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new(format!("ipc://@omq-libzmq-abstract-{}", std::process::id())).unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(push, 2000);
    set_timeo(pull, 2000);

    let rc = zmq_send(push, b"abstract".as_ptr().cast(), 8, 0);
    assert_eq!(rc, 8);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 8);
    assert_eq!(&buf[..8], b"abstract");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[cfg(target_os = "linux")]
#[test]
fn ipc_abstract_pub_sub() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = CString::new(format!(
        "ipc://@omq-libzmq-abstract-ps-{}",
        std::process::id()
    ))
    .unwrap();
    zmq_bind(pub_, addr.as_ptr());
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(sub, 2000);

    zmq_send(pub_, b"ipc-pub".as_ptr().cast(), 7, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"ipc-pub");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

// --- tcp pub/sub (cross-platform) ---

#[test]
fn tcp_pub_sub() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub = zmq_socket(ctx, ZMQ_SUB);

    let addr = helpers::bind_random_tcp(pub_);
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(sub, 2000);

    zmq_send(pub_, b"tcp-pub".as_ptr().cast(), 7, 0);

    let mut buf = [0u8; 64];
    let rc = zmq_recv(sub, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"tcp-pub");

    zmq_close(sub);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

#[test]
fn tcp_pub_sub_multiple_subscribers() {
    let ctx = zmq_ctx_new();
    let pub_ = zmq_socket(ctx, ZMQ_PUB);
    let sub1 = zmq_socket(ctx, ZMQ_SUB);
    let sub2 = zmq_socket(ctx, ZMQ_SUB);

    let addr = helpers::bind_random_tcp(pub_);
    zmq_setsockopt(sub1, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_setsockopt(sub2, ZMQ_SUBSCRIBE, b"".as_ptr().cast(), 0);
    zmq_connect(sub1, addr.as_ptr());
    zmq_connect(sub2, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(sub1, 2000);
    set_timeo(sub2, 2000);

    zmq_send(pub_, b"multi".as_ptr().cast(), 5, 0);

    let mut buf1 = [0u8; 64];
    let mut buf2 = [0u8; 64];
    let rc1 = zmq_recv(sub1, buf1.as_mut_ptr().cast(), buf1.len(), 0);
    let rc2 = zmq_recv(sub2, buf2.as_mut_ptr().cast(), buf2.len(), 0);
    assert_eq!(rc1, 5);
    assert_eq!(rc2, 5);
    assert_eq!(&buf1[..5], b"multi");
    assert_eq!(&buf2[..5], b"multi");

    zmq_close(sub1);
    zmq_close(sub2);
    zmq_close(pub_);
    zmq_ctx_term(ctx);
}

// --- lz4+tcp compression ---

#[test]
fn lz4_tcp_push_pull() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = helpers::bind_random_lz4_tcp(pull);
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(push, 2000);
    set_timeo(pull, 2000);

    let payload = vec![0x42u8; 4096];
    let rc = zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);
    assert_eq!(rc, 4096);

    let mut buf = vec![0u8; 8192];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 4096);
    assert!(buf[..4096].iter().all(|&b| b == 0x42));

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn lz4_tcp_multiple_messages() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = helpers::bind_random_lz4_tcp(pull);
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(push, 2000);
    set_timeo(pull, 2000);

    for i in 0u8..10 {
        let msg = vec![i; 256];
        zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
    }

    let mut buf = [0u8; 512];
    for i in 0u8..10 {
        let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 256, "lz4 recv {i}");
        assert!(buf[..256].iter().all(|&b| b == i), "lz4 content {i}");
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
