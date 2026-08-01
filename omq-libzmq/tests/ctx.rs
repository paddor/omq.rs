//! Context lifecycle tests.

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_get, zmq_ctx_new, zmq_ctx_set, zmq_ctx_shutdown,
    zmq_ctx_term, zmq_getsockopt, zmq_init, zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
    zmq_term,
};
use std::ffi::CString;
use std::ffi::c_void;
use std::mem::size_of;

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_IO_THREADS: i32 = 1;
const ZMQ_MAX_SOCKETS: i32 = 2;
const ZMQ_SOCKET_LIMIT: i32 = 3;
const ZMQ_MAX_MSGSZ: i32 = 5;
const ZMQ_MSG_T_SIZE: i32 = 6;
const ZMQ_IPV6: i32 = 42;
const ZMQ_BLOCKY: i32 = 70;
const ZMQ_LINGER: i32 = 17;

#[test]
fn ctx_new_term() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_destroy_alias() {
    let ctx = zmq_ctx_new();
    assert!(!ctx.is_null());
    assert_eq!(omq_zmq::zmq_ctx_destroy(ctx), 0);
}

#[test]
fn ctx_shutdown_then_term() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_null_term_returns_error() {
    let rc = zmq_ctx_term(std::ptr::null_mut::<c_void>());
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
}

#[test]
fn ctx_null_aliases_return_efault() {
    assert_eq!(zmq_term(std::ptr::null_mut::<c_void>()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
    assert_eq!(zmq_ctx_shutdown(std::ptr::null_mut::<c_void>()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
}

#[test]
fn ctx_get_io_threads_default() {
    let ctx = zmq_ctx_new();
    let n = zmq_ctx_get(ctx, ZMQ_IO_THREADS);
    assert_eq!(n, 1);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_get_msg_t_size() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MSG_T_SIZE), 64);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_zero_io_threads() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, 0), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_IO_THREADS), 0);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_io_threads_before_socket_creation() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, 3), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_IO_THREADS), 3);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_io_threads_rejects_negative_and_late_changes() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, -1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let sock = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!sock.is_null());
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IO_THREADS, 2), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    zmq_close(sock);
    zmq_ctx_term(ctx);
}

#[test]
fn zero_io_threads_support_inproc_push_pull() {
    let ctx = zmq_init(0);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://zero-io-threads").unwrap();

    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"hello".as_ptr().cast(), 5, 0), 5);

    let mut buf = [0u8; 8];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 5);
    assert_eq!(&buf[..5], b"hello");

    zmq_close(push);
    zmq_close(pull);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn zero_io_threads_reject_unsupported_transports() {
    let ctx = zmq_init(0);
    let pair = zmq_socket(ctx, 0);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let tcp = CString::new("tcp://127.0.0.1:*").unwrap();
    let inproc = CString::new("inproc://zero-io-unsupported").unwrap();

    assert_eq!(zmq_bind(pair, inproc.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::ENOTSUP);
    assert_eq!(zmq_bind(push, tcp.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::ENOTSUP);

    zmq_close(pair);
    zmq_close(push);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_get_max_sockets_default() {
    let ctx = zmq_ctx_new();
    let n = zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS);
    assert_eq!(n, 1023);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_get_socket_limit_alias() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_get(ctx, ZMQ_SOCKET_LIMIT), 1023);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_max_sockets() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_SOCKETS, 512), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS), 512);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_negative_max_sockets_returns_einval() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_SOCKETS, -1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_SOCKETS), 1023);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_set_max_msgsz() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_MAX_MSGSZ, 65536), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_MAX_MSGSZ), 65536);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_ipv6_option_applies_to_new_sockets() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_get(ctx, ZMQ_IPV6), 0);
    assert_eq!(zmq_ctx_set(ctx, ZMQ_IPV6, 1), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_IPV6), 1);

    let socket = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!socket.is_null());

    let mut v = 0i32;
    let mut sz = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(socket, ZMQ_IPV6, (&mut v as *mut i32).cast(), &mut sz),
        0
    );
    assert_eq!(sz, size_of::<i32>());
    assert_eq!(v, 1);

    zmq_close(socket);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_blocky_zero_sets_linger_zero_on_new_sockets() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_get(ctx, ZMQ_BLOCKY), 1);
    assert_eq!(zmq_ctx_set(ctx, ZMQ_BLOCKY, 0), 0);
    assert_eq!(zmq_ctx_get(ctx, ZMQ_BLOCKY), 0);

    let socket = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!socket.is_null());

    let mut v = -1i32;
    let mut sz = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(socket, ZMQ_LINGER, (&mut v as *mut i32).cast(), &mut sz),
        0
    );
    assert_eq!(v, 0);

    zmq_close(socket);
    zmq_ctx_term(ctx);
}

#[test]
fn explicit_linger_overrides_ctx_blocky_default() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, ZMQ_BLOCKY, 0), 0);

    let socket = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!socket.is_null());
    let linger = 25i32;
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_LINGER,
            (&linger as *const i32).cast(),
            size_of::<i32>(),
        ),
        0
    );

    let mut v = 0i32;
    let mut sz = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(socket, ZMQ_LINGER, (&mut v as *mut i32).cast(), &mut sz),
        0
    );
    assert_eq!(v, linger);

    zmq_close(socket);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_shutdown_rejects_later_socket_creation() {
    let ctx = zmq_ctx_new();
    let socket = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!socket.is_null());
    assert_eq!(zmq_close(socket), 0);

    assert_eq!(zmq_ctx_shutdown(ctx), 0);
    assert!(zmq_socket(ctx, ZMQ_PUSH).is_null());
    assert_eq!(omq_zmq::zmq_errno(), 156_384_765);

    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_invalid_options_return_einval() {
    let ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(ctx, -1, 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(zmq_ctx_get(ctx, -1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_term_waits_for_socket_close() {
    use std::sync::{Arc, Barrier};

    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!sock.is_null());

    // Close the socket on a separate thread, then term the context.
    let ctx_copy = ctx as usize; // send across threads as usize
    let sock_copy = sock as usize;
    let barrier = Arc::new(Barrier::new(2));
    let b2 = barrier.clone();

    let t = std::thread::spawn(move || {
        b2.wait(); // sync: main thread is in ctx_term waiting
        // Give main thread time to enter the wait
        std::thread::sleep(std::time::Duration::from_millis(20));
        zmq_close(sock_copy as *mut c_void);
    });

    barrier.wait();
    // ctx_term blocks until the socket count reaches 0 (i.e. zmq_close is called)
    let rc = zmq_ctx_term(ctx_copy as *mut c_void);
    assert_eq!(rc, 0);
    t.join().unwrap();
}

#[test]
fn ctx_multiple_sockets_closed_before_term() {
    let ctx = zmq_ctx_new();
    let s1 = zmq_socket(ctx, ZMQ_PUSH);
    let s2 = zmq_socket(ctx, ZMQ_PULL);
    assert!(!s1.is_null());
    assert!(!s2.is_null());
    zmq_close(s1);
    zmq_close(s2);
    assert_eq!(zmq_ctx_term(ctx), 0);
}

#[test]
fn ctx_max_sockets_enforced() {
    let ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_MAX_SOCKETS, 2);

    let s1 = zmq_socket(ctx, ZMQ_PUSH);
    let s2 = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!s1.is_null());
    assert!(!s2.is_null());

    let s3 = zmq_socket(ctx, ZMQ_PUSH);
    assert!(s3.is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EMFILE);

    zmq_close(s1);
    let s4 = zmq_socket(ctx, ZMQ_PUSH);
    assert!(!s4.is_null());

    zmq_close(s2);
    zmq_close(s4);
    zmq_ctx_term(ctx);
}

#[test]
fn ctx_max_msgsz_enforced() {
    let ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_MAX_MSGSZ, 100);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let port = {
        let addr = std::ffi::CString::new("tcp://127.0.0.1:*").unwrap();
        omq_zmq::zmq_bind(pull, addr.as_ptr());
        let mut buf = [0u8; 256];
        let mut sz = buf.len();
        omq_zmq::zmq_getsockopt(pull, 32, buf.as_mut_ptr().cast(), &raw mut sz);
        String::from_utf8_lossy(&buf[..sz - 1]).to_string()
    };
    let addr = std::ffi::CString::new(port).unwrap();
    omq_zmq::zmq_connect(push, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(50));

    let small = [0u8; 50];
    let rc = omq_zmq::zmq_send(push, small.as_ptr().cast(), small.len(), 0);
    assert_eq!(rc, 50);

    let big = [0u8; 200];
    let rc = omq_zmq::zmq_send(push, big.as_ptr().cast(), big.len(), 0);
    assert_eq!(rc, -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EMSGSIZE);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
