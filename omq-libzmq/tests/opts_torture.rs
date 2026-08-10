//! Socket option edge cases beyond round-trip smoke coverage.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::c_void;
use std::mem::size_of;

use omq_zmq::{
    zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_SUB: i32 = 2;

const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_LINGER: i32 = 17;
const ZMQ_IDENTITY: i32 = 5;
const ZMQ_TYPE: i32 = 16;
const ZMQ_EVENTS: i32 = 15;
const ZMQ_MAXMSGSIZE: i32 = 22;
const ZMQ_SUBSCRIBE: i32 = 6;
const ZMQ_UNSUBSCRIBE: i32 = 7;
const ZMQ_CURVE_SERVERKEY: i32 = 50;
const ZMQ_WSS_KEY_PEM: i32 = 103;

const ZMQ_POLLOUT: i32 = 2;
const ZMQ_ENOTSOCK: i32 = 156_384_721;

fn set_i32(sock: *mut c_void, opt: i32, val: i32) -> i32 {
    zmq_setsockopt(sock, opt, (&val as *const i32).cast(), size_of::<i32>())
}

fn get_i32(sock: *mut c_void, opt: i32) -> i32 {
    let mut val = 0i32;
    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(sock, opt, (&mut val as *mut i32).cast(), &mut len),
        0
    );
    assert_eq!(len, size_of::<i32>());
    val
}

fn set_i64(sock: *mut c_void, opt: i32, val: i64) -> i32 {
    zmq_setsockopt(sock, opt, (&val as *const i64).cast(), size_of::<i64>())
}

fn get_i64(sock: *mut c_void, opt: i32) -> i64 {
    let mut val = 0i64;
    let mut len = size_of::<i64>();
    assert_eq!(
        zmq_getsockopt(sock, opt, (&mut val as *mut i64).cast(), &mut len),
        0
    );
    assert_eq!(len, size_of::<i64>());
    val
}

fn set_bytes(sock: *mut c_void, opt: i32, data: &[u8]) -> i32 {
    zmq_setsockopt(sock, opt, data.as_ptr().cast(), data.len())
}

fn get_bytes(sock: *mut c_void, opt: i32, buf: &mut [u8]) -> usize {
    let mut len = buf.len();
    assert_eq!(
        zmq_getsockopt(sock, opt, buf.as_mut_ptr().cast(), &mut len),
        0
    );
    len
}

#[test]
fn invalid_socket_option_calls_report_expected_errno() {
    let val = 1i32;
    assert_eq!(
        zmq_setsockopt(
            std::ptr::null_mut(),
            ZMQ_SNDHWM,
            (&val as *const i32).cast(),
            size_of::<i32>(),
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    let mut out = 0i32;
    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(
            std::ptr::null_mut(),
            ZMQ_SNDHWM,
            (&mut out as *mut i32).cast(),
            &mut len,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(set_i32(sock, 9_999_999, 1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(
        zmq_getsockopt(sock, 9_999_999, (&mut out as *mut i32).cast(), &mut len,),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    zmq_close(sock);
    zmq_ctx_term(ctx);
}

#[test]
fn invalid_option_sizes_and_values_preserve_previous_state() {
    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(set_i32(sock, ZMQ_SNDHWM, 10), 0);
    let bad_hwm = 20i32;
    assert_eq!(
        zmq_setsockopt(sock, ZMQ_SNDHWM, (&bad_hwm as *const i32).cast(), 3),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(set_i32(sock, ZMQ_SNDHWM, -1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(get_i32(sock, ZMQ_SNDHWM), 10);

    assert_eq!(set_i32(sock, ZMQ_RCVHWM, 11), 0);
    assert_eq!(set_i32(sock, ZMQ_RCVHWM, 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(get_i32(sock, ZMQ_RCVHWM), 11);

    assert_eq!(set_i64(sock, ZMQ_MAXMSGSIZE, 99), 0);
    let bad_max = 123i64;
    assert_eq!(
        zmq_setsockopt(sock, ZMQ_MAXMSGSIZE, (&bad_max as *const i64).cast(), 4),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(get_i64(sock, ZMQ_MAXMSGSIZE), 99);

    let good_key = [0x42u8; 32];
    let bad_key = [0x13u8; 31];
    assert_eq!(set_bytes(sock, ZMQ_CURVE_SERVERKEY, &good_key), 0);
    assert_eq!(set_bytes(sock, ZMQ_CURVE_SERVERKEY, &bad_key), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    let mut key_out = [0u8; 32];
    assert_eq!(get_bytes(sock, ZMQ_CURVE_SERVERKEY, &mut key_out), 32);
    assert_eq!(key_out, good_key);

    assert_eq!(set_bytes(sock, ZMQ_WSS_KEY_PEM, b"server-key"), 0);
    assert_eq!(
        zmq_setsockopt(sock, ZMQ_WSS_KEY_PEM, std::ptr::null(), 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    let mut pem = [0u8; 32];
    let len = get_bytes(sock, ZMQ_WSS_KEY_PEM, &mut pem);
    assert_eq!(&pem[..len], b"server-key");

    zmq_close(sock);
    zmq_ctx_term(ctx);
}

#[test]
fn getsockopt_rejects_too_small_or_null_outputs() {
    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(set_i32(sock, ZMQ_SNDHWM, 10), 0);
    assert_eq!(set_i64(sock, ZMQ_MAXMSGSIZE, 99), 0);
    assert_eq!(set_bytes(sock, ZMQ_IDENTITY, b"identity"), 0);

    let mut tiny = [0u8; 3];
    let mut len = tiny.len();
    assert_eq!(
        zmq_getsockopt(sock, ZMQ_SNDHWM, tiny.as_mut_ptr().cast(), &mut len),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(sock, ZMQ_TYPE, std::ptr::null_mut(), &mut len),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let mut out = 0i64;
    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(
            sock,
            ZMQ_MAXMSGSIZE,
            (&mut out as *mut i64).cast(),
            &mut len,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let mut short = [0u8; 4];
    let mut len = short.len();
    assert_eq!(
        zmq_getsockopt(sock, ZMQ_IDENTITY, short.as_mut_ptr().cast(), &mut len),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    zmq_close(sock);
    zmq_ctx_term(ctx);
}

#[test]
fn read_only_options_accept_set_without_corrupting_state() {
    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(get_i32(sock, ZMQ_TYPE), ZMQ_PUSH);
    assert_eq!(set_i32(sock, ZMQ_TYPE, ZMQ_PULL), 0);
    assert_eq!(get_i32(sock, ZMQ_TYPE), ZMQ_PUSH);

    let events = get_i32(sock, ZMQ_EVENTS);
    assert_ne!(events & ZMQ_POLLOUT, 0);
    assert_eq!(set_i32(sock, ZMQ_EVENTS, 0), 0);
    assert_eq!(get_i32(sock, ZMQ_EVENTS), events);

    zmq_close(sock);
    zmq_ctx_term(ctx);
}

#[test]
fn options_can_be_changed_after_materialization() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    assert_eq!(set_i32(push, ZMQ_SNDTIMEO, 50), 0);
    assert_eq!(set_i32(push, ZMQ_LINGER, 0), 0);

    let endpoint = helpers::bind_random_tcp(pull);
    assert_eq!(zmq_connect(push, endpoint.as_ptr()), 0);

    assert_eq!(set_i32(push, ZMQ_SNDTIMEO, 123), 0);
    assert_eq!(set_i32(push, ZMQ_LINGER, 321), 0);
    assert_eq!(get_i32(push, ZMQ_SNDTIMEO), 123);
    assert_eq!(get_i32(push, ZMQ_LINGER), 321);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn empty_subscribe_and_unsubscribe_accept_null_pointer() {
    let ctx = zmq_ctx_new();
    let sub = zmq_socket(ctx, ZMQ_SUB);

    assert_eq!(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, std::ptr::null(), 0), 0);
    assert_eq!(zmq_setsockopt(sub, ZMQ_UNSUBSCRIBE, std::ptr::null(), 0), 0);

    zmq_close(sub);
    zmq_ctx_term(ctx);
}
