//! `zmq_setsockopt` / `zmq_getsockopt` round-trip tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::c_void;
use std::mem::size_of;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
#[allow(dead_code)]
const ZMQ_PUB: i32 = 1;
const ZMQ_SUB: i32 = 2;
const ZMQ_ROUTER: i32 = 6;
const ZMQ_DEALER: i32 = 5;
const ZMQ_REQ: i32 = 3;

const ZMQ_SNDHWM: i32 = 23;
const ZMQ_RCVHWM: i32 = 24;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_LINGER: i32 = 17;
const ZMQ_IDENTITY: i32 = 5;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_TYPE: i32 = 16;
const ZMQ_RECONNECT_IVL: i32 = 18;
const ZMQ_RECONNECT_IVL_MAX: i32 = 21;
const ZMQ_HEARTBEAT_IVL: i32 = 75;
const ZMQ_HEARTBEAT_TTL: i32 = 76;
const ZMQ_HEARTBEAT_TIMEOUT: i32 = 77;
const ZMQ_HANDSHAKE_IVL: i32 = 66;
const ZMQ_MAXMSGSIZE: i32 = 22;
const ZMQ_ROUTER_MANDATORY: i32 = 33;
const ZMQ_CONFLATE: i32 = 54;
const ZMQ_TCP_KEEPALIVE: i32 = 34;
const ZMQ_TCP_KEEPALIVE_CNT: i32 = 35;
const ZMQ_TCP_KEEPALIVE_IDLE: i32 = 36;
const ZMQ_TCP_KEEPALIVE_INTVL: i32 = 37;
const ZMQ_SNDBUF: i32 = 11;
const ZMQ_RCVBUF: i32 = 12;
const ZMQ_MECHANISM: i32 = 43;
const ZMQ_PLAIN_SERVER: i32 = 44;
const ZMQ_PLAIN_USERNAME: i32 = 45;
const ZMQ_PLAIN_PASSWORD: i32 = 46;
const ZMQ_CURVE_SERVER: i32 = 47;
const ZMQ_CURVE_PUBLICKEY: i32 = 48;
const ZMQ_CURVE_SECRETKEY: i32 = 49;
const ZMQ_CURVE_SERVERKEY: i32 = 50;

const ZMQ_NULL: i32 = 0;
const ZMQ_PLAIN: i32 = 1;
const ZMQ_CURVE: i32 = 2;

fn set_i32(sock: *mut c_void, opt: i32, val: i32) -> i32 {
    zmq_setsockopt(sock, opt, (&val as *const i32).cast(), size_of::<i32>())
}

fn get_i32(sock: *mut c_void, opt: i32) -> i32 {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, opt, (&mut v as *mut i32).cast(), &mut sz);
    v
}

fn set_i64(sock: *mut c_void, opt: i32, val: i64) -> i32 {
    zmq_setsockopt(sock, opt, (&val as *const i64).cast(), size_of::<i64>())
}

fn get_i64(sock: *mut c_void, opt: i32) -> i64 {
    let mut v: i64 = 0;
    let mut sz = size_of::<i64>();
    zmq_getsockopt(sock, opt, (&mut v as *mut i64).cast(), &mut sz);
    v
}

fn set_bytes(sock: *mut c_void, opt: i32, data: &[u8]) -> i32 {
    zmq_setsockopt(sock, opt, data.as_ptr().cast(), data.len())
}

fn get_bytes(sock: *mut c_void, opt: i32, buf: &mut [u8]) -> usize {
    let mut sz = buf.len();
    zmq_getsockopt(sock, opt, buf.as_mut_ptr().cast(), &mut sz);
    sz
}

#[test]
fn hwm_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_SNDHWM, 500);
    assert_eq!(get_i32(s, ZMQ_SNDHWM), 500);

    set_i32(s, ZMQ_RCVHWM, 2000);
    assert_eq!(get_i32(s, ZMQ_RCVHWM), 2000);

    set_i32(s, ZMQ_SNDHWM, 0);
    assert_eq!(get_i32(s, ZMQ_SNDHWM), 0);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn timeout_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PULL);

    set_i32(s, ZMQ_SNDTIMEO, 1234);
    assert_eq!(get_i32(s, ZMQ_SNDTIMEO), 1234);

    set_i32(s, ZMQ_RCVTIMEO, 5678);
    assert_eq!(get_i32(s, ZMQ_RCVTIMEO), 5678);

    set_i32(s, ZMQ_SNDTIMEO, -1);
    assert_eq!(get_i32(s, ZMQ_SNDTIMEO), -1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn linger_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_LINGER, 100);
    assert_eq!(get_i32(s, ZMQ_LINGER), 100);

    set_i32(s, ZMQ_LINGER, 0);
    assert_eq!(get_i32(s, ZMQ_LINGER), 0);

    set_i32(s, ZMQ_LINGER, -1);
    assert_eq!(get_i32(s, ZMQ_LINGER), -1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn identity_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_DEALER);

    set_bytes(s, ZMQ_IDENTITY, b"my-dealer");
    let mut buf = [0u8; 64];
    let len = get_bytes(s, ZMQ_IDENTITY, &mut buf);
    assert_eq!(&buf[..len], b"my-dealer");

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn type_query() {
    let ctx = zmq_ctx_new();

    let push = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(get_i32(push, ZMQ_TYPE), ZMQ_PUSH);

    let pull = zmq_socket(ctx, ZMQ_PULL);
    assert_eq!(get_i32(pull, ZMQ_TYPE), ZMQ_PULL);

    let router = zmq_socket(ctx, ZMQ_ROUTER);
    assert_eq!(get_i32(router, ZMQ_TYPE), ZMQ_ROUTER);

    zmq_close(push);
    zmq_close(pull);
    zmq_close(router);
    zmq_ctx_term(ctx);
}

#[test]
fn rcvmore_initially_false() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PULL);
    assert_eq!(get_i32(s, ZMQ_RCVMORE), 0);
    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn reconnect_ivl_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_RECONNECT_IVL, 200);
    assert_eq!(get_i32(s, ZMQ_RECONNECT_IVL), 200);

    set_i32(s, ZMQ_RECONNECT_IVL_MAX, 5000);
    assert_eq!(get_i32(s, ZMQ_RECONNECT_IVL_MAX), 5000);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn heartbeat_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_HEARTBEAT_IVL, 500);
    assert_eq!(get_i32(s, ZMQ_HEARTBEAT_IVL), 500);

    set_i32(s, ZMQ_HEARTBEAT_TTL, 3000);
    assert_eq!(get_i32(s, ZMQ_HEARTBEAT_TTL), 3000);

    set_i32(s, ZMQ_HEARTBEAT_TIMEOUT, 10000);
    assert_eq!(get_i32(s, ZMQ_HEARTBEAT_TIMEOUT), 10000);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn handshake_ivl_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(get_i32(s, ZMQ_HANDSHAKE_IVL), 30);

    set_i32(s, ZMQ_HANDSHAKE_IVL, 10);
    assert_eq!(get_i32(s, ZMQ_HANDSHAKE_IVL), 10);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn maxmsgsize_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(get_i64(s, ZMQ_MAXMSGSIZE), -1);

    set_i64(s, ZMQ_MAXMSGSIZE, 1024 * 1024);
    assert_eq!(get_i64(s, ZMQ_MAXMSGSIZE), 1024 * 1024);

    set_i64(s, ZMQ_MAXMSGSIZE, -1);
    assert_eq!(get_i64(s, ZMQ_MAXMSGSIZE), -1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn router_mandatory_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_ROUTER);

    assert_eq!(get_i32(s, ZMQ_ROUTER_MANDATORY), 0);
    set_i32(s, ZMQ_ROUTER_MANDATORY, 1);
    assert_eq!(get_i32(s, ZMQ_ROUTER_MANDATORY), 1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn conflate_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_SUB);

    assert_eq!(get_i32(s, ZMQ_CONFLATE), 0);
    set_i32(s, ZMQ_CONFLATE, 1);
    assert_eq!(get_i32(s, ZMQ_CONFLATE), 1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn tcp_keepalive_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_TCP_KEEPALIVE, 1);
    assert_eq!(get_i32(s, ZMQ_TCP_KEEPALIVE), 1);

    set_i32(s, ZMQ_TCP_KEEPALIVE_CNT, 5);
    assert_eq!(get_i32(s, ZMQ_TCP_KEEPALIVE_CNT), 5);

    set_i32(s, ZMQ_TCP_KEEPALIVE_IDLE, 120);
    assert_eq!(get_i32(s, ZMQ_TCP_KEEPALIVE_IDLE), 120);

    set_i32(s, ZMQ_TCP_KEEPALIVE_INTVL, 30);
    assert_eq!(get_i32(s, ZMQ_TCP_KEEPALIVE_INTVL), 30);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn sndbuf_rcvbuf_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    set_i32(s, ZMQ_SNDBUF, 65_536);
    assert_eq!(get_i32(s, ZMQ_SNDBUF), 65_536);

    set_i32(s, ZMQ_RCVBUF, 131_072);
    assert_eq!(get_i32(s, ZMQ_RCVBUF), 131_072);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn mechanism_default_null() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_NULL);
    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_server_mechanism() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_REQ);

    set_i32(s, ZMQ_PLAIN_SERVER, 1);
    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_PLAIN);
    assert_eq!(get_i32(s, ZMQ_PLAIN_SERVER), 1);

    set_i32(s, ZMQ_PLAIN_SERVER, 0);
    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_NULL);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_client_username_password() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_REQ);

    set_bytes(s, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(s, ZMQ_PLAIN_PASSWORD, b"secret123");

    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_PLAIN);

    let mut buf = [0u8; 64];
    let len = get_bytes(s, ZMQ_PLAIN_USERNAME, &mut buf);
    assert_eq!(&buf[..len - 1], b"alice");

    let len = get_bytes(s, ZMQ_PLAIN_PASSWORD, &mut buf);
    assert_eq!(&buf[..len - 1], b"secret123");

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn curve_server_mechanism() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_REQ);

    set_i32(s, ZMQ_CURVE_SERVER, 1);
    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_CURVE);
    assert_eq!(get_i32(s, ZMQ_CURVE_SERVER), 1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn curve_client_keys_binary() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_REQ);

    let pub_key = [0x42u8; 32];
    let sec_key = [0x43u8; 32];
    let srv_key = [0x44u8; 32];

    set_bytes(s, ZMQ_CURVE_PUBLICKEY, &pub_key);
    set_bytes(s, ZMQ_CURVE_SECRETKEY, &sec_key);
    set_bytes(s, ZMQ_CURVE_SERVERKEY, &srv_key);

    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_CURVE);

    let mut buf = [0u8; 32];
    let mut sz = 32usize;
    zmq_getsockopt(s, ZMQ_CURVE_PUBLICKEY, buf.as_mut_ptr().cast(), &mut sz);
    assert_eq!(sz, 32);
    assert_eq!(buf, pub_key);

    zmq_getsockopt(s, ZMQ_CURVE_SECRETKEY, buf.as_mut_ptr().cast(), &mut sz);
    assert_eq!(buf, sec_key);

    zmq_getsockopt(s, ZMQ_CURVE_SERVERKEY, buf.as_mut_ptr().cast(), &mut sz);
    assert_eq!(buf, srv_key);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn curve_client_keys_z85() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_REQ);

    let mut pub_z85 = [0u8; 41];
    let mut sec_z85 = [0u8; 41];
    omq_zmq::zmq_curve_keypair(pub_z85.as_mut_ptr().cast(), sec_z85.as_mut_ptr().cast());

    set_bytes(s, ZMQ_CURVE_PUBLICKEY, &pub_z85[..40]);
    set_bytes(s, ZMQ_CURVE_SECRETKEY, &sec_z85[..40]);
    set_bytes(s, ZMQ_CURVE_SERVERKEY, &pub_z85[..40]);

    assert_eq!(get_i32(s, ZMQ_MECHANISM), ZMQ_CURVE);

    let mut out = [0u8; 41];
    let mut sz = 41usize;
    zmq_getsockopt(s, ZMQ_CURVE_PUBLICKEY, out.as_mut_ptr().cast(), &mut sz);
    assert_eq!(sz, 41);
    assert_eq!(&out[..40], &pub_z85[..40]);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

// --- New option round-trips ---

const ZMQ_BACKLOG: i32 = 19;
const ZMQ_IMMEDIATE: i32 = 39;
const ZMQ_IPV6: i32 = 42;
const ZMQ_PROBE_ROUTER: i32 = 51;
const ZMQ_REQ_CORRELATE: i32 = 52;
const ZMQ_REQ_RELAXED: i32 = 53;
const ZMQ_ROUTER_HANDOVER: i32 = 56;
const ZMQ_XPUB_NODROP: i32 = 69;
const ZMQ_CONNECT_TIMEOUT: i32 = 79;
const ZMQ_LAST_ENDPOINT: i32 = 32;
const ZMQ_EVENTS: i32 = 15;
const ZMQ_POLLIN_: i32 = 1;
const ZMQ_POLLOUT_: i32 = 2;

#[test]
fn ipv6_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(get_i32(s, ZMQ_IPV6), 0);
    set_i32(s, ZMQ_IPV6, 1);
    assert_eq!(get_i32(s, ZMQ_IPV6), 1);
    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn compat_noop_options_roundtrip() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    assert_eq!(set_i32(s, ZMQ_BACKLOG, 128), 0);
    assert_eq!(get_i32(s, ZMQ_BACKLOG), 128);

    assert_eq!(set_i32(s, ZMQ_IMMEDIATE, 1), 0);
    assert_eq!(get_i32(s, ZMQ_IMMEDIATE), 1);

    assert_eq!(set_i32(s, ZMQ_CONNECT_TIMEOUT, 5000), 0);
    assert_eq!(get_i32(s, ZMQ_CONNECT_TIMEOUT), 5000);

    assert_eq!(set_i32(s, ZMQ_PROBE_ROUTER, 1), 0);
    assert_eq!(get_i32(s, ZMQ_PROBE_ROUTER), 1);

    assert_eq!(set_i32(s, ZMQ_REQ_CORRELATE, 1), 0);
    assert_eq!(get_i32(s, ZMQ_REQ_CORRELATE), 1);

    assert_eq!(set_i32(s, ZMQ_REQ_RELAXED, 1), 0);
    assert_eq!(get_i32(s, ZMQ_REQ_RELAXED), 1);

    assert_eq!(set_i32(s, ZMQ_XPUB_NODROP, 1), 0);
    assert_eq!(get_i32(s, ZMQ_XPUB_NODROP), 1);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn silent_noop_options_accepted() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);

    for opt in [4, 8, 9, 25, 55, 57, 61, 68, 74, 80, 92, 96, 97] {
        assert_eq!(set_i32(s, opt, 1), 0, "option {opt} should be accepted");
    }

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn last_endpoint_after_bind() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PULL);
    let port = helpers::free_port();
    let addr_str = format!("tcp://127.0.0.1:{port}");
    let addr = std::ffi::CString::new(addr_str.clone()).unwrap();
    zmq_bind(s, addr.as_ptr());

    let mut buf = [0u8; 256];
    let len = get_bytes(s, ZMQ_LAST_ENDPOINT, &mut buf);
    let got = std::ffi::CStr::from_bytes_until_nul(&buf[..len])
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(got, addr_str);

    zmq_close(s);
    zmq_ctx_term(ctx);
}

#[test]
fn events_after_send() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = std::ffi::CString::new("inproc://test-events").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(20));

    let events = get_i32(push, ZMQ_EVENTS);
    assert_ne!(events & ZMQ_POLLOUT_, 0, "push should be writable");

    zmq_send(push, b"ev".as_ptr().cast(), 2, 0);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let events = get_i32(pull, ZMQ_EVENTS);
    assert_ne!(
        events & ZMQ_POLLIN_,
        0,
        "pull should be readable after send"
    );

    let mut buf = [0u8; 16];
    let timeo: i32 = 500;
    zmq_setsockopt(pull, 27, (&timeo as *const i32).cast(), 4);
    zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);

    let events = get_i32(pull, ZMQ_EVENTS);
    assert_eq!(
        events & ZMQ_POLLIN_,
        0,
        "pull should not be readable after drain"
    );

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn router_handover_always_on() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_ROUTER);
    // omq always does identity handover; set succeeds, get returns 1.
    assert_eq!(set_i32(s, ZMQ_ROUTER_HANDOVER, 1), 0);
    assert_eq!(get_i32(s, ZMQ_ROUTER_HANDOVER), 1);
    zmq_close(s);
    zmq_ctx_term(ctx);
}

// --- IPv6 ---

#[test]
fn ipv6_tcp_push_pull() {
    // Check if IPv6 loopback is available.
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        eprintln!("skipping: IPv6 loopback not available");
        return;
    }

    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let port = helpers::free_port();
    let addr = std::ffi::CString::new(format!("tcp://[::1]:{port}")).unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(100));

    let timeo: i32 = 2000;
    zmq_setsockopt(push, 28, (&timeo as *const i32).cast(), 4);
    zmq_setsockopt(pull, 27, (&timeo as *const i32).cast(), 4);

    let rc = zmq_send(push, b"ipv6!".as_ptr().cast(), 5, 0);
    assert_eq!(rc, 5, "IPv6 send failed (errno={})", omq_zmq::zmq_errno());

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5, "IPv6 recv failed (errno={})", omq_zmq::zmq_errno());
    assert_eq!(&buf[..5], b"ipv6!");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn ipv6_req_rep() {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        eprintln!("skipping: IPv6 loopback not available");
        return;
    }

    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, 4); // ZMQ_REP
    let req = zmq_socket(ctx, 3); // ZMQ_REQ

    let port = helpers::free_port();
    let addr = std::ffi::CString::new(format!("tcp://[::1]:{port}")).unwrap();
    zmq_bind(rep, addr.as_ptr());
    zmq_connect(req, addr.as_ptr());
    std::thread::sleep(std::time::Duration::from_millis(100));

    let timeo: i32 = 2000;
    zmq_setsockopt(rep, 27, (&timeo as *const i32).cast(), 4);
    zmq_setsockopt(rep, 28, (&timeo as *const i32).cast(), 4);
    zmq_setsockopt(req, 27, (&timeo as *const i32).cast(), 4);
    zmq_setsockopt(req, 28, (&timeo as *const i32).cast(), 4);

    zmq_send(req, b"ping6".as_ptr().cast(), 5, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"ping6");

    zmq_send(rep, b"pong6".as_ptr().cast(), 5, 0);
    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"pong6");

    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}
