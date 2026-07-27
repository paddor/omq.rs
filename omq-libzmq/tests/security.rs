//! CURVE keypair generation, Z85 encode/decode, and end-to-end security tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::c_void;
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_curve_keypair, zmq_curve_public,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket, zmq_z85_decode, zmq_z85_encode,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_CURVE_SERVER: i32 = 47;
const ZMQ_CURVE_PUBLICKEY: i32 = 48;
const ZMQ_CURVE_SECRETKEY: i32 = 49;
const ZMQ_CURVE_SERVERKEY: i32 = 50;
const ZMQ_PLAIN_SERVER: i32 = 44;
const ZMQ_PLAIN_USERNAME: i32 = 45;
const ZMQ_PLAIN_PASSWORD: i32 = 46;

fn set_i32(sock: *mut c_void, opt: i32, val: i32) {
    zmq_setsockopt(sock, opt, (&val as *const i32).cast(), size_of::<i32>());
}

fn set_bytes(sock: *mut c_void, opt: i32, data: &[u8]) {
    zmq_setsockopt(sock, opt, data.as_ptr().cast(), data.len());
}

fn set_timeo(sock: *mut c_void, ms: i32) {
    set_i32(sock, ZMQ_RCVTIMEO, ms);
    set_i32(sock, ZMQ_SNDTIMEO, ms);
}

#[test]
fn z85_encode_decode_roundtrip() {
    let data = [0x86u8, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
    let mut encoded = [0u8; 11]; // 8 bytes -> 10 Z85 chars + null
    let ret = zmq_z85_encode(encoded.as_mut_ptr().cast(), data.as_ptr(), data.len());
    assert!(!ret.is_null());
    let z85_str = std::str::from_utf8(&encoded[..10]).unwrap();
    assert_eq!(z85_str.len(), 10);

    let mut decoded = [0u8; 8];
    let ret = zmq_z85_decode(decoded.as_mut_ptr(), encoded.as_ptr().cast());
    assert!(!ret.is_null());
    assert_eq!(decoded, data);
}

#[test]
fn z85_invalid_size_returns_null() {
    let data = [0u8; 3]; // not multiple of 4
    let mut encoded = [0u8; 10];
    let ret = zmq_z85_encode(encoded.as_mut_ptr().cast(), data.as_ptr(), data.len());
    assert!(ret.is_null());
}

#[test]
fn z85_32_byte_key_roundtrip() {
    let key = [0x55u8; 32];
    let mut z85 = [0u8; 41]; // 40 chars + null
    let ret = zmq_z85_encode(z85.as_mut_ptr().cast(), key.as_ptr(), 32);
    assert!(!ret.is_null());
    assert_eq!(z85[40], 0); // null terminated

    let mut decoded = [0u8; 32];
    let ret = zmq_z85_decode(decoded.as_mut_ptr(), z85.as_ptr().cast());
    assert!(!ret.is_null());
    assert_eq!(decoded, key);
}

#[test]
fn curve_keypair_generates_valid_z85() {
    let mut pub_key = [0u8; 41];
    let mut sec_key = [0u8; 41];
    let rc = zmq_curve_keypair(pub_key.as_mut_ptr().cast(), sec_key.as_mut_ptr().cast());
    assert_eq!(rc, 0);

    assert_eq!(pub_key[40], 0);
    assert_eq!(sec_key[40], 0);

    let pub_str = std::str::from_utf8(&pub_key[..40]).unwrap();
    let sec_str = std::str::from_utf8(&sec_key[..40]).unwrap();
    assert_eq!(pub_str.len(), 40);
    assert_eq!(sec_str.len(), 40);

    let mut decoded = [0u8; 32];
    assert!(!zmq_z85_decode(decoded.as_mut_ptr(), pub_key.as_ptr().cast()).is_null());
    assert!(!zmq_z85_decode(decoded.as_mut_ptr(), sec_key.as_ptr().cast()).is_null());
}

#[test]
fn curve_keypair_unique() {
    let mut pub1 = [0u8; 41];
    let mut sec1 = [0u8; 41];
    let mut pub2 = [0u8; 41];
    let mut sec2 = [0u8; 41];

    zmq_curve_keypair(pub1.as_mut_ptr().cast(), sec1.as_mut_ptr().cast());
    zmq_curve_keypair(pub2.as_mut_ptr().cast(), sec2.as_mut_ptr().cast());

    assert_ne!(pub1, pub2);
    assert_ne!(sec1, sec2);
}

#[test]
fn curve_public_derives_from_secret() {
    let mut pub_key = [0u8; 41];
    let mut sec_key = [0u8; 41];
    zmq_curve_keypair(pub_key.as_mut_ptr().cast(), sec_key.as_mut_ptr().cast());

    let mut derived_pub = [0u8; 41];
    let rc = zmq_curve_public(derived_pub.as_mut_ptr().cast(), sec_key.as_ptr().cast());
    assert_eq!(rc, 0);
    assert_eq!(&derived_pub[..40], &pub_key[..40]);
}

#[test]
fn curve_req_rep_tcp() {
    let mut srv_pub = [0u8; 41];
    let mut srv_sec = [0u8; 41];
    zmq_curve_keypair(srv_pub.as_mut_ptr().cast(), srv_sec.as_mut_ptr().cast());

    let mut cli_pub = [0u8; 41];
    let mut cli_sec = [0u8; 41];
    zmq_curve_keypair(cli_pub.as_mut_ptr().cast(), cli_sec.as_mut_ptr().cast());

    let ctx = zmq_ctx_new();
    let rep = zmq_socket(ctx, ZMQ_REP);
    set_i32(rep, ZMQ_CURVE_SERVER, 1);
    set_bytes(rep, ZMQ_CURVE_SECRETKEY, &srv_sec[..40]);
    let addr = helpers::bind_random_tcp(rep);
    set_timeo(rep, 5000);

    let req = zmq_socket(ctx, ZMQ_REQ);
    set_bytes(req, ZMQ_CURVE_PUBLICKEY, &cli_pub[..40]);
    set_bytes(req, ZMQ_CURVE_SECRETKEY, &cli_sec[..40]);
    set_bytes(req, ZMQ_CURVE_SERVERKEY, &srv_pub[..40]);
    zmq_connect(req, addr.as_ptr());
    set_timeo(req, 5000);

    std::thread::sleep(Duration::from_millis(200));

    let rc = zmq_send(req, b"ping".as_ptr().cast(), 4, 0);
    assert_eq!(
        rc,
        4,
        "CURVE REQ send failed (errno={})",
        omq_zmq::zmq_errno()
    );

    let mut buf = [0u8; 64];
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc,
        4,
        "CURVE REP recv failed (errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(&buf[..4], b"ping");

    let rc = zmq_send(rep, b"pong".as_ptr().cast(), 4, 0);
    assert_eq!(rc, 4);

    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 4);
    assert_eq!(&buf[..4], b"pong");

    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_push_pull_tcp() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    let addr = helpers::bind_random_tcp(pull);
    set_timeo(pull, 5000);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"user");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"pass");
    zmq_connect(push, addr.as_ptr());
    set_timeo(push, 5000);

    std::thread::sleep(Duration::from_millis(200));

    let rc = zmq_send(push, b"hello".as_ptr().cast(), 5, 0);
    assert_eq!(rc, 5, "PLAIN send failed (errno={})", omq_zmq::zmq_errno());

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5, "PLAIN recv failed (errno={})", omq_zmq::zmq_errno());
    assert_eq!(&buf[..5], b"hello");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
