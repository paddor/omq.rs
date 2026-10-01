//! C ABI over QUIC.
#![cfg(feature = "quic")]
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_errno, zmq_getsockopt,
    zmq_has, zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_LINGER: i32 = 17;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const OMQ_QUIC_CERT_PEM: i32 = 1011;
const OMQ_QUIC_KEY_PEM: i32 = 1012;
const OMQ_QUIC_TRUST_PEM: i32 = 1013;
const OMQ_QUIC_TRUST_SYSTEM: i32 = 1015;
const OMQ_QUIC_STREAM_WINDOW: i32 = 1016;
const OMQ_QUIC_MAX_READY_PEERS: i32 = 1018;

fn set_i32(sock: *mut c_void, opt: i32, value: i32) -> i32 {
    zmq_setsockopt(sock, opt, (&value as *const i32).cast(), size_of::<i32>())
}

fn set_bytes(sock: *mut c_void, opt: i32, data: &[u8]) -> i32 {
    zmq_setsockopt(sock, opt, data.as_ptr().cast(), data.len())
}

fn get_i32(sock: *mut c_void, opt: i32) -> i32 {
    let mut value = 0i32;
    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(sock, opt, (&mut value as *mut i32).cast(), &mut len),
        0
    );
    value
}

fn tls() -> (Vec<u8>, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    (
        certified.cert.pem().into_bytes(),
        certified.signing_key.serialize_pem().into_bytes(),
    )
}

fn round_trip(scheme_addr: &str) {
    let (cert, key) = tls();
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let push = zmq_socket(ctx, ZMQ_PUSH);
    for sock in [pull, push] {
        assert_eq!(set_i32(sock, ZMQ_LINGER, 0), 0);
        assert_eq!(set_i32(sock, ZMQ_RCVTIMEO, 5000), 0);
        assert_eq!(set_i32(sock, ZMQ_SNDTIMEO, 5000), 0);
    }
    assert_eq!(set_bytes(pull, OMQ_QUIC_CERT_PEM, &cert), 0);
    assert_eq!(set_bytes(pull, OMQ_QUIC_KEY_PEM, &key), 0);
    let bind = CString::new(scheme_addr).unwrap();
    assert_eq!(
        zmq_bind(pull, bind.as_ptr()),
        0,
        "bind errno={}",
        zmq_errno()
    );
    let endpoint = helpers::last_endpoint(pull);

    assert_eq!(set_i32(push, OMQ_QUIC_TRUST_SYSTEM, 0), 0);
    assert_eq!(set_bytes(push, OMQ_QUIC_TRUST_PEM, &cert), 0);
    assert_eq!(
        zmq_connect(push, endpoint.as_ptr()),
        0,
        "connect errno={}",
        zmq_errno()
    );
    // Security options are fixed once the socket exists.
    assert_eq!(set_i32(push, OMQ_QUIC_TRUST_SYSTEM, 1), -1);
    assert_eq!(zmq_errno(), libc::EBUSY);

    let payload = b"quic-c-abi";
    assert_eq!(
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0),
        i32::try_from(payload.len()).unwrap()
    );
    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(
        rc,
        i32::try_from(payload.len()).unwrap(),
        "recv errno={}",
        zmq_errno()
    );
    assert_eq!(&buf[..payload.len()], payload);
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn quic_push_pull_through_c_options() {
    let cap = CString::new("quic").unwrap();
    assert_eq!(zmq_has(cap.as_ptr()), 1);
    round_trip("quic://127.0.0.1:0");
}

#[test]
fn quic_options_validate_and_read_back() {
    let ctx = zmq_ctx_new();
    let sock = zmq_socket(ctx, ZMQ_PULL);
    assert_eq!(get_i32(sock, OMQ_QUIC_STREAM_WINDOW), 1024 * 1024);
    assert_eq!(set_i32(sock, OMQ_QUIC_STREAM_WINDOW, 1024), -1);
    assert_eq!(zmq_errno(), libc::EINVAL);
    assert_eq!(set_i32(sock, OMQ_QUIC_STREAM_WINDOW, 64 * 1024), 0);
    assert_eq!(get_i32(sock, OMQ_QUIC_STREAM_WINDOW), 64 * 1024);
    assert_eq!(set_i32(sock, OMQ_QUIC_MAX_READY_PEERS, 0), -1);
    assert_eq!(set_i32(sock, OMQ_QUIC_MAX_READY_PEERS, 7), 0);
    assert_eq!(get_i32(sock, OMQ_QUIC_MAX_READY_PEERS), 7);
    zmq_close(sock);
    zmq_ctx_term(ctx);
}
