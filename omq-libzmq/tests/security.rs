//! CURVE keypair generation, Z85 encode/decode, and end-to-end security tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::c_void;
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    OmqPlainCredential, omq_socket_set_plain_server_credentials, zmq_bind, zmq_close, zmq_connect,
    zmq_ctx_new, zmq_ctx_set, zmq_ctx_term, zmq_curve_keypair, zmq_curve_public, zmq_getsockopt,
    zmq_msg_close, zmq_msg_copy, zmq_msg_data, zmq_msg_gets, zmq_msg_init, zmq_msg_more,
    zmq_msg_recv, zmq_msg_size, zmq_recv, zmq_send, zmq_setsockopt, zmq_socket, zmq_unbind,
    zmq_z85_decode, zmq_z85_encode,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_DEALER: i32 = 5;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_ROUTER: i32 = 6;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_CURVE_SERVER: i32 = 47;
const ZMQ_CURVE_PUBLICKEY: i32 = 48;
const ZMQ_CURVE_SECRETKEY: i32 = 49;
const ZMQ_CURVE_SERVERKEY: i32 = 50;
const ZMQ_PLAIN_SERVER: i32 = 44;
const ZMQ_PLAIN_USERNAME: i32 = 45;
const ZMQ_PLAIN_PASSWORD: i32 = 46;
const ZMQ_ZAP_DOMAIN: i32 = 55;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_LINGER: i32 = 17;
const ZMQ_IDENTITY: i32 = 5;
const ZMQ_ZAP_ENFORCE_DOMAIN: i32 = 93;
const ZMQ_IO_THREADS: i32 = 1;
const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

#[repr(C)]
struct ZmqMsg([usize; ZMQ_MSG_WORDS]);

impl ZmqMsg {
    fn new() -> Self {
        let mut message = Self([0; ZMQ_MSG_WORDS]);
        assert_eq!(zmq_msg_init(message.0.as_mut_ptr().cast()), 0);
        message
    }
}

fn msg_property(message: &ZmqMsg, property: &std::ffi::CStr) -> Option<Vec<u8>> {
    let value = zmq_msg_gets(message.0.as_ptr().cast(), property.as_ptr());
    (!value.is_null()).then(|| {
        unsafe { std::ffi::CStr::from_ptr(value) }
            .to_bytes()
            .to_vec()
    })
}

fn msg_bytes(message: &mut ZmqMsg) -> Vec<u8> {
    let size = zmq_msg_size(message.0.as_ptr().cast());
    let data = zmq_msg_data(message.0.as_mut_ptr().cast()).cast::<u8>();
    if size == 0 {
        return Vec::new();
    }
    assert!(!data.is_null());
    // SAFETY: zmq_msg_data exposes `size` readable bytes until message close.
    unsafe { std::slice::from_raw_parts(data, size) }.to_vec()
}

fn set_i32(sock: *mut c_void, opt: i32, val: i32) {
    zmq_setsockopt(sock, opt, (&val as *const i32).cast(), size_of::<i32>());
}

fn set_bytes(sock: *mut c_void, opt: i32, data: &[u8]) {
    zmq_setsockopt(sock, opt, data.as_ptr().cast(), data.len());
}

fn plain_credential(username: &[u8], password: &[u8]) -> OmqPlainCredential {
    OmqPlainCredential {
        username: username.as_ptr(),
        username_size: username.len(),
        password: password.as_ptr(),
        password_size: password.len(),
    }
}

fn set_timeo(sock: *mut c_void, ms: i32) {
    set_i32(sock, ZMQ_RCVTIMEO, ms);
    set_i32(sock, ZMQ_SNDTIMEO, ms);
}

fn get_i32(sock: *mut c_void, option: i32) -> i32 {
    let mut value = 0i32;
    let mut len = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(sock, option, (&raw mut value).cast(), &raw mut len),
        0
    );
    value
}

fn recv_multipart(sock: *mut c_void) -> Vec<Vec<u8>> {
    let mut parts = Vec::new();
    loop {
        let mut buf = [0u8; 512];
        let size = zmq_recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(size >= 0, "ZAP recv failed, errno={}", omq_zmq::zmq_errno());
        parts.push(buf[..size as usize].to_vec());
        if get_i32(sock, ZMQ_RCVMORE) == 0 {
            return parts;
        }
    }
}

fn send_multipart(sock: *mut c_void, parts: &[&[u8]]) {
    for (index, part) in parts.iter().enumerate() {
        let flags = if index + 1 == parts.len() {
            0
        } else {
            ZMQ_SNDMORE
        };
        assert_eq!(
            zmq_send(sock, part.as_ptr().cast(), part.len(), flags),
            i32::try_from(part.len()).unwrap()
        );
    }
}

fn start_zap_handler<F>(
    ctx: *mut c_void,
    socket_type: i32,
    respond: F,
) -> std::thread::JoinHandle<Vec<Vec<u8>>>
where
    F: FnOnce(&[Vec<u8>]) -> Vec<Vec<u8>> + Send + 'static,
{
    let ctx = ctx as usize;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let ctx = ctx as *mut c_void;
        let zap = zmq_socket(ctx, socket_type);
        set_timeo(zap, 5000);
        assert_eq!(zmq_bind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);
        ready_tx.send(()).unwrap();

        let request = recv_multipart(zap);
        let response = respond(&request);
        let response: Vec<&[u8]> = response.iter().map(Vec::as_slice).collect();
        send_multipart(zap, &response);
        zmq_close(zap);
        request
    });
    ready_rx.recv().unwrap();
    thread
}

fn start_plain_zap_handler(ctx: *mut c_void) -> std::thread::JoinHandle<Vec<Vec<u8>>> {
    start_plain_zap_handler_with_status(ctx, b"200")
}

fn start_plain_zap_handler_with_status(
    ctx: *mut c_void,
    status: &'static [u8],
) -> std::thread::JoinHandle<Vec<Vec<u8>>> {
    let ctx = ctx as usize;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let ctx = ctx as *mut c_void;
        let zap = zmq_socket(ctx, ZMQ_REP);
        set_timeo(zap, 5000);
        assert_eq!(zmq_bind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);
        ready_tx.send(()).unwrap();

        let request = recv_multipart(zap);
        assert_eq!(request.len(), 8);
        let user_id: &[u8] = if status == b"200" { b"alice" } else { b"" };
        let reply = [
            b"1.0".as_slice(),
            request[1].as_slice(),
            status,
            b"OK".as_slice(),
            user_id,
            b"".as_slice(),
        ];
        send_multipart(zap, &reply);
        zmq_close(zap);
        request
    });
    ready_rx.recv().unwrap();
    thread
}

#[test]
fn z85_encode_decode_roundtrip() {
    let data = [0x86u8, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
    let mut encoded = [0u8; 11]; // 8 bytes -> 10 Z85 chars + null
    let ret = zmq_z85_encode(encoded.as_mut_ptr().cast(), data.as_ptr(), data.len());
    assert!(!ret.is_null());
    assert_eq!(&encoded[..10], b"HelloWorld");
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
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
}

#[test]
fn z85_invalid_inputs_set_einval() {
    let data = [0u8; 4];
    let mut encoded = [0u8; 6];
    assert!(zmq_z85_encode(std::ptr::null_mut(), data.as_ptr(), data.len()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert!(zmq_z85_encode(encoded.as_mut_ptr().cast(), std::ptr::null(), data.len()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let mut decoded = [0u8; 8];
    for invalid in [
        c"0".as_ptr(),
        c"01234567".as_ptr(),
        c"#####".as_ptr(),
        c"%nSc1".as_ptr(),
    ] {
        assert!(zmq_z85_decode(decoded.as_mut_ptr(), invalid).is_null());
        assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    }

    assert!(zmq_z85_decode(std::ptr::null_mut(), c"HelloWorld".as_ptr()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert!(zmq_z85_decode(decoded.as_mut_ptr(), std::ptr::null()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
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
fn mismatched_curve_keypair_returns_einval_and_can_be_corrected() {
    let mut first_pub = [0u8; 41];
    let mut first_sec = [0u8; 41];
    let mut second_pub = [0u8; 41];
    let mut second_sec = [0u8; 41];
    zmq_curve_keypair(first_pub.as_mut_ptr().cast(), first_sec.as_mut_ptr().cast());
    zmq_curve_keypair(
        second_pub.as_mut_ptr().cast(),
        second_sec.as_mut_ptr().cast(),
    );

    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_CURVE_PUBLICKEY, &first_pub[..40]);
    set_bytes(push, ZMQ_CURVE_SECRETKEY, &second_sec[..40]);
    set_bytes(push, ZMQ_CURVE_SERVERKEY, &first_pub[..40]);

    assert_eq!(zmq_connect(push, c"tcp://127.0.0.1:1".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    set_bytes(push, ZMQ_CURVE_PUBLICKEY, &second_pub[..40]);
    assert_eq!(zmq_connect(push, c"tcp://127.0.0.1:1".as_ptr()), 0);

    zmq_close(push);
    zmq_ctx_term(ctx);
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
    let zap = start_plain_zap_handler(ctx);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
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

    let request = zap.join().unwrap();

    let mut buf = [0u8; 64];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5, "PLAIN recv failed (errno={})", omq_zmq::zmq_errno());
    assert_eq!(&buf[..5], b"hello");

    assert_eq!(request[0], b"1.0");
    assert_eq!(request[2], b"global");
    assert_eq!(request[3], b"127.0.0.1");
    assert!(request[4].is_empty());
    assert_eq!(request[5], b"PLAIN");
    assert_eq!(request[6], b"user");
    assert_eq!(request[7], b"pass");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_zap_rep_receives_user_id_and_can_reply() {
    let ctx = zmq_ctx_new();
    let zap = start_plain_zap_handler(ctx);
    let rep = zmq_socket(ctx, ZMQ_REP);
    set_i32(rep, ZMQ_PLAIN_SERVER, 1);
    set_bytes(rep, ZMQ_ZAP_DOMAIN, b"global");
    set_timeo(rep, 5000);
    let addr = helpers::bind_random_tcp(rep);

    let req = zmq_socket(ctx, ZMQ_REQ);
    set_bytes(req, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(req, ZMQ_PLAIN_PASSWORD, b"secret");
    set_timeo(req, 5000);
    assert_eq!(zmq_connect(req, addr.as_ptr()), 0);
    assert_eq!(zmq_send(req, b"ping".as_ptr().cast(), 4, 0), 4);

    let mut request = ZmqMsg::new();
    assert_eq!(zmq_msg_recv(request.0.as_mut_ptr().cast(), rep, 0), 4);
    assert_eq!(msg_bytes(&mut request), b"ping");
    assert_eq!(msg_property(&request, c"User-Id"), Some(b"alice".to_vec()));
    assert_eq!(zmq_send(rep, b"pong".as_ptr().cast(), 4, 0), 4);

    let mut reply = [0; 4];
    assert_eq!(zmq_recv(req, reply.as_mut_ptr().cast(), reply.len(), 0), 4);
    assert_eq!(&reply, b"pong");

    zap.join().unwrap();
    zmq_msg_close(request.0.as_mut_ptr().cast());
    zmq_close(req);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_zap_router_receives_user_id_and_can_route_reply() {
    let ctx = zmq_ctx_new();
    let zap = start_plain_zap_handler(ctx);
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    set_i32(router, ZMQ_PLAIN_SERVER, 1);
    set_bytes(router, ZMQ_ZAP_DOMAIN, b"global");
    set_timeo(router, 5000);
    let addr = helpers::bind_random_tcp(router);

    let dealer = zmq_socket(ctx, ZMQ_DEALER);
    set_bytes(dealer, ZMQ_IDENTITY, b"client");
    set_bytes(dealer, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(dealer, ZMQ_PLAIN_PASSWORD, b"secret");
    set_timeo(dealer, 5000);
    assert_eq!(zmq_connect(dealer, addr.as_ptr()), 0);
    assert_eq!(zmq_send(dealer, b"ping".as_ptr().cast(), 4, 0), 4);

    let mut identity = ZmqMsg::new();
    assert_eq!(zmq_msg_recv(identity.0.as_mut_ptr().cast(), router, 0), 6);
    assert_eq!(msg_bytes(&mut identity), b"client");
    assert_eq!(msg_property(&identity, c"User-Id"), Some(b"alice".to_vec()));
    assert_eq!(zmq_msg_more(identity.0.as_ptr().cast()), 1);

    let mut request = ZmqMsg::new();
    assert_eq!(zmq_msg_recv(request.0.as_mut_ptr().cast(), router, 0), 4);
    assert_eq!(msg_bytes(&mut request), b"ping");
    assert_eq!(msg_property(&request, c"User-Id"), Some(b"alice".to_vec()));
    send_multipart(router, &[b"client", b"pong"]);

    let mut reply = [0; 4];
    assert_eq!(
        zmq_recv(dealer, reply.as_mut_ptr().cast(), reply.len(), 0),
        4
    );
    assert_eq!(&reply, b"pong");

    zap.join().unwrap();
    zmq_msg_close(request.0.as_mut_ptr().cast());
    zmq_msg_close(identity.0.as_mut_ptr().cast());
    zmq_close(dealer);
    zmq_close(router);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_zap_router_exposes_and_requires_routing_envelope() {
    let ctx = zmq_ctx_new();
    let zap = start_zap_handler(ctx, ZMQ_ROUTER, |request| {
        assert_eq!(request.len(), 10);
        assert_eq!(request[0].len(), size_of::<u64>());
        assert!(request[1].is_empty());
        vec![
            request[0].clone(),
            Vec::new(),
            b"1.0".to_vec(),
            request[3].clone(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"alice".to_vec(),
            Vec::new(),
        ]
    });
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_bytes(pull, ZMQ_IDENTITY, b"server-id");
    set_timeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"secret");
    set_timeo(push, 5000);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"routed".as_ptr().cast(), 6, 0), 6);

    let request = zap.join().unwrap();
    assert_eq!(request[2], b"1.0");
    assert_eq!(request[4], b"global");
    assert_eq!(request[5], b"127.0.0.1");
    assert_eq!(request[6], b"server-id");
    assert_eq!(request[7], b"PLAIN");
    assert_eq!(request[8], b"alice");
    assert_eq!(request[9], b"secret");

    let mut buf = [0u8; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 6);
    assert_eq!(&buf[..6], b"routed");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn malformed_zap_router_reply_fails_authentication_closed() {
    let ctx = zmq_ctx_new();
    let zap = start_zap_handler(ctx, ZMQ_ROUTER, |request| {
        vec![
            request[0].clone(),
            b"1.0".to_vec(),
            request[3].clone(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"alice".to_vec(),
            Vec::new(),
        ]
    });
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_timeo(pull, 300);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"secret");
    set_timeo(push, 300);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"blocked".as_ptr().cast(), 7, 0), 7);

    zap.join().unwrap();
    let mut buf = [0; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn zap_user_id_and_metadata_are_attached_to_every_received_frame() {
    let ctx = zmq_ctx_new();
    let zap = start_zap_handler(ctx, ZMQ_REP, |request| {
        let mut metadata = vec![4];
        metadata.extend_from_slice(b"Role");
        metadata.extend_from_slice(&5u32.to_be_bytes());
        metadata.extend_from_slice(b"admin");
        vec![
            b"1.0".to_vec(),
            request[1].clone(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"alice".to_vec(),
            metadata,
        ]
    });
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_timeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"secret");
    set_timeo(push, 5000);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    send_multipart(push, &[b"one", b"two"]);
    zap.join().unwrap();

    let mut first = ZmqMsg::new();
    assert_eq!(zmq_msg_recv(first.0.as_mut_ptr().cast(), pull, 0), 3);
    assert_eq!(zmq_msg_more(first.0.as_ptr().cast()), 1);
    assert_eq!(msg_property(&first, c"User-Id"), Some(b"alice".to_vec()));
    assert_eq!(msg_property(&first, c"Role"), Some(b"admin".to_vec()));
    assert_eq!(msg_property(&first, c"Socket-Type"), Some(b"PUSH".to_vec()));

    let mut copied = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(copied.0.as_mut_ptr().cast(), first.0.as_ptr().cast()),
        0
    );
    assert_eq!(msg_property(&copied, c"User-Id"), Some(b"alice".to_vec()));

    let mut second = ZmqMsg::new();
    assert_eq!(zmq_msg_recv(second.0.as_mut_ptr().cast(), pull, 0), 3);
    assert_eq!(zmq_msg_more(second.0.as_ptr().cast()), 0);
    assert_eq!(msg_property(&second, c"Role"), Some(b"admin".to_vec()));

    zmq_msg_close(second.0.as_mut_ptr().cast());
    zmq_msg_close(copied.0.as_mut_ptr().cast());
    zmq_msg_close(first.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn zap_router_can_pipeline_requests_from_multiple_servers() {
    let server_ctx = zmq_ctx_new();
    assert_eq!(zmq_ctx_set(server_ctx, ZMQ_IO_THREADS, 2), 0);
    let handler_ctx = server_ctx as usize;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let zap = std::thread::spawn(move || {
        let ctx = handler_ctx as *mut c_void;
        let router = zmq_socket(ctx, ZMQ_ROUTER);
        set_timeo(router, 5000);
        assert_eq!(zmq_bind(router, c"inproc://zeromq.zap.01".as_ptr()), 0);
        ready_tx.send(()).unwrap();
        let requests = [recv_multipart(router), recv_multipart(router)];
        assert_ne!(requests[0][0], requests[1][0]);
        for request in &requests {
            let response = [
                request[0].as_slice(),
                b"",
                b"1.0",
                request[3].as_slice(),
                b"200",
                b"OK",
                request[8].as_slice(),
                b"",
            ];
            send_multipart(router, &response);
        }
        zmq_close(router);
        requests
    });
    ready_rx.recv().unwrap();

    let pull = zmq_socket(server_ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_timeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let client_contexts = [zmq_ctx_new(), zmq_ctx_new()];
    let credentials = [
        (b"alice".as_slice(), b"secret".as_slice()),
        (b"bob".as_slice(), b"hunter2".as_slice()),
    ];
    let clients: [*mut c_void; 2] = std::array::from_fn(|index| {
        let ctx = client_contexts[index];
        let (username, password) = credentials[index];
        let push = zmq_socket(ctx, ZMQ_PUSH);
        set_bytes(push, ZMQ_PLAIN_USERNAME, username);
        set_bytes(push, ZMQ_PLAIN_PASSWORD, password);
        set_timeo(push, 5000);
        assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
        assert_eq!(
            zmq_send(push, username.as_ptr().cast(), username.len(), 0),
            i32::try_from(username.len()).unwrap()
        );
        push
    });

    let requests = zap.join().unwrap();
    assert_eq!(requests[0][7], b"PLAIN");
    assert_eq!(requests[1][7], b"PLAIN");
    let mut received = std::collections::HashSet::new();
    for _ in 0..2 {
        let mut message = ZmqMsg::new();
        assert!(zmq_msg_recv(message.0.as_mut_ptr().cast(), pull, 0) > 0);
        let body = msg_bytes(&mut message);
        let user_id = msg_property(&message, c"User-Id").unwrap();
        assert_eq!(body, user_id);
        received.insert(body);
        zmq_msg_close(message.0.as_mut_ptr().cast());
    }
    assert_eq!(
        received,
        [b"alice".to_vec(), b"bob".to_vec()].into_iter().collect()
    );

    for (push, ctx) in clients.into_iter().zip(client_contexts) {
        zmq_close(push);
        zmq_ctx_term(ctx);
    }
    zmq_close(pull);
    zmq_ctx_term(server_ctx);
}

#[test]
fn null_zap_request_has_no_credentials() {
    let ctx = zmq_ctx_new();
    let zap = start_zap_handler(ctx, ZMQ_REP, |request| {
        assert_eq!(request.len(), 6);
        vec![
            b"1.0".to_vec(),
            request[1].clone(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"guest".to_vec(),
            Vec::new(),
        ]
    });
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"public");
    set_timeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_timeo(push, 5000);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"null".as_ptr().cast(), 4, 0), 4);

    let request = zap.join().unwrap();
    assert_eq!(request[0], b"1.0");
    assert_eq!(request[2], b"public");
    assert_eq!(request[3], b"127.0.0.1");
    assert_eq!(request[5], b"NULL");
    let mut buf = [0u8; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 4);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn curve_zap_request_carries_raw_client_public_key() {
    let mut server_public = [0u8; 41];
    let mut server_secret = [0u8; 41];
    zmq_curve_keypair(
        server_public.as_mut_ptr().cast(),
        server_secret.as_mut_ptr().cast(),
    );
    let mut client_public = [0u8; 41];
    let mut client_secret = [0u8; 41];
    zmq_curve_keypair(
        client_public.as_mut_ptr().cast(),
        client_secret.as_mut_ptr().cast(),
    );
    let mut raw_client_public = [0u8; 32];
    assert!(
        !zmq_z85_decode(
            raw_client_public.as_mut_ptr(),
            client_public.as_ptr().cast()
        )
        .is_null()
    );

    let ctx = zmq_ctx_new();
    let zap = start_zap_handler(ctx, ZMQ_REP, |request| {
        assert_eq!(request.len(), 7);
        vec![
            b"1.0".to_vec(),
            request[1].clone(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"curve-user".to_vec(),
            Vec::new(),
        ]
    });
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_CURVE_SERVER, 1);
    set_bytes(pull, ZMQ_CURVE_SECRETKEY, &server_secret[..40]);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"secure");
    set_timeo(pull, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_CURVE_PUBLICKEY, &client_public[..40]);
    set_bytes(push, ZMQ_CURVE_SECRETKEY, &client_secret[..40]);
    set_bytes(push, ZMQ_CURVE_SERVERKEY, &server_public[..40]);
    set_timeo(push, 5000);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"curve".as_ptr().cast(), 5, 0), 5);

    let request = zap.join().unwrap();
    assert_eq!(request[2], b"secure");
    assert_eq!(request[5], b"CURVE");
    assert_eq!(request[6], raw_client_public);
    let mut buf = [0u8; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), 5);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_server_without_zap_fails_closed() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_i32(pull, ZMQ_RCVTIMEO, 300);
    set_i32(pull, ZMQ_LINGER, 0);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"user");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"pass");
    set_i32(push, ZMQ_LINGER, 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"denied".as_ptr().cast(), 6, 0), 6);

    let mut buf = [0u8; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn zap_endpoint_requires_a_dedicated_rep_or_router_socket() {
    let ctx = zmq_ctx_new();

    let push = zmq_socket(ctx, ZMQ_PUSH);
    assert_eq!(zmq_bind(push, c"inproc://zeromq.zap.01".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let zap = zmq_socket(ctx, ZMQ_REP);
    assert_eq!(zmq_bind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);
    assert_eq!(zmq_bind(zap, c"tcp://127.0.0.1:0".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(zmq_unbind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);
    assert_eq!(zmq_bind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);

    assert_eq!(zmq_unbind(zap, c"inproc://zeromq.zap.01".as_ptr()), 0);
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    assert_eq!(zmq_bind(router, c"inproc://zeromq.zap.01".as_ptr()), 0);

    let regular = zmq_socket(ctx, ZMQ_REP);
    let _ = helpers::bind_random_tcp(regular);
    assert_eq!(zmq_bind(regular, c"inproc://zeromq.zap.01".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    zmq_close(regular);
    zmq_close(router);
    zmq_close(zap);
    zmq_close(push);
    zmq_ctx_term(ctx);
}

#[test]
fn zap_and_plain_string_options_enforce_rfc_limits() {
    let ctx = zmq_ctx_new();
    let socket = zmq_socket(ctx, ZMQ_PULL);
    let too_long = [b'x'; 256];
    let non_ascii = [0x80u8];

    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_ZAP_DOMAIN,
            too_long.as_ptr().cast(),
            too_long.len(),
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_ZAP_DOMAIN,
            non_ascii.as_ptr().cast(),
            non_ascii.len(),
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(
        zmq_setsockopt(socket, ZMQ_PLAIN_USERNAME, b"has space".as_ptr().cast(), 9,),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(
        zmq_setsockopt(
            socket,
            ZMQ_IDENTITY,
            too_long.as_ptr().cast(),
            too_long.len(),
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let enforced = zmq_socket(ctx, ZMQ_PULL);
    set_i32(enforced, ZMQ_ZAP_ENFORCE_DOMAIN, 1);
    assert_eq!(get_i32(enforced, ZMQ_ZAP_ENFORCE_DOMAIN), 1);
    assert_eq!(zmq_bind(enforced, c"tcp://127.0.0.1:0".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    zmq_close(enforced);
    zmq_close(socket);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_zap_rejects_an_empty_domain_before_bind() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    assert_eq!(zmq_bind(pull, c"tcp://127.0.0.1:0".as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn plain_server_honors_zap_rejection() {
    let ctx = zmq_ctx_new();
    let zap = start_plain_zap_handler_with_status(ctx, b"400");
    let pull = zmq_socket(ctx, ZMQ_PULL);
    set_i32(pull, ZMQ_PLAIN_SERVER, 1);
    set_bytes(pull, ZMQ_ZAP_DOMAIN, b"global");
    set_i32(pull, ZMQ_RCVTIMEO, 300);
    set_i32(pull, ZMQ_LINGER, 0);
    let addr = helpers::bind_random_tcp(pull);

    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(push, ZMQ_PLAIN_USERNAME, b"mallory");
    set_bytes(push, ZMQ_PLAIN_PASSWORD, b"wrong");
    set_i32(push, ZMQ_LINGER, 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    assert_eq!(zmq_send(push, b"denied".as_ptr().cast(), 6, 0), 6);

    let mut buf = [0u8; 16];
    assert_eq!(zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);
    let request = zap.join().unwrap();
    assert_eq!(request[6], b"mallory");
    assert_eq!(request[7], b"wrong");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn omq_fixed_plain_credential_allowlist_is_enforced_without_zap() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let credentials = [
        plain_credential(b"alice", b"secret"),
        plain_credential(b"bob", b"hunter2"),
    ];
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, credentials.as_ptr(), credentials.len()),
        0
    );
    set_i32(pull, ZMQ_RCVTIMEO, 5000);
    let addr = helpers::bind_random_tcp(pull);

    let alice = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(alice, ZMQ_PLAIN_USERNAME, b"alice");
    set_bytes(alice, ZMQ_PLAIN_PASSWORD, b"secret");
    assert_eq!(zmq_connect(alice, addr.as_ptr()), 0);
    assert_eq!(zmq_send(alice, b"alice".as_ptr().cast(), 5, 0), 5);

    let bob = zmq_socket(ctx, ZMQ_PUSH);
    set_bytes(bob, ZMQ_PLAIN_USERNAME, b"bob");
    set_bytes(bob, ZMQ_PLAIN_PASSWORD, b"hunter2");
    assert_eq!(zmq_connect(bob, addr.as_ptr()), 0);
    assert_eq!(zmq_send(bob, b"bob".as_ptr().cast(), 3, 0), 3);

    let mut buf = [0u8; 16];
    let first_len = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(matches!(first_len, 3 | 5));
    let first = buf[..first_len as usize].to_vec();
    let second_len = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(matches!(second_len, 3 | 5));
    let second = buf[..second_len as usize].to_vec();
    assert!((first == b"alice" && second == b"bob") || (first == b"bob" && second == b"alice"));

    zmq_close(alice);
    zmq_close(bob);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn omq_fixed_plain_credentials_validate_c_inputs() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let valid = plain_credential(b"alice", b"secret");

    assert_eq!(
        omq_socket_set_plain_server_credentials(std::ptr::null_mut(), &raw const valid, 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, std::ptr::null(), 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let null_username = OmqPlainCredential {
        username: std::ptr::null(),
        username_size: 1,
        password: b"secret".as_ptr(),
        password_size: 6,
    };
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, &raw const null_username, 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    let invalid_ascii = plain_credential(b"alice smith", b"secret");
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, &raw const invalid_ascii, 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let non_ascii = plain_credential(b"alice", b"secr\xc3\xa9t");
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, &raw const non_ascii, 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    let overlong = OmqPlainCredential {
        username: b"x".as_ptr(),
        username_size: 256,
        password: b"secret".as_ptr(),
        password_size: 6,
    };
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, &raw const overlong, 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, std::ptr::null(), 0),
        0
    );
    assert_eq!(get_i32(pull, ZMQ_PLAIN_SERVER), 1);

    let _addr = helpers::bind_random_tcp(pull);
    assert_eq!(
        omq_socket_set_plain_server_credentials(pull, std::ptr::null(), 0),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    zmq_close(pull);
    zmq_ctx_term(ctx);
}
