//! Draft socket types: SERVER/CLIENT, RADIO/DISH, SCATTER/GATHER, PEER, CHANNEL.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_join,
    zmq_msg_close, zmq_msg_init_buffer, zmq_msg_send, zmq_msg_set_group, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_SERVER: i32 = 12;
const ZMQ_CLIENT: i32 = 13;
const ZMQ_RADIO: i32 = 14;
const ZMQ_DISH: i32 = 15;
const ZMQ_GATHER: i32 = 16;
const ZMQ_SCATTER: i32 = 17;
const ZMQ_PEER: i32 = 19;
const ZMQ_CHANNEL: i32 = 20;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_TYPE: i32 = 16;
const ZMQ_IDENTITY: i32 = 5;

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

#[repr(C)]
struct ZmqMsg([usize; ZMQ_MSG_WORDS]);

impl ZmqMsg {
    fn zeroed() -> Self {
        Self([0; ZMQ_MSG_WORDS])
    }
}

fn radio_send(sock: *mut c_void, group: &std::ffi::CStr, data: &[u8]) -> i32 {
    let mut m = ZmqMsg::zeroed();
    zmq_msg_init_buffer(m.0.as_mut_ptr().cast(), data.as_ptr().cast(), data.len());
    zmq_msg_set_group(m.0.as_mut_ptr().cast(), group.as_ptr());
    let rc = zmq_msg_send(m.0.as_mut_ptr().cast(), sock, 0);
    if rc < 0 {
        zmq_msg_close(m.0.as_mut_ptr().cast());
    }
    rc
}

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

fn get_type(sock: *mut c_void) -> i32 {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_TYPE, (&mut v as *mut i32).cast(), &mut sz);
    v
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

/// SERVER/CLIENT: SERVER receives [`routing_id`, body], replies [`routing_id`, body].
#[test]
fn server_client_roundtrip() {
    let ctx = zmq_ctx_new();
    let server = zmq_socket(ctx, ZMQ_SERVER);
    let client = zmq_socket(ctx, ZMQ_CLIENT);
    assert_eq!(get_type(server), ZMQ_SERVER);
    assert_eq!(get_type(client), ZMQ_CLIENT);

    let addr = CString::new("inproc://test-server-client").unwrap();
    zmq_bind(server, addr.as_ptr());
    zmq_connect(client, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(server, 2000);
    set_timeo(client, 2000);

    let rc = zmq_send(client, b"request".as_ptr().cast(), 7, 0);
    assert_eq!(rc, 7);

    // SERVER recv: identity frame first (like ROUTER).
    let mut buf = [0u8; 64];
    let rc = zmq_recv(server, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(
        rc > 0,
        "server recv identity failed (errno={})",
        omq_zmq::zmq_errno()
    );
    let id_len = rc as usize;
    let id = buf[..id_len].to_vec();
    assert!(rcvmore(server), "identity frame should have more");

    // Payload frame.
    let rc = zmq_recv(server, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"request");

    // SERVER reply: [routing_id, reply_body].
    zmq_send(server, id.as_ptr().cast(), id_len, ZMQ_SNDMORE);
    zmq_send(server, b"reply".as_ptr().cast(), 5, 0);

    // CLIENT receives reply.
    let rc = zmq_recv(client, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"reply");

    zmq_close(client);
    zmq_close(server);
    zmq_ctx_term(ctx);
}

/// SCATTER/GATHER over TCP.
#[test]
fn scatter_gather_tcp() {
    let ctx = zmq_ctx_new();
    let scatter = zmq_socket(ctx, ZMQ_SCATTER);
    let gather = zmq_socket(ctx, ZMQ_GATHER);
    assert_eq!(get_type(scatter), ZMQ_SCATTER);
    assert_eq!(get_type(gather), ZMQ_GATHER);

    let addr = helpers::bind_random_tcp(gather);
    zmq_connect(scatter, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));
    set_timeo(scatter, 2000);
    set_timeo(gather, 2000);

    for i in 0u8..5 {
        let msg = [i; 4];
        let rc = zmq_send(scatter, msg.as_ptr().cast(), 4, 0);
        assert_eq!(rc, 4, "scatter send {i}");
    }

    let mut buf = [0u8; 64];
    for i in 0u8..5 {
        let rc = zmq_recv(gather, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 4, "gather recv {i}");
        assert!(buf[..4].iter().all(|&b| b == i));
    }

    zmq_close(scatter);
    zmq_close(gather);
    zmq_ctx_term(ctx);
}

/// RADIO/DISH with group membership.
#[test]
fn radio_dish_inproc() {
    let ctx = zmq_ctx_new();
    let radio = zmq_socket(ctx, ZMQ_RADIO);
    let dish = zmq_socket(ctx, ZMQ_DISH);
    assert_eq!(get_type(radio), ZMQ_RADIO);
    assert_eq!(get_type(dish), ZMQ_DISH);

    let addr = CString::new("inproc://test-radio-dish").unwrap();
    zmq_bind(radio, addr.as_ptr());

    let group = CString::new("weather").unwrap();
    zmq_join(dish, group.as_ptr());
    zmq_connect(dish, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(dish, 2000);

    radio_send(radio, c"weather", b"weather sunny");

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dish, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 13, "dish recv failed (errno={})", omq_zmq::zmq_errno());
    assert_eq!(&buf[..13], b"weather sunny");

    zmq_close(dish);
    zmq_close(radio);
    zmq_ctx_term(ctx);
}

/// RADIO/DISH: messages to non-joined groups are filtered.
#[test]
fn radio_dish_filtered() {
    let ctx = zmq_ctx_new();
    let radio = zmq_socket(ctx, ZMQ_RADIO);
    let dish = zmq_socket(ctx, ZMQ_DISH);

    let addr = CString::new("inproc://test-radio-dish-filter").unwrap();
    zmq_bind(radio, addr.as_ptr());

    let sports = CString::new("sports").unwrap();
    zmq_join(dish, sports.as_ptr());
    zmq_connect(dish, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(dish, 500);

    radio_send(radio, c"news", b"news headline");
    radio_send(radio, c"sports", b"sports goal");

    let mut buf = [0u8; 64];
    let rc = zmq_recv(dish, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 11);
    assert_eq!(&buf[..11], b"sports goal");

    zmq_close(dish);
    zmq_close(radio);
    zmq_ctx_term(ctx);
}

/// CHANNEL: bidirectional, like PAIR.
#[test]
fn channel_inproc() {
    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_CHANNEL);
    let b = zmq_socket(ctx, ZMQ_CHANNEL);
    assert_eq!(get_type(a), ZMQ_CHANNEL);

    let addr = CString::new("inproc://test-channel").unwrap();
    zmq_bind(a, addr.as_ptr());
    zmq_connect(b, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(a, 1000);
    set_timeo(b, 1000);

    zmq_send(a, b"chan-a".as_ptr().cast(), 6, 0);
    zmq_send(b, b"chan-b".as_ptr().cast(), 6, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"chan-a");

    let rc = zmq_recv(a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"chan-b");

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}

/// PEER: identity-routed, bidirectional. Both peers need explicit identities.
#[test]
fn peer_inproc() {
    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_PEER);
    let b = zmq_socket(ctx, ZMQ_PEER);
    assert_eq!(get_type(a), ZMQ_PEER);
    assert_eq!(get_type(b), ZMQ_PEER);

    zmq_setsockopt(a, ZMQ_IDENTITY, b"peer-a".as_ptr().cast(), 6);
    zmq_setsockopt(b, ZMQ_IDENTITY, b"peer-b".as_ptr().cast(), 6);

    let addr = CString::new("inproc://test-peer").unwrap();
    zmq_bind(a, addr.as_ptr());
    zmq_connect(b, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));
    set_timeo(a, 2000);
    set_timeo(b, 2000);

    // B sends to A: [target_identity="peer-a", body].
    zmq_send(b, b"peer-a".as_ptr().cast(), 6, ZMQ_SNDMORE);
    let rc = zmq_send(b, b"hello-a".as_ptr().cast(), 7, 0);
    assert_eq!(rc, 7, "peer B send failed (errno={})", omq_zmq::zmq_errno());

    // A receives: [sender_identity="peer-b", body].
    let mut buf = [0u8; 64];
    let rc = zmq_recv(a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(
        rc > 0,
        "peer A recv identity failed (errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(&buf[..rc as usize], b"peer-b");
    assert!(rcvmore(a));

    let rc = zmq_recv(a, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"hello-a");

    // A replies to B: [target_identity="peer-b", body].
    zmq_send(a, b"peer-b".as_ptr().cast(), 6, ZMQ_SNDMORE);
    let rc = zmq_send(a, b"hello-b".as_ptr().cast(), 7, 0);
    assert_eq!(rc, 7);

    // B receives: [sender_identity="peer-a", body].
    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(
        rc > 0,
        "peer B recv identity failed (errno={})",
        omq_zmq::zmq_errno()
    );
    assert_eq!(&buf[..rc as usize], b"peer-a");
    assert!(rcvmore(b));

    let rc = zmq_recv(b, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 7);
    assert_eq!(&buf[..7], b"hello-b");

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}

/// SERVER/CLIENT over TCP with multiple clients.
#[test]
fn server_multiple_clients_tcp() {
    let ctx = zmq_ctx_new();
    let server = zmq_socket(ctx, ZMQ_SERVER);
    let addr = helpers::bind_random_tcp(server);
    set_timeo(server, 2000);

    let mut clients = Vec::new();
    for _ in 0..3 {
        let c = zmq_socket(ctx, ZMQ_CLIENT);
        zmq_connect(c, addr.as_ptr());
        set_timeo(c, 2000);
        clients.push(c);
    }
    std::thread::sleep(Duration::from_millis(100));

    // Each client sends one message.
    for (i, c) in clients.iter().enumerate() {
        let msg = format!("req{i}");
        zmq_send(*c, msg.as_ptr().cast(), msg.len(), 0);
    }

    let mut buf = [0u8; 64];
    // Server receives and echoes back.
    for _ in 0..3 {
        // Identity frame.
        let rc = zmq_recv(server, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0);
        let id_len = rc as usize;
        let id = buf[..id_len].to_vec();
        assert!(rcvmore(server));

        // Payload.
        let rc = zmq_recv(server, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0);
        let plen = rc as usize;

        // Echo back: [id, payload].
        zmq_send(server, id.as_ptr().cast(), id_len, ZMQ_SNDMORE);
        zmq_send(server, buf[..plen].as_ptr().cast(), plen, 0);
    }

    // Each client receives its echo.
    for (i, c) in clients.iter().enumerate() {
        let rc = zmq_recv(*c, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0, "client {i} recv failed");
        let got = std::str::from_utf8(&buf[..rc as usize]).unwrap();
        assert_eq!(got, format!("req{i}"));
    }

    for c in clients {
        zmq_close(c);
    }
    zmq_close(server);
    zmq_ctx_term(ctx);
}
