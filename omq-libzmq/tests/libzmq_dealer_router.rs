//! Port of `libzmq/tests/test_dealer_router.cpp` (subset)
//! DEALER/ROUTER: identity-routed, async request-reply.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]
#![allow(clippy::similar_names)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_DEALER: i32 = 5;
const ZMQ_ROUTER: i32 = 6;
const ZMQ_REQ: i32 = 3;
const ZMQ_REP: i32 = 4;
const ZMQ_SNDMORE: i32 = 2;
#[allow(dead_code)]
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_ROUTING_ID: i32 = 5;
const TIMEOUT_MS: i32 = 2000;

fn set_timeo(sock: *mut c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

fn set_identity(sock: *mut c_void, id: &[u8]) {
    zmq_setsockopt(sock, ZMQ_ROUTING_ID, id.as_ptr().cast(), id.len());
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

fn recv_frame(sock: *mut c_void, buf: &mut [u8]) -> &[u8] {
    let rc = zmq_recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc >= 0, "recv failed (errno {})", omq_zmq::zmq_errno());
    &buf[..rc as usize]
}

/// DEALER connects to ROUTER; ROUTER sees identity frame + payload.
#[test]
fn dealer_router_identity_routing() {
    let ctx = zmq_ctx_new();
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    let dealer = zmq_socket(ctx, ZMQ_DEALER);

    let addr = CString::new("inproc://test-dr-basic").unwrap();
    zmq_bind(router, addr.as_ptr());

    set_identity(dealer, b"client1");
    zmq_connect(dealer, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    set_timeo(router, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(router, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(dealer, ZMQ_RCVTIMEO, TIMEOUT_MS);

    // Dealer sends payload.
    zmq_send(dealer, b"hello".as_ptr().cast(), 5, 0);

    // Router receives: [identity, empty-delimiter?, payload]
    // With omq, DEALER→ROUTER over inproc: identity frame + payload.
    let mut id_buf = [0u8; 64];
    let id = recv_frame(router, &mut id_buf);
    assert!(!id.is_empty(), "expected identity frame");

    // May have an empty delimiter depending on implementation.
    // Drain until we reach the payload.
    let mut buf = [0u8; 64];
    let mut payload_len = 0;
    loop {
        let more = rcvmore(router);
        if !more {
            // This was the last frame we already read (id), wait for payload below.
            break;
        }
        let rc = zmq_recv(router, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc >= 0);
        if !rcvmore(router) {
            payload_len = rc as usize;
            break;
        }
    }
    if payload_len == 0 {
        // id was the only prior frame; check if there are more
        if rcvmore(router) {
            let rc = zmq_recv(router, buf.as_mut_ptr().cast(), buf.len(), 0);
            assert!(rc >= 0);
            payload_len = rc as usize;
        }
    }
    assert_eq!(&buf[..payload_len], b"hello");

    // Router replies: send id + payload back.
    zmq_send(router, id.as_ptr().cast(), id.len(), ZMQ_SNDMORE);
    zmq_send(router, b"world".as_ptr().cast(), 5, 0);

    let rc = zmq_recv(dealer, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"world");

    zmq_close(dealer);
    zmq_close(router);
    zmq_ctx_term(ctx);
}

/// DEALER/ROUTER over TCP
#[test]
fn dealer_router_tcp() {
    let ctx = zmq_ctx_new();
    let router = zmq_socket(ctx, ZMQ_ROUTER);
    let dealer = zmq_socket(ctx, ZMQ_DEALER);

    let addr = helpers::bind_random_tcp(router);
    set_identity(dealer, b"d1");
    zmq_connect(dealer, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));

    set_timeo(router, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(router, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(dealer, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(dealer, ZMQ_SNDTIMEO, TIMEOUT_MS);

    // Dealer sends 3 messages.
    for i in 0..3u8 {
        let msg = [i; 4];
        zmq_send(dealer, msg.as_ptr().cast(), 4, 0);
    }

    // Router receives all, replies.
    let mut id_buf = [0u8; 64];
    let mut buf = [0u8; 64];
    for i in 0..3u8 {
        // Drain the identity frame(s) and get payload.
        let id_len = {
            let id = recv_frame(router, &mut id_buf);
            id.len()
        };
        assert!(id_len > 0);

        // Drain frames until payload (last frame).
        let mut payload = [0u8; 64];
        let mut plen = 0;
        loop {
            let more = rcvmore(router);
            if !more {
                break;
            }
            let rc = zmq_recv(router, payload.as_mut_ptr().cast(), payload.len(), 0);
            assert!(rc >= 0);
            plen = rc as usize;
            if !rcvmore(router) {
                break;
            }
        }
        if plen == 0 {
            // Payload was the immediately next frame after id (no delimiter).
            plen = {
                let r = zmq_recv(router, payload.as_mut_ptr().cast(), payload.len(), 0);
                r as usize
            };
        }
        assert_eq!(plen, 4);
        assert!(payload[..4].iter().all(|&b| b == i));

        // Reply.
        zmq_send(
            router,
            id_buf[..id_len].as_ptr().cast(),
            id_len,
            ZMQ_SNDMORE,
        );
        let reply = [i + 100; 2];
        zmq_send(router, reply.as_ptr().cast(), 2, 0);

        let rc = zmq_recv(dealer, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert_eq!(rc, 2);
        assert!(buf[..2].iter().all(|&b| b == i + 100));
    }

    zmq_close(dealer);
    zmq_close(router);
    zmq_ctx_term(ctx);
}

/// Multiple DEALERs -> one ROUTER
#[test]
fn multiple_dealers_one_router() {
    const N: usize = 3;

    let ctx = zmq_ctx_new();
    let router = zmq_socket(ctx, ZMQ_ROUTER);

    let addr = helpers::bind_random_tcp(router);

    set_timeo(router, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(router, ZMQ_SNDTIMEO, TIMEOUT_MS);
    let mut dealers = Vec::new();
    for i in 0..N {
        let d = zmq_socket(ctx, ZMQ_DEALER);
        set_identity(d, format!("d{i}").as_bytes());
        zmq_connect(d, addr.as_ptr());
        set_timeo(d, ZMQ_RCVTIMEO, TIMEOUT_MS);
        dealers.push(d);
    }
    std::thread::sleep(Duration::from_millis(150));

    // Each dealer sends one message.
    for (i, d) in dealers.iter().enumerate() {
        let msg = format!("msg{i}");
        zmq_send(*d, msg.as_ptr().cast(), msg.len(), 0);
    }

    // Router receives N messages and echoes each back.
    let mut id_buf = [0u8; 64];
    let mut buf = [0u8; 64];
    for _ in 0..N {
        let id_len = {
            let id = recv_frame(router, &mut id_buf);
            id.len()
        };

        // Drain to payload.
        let mut payload = [0u8; 64];
        let mut plen = 0;
        loop {
            let more = rcvmore(router);
            if !more {
                break;
            }
            let rc = zmq_recv(router, payload.as_mut_ptr().cast(), payload.len(), 0);
            assert!(rc >= 0);
            plen = rc as usize;
            if !rcvmore(router) {
                break;
            }
        }
        if plen == 0 {
            plen = zmq_recv(router, payload.as_mut_ptr().cast(), payload.len(), 0) as usize;
        }
        assert!(plen > 0);

        // Echo.
        zmq_send(
            router,
            id_buf[..id_len].as_ptr().cast(),
            id_len,
            ZMQ_SNDMORE,
        );
        zmq_send(router, payload[..plen].as_ptr().cast(), plen, 0);
    }

    // Each dealer receives its echo.
    for (i, d) in dealers.iter().enumerate() {
        let rc = zmq_recv(*d, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc > 0, "dealer {i} expected reply");
    }

    for d in dealers {
        zmq_close(d);
    }
    zmq_close(router);
    zmq_ctx_term(ctx);
}

/// REQ talking through a ROUTER/DEALER proxy.
/// Pattern: REQ <-> ROUTER (frontend) <--> DEALER (backend) <-> REP
/// This is the canonical libzmq load-balancer proxy.
#[test]
fn req_through_dealer_router() {
    let ctx = zmq_ctx_new();

    let router_fe = zmq_socket(ctx, ZMQ_ROUTER);
    let dealer_be = zmq_socket(ctx, ZMQ_DEALER);
    let req = zmq_socket(ctx, ZMQ_REQ);
    let rep = zmq_socket(ctx, ZMQ_REP);

    let addr_fe = helpers::bind_random_tcp(router_fe);
    let addr_be = helpers::bind_random_tcp(dealer_be);
    zmq_connect(req, addr_fe.as_ptr());
    zmq_connect(rep, addr_be.as_ptr());
    std::thread::sleep(Duration::from_millis(150));

    set_timeo(req, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(req, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(router_fe, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(router_fe, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(dealer_be, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(dealer_be, ZMQ_SNDTIMEO, TIMEOUT_MS);
    set_timeo(rep, ZMQ_RCVTIMEO, TIMEOUT_MS);
    set_timeo(rep, ZMQ_SNDTIMEO, TIMEOUT_MS);

    // REQ sends.
    zmq_send(req, b"query".as_ptr().cast(), 5, 0);

    // Proxy: router_fe -> dealer_be (forward all frames including routing envelope).
    let mut buf = [0u8; 256];
    loop {
        let rc = zmq_recv(router_fe, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc >= 0);
        let more = rcvmore(router_fe);
        let send_flags = if more { ZMQ_SNDMORE } else { 0 };
        zmq_send(
            dealer_be,
            buf[..rc as usize].as_ptr().cast(),
            rc as usize,
            send_flags,
        );
        if !more {
            break;
        }
    }

    // REP receives "query".
    let rc = zmq_recv(rep, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"query");

    // REP sends reply.
    zmq_send(rep, b"answer".as_ptr().cast(), 6, 0);

    // Proxy: dealer_be -> router_fe (forward all frames including routing envelope).
    loop {
        let rc = zmq_recv(dealer_be, buf.as_mut_ptr().cast(), buf.len(), 0);
        assert!(rc >= 0);
        let more = rcvmore(dealer_be);
        let send_flags = if more { ZMQ_SNDMORE } else { 0 };
        zmq_send(
            router_fe,
            buf[..rc as usize].as_ptr().cast(),
            rc as usize,
            send_flags,
        );
        if !more {
            break;
        }
    }

    // REQ receives "answer".
    let rc = zmq_recv(req, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 6);
    assert_eq!(&buf[..6], b"answer");

    zmq_close(req);
    zmq_close(router_fe);
    zmq_close(dealer_be);
    zmq_close(rep);
    zmq_ctx_term(ctx);
}
