//! `zmq_socket_monitor` tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_recv, zmq_setsockopt, zmq_socket,
    zmq_socket_get_peer_state, zmq_socket_monitor, zmq_socket_monitor_pipes_stats,
    zmq_socket_monitor_versioned,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_PAIR: i32 = 0;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;

const ZMQ_EVENT_LISTENING: u16 = 0x0008;
const ZMQ_EVENT_ACCEPTED: u16 = 0x0020;
#[allow(dead_code)]
const ZMQ_EVENT_CONNECTED: u16 = 0x0001;
const ZMQ_EVENT_HANDSHAKE_SUCCEEDED: u16 = 0x1000;
const ZMQ_EVENT_ALL: i32 = 0xFFFF;
const ZMQ_CURRENT_EVENT_VERSION: i32 = 1;
const ZMQ_CURRENT_EVENT_VERSION_DRAFT: i32 = 2;
const ZMQ_ENOTSUP: i32 = 156_384_713;

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

fn recv_monitor_event(mon: *mut c_void) -> Option<(u16, String)> {
    let mut header = [0u8; 64];
    let rc = zmq_recv(mon, header.as_mut_ptr().cast(), header.len(), 0);
    if rc < 6 {
        return None;
    }
    let event_id = u16::from_le_bytes([header[0], header[1]]);

    let mut ep_buf = [0u8; 256];
    let rc2 = zmq_recv(mon, ep_buf.as_mut_ptr().cast(), ep_buf.len(), 0);
    let endpoint = if rc2 > 0 {
        String::from_utf8_lossy(&ep_buf[..rc2 as usize]).to_string()
    } else {
        String::new()
    };

    Some((event_id, endpoint))
}

#[test]
fn monitor_versioned_v1_delegates_and_v2_stubs_return_enotsup() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let mon_addr = CString::new("inproc://test-monitor-versioned-v1").unwrap();

    assert_eq!(
        zmq_socket_monitor_versioned(
            pull,
            mon_addr.as_ptr(),
            ZMQ_EVENT_ALL as u64,
            ZMQ_CURRENT_EVENT_VERSION,
            ZMQ_PAIR,
        ),
        0
    );

    let pull2 = zmq_socket(ctx, ZMQ_PULL);
    let mon_addr2 = CString::new("inproc://test-monitor-versioned-v2").unwrap();
    assert_eq!(
        zmq_socket_monitor_versioned(
            pull2,
            mon_addr2.as_ptr(),
            ZMQ_EVENT_ALL as u64,
            ZMQ_CURRENT_EVENT_VERSION_DRAFT,
            ZMQ_PAIR,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSUP);
    assert_eq!(zmq_socket_monitor_pipes_stats(pull2), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSUP);
    assert_eq!(zmq_socket_get_peer_state(pull2, std::ptr::null(), 0), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSUP);

    zmq_close(pull2);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn monitor_receives_listening_and_accepted() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let mon_addr = CString::new("inproc://test-monitor").unwrap();
    let rc = zmq_socket_monitor(pull, mon_addr.as_ptr(), ZMQ_EVENT_ALL);
    assert_eq!(
        rc,
        0,
        "monitor setup failed (errno={})",
        omq_zmq::zmq_errno()
    );

    let mon = zmq_socket(ctx, ZMQ_PAIR);
    zmq_connect(mon, mon_addr.as_ptr());
    set_timeo(mon, 2000);

    let addr = helpers::bind_random_tcp(pull);

    // Should receive LISTENING event.
    let ev = recv_monitor_event(mon);
    assert!(ev.is_some(), "expected LISTENING event");
    let (id, ep) = ev.unwrap();
    assert_eq!(id, ZMQ_EVENT_LISTENING, "event={id:#06x}");
    assert_eq!(ep, addr.to_str().unwrap());

    // Connect a PUSH socket.
    let push = zmq_socket(ctx, ZMQ_PUSH);
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));

    // Should receive ACCEPTED and/or HANDSHAKE_SUCCEEDED.
    let mut got_accepted = false;
    let mut got_handshake = false;
    for _ in 0..5 {
        if let Some((id, _)) = recv_monitor_event(mon) {
            if id == ZMQ_EVENT_ACCEPTED {
                got_accepted = true;
            }
            if id == ZMQ_EVENT_HANDSHAKE_SUCCEEDED {
                got_handshake = true;
            }
        } else {
            break;
        }
    }
    assert!(got_accepted, "expected ACCEPTED event");
    assert!(got_handshake, "expected HANDSHAKE_SUCCEEDED event");

    zmq_close(push);
    zmq_close(mon);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn monitor_event_filter() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);

    // Only subscribe to LISTENING events.
    let mon_addr = CString::new("inproc://test-monitor-filter").unwrap();
    let rc = zmq_socket_monitor(pull, mon_addr.as_ptr(), i32::from(ZMQ_EVENT_LISTENING));
    assert_eq!(rc, 0);

    let mon = zmq_socket(ctx, ZMQ_PAIR);
    zmq_connect(mon, mon_addr.as_ptr());
    set_timeo(mon, 1000);

    let addr = helpers::bind_random_tcp(pull);

    // Connect a peer (generates ACCEPTED, but we're not subscribed).
    let push = zmq_socket(ctx, ZMQ_PUSH);
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(100));

    // First event should be LISTENING.
    let ev = recv_monitor_event(mon);
    assert!(ev.is_some());
    assert_eq!(ev.unwrap().0, ZMQ_EVENT_LISTENING);

    // Next recv should timeout (ACCEPTED is filtered out).
    set_timeo(mon, 200);
    let mut buf = [0u8; 64];
    let rc = zmq_recv(mon, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert!(rc < 0, "should timeout: only LISTENING subscribed");

    zmq_close(push);
    zmq_close(mon);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn monitor_null_addr_stops() {
    let ctx = zmq_ctx_new();
    let s = zmq_socket(ctx, ZMQ_PUSH);
    let rc = zmq_socket_monitor(s, std::ptr::null(), 0);
    assert_eq!(rc, 0, "null addr should succeed (stop monitoring)");
    zmq_close(s);
    zmq_ctx_term(ctx);
}
