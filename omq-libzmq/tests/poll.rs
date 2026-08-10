//! `zmq_poll` smoke tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use serial_test::serial;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_poll, zmq_ppoll, zmq_recv,
    zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
#[allow(dead_code)]
const ZMQ_PAIR: i32 = 0;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_POLLIN: i16 = 1;
const ZMQ_POLLOUT: i16 = 2;
const ZMQ_ENOTSUP: i32 = 156_384_713;

#[repr(C)]
struct PollItem {
    socket: *mut c_void,
    fd: i32,
    events: i16,
    revents: i16,
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

#[test]
fn poll_null_items_zero_count_is_valid() {
    let rc = zmq_poll(std::ptr::null_mut(), 0, 0);
    assert_eq!(rc, 0);
}

#[test]
fn ppoll_null_sigmask_delegates_to_poll() {
    assert_eq!(zmq_ppoll(std::ptr::null_mut(), 0, 0, std::ptr::null()), 0);
}

#[cfg(unix)]
#[test]
fn ppoll_non_null_sigmask_returns_enotsup() {
    let mask = std::mem::MaybeUninit::<libc::sigset_t>::zeroed();
    assert_eq!(
        zmq_ppoll(std::ptr::null_mut(), 0, 0, mask.as_ptr().cast()),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSUP);
}

#[test]
fn poll_timeout_no_events() {
    let ctx = zmq_ctx_new();
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://test-poll-timeout").unwrap();
    zmq_bind(pull, addr.as_ptr());

    let mut items = [PollItem {
        socket: pull,
        fd: -1,
        events: ZMQ_POLLIN,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 10);
    assert_eq!(rc, 0, "expected 0 ready items on timeout");
    assert_eq!(items[0].revents, 0);

    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_detects_readable() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-poll-readable").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(push, 1000);
    set_timeo(pull, 1000);

    zmq_send(push, b"msg".as_ptr().cast(), 3, 0);
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [PollItem {
        socket: pull,
        fd: -1,
        events: ZMQ_POLLIN,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 1000);
    assert_eq!(rc, 1);
    assert_ne!(items[0].revents & ZMQ_POLLIN, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 3);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_multiple_sockets() {
    let ctx = zmq_ctx_new();
    let push1 = zmq_socket(ctx, ZMQ_PUSH);
    let pull1 = zmq_socket(ctx, ZMQ_PULL);
    let push2 = zmq_socket(ctx, ZMQ_PUSH);
    let pull2 = zmq_socket(ctx, ZMQ_PULL);

    let addr1 = CString::new("inproc://poll-multi-1").unwrap();
    let addr2 = CString::new("inproc://poll-multi-2").unwrap();
    zmq_bind(pull1, addr1.as_ptr());
    zmq_connect(push1, addr1.as_ptr());
    zmq_bind(pull2, addr2.as_ptr());
    zmq_connect(push2, addr2.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(push1, 1000);
    set_timeo(push2, 1000);

    zmq_send(push2, b"two".as_ptr().cast(), 3, 0);
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [
        PollItem {
            socket: pull1,
            fd: -1,
            events: ZMQ_POLLIN,
            revents: 0,
        },
        PollItem {
            socket: pull2,
            fd: -1,
            events: ZMQ_POLLIN,
            revents: 0,
        },
    ];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 2, 1000);
    assert!(rc >= 1);
    assert_eq!(
        items[0].revents & ZMQ_POLLIN,
        0,
        "pull1 should not be readable"
    );
    assert_ne!(items[1].revents & ZMQ_POLLIN, 0, "pull2 should be readable");

    zmq_close(push1);
    zmq_close(pull1);
    zmq_close(push2);
    zmq_close(pull2);
    zmq_ctx_term(ctx);
}

#[test]
fn poll_pollout_on_empty_socket() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://poll-pollout").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));

    let mut items = [PollItem {
        socket: push,
        fd: -1,
        events: ZMQ_POLLOUT,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 100);
    assert_eq!(rc, 1);
    assert_ne!(items[0].revents & ZMQ_POLLOUT, 0);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Helper to create N push-pull socket pairs over inproc.
/// Returns (ctx, vec of (push, pull) pairs, addresses for cleanup).
fn create_n_socket_pairs(n: usize) -> (*mut c_void, Vec<(*mut c_void, *mut c_void)>, Vec<CString>) {
    let ctx = zmq_ctx_new();
    let mut pairs = Vec::new();
    let mut addrs = Vec::new();

    for i in 0..n {
        let push = zmq_socket(ctx, ZMQ_PUSH);
        let pull = zmq_socket(ctx, ZMQ_PULL);
        assert!(!push.is_null(), "failed to create push socket {i}");
        assert!(!pull.is_null(), "failed to create pull socket {i}");

        let addr_str = format!("inproc://poll-test-{i}");
        let addr = CString::new(addr_str).unwrap();

        assert_eq!(zmq_bind(pull, addr.as_ptr()), 0, "bind failed for {i}");
        assert_eq!(
            zmq_connect(push, addr.as_ptr()),
            0,
            "connect failed for {i}"
        );

        set_timeo(push, 100);
        set_timeo(pull, 100);

        pairs.push((push, pull));
        addrs.push(addr);
    }

    // Allow connections to settle
    std::thread::sleep(Duration::from_millis(50));

    (ctx, pairs, addrs)
}

/// Create poll items for all pull sockets with POLLIN events.
fn create_poll_items(pairs: &[(*mut c_void, *mut c_void)]) -> Vec<PollItem> {
    pairs
        .iter()
        .map(|(_push, pull)| PollItem {
            socket: *pull,
            fd: -1,
            events: ZMQ_POLLIN,
            revents: 0,
        })
        .collect()
}

#[test]
#[serial]
#[cfg(unix)]
fn poll_many_sockets_unix_smoke() {
    let (ctx, pairs, _addrs) = create_n_socket_pairs(16);
    let target_indices = [2, 7, 13];

    for &idx in &target_indices {
        let (push, _) = pairs[idx];
        let msg = format!("msg{idx}");
        let len = i32::try_from(msg.len()).unwrap();
        assert_eq!(zmq_send(push, msg.as_ptr().cast(), msg.len(), 0), len);
    }
    std::thread::sleep(Duration::from_millis(20));

    let mut items = create_poll_items(&pairs);
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 100);

    assert_eq!(
        rc as usize,
        target_indices.len(),
        "expected {} ready sockets, got {rc}",
        target_indices.len()
    );
    for &idx in &target_indices {
        assert_ne!(
            items[idx].revents & ZMQ_POLLIN,
            0,
            "Socket {idx} should be readable"
        );
    }

    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
fn poll_simple_two_socket_test() {
    let ctx = zmq_ctx_new();
    let push1 = zmq_socket(ctx, ZMQ_PUSH);
    let pull1 = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://simple-test").unwrap();
    zmq_bind(pull1, addr.as_ptr());
    zmq_connect(push1, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(50));

    // Send a message
    zmq_send(push1, b"hello".as_ptr().cast(), 5, 0);

    std::thread::sleep(Duration::from_millis(50));

    let mut items = [PollItem {
        socket: pull1,
        fd: -1,
        events: ZMQ_POLLIN,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 1000);

    // Try to receive - should work because poll detected it
    let mut buf = [0u8; 32];
    zmq_recv(pull1, buf.as_mut_ptr().cast(), buf.len(), 0);

    // The important assertion
    assert_eq!(rc, 1, "poll() should have detected readable socket");

    zmq_close(push1);
    zmq_close(pull1);
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
#[cfg(windows)]
fn poll_65_sockets_boundary() {
    // Test: 65 sockets (crosses batch 0→1 on Windows)
    // Send to socket at index 50, poll, verify detection
    let (ctx, pairs, _addrs) = create_n_socket_pairs(65);

    // Send message to socket 50
    let (push_50, _pull_50) = pairs[50];
    zmq_send(push_50, b"msg50".as_ptr().cast(), 5, 0);
    std::thread::sleep(Duration::from_millis(20));

    // Poll all 65 sockets
    let mut items = create_poll_items(&pairs);
    // In this test we know this is safe
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 100);

    // Should detect at least socket 50
    assert!(rc >= 1, "Expected at least 1 ready socket, got {rc}");
    assert_ne!(
        items[50].revents & ZMQ_POLLIN,
        0,
        "Socket 50 should be readable"
    );

    // Cleanup
    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
#[cfg(windows)]
fn poll_128_sockets_boundary() {
    // Test: 128 sockets (crosses batch 1→2 on Windows)
    // Send to socket at indices 30 and 100, verify both detected
    let (ctx, pairs, _addrs) = create_n_socket_pairs(128);

    // Send to socket 30 (in batch 0)
    let (push_30, _pull_30) = pairs[30];
    zmq_send(push_30, b"msg30".as_ptr().cast(), 5, 0);

    // Send to socket 100 (in batch 1)
    let (push_100, _pull_100) = pairs[100];
    zmq_send(push_100, b"msg100".as_ptr().cast(), 7, 0);

    std::thread::sleep(Duration::from_millis(20));

    // Poll all 128 sockets
    let mut items = create_poll_items(&pairs);
    // In this test we know this is safe
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 100);

    // Should detect both sockets
    assert!(rc >= 2, "Expected at least 2 ready sockets, got {rc}");
    assert_ne!(
        items[30].revents & ZMQ_POLLIN,
        0,
        "Socket 30 should be readable"
    );
    assert_ne!(
        items[100].revents & ZMQ_POLLIN,
        0,
        "Socket 100 should be readable"
    );

    // Cleanup
    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
#[cfg(windows)]
fn poll_256_sockets_boundary() {
    // Test: 256 sockets (3 batches on Windows)
    // Send to socket indices 20, 90, 200, verify all detected
    let (ctx, pairs, _addrs) = create_n_socket_pairs(256);

    // Send to sockets across all 3 batches
    let (push_20, _) = pairs[20];
    zmq_send(push_20, b"msg20".as_ptr().cast(), 5, 0);

    let (push_90, _) = pairs[90];
    zmq_send(push_90, b"msg90".as_ptr().cast(), 5, 0);

    let (push_200, _) = pairs[200];
    zmq_send(push_200, b"msg200".as_ptr().cast(), 7, 0);

    std::thread::sleep(Duration::from_millis(20));

    // Poll all 256 sockets
    let mut items = create_poll_items(&pairs);
    // In this test we know this is safe
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 100);

    // Should detect all three sockets
    assert!(rc >= 3, "Expected at least 3 ready sockets, got {rc}");
    assert_ne!(
        items[20].revents & ZMQ_POLLIN,
        0,
        "Socket 20 should be readable"
    );
    assert_ne!(
        items[90].revents & ZMQ_POLLIN,
        0,
        "Socket 90 should be readable"
    );
    assert_ne!(
        items[200].revents & ZMQ_POLLIN,
        0,
        "Socket 200 should be readable"
    );

    // Cleanup
    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
#[cfg(windows)]
fn poll_128_sockets_timeout() {
    // Test: 128 sockets with no messages, verify timeout honored
    let (ctx, pairs, _addrs) = create_n_socket_pairs(128);

    // Create poll items but don't send any messages
    let mut items = create_poll_items(&pairs);

    let start = std::time::Instant::now();
    // In this test we know this is safe
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 50);
    let elapsed = start.elapsed();

    // Should timeout and return 0
    assert_eq!(rc, 0, "Expected 0 ready sockets on timeout, got {rc}");
    // Verify timeout was actually honored (at least 40ms, allow some margin)
    assert!(elapsed.as_millis() >= 40, "Timeout too short: {elapsed:?}");

    // Cleanup
    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
#[cfg(windows)]
fn poll_128_sockets_fairness() {
    // Test: 128 sockets with sends scattered across all batches
    // Verify all sends are detected in a single poll
    let (ctx, pairs, _addrs) = create_n_socket_pairs(128);

    // Send to specific indices to test fairness across batches
    let target_indices = vec![5, 32, 50, 80, 95, 110, 125];

    for &idx in &target_indices {
        let (push, _) = pairs[idx];
        let msg = format!("msg{idx}").into_bytes();
        zmq_send(push, msg.as_ptr().cast(), msg.len(), 0);
    }

    std::thread::sleep(Duration::from_millis(20));

    // Poll all 128 sockets
    let mut items = create_poll_items(&pairs);
    // In this test we know this is safe
    #[allow(clippy::cast_possible_wrap)]
    let rc = zmq_poll(items.as_mut_ptr().cast(), items.len() as i32, 100);

    // Should detect all sent sockets
    assert_eq!(
        rc as usize,
        target_indices.len(),
        "Expected {} ready sockets, got {}",
        target_indices.len(),
        rc
    );

    // Verify each target is detected
    for &idx in &target_indices {
        assert_ne!(
            items[idx].revents & ZMQ_POLLIN,
            0,
            "Socket {idx} should be readable"
        );
    }

    // Cleanup
    for (push, pull) in pairs {
        zmq_close(push);
        zmq_close(pull);
    }
    zmq_ctx_term(ctx);
}

#[test]
#[serial]
fn poll_both_events_counts_as_one_item() {
    const ZMQ_LAST_ENDPOINT: i32 = 32;

    let ctx = zmq_ctx_new();
    let a = zmq_socket(ctx, ZMQ_PAIR);
    let b = zmq_socket(ctx, ZMQ_PAIR);
    assert!(!a.is_null(), "failed to create pair socket a");
    assert!(!b.is_null(), "failed to create pair socket b");
    set_timeo(a, 1000);
    set_timeo(b, 1000);

    let addr = CString::new("tcp://127.0.0.1:*").unwrap();
    assert_eq!(zmq_bind(a, addr.as_ptr()), 0);

    let mut ep_buf = [0u8; 256];
    let mut ep_sz = ep_buf.len();
    assert_eq!(
        omq_zmq::zmq_getsockopt(a, ZMQ_LAST_ENDPOINT, ep_buf.as_mut_ptr().cast(), &mut ep_sz),
        0
    );
    assert!(ep_sz > 0, "ZMQ_LAST_ENDPOINT returned empty endpoint");
    let ep_end = if ep_sz > 0 && ep_buf[ep_sz - 1] == 0 {
        ep_sz - 1
    } else {
        ep_sz
    };
    let ep = CString::new(&ep_buf[..ep_end]).unwrap();
    assert_eq!(zmq_connect(b, ep.as_ptr()), 0);

    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(zmq_send(b, b"hi".as_ptr().cast(), 2, 0), 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut items = [PollItem {
        socket: a,
        fd: -1,
        events: ZMQ_POLLIN | ZMQ_POLLOUT,
        revents: 0,
    }];

    let rc = zmq_poll(items.as_mut_ptr().cast(), 1, 1000);
    assert_ne!(items[0].revents & ZMQ_POLLIN, 0, "should be readable");
    assert_ne!(items[0].revents & ZMQ_POLLOUT, 0, "should be writable");
    assert_eq!(rc, 1, "ready_count should count items, not event types");

    let mut buf = [0u8; 16];
    zmq_recv(a, buf.as_mut_ptr().cast(), buf.len(), 0);

    zmq_close(a);
    zmq_close(b);
    zmq_ctx_term(ctx);
}
