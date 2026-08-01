//! Multipart message tests using `zmq_msg_*` API.
//! Tests `SNDMORE` accumulation, `RCVMORE` drain, `zmq_msg_more` flag.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_getsockopt, zmq_msg_close,
    zmq_msg_copy, zmq_msg_data, zmq_msg_get, zmq_msg_gets, zmq_msg_group, zmq_msg_init,
    zmq_msg_init_buffer, zmq_msg_init_data, zmq_msg_init_size, zmq_msg_more, zmq_msg_move,
    zmq_msg_recv, zmq_msg_send, zmq_msg_set, zmq_msg_set_group, zmq_msg_size, zmq_recv, zmq_send,
    zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_RCVMORE: i32 = 13;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_MORE: i32 = 1;
const ZMQ_SHARED: i32 = 3;
const ZMQ_ROUTING_ID: i32 = 5;

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

// 64-byte opaque zmq_msg_t, aligned to pointer size.
#[repr(C)]
struct ZmqMsg([usize; ZMQ_MSG_WORDS]);

impl ZmqMsg {
    fn zeroed() -> Self {
        Self([0; ZMQ_MSG_WORDS])
    }

    fn new() -> Self {
        let mut m = Self::zeroed();
        zmq_msg_init(m.0.as_mut_ptr().cast());
        m
    }
}

fn set_timeo(sock: *mut c_void, opt: i32, ms: i32) {
    zmq_setsockopt(sock, opt, (&ms as *const i32).cast(), size_of::<i32>());
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut v: i32 = 0;
    let mut sz = size_of::<i32>();
    zmq_getsockopt(sock, ZMQ_RCVMORE, (&mut v as *mut i32).cast(), &mut sz);
    v != 0
}

/// `zmq_msg_init` / close lifecycle
#[test]
fn msg_init_close() {
    let mut m = ZmqMsg::new();
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_more(m.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(m.0.as_mut_ptr().cast()), 0);
}

/// `zmq_msg_init_size` allocates writable memory
#[test]
fn msg_init_size() {
    let mut m = ZmqMsg::zeroed();
    assert_eq!(zmq_msg_init_size(m.0.as_mut_ptr().cast(), 16), 0);
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 16);

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    assert!(!data.is_null());
    // Write through the pointer.
    unsafe { std::ptr::write_bytes(data.cast::<u8>(), 0xAB, 16) };

    zmq_msg_close(m.0.as_mut_ptr().cast());
}

#[test]
fn msg_init_size_zero_and_init_buffer_null_zero() {
    let mut sized = ZmqMsg::zeroed();
    assert_eq!(zmq_msg_init_size(sized.0.as_mut_ptr().cast(), 0), 0);
    assert_eq!(zmq_msg_size(sized.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(sized.0.as_mut_ptr().cast()), 0);

    let mut copied = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_buffer(copied.0.as_mut_ptr().cast(), std::ptr::null(), 0),
        0
    );
    assert_eq!(zmq_msg_size(copied.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(copied.0.as_mut_ptr().cast()), 0);
}

#[test]
fn msg_init_data_and_buffer_reject_null_nonzero_payload() {
    let mut m = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_data(
            m.0.as_mut_ptr().cast(),
            std::ptr::null_mut(),
            1,
            None,
            std::ptr::null_mut()
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    assert_eq!(
        zmq_msg_init_buffer(m.0.as_mut_ptr().cast(), std::ptr::null(), 1),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
}

#[test]
fn msg_move_and_copy_self_alias_are_noops() {
    let payload = b"alias";
    let mut m = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_buffer(
            m.0.as_mut_ptr().cast(),
            payload.as_ptr().cast(),
            payload.len()
        ),
        0
    );

    assert_eq!(
        zmq_msg_move(m.0.as_mut_ptr().cast(), m.0.as_mut_ptr().cast()),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), payload.len());
    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let slice = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), payload.len()) };
    assert_eq!(slice, payload);

    assert_eq!(
        zmq_msg_copy(m.0.as_mut_ptr().cast(), m.0.as_ptr().cast()),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), payload.len());
    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let slice = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), payload.len()) };
    assert_eq!(slice, payload);

    zmq_msg_close(m.0.as_mut_ptr().cast());
}

#[test]
fn msg_set_handles_supported_and_invalid_properties() {
    let mut m = ZmqMsg::new();

    assert_eq!(zmq_msg_set(m.0.as_mut_ptr().cast(), ZMQ_MORE, 1), 0);
    assert_eq!(zmq_msg_more(m.0.as_ptr().cast()), 1);

    assert_eq!(
        zmq_msg_set(m.0.as_mut_ptr().cast(), ZMQ_ROUTING_ID, 1234),
        0
    );
    assert_eq!(omq_zmq::zmq_msg_routing_id(m.0.as_ptr().cast()), 1234);

    assert_eq!(zmq_msg_set(m.0.as_mut_ptr().cast(), 9999, 1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(zmq_msg_set(std::ptr::null_mut(), ZMQ_MORE, 1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    zmq_msg_close(m.0.as_mut_ptr().cast());
}

/// `zmq_msg_init_buffer` copies the buffer
#[test]
fn msg_init_buffer() {
    let payload = b"hello buffer";
    let mut m = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_buffer(
            m.0.as_mut_ptr().cast(),
            payload.as_ptr().cast(),
            payload.len()
        ),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), payload.len());

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let slice = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), payload.len()) };
    assert_eq!(slice, payload);
    zmq_msg_close(m.0.as_mut_ptr().cast());
}

#[test]
fn msg_get_reports_shared_for_external_data_and_large_copy() {
    let mut external = ZmqMsg::zeroed();
    let mut payload = [0x42u8; 5];
    assert_eq!(
        zmq_msg_init_data(
            external.0.as_mut_ptr().cast(),
            payload.as_mut_ptr().cast(),
            payload.len(),
            None,
            std::ptr::null_mut(),
        ),
        0
    );
    assert_eq!(zmq_msg_get(external.0.as_ptr().cast(), ZMQ_SHARED), 1);

    let mut large = ZmqMsg::zeroed();
    assert_eq!(zmq_msg_init_size(large.0.as_mut_ptr().cast(), 1024), 0);
    assert_eq!(zmq_msg_get(large.0.as_ptr().cast(), ZMQ_SHARED), 0);

    let mut copied = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(copied.0.as_mut_ptr().cast(), large.0.as_ptr().cast()),
        0
    );
    assert_eq!(zmq_msg_get(copied.0.as_ptr().cast(), ZMQ_SHARED), 1);

    assert_eq!(zmq_msg_close(copied.0.as_mut_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(large.0.as_mut_ptr().cast()), 0);
    assert_eq!(zmq_msg_close(external.0.as_mut_ptr().cast()), 0);
}

#[test]
fn msg_get_and_gets_validate_properties() {
    let mut m = ZmqMsg::new();
    assert_eq!(zmq_msg_get(m.0.as_ptr().cast(), ZMQ_MORE), 0);
    assert_eq!(zmq_msg_get(m.0.as_ptr().cast(), ZMQ_SHARED), 0);
    assert_eq!(zmq_msg_get(m.0.as_ptr().cast(), ZMQ_ROUTING_ID), 0);

    assert_eq!(zmq_msg_get(m.0.as_ptr().cast(), 9999), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(zmq_msg_get(std::ptr::null(), ZMQ_MORE), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    for property in [
        c"Socket-Type".as_ptr(),
        c"Identity".as_ptr(),
        c"Routing-Id".as_ptr(),
        c"Peer-Address".as_ptr(),
    ] {
        let value = zmq_msg_gets(m.0.as_ptr().cast(), property);
        assert!(!value.is_null());
        assert_eq!(unsafe { std::ffi::CStr::from_ptr(value) }.to_bytes(), b"");
    }

    assert!(zmq_msg_gets(m.0.as_ptr().cast(), c"Unknown".as_ptr()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert!(zmq_msg_gets(std::ptr::null(), c"Socket-Type".as_ptr()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert!(zmq_msg_gets(m.0.as_ptr().cast(), std::ptr::null()).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);

    assert_eq!(zmq_msg_close(m.0.as_mut_ptr().cast()), 0);
}

#[test]
fn msg_routing_id_and_group_roundtrip() {
    let mut m = ZmqMsg::new();
    assert_eq!(omq_zmq::zmq_msg_routing_id(m.0.as_ptr().cast()), 0);
    assert_eq!(
        unsafe { std::ffi::CStr::from_ptr(zmq_msg_group(m.0.as_ptr().cast())) }.to_bytes(),
        b""
    );

    assert_eq!(
        omq_zmq::zmq_msg_set_routing_id(m.0.as_mut_ptr().cast(), 42),
        0
    );
    assert_eq!(omq_zmq::zmq_msg_routing_id(m.0.as_ptr().cast()), 42);

    assert_eq!(
        zmq_msg_set_group(m.0.as_mut_ptr().cast(), c"group-a".as_ptr()),
        0
    );
    let group = unsafe { std::ffi::CStr::from_ptr(zmq_msg_group(m.0.as_ptr().cast())) };
    assert_eq!(group.to_bytes(), b"group-a");

    assert_eq!(
        zmq_msg_set_group(m.0.as_mut_ptr().cast(), c"too-long-group".as_ptr()),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EINVAL);
    assert_eq!(omq_zmq::zmq_msg_set_routing_id(std::ptr::null_mut(), 1), -1);
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
    assert_eq!(
        zmq_msg_set_group(std::ptr::null_mut(), c"group".as_ptr()),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
    assert_eq!(
        zmq_msg_set_group(m.0.as_mut_ptr().cast(), std::ptr::null()),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);

    assert_eq!(zmq_msg_close(m.0.as_mut_ptr().cast()), 0);
}

/// `zmq_msg_init_data` with `free_fn`
#[test]
fn msg_init_data_with_free_fn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static FREED: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" fn my_free(_data: *mut c_void, _hint: *mut c_void) {
        FREED.store(true, Ordering::SeqCst);
    }

    // Allocate a heap buffer to pass.
    let buf = unsafe { libc::malloc(8) };
    assert!(!buf.is_null());
    unsafe { std::ptr::write_bytes(buf.cast::<u8>(), 0x55, 8) };

    let mut m = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_data(
            m.0.as_mut_ptr().cast(),
            buf,
            8,
            Some(my_free),
            std::ptr::null_mut(),
        ),
        0
    );
    assert_eq!(zmq_msg_size(m.0.as_ptr().cast()), 8);

    zmq_msg_close(m.0.as_mut_ptr().cast());
    assert!(FREED.load(Ordering::SeqCst), "free_fn was not called");
}

#[test]
fn msg_init_data_free_fn_runs_after_successful_send() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    unsafe extern "C" fn count_free(_data: *mut c_void, hint: *mut c_void) {
        let counter = unsafe { &*hint.cast::<AtomicUsize>() };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = CString::new("inproc://test-msg-free-on-send").unwrap();
    assert_eq!(zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    let freed = AtomicUsize::new(0);
    let mut payload = *b"external-data";
    let mut m = ZmqMsg::zeroed();
    assert_eq!(
        zmq_msg_init_data(
            m.0.as_mut_ptr().cast(),
            payload.as_mut_ptr().cast(),
            payload.len(),
            Some(count_free),
            (&freed as *const AtomicUsize).cast_mut().cast(),
        ),
        0
    );

    assert_eq!(
        zmq_msg_send(m.0.as_mut_ptr().cast(), push, 0),
        i32::try_from(payload.len()).unwrap()
    );
    assert_eq!(freed.load(Ordering::SeqCst), 1);

    let mut buf = [0u8; 32];
    assert_eq!(
        zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0),
        i32::try_from(payload.len()).unwrap()
    );
    assert_eq!(&buf[..payload.len()], &payload);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// `zmq_msg_move` transfers ownership; src becomes empty
#[test]
fn msg_move() {
    let mut src = ZmqMsg::zeroed();
    zmq_msg_init_buffer(src.0.as_mut_ptr().cast(), b"move-me".as_ptr().cast(), 7);

    let mut dst = ZmqMsg::new();
    assert_eq!(
        zmq_msg_move(dst.0.as_mut_ptr().cast(), src.0.as_mut_ptr().cast()),
        0
    );

    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), 7);
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), 0);

    zmq_msg_close(dst.0.as_mut_ptr().cast());
    zmq_msg_close(src.0.as_mut_ptr().cast());
}

/// `zmq_msg_copy` makes an independent copy
#[test]
fn msg_copy() {
    let mut src = ZmqMsg::zeroed();
    zmq_msg_init_buffer(src.0.as_mut_ptr().cast(), b"copy-me".as_ptr().cast(), 7);

    let mut dst = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(dst.0.as_mut_ptr().cast(), src.0.as_ptr().cast()),
        0
    );

    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), 7);
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), 7);

    zmq_msg_close(dst.0.as_mut_ptr().cast());
    zmq_msg_close(src.0.as_mut_ptr().cast());
}

/// `zmq_msg_send` / `zmq_msg_recv` roundtrip
#[test]
fn msg_send_recv_roundtrip() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-rtt").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    let payload = b"msg api roundtrip";
    let mut out_m = ZmqMsg::zeroed();
    zmq_msg_init_buffer(
        out_m.0.as_mut_ptr().cast(),
        payload.as_ptr().cast(),
        payload.len(),
    );
    let rc = zmq_msg_send(out_m.0.as_mut_ptr().cast(), push, 0);
    assert_eq!(rc as usize, payload.len());

    let mut in_m = ZmqMsg::new();
    let rc = zmq_msg_recv(in_m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc as usize, payload.len());
    assert_eq!(zmq_msg_more(in_m.0.as_ptr().cast()), 0);

    let data = zmq_msg_data(in_m.0.as_mut_ptr().cast());
    let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), rc as usize) };
    assert_eq!(got, payload);

    zmq_msg_close(in_m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Multipart with `zmq_msg_more` flag set correctly
#[test]
fn msg_more_flag_in_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-more").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    // Send 3-part message.
    zmq_send(push, b"A".as_ptr().cast(), 1, ZMQ_SNDMORE);
    zmq_send(push, b"B".as_ptr().cast(), 1, ZMQ_SNDMORE);
    zmq_send(push, b"C".as_ptr().cast(), 1, 0);

    let mut m = ZmqMsg::new();

    // Frame 1: more=1
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        1,
        "frame 1 should have more=1"
    );
    assert!(rcvmore(pull), "RCVMORE after frame 1");

    // Frame 2: more=1
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        1,
        "frame 2 should have more=1"
    );

    // Frame 3: more=0
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 1);
    assert_eq!(
        zmq_msg_more(m.0.as_ptr().cast()),
        0,
        "frame 3 should have more=0"
    );
    assert!(!rcvmore(pull), "RCVMORE should be clear after last frame");

    zmq_msg_close(m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// Mixed `zmq_msg_send` and `zmq_recv`
#[test]
fn msg_send_then_raw_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-mixed").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    let mut m = ZmqMsg::zeroed();
    zmq_msg_init_buffer(m.0.as_mut_ptr().cast(), b"raw recv".as_ptr().cast(), 8);
    zmq_msg_send(m.0.as_mut_ptr().cast(), push, 0);

    let mut buf = [0u8; 32];
    let rc = zmq_recv(pull, buf.as_mut_ptr().cast(), buf.len(), 0);
    assert_eq!(rc, 8);
    assert_eq!(&buf[..8], b"raw recv");

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

/// `KIND_BYTES` arc is stolen on `zmq_msg_send` — no copy, no double-free.
///
/// Pattern: send via raw API -> recv as `KIND_BYTES` -> forward via `zmq_msg_send`.
/// After a successful send the msg is zeroed. Any double-free of the arc
/// would crash immediately.
#[test]
fn kind_bytes_arc_stolen_on_zmq_msg_send() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let fwd_push = zmq_socket(ctx, ZMQ_PUSH);
    let fwd_pull = zmq_socket(ctx, ZMQ_PULL);

    let addr1 = CString::new("inproc://zero-copy-src").unwrap();
    let addr2 = CString::new("inproc://zero-copy-dst").unwrap();
    zmq_bind(pull, addr1.as_ptr());
    zmq_connect(push, addr1.as_ptr());
    zmq_bind(fwd_pull, addr2.as_ptr());
    zmq_connect(fwd_push, addr2.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);
    set_timeo(fwd_pull, ZMQ_RCVTIMEO, 1000);

    let payload = b"zero-copy-kind-bytes";

    for _ in 0..50 {
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0);

        // recv -> KIND_BYTES (boxed = Box<Bytes>, non-null)
        let mut msg = ZmqMsg::new();
        let rc = zmq_msg_recv(msg.0.as_mut_ptr().cast(), pull, 0);
        assert_eq!(rc as usize, payload.len());

        // Forward via zmq_msg_send. Must not double-free regardless of
        // internal kind (KIND_HEAP for small, KIND_BYTES for large).
        let rc = zmq_msg_send(msg.0.as_mut_ptr().cast(), fwd_push, 0);
        assert_eq!(rc as usize, payload.len());

        // zmq_msg_send zeros the msg on success.
        assert_eq!(
            msg.0,
            ZmqMsg::zeroed().0,
            "msg must be zeroed after zmq_msg_send"
        );

        // Verify forwarded data arrived intact.
        let mut out = ZmqMsg::new();
        let rc = zmq_msg_recv(out.0.as_mut_ptr().cast(), fwd_pull, 0);
        assert_eq!(rc as usize, payload.len());
        let data = zmq_msg_data(out.0.as_mut_ptr().cast());
        let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), rc as usize) };
        assert_eq!(got, payload);
        zmq_msg_close(out.0.as_mut_ptr().cast());
    }

    zmq_close(push);
    zmq_close(pull);
    zmq_close(fwd_push);
    zmq_close(fwd_pull);
    zmq_ctx_term(ctx);
}

/// Mixed `zmq_send` and `zmq_msg_recv`
#[test]
fn raw_send_then_msg_recv() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);

    let addr = CString::new("inproc://test-msg-mixed2").unwrap();
    zmq_bind(pull, addr.as_ptr());
    zmq_connect(push, addr.as_ptr());
    std::thread::sleep(Duration::from_millis(20));
    set_timeo(pull, ZMQ_RCVTIMEO, 1000);

    zmq_send(push, b"msg recv".as_ptr().cast(), 8, 0);

    let mut m = ZmqMsg::new();
    let rc = zmq_msg_recv(m.0.as_mut_ptr().cast(), pull, 0);
    assert_eq!(rc, 8);

    let data = zmq_msg_data(m.0.as_mut_ptr().cast());
    let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), 8) };
    assert_eq!(got, b"msg recv");

    zmq_msg_close(m.0.as_mut_ptr().cast());
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
