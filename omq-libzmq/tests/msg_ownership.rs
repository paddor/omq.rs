//! `zmq_msg_t` ownership and callback lifetime tests.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::c_void;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_zmq::{
    zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term, zmq_msg_close, zmq_msg_copy, zmq_msg_data,
    zmq_msg_init, zmq_msg_init_data, zmq_msg_move, zmq_msg_recv, zmq_msg_send, zmq_msg_size,
    zmq_recv, zmq_send, zmq_setsockopt, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_RCVTIMEO: i32 = 27;
const ZMQ_SNDTIMEO: i32 = 28;
const ZMQ_LINGER: i32 = 17;

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

#[repr(C)]
#[derive(Clone, Copy)]
struct ZmqMsg([usize; ZMQ_MSG_WORDS]);

impl ZmqMsg {
    fn zeroed() -> Self {
        Self([0; ZMQ_MSG_WORDS])
    }

    fn new() -> Self {
        let mut msg = Self::zeroed();
        assert_eq!(zmq_msg_init(msg.0.as_mut_ptr().cast()), 0);
        msg
    }
}

unsafe extern "C" fn count_free(_data: *mut c_void, hint: *mut c_void) {
    let counter = unsafe { &*hint.cast::<AtomicUsize>() };
    counter.fetch_add(1, Ordering::SeqCst);
}

fn set_i32(sock: *mut c_void, opt: i32, val: i32) {
    assert_eq!(
        zmq_setsockopt(sock, opt, (&val as *const i32).cast(), size_of::<i32>()),
        0
    );
}

fn init_external<'a>(
    msg: &mut ZmqMsg,
    payload: &'a mut [u8],
    counter: &'a AtomicUsize,
) -> *mut c_void {
    let ptr = payload.as_mut_ptr().cast();
    assert_eq!(
        zmq_msg_init_data(
            msg.0.as_mut_ptr().cast(),
            ptr,
            payload.len(),
            Some(count_free),
            std::ptr::from_ref(counter).cast::<c_void>().cast_mut(),
        ),
        0
    );
    ptr
}

#[test]
fn external_free_callback_runs_once_on_double_close() {
    let counter = AtomicUsize::new(0);
    let mut payload = *b"external";
    let mut msg = ZmqMsg::zeroed();
    init_external(&mut msg, &mut payload, &counter);

    assert_eq!(zmq_msg_close(msg.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(zmq_msg_close(msg.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn copying_external_message_does_not_take_external_ownership() {
    let counter = AtomicUsize::new(0);
    let mut payload = *b"copy-external";
    let mut src = ZmqMsg::zeroed();
    init_external(&mut src, &mut payload, &counter);

    let mut dst = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(dst.0.as_mut_ptr().cast(), src.0.as_ptr().cast()),
        0
    );
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), payload.len());
    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), payload.len());

    assert_eq!(zmq_msg_close(dst.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(zmq_msg_close(src.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn moving_external_message_closes_destination_and_transfers_ownership() {
    let old_counter = AtomicUsize::new(0);
    let new_counter = AtomicUsize::new(0);
    let mut old_payload = *b"old-dst";
    let mut new_payload = *b"new-src";

    let mut dst = ZmqMsg::zeroed();
    let mut src = ZmqMsg::zeroed();
    init_external(&mut dst, &mut old_payload, &old_counter);
    init_external(&mut src, &mut new_payload, &new_counter);

    assert_eq!(
        zmq_msg_move(dst.0.as_mut_ptr().cast(), src.0.as_mut_ptr().cast()),
        0
    );
    assert_eq!(old_counter.load(Ordering::SeqCst), 1);
    assert_eq!(new_counter.load(Ordering::SeqCst), 0);
    assert_eq!(zmq_msg_size(src.0.as_ptr().cast()), 0);
    assert_eq!(zmq_msg_size(dst.0.as_ptr().cast()), new_payload.len());

    assert_eq!(zmq_msg_close(src.0.as_mut_ptr().cast()), 0);
    assert_eq!(new_counter.load(Ordering::SeqCst), 0);
    assert_eq!(zmq_msg_close(dst.0.as_mut_ptr().cast()), 0);
    assert_eq!(new_counter.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_msg_send_preserves_external_message_until_close() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    set_i32(push, ZMQ_LINGER, 0);
    set_i32(push, ZMQ_SNDTIMEO, 0);
    let _endpoint = helpers::bind_random_tcp(push);

    let counter = AtomicUsize::new(0);
    let mut payload = *b"retry-after-eagain";
    let mut msg = ZmqMsg::zeroed();
    let data_ptr = init_external(&mut msg, &mut payload, &counter);

    assert_eq!(
        zmq_msg_send(msg.0.as_mut_ptr().cast(), push, ZMQ_DONTWAIT),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(zmq_msg_size(msg.0.as_ptr().cast()), payload.len());
    assert_eq!(zmq_msg_data(msg.0.as_mut_ptr().cast()), data_ptr);

    assert_eq!(zmq_msg_close(msg.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    zmq_close(push);
    zmq_ctx_term(ctx);
}

#[test]
fn msg_recv_closes_existing_destination_once_before_replacement() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = c"inproc://msg-recv-replaces-old";
    assert_eq!(omq_zmq::zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(20));
    set_i32(pull, ZMQ_RCVTIMEO, 1000);

    let counter = AtomicUsize::new(0);
    let mut old_payload = *b"old";
    let mut msg = ZmqMsg::zeroed();
    init_external(&mut msg, &mut old_payload, &counter);

    assert_eq!(zmq_send(push, b"new".as_ptr().cast(), 3, 0), 3);
    assert_eq!(zmq_msg_recv(msg.0.as_mut_ptr().cast(), pull, 0), 3);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(zmq_msg_size(msg.0.as_ptr().cast()), 3);

    assert_eq!(zmq_msg_close(msg.0.as_mut_ptr().cast()), 0);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

#[test]
fn copied_large_received_message_survives_original_close() {
    let ctx = zmq_ctx_new();
    let push = zmq_socket(ctx, ZMQ_PUSH);
    let pull = zmq_socket(ctx, ZMQ_PULL);
    let addr = c"inproc://msg-copy-large-recv";
    assert_eq!(omq_zmq::zmq_bind(pull, addr.as_ptr()), 0);
    assert_eq!(zmq_connect(push, addr.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(20));
    set_i32(pull, ZMQ_RCVTIMEO, 1000);

    let payload = vec![0xA5u8; 512];
    assert_eq!(
        zmq_send(push, payload.as_ptr().cast(), payload.len(), 0),
        i32::try_from(payload.len()).unwrap()
    );

    let mut original = ZmqMsg::new();
    assert_eq!(
        zmq_msg_recv(original.0.as_mut_ptr().cast(), pull, 0),
        i32::try_from(payload.len()).unwrap()
    );

    let mut copied = ZmqMsg::new();
    assert_eq!(
        zmq_msg_copy(copied.0.as_mut_ptr().cast(), original.0.as_ptr().cast()),
        0
    );
    assert_eq!(zmq_msg_close(original.0.as_mut_ptr().cast()), 0);

    let data = zmq_msg_data(copied.0.as_mut_ptr().cast());
    let got = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), payload.len()) };
    assert_eq!(got, payload.as_slice());

    assert_eq!(zmq_msg_close(copied.0.as_mut_ptr().cast()), 0);

    let mut raw = [0u8; 1];
    assert_eq!(
        zmq_recv(pull, raw.as_mut_ptr().cast(), raw.len(), ZMQ_DONTWAIT),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), libc::EAGAIN);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}
