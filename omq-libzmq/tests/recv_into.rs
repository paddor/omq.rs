//! `omq_recv_into`: `zmq_recv` without truncation. Frames that fit land in
//! the caller buffer; larger ones go whole into the overflow `zmq_msg_t`.
//! The frame info carries the more flag, routing ID, and group.
#![allow(clippy::borrow_as_ptr, clippy::ref_as_ptr)]

mod helpers;

use std::ffi::{CStr, CString, c_void};
use std::mem::size_of;
use std::time::Duration;

use omq_zmq::{
    OmqFrameInfo, omq_recv_into, zmq_bind, zmq_close, zmq_connect, zmq_ctx_new, zmq_ctx_term,
    zmq_errno, zmq_getsockopt, zmq_join, zmq_msg_close, zmq_msg_data, zmq_msg_group, zmq_msg_init,
    zmq_msg_init_buffer, zmq_msg_recv, zmq_msg_routing_id, zmq_msg_send, zmq_msg_set_group,
    zmq_msg_set_routing_id, zmq_msg_size, zmq_recv, zmq_send, zmq_socket,
};

const ZMQ_PUSH: i32 = 8;
const ZMQ_PULL: i32 = 7;
const ZMQ_SERVER: i32 = 12;
const ZMQ_CLIENT: i32 = 13;
const ZMQ_RADIO: i32 = 14;
const ZMQ_DISH: i32 = 15;
const ZMQ_SNDMORE: i32 = 2;
const ZMQ_DONTWAIT: i32 = 1;
const ZMQ_RCVMORE: i32 = 13;

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

/// Caller-owned `zmq_msg_t` storage, closed on drop.
#[repr(C)]
struct Msg([usize; ZMQ_MSG_WORDS]);

impl Msg {
    fn new() -> Self {
        let mut msg = Self([0; ZMQ_MSG_WORDS]);
        assert_eq!(zmq_msg_init(msg.ptr()), 0);
        msg
    }

    fn ptr(&mut self) -> *mut omq_zmq::OmqMsgRepr {
        self.0.as_mut_ptr().cast()
    }

    fn bytes(&mut self) -> Vec<u8> {
        let size = zmq_msg_size(self.ptr());
        let data = zmq_msg_data(self.ptr()).cast::<u8>();
        // SAFETY: data points to size readable bytes of the message.
        unsafe { std::slice::from_raw_parts(data, size) }.to_vec()
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        zmq_msg_close(self.ptr());
    }
}

/// A connected pair on inproc with 2 s receive timeouts.
struct Pair {
    ctx: *mut c_void,
    recv: *mut c_void,
    send: *mut c_void,
}

impl Pair {
    fn new(recv_type: i32, send_type: i32, name: &str) -> Self {
        let ctx = zmq_ctx_new();
        let recv = zmq_socket(ctx, recv_type);
        let send = zmq_socket(ctx, send_type);
        let endpoint = CString::new(format!("inproc://recv-into-{name}")).unwrap();
        assert_eq!(zmq_bind(recv, endpoint.as_ptr()), 0);
        assert_eq!(zmq_connect(send, endpoint.as_ptr()), 0);
        helpers::set_timeouts(recv, 2000);
        helpers::set_timeouts(send, 2000);
        Self { ctx, recv, send }
    }

    fn send(&self, data: &[u8], flags: i32) {
        let rc = zmq_send(self.send, data.as_ptr().cast(), data.len(), flags);
        assert_eq!(
            rc,
            i32::try_from(data.len()).unwrap(),
            "errno {}",
            zmq_errno()
        );
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        zmq_close(self.send);
        zmq_close(self.recv);
        zmq_ctx_term(self.ctx);
    }
}

/// One `omq_recv_into` call with a `capacity`-byte buffer filled with 0xAA.
fn recv_into(
    sock: *mut c_void,
    capacity: usize,
    overflow: &mut Msg,
) -> (i32, Vec<u8>, OmqFrameInfo) {
    let mut buf = vec![0xAA; capacity];
    let mut info = OmqFrameInfo::default();
    let rc = omq_recv_into(
        sock,
        buf.as_mut_ptr().cast(),
        capacity,
        overflow.ptr(),
        0,
        &mut info,
    );
    assert!(rc >= 0, "omq_recv_into failed: errno {}", zmq_errno());
    (rc, buf, info)
}

fn group(info: &OmqFrameInfo) -> &CStr {
    CStr::from_bytes_until_nul(group_bytes(info)).unwrap()
}

fn group_bytes(info: &OmqFrameInfo) -> &[u8] {
    // SAFETY: c_char and u8 have the same size and alignment.
    unsafe { std::slice::from_raw_parts(info.group.as_ptr().cast::<u8>(), info.group.len()) }
}

fn rcvmore(sock: *mut c_void) -> bool {
    let mut value = 0i32;
    let mut size = size_of::<i32>();
    assert_eq!(
        zmq_getsockopt(
            sock,
            ZMQ_RCVMORE,
            (&mut value as *mut i32).cast(),
            &mut size
        ),
        0
    );
    value != 0
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

#[test]
fn a_frame_that_fits_is_copied_into_the_buffer() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "fits");
    pair.send(b"hello", 0);
    let mut overflow = Msg::new();

    let (rc, buf, info) = recv_into(pair.recv, 64, &mut overflow);

    assert_eq!(rc, 5);
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(buf[5], 0xAA);
    assert_eq!(info, OmqFrameInfo::default());
    assert_eq!(zmq_msg_size(overflow.ptr()), 0);
}

#[test]
fn a_larger_frame_goes_whole_to_the_overflow_message() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "overflow");
    // Below and above the 128-byte inline-copy threshold of zmq_msg_recv.
    for len in [100, 100_000] {
        let payload = pattern(len);
        pair.send(&payload, 0);
        let mut overflow = Msg::new();

        let (rc, buf, info) = recv_into(pair.recv, 16, &mut overflow);

        assert_eq!(usize::try_from(rc).unwrap(), len);
        assert!(
            buf.iter().all(|&byte| byte == 0xAA),
            "buffer must be untouched"
        );
        assert_eq!(info.more, 0);
        assert_eq!(overflow.bytes(), payload);
    }
}

#[test]
fn an_exact_fit_and_an_empty_frame_use_the_buffer() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "edges");
    pair.send(b"12345678", 0);
    pair.send(b"", 0);
    let mut overflow = Msg::new();

    let (rc, buf, _) = recv_into(pair.recv, 8, &mut overflow);
    assert_eq!(rc, 8);
    assert_eq!(&buf, b"12345678");

    let mut info = OmqFrameInfo::default();
    let rc = omq_recv_into(
        pair.recv,
        std::ptr::null_mut(),
        0,
        overflow.ptr(),
        0,
        &mut info,
    );
    assert_eq!(rc, 0);
    assert_eq!(zmq_msg_size(overflow.ptr()), 0);
}

#[test]
fn multipart_frames_report_more_and_may_mix_buffer_and_overflow() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "multipart");
    let big = pattern(500);
    pair.send(b"abc", ZMQ_SNDMORE);
    pair.send(&big, ZMQ_SNDMORE);
    pair.send(b"z", 0);
    let mut overflow = Msg::new();

    let (rc, buf, info) = recv_into(pair.recv, 64, &mut overflow);
    assert_eq!((rc, &buf[..3], info.more), (3, &b"abc"[..], 1));
    assert!(rcvmore(pair.recv));

    let (rc, _, info) = recv_into(pair.recv, 64, &mut overflow);
    assert_eq!((rc, info.more), (500, 1));
    assert_eq!(overflow.bytes(), big);
    assert!(rcvmore(pair.recv));

    let (rc, buf, info) = recv_into(pair.recv, 64, &mut overflow);
    assert_eq!((rc, &buf[..1], info.more), (1, &b"z"[..], 0));
    assert!(!rcvmore(pair.recv));
}

#[test]
fn frames_interleave_with_zmq_recv_and_zmq_msg_recv() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "interleave");
    pair.send(b"one", ZMQ_SNDMORE);
    pair.send(b"two", ZMQ_SNDMORE);
    pair.send(b"three", 0);
    let mut msg = Msg::new();

    assert_eq!(zmq_msg_recv(msg.ptr(), pair.recv, 0), 3);
    // The rest of the message comes from the multipart drain.
    let (rc, buf, info) = recv_into(pair.recv, 64, &mut msg);
    assert_eq!((rc, &buf[..3], info.more), (3, &b"two"[..], 1));
    let mut last = [0u8; 8];
    assert_eq!(
        zmq_recv(pair.recv, last.as_mut_ptr().cast(), last.len(), 0),
        5
    );
    assert_eq!(&last[..5], b"three");
}

#[test]
fn server_frames_report_the_routing_id() {
    let pair = Pair::new(ZMQ_SERVER, ZMQ_CLIENT, "server");
    pair.send(b"small", 0);
    pair.send(&pattern(1000), 0);
    let mut overflow = Msg::new();

    let (rc, buf, small) = recv_into(pair.recv, 64, &mut overflow);
    assert_eq!((rc, &buf[..5]), (5, &b"small"[..]));
    assert_ne!(small.routing_id, 0);

    let (rc, _, large) = recv_into(pair.recv, 64, &mut overflow);
    assert_eq!(rc, 1000);
    assert_eq!(large.routing_id, small.routing_id);
    assert_eq!(zmq_msg_routing_id(overflow.ptr()), small.routing_id);

    // The reported ID routes a reply.
    let mut reply = Msg::new();
    assert_eq!(
        zmq_msg_init_buffer(reply.ptr(), b"back".as_ptr().cast(), 4),
        0
    );
    assert_eq!(zmq_msg_set_routing_id(reply.ptr(), small.routing_id), 0);
    assert_eq!(zmq_msg_send(reply.ptr(), pair.recv, 0), 4);
    let mut back = [0u8; 8];
    assert_eq!(
        zmq_recv(pair.send, back.as_mut_ptr().cast(), back.len(), 0),
        4
    );
}

#[test]
fn dish_frames_report_their_group() {
    let ctx = zmq_ctx_new();
    let radio = zmq_socket(ctx, ZMQ_RADIO);
    let dish = zmq_socket(ctx, ZMQ_DISH);
    let endpoint = c"inproc://recv-into-dish";
    assert_eq!(zmq_bind(radio, endpoint.as_ptr()), 0);
    assert_eq!(zmq_join(dish, c"weather".as_ptr()), 0);
    assert_eq!(zmq_connect(dish, endpoint.as_ptr()), 0);
    std::thread::sleep(Duration::from_millis(50));
    helpers::set_timeouts(dish, 2000);

    let payload = pattern(300);
    for data in [&b"sun"[..], &payload] {
        let mut msg = Msg::new();
        assert_eq!(
            zmq_msg_init_buffer(msg.ptr(), data.as_ptr().cast(), data.len()),
            0
        );
        assert_eq!(zmq_msg_set_group(msg.ptr(), c"weather".as_ptr()), 0);
        assert_eq!(
            zmq_msg_send(msg.ptr(), radio, 0),
            i32::try_from(data.len()).unwrap()
        );
    }

    let mut overflow = Msg::new();
    let (rc, buf, info) = recv_into(dish, 16, &mut overflow);
    assert_eq!((rc, &buf[..3]), (3, &b"sun"[..]));
    assert_eq!(group(&info), c"weather");

    let (rc, _, info) = recv_into(dish, 16, &mut overflow);
    assert_eq!(group(&info), c"weather");
    assert_eq!((rc, info.more), (300, 0));
    assert_eq!(overflow.bytes(), payload);
    // SAFETY: zmq_msg_group returns a NUL-terminated string owned by the message.
    let group = unsafe { CStr::from_ptr(zmq_msg_group(overflow.ptr())) };
    assert_eq!(group, c"weather");

    zmq_close(dish);
    zmq_close(radio);
    zmq_ctx_term(ctx);
}

#[test]
fn would_block_and_bad_arguments_set_errno() {
    let pair = Pair::new(ZMQ_PULL, ZMQ_PUSH, "errors");
    let mut overflow = Msg::new();
    let mut buf = [0u8; 8];
    let null = std::ptr::null_mut::<c_void>();
    let no_info = std::ptr::null_mut::<OmqFrameInfo>();

    let rc = omq_recv_into(
        pair.recv,
        buf.as_mut_ptr().cast(),
        8,
        overflow.ptr(),
        ZMQ_DONTWAIT,
        no_info,
    );
    assert_eq!((rc, zmq_errno()), (-1, libc::EAGAIN));

    let rc = omq_recv_into(null, buf.as_mut_ptr().cast(), 8, overflow.ptr(), 0, no_info);
    assert_eq!((rc, zmq_errno()), (-1, libc::EFAULT));
    let rc = omq_recv_into(
        pair.recv,
        buf.as_mut_ptr().cast(),
        8,
        std::ptr::null_mut(),
        0,
        no_info,
    );
    assert_eq!((rc, zmq_errno()), (-1, libc::EFAULT));
    let rc = omq_recv_into(pair.recv, null, 8, overflow.ptr(), 0, no_info);
    assert_eq!((rc, zmq_errno()), (-1, libc::EFAULT));

    // info is optional.
    pair.send(b"ok", 0);
    let rc = omq_recv_into(
        pair.recv,
        buf.as_mut_ptr().cast(),
        8,
        overflow.ptr(),
        0,
        no_info,
    );
    assert_eq!(rc, 2);
}
