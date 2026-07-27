#![allow(dead_code)]

use std::ffi::{CStr, CString, c_void};

use omq_zmq::{zmq_bind, zmq_getsockopt};

const ZMQ_LAST_ENDPOINT: i32 = 32;

pub(crate) fn bind_random_tcp(sock: *mut c_void) -> CString {
    bind_random(sock, "tcp://127.0.0.1:0")
}

pub(crate) fn bind_random_lz4_tcp(sock: *mut c_void) -> CString {
    bind_random(sock, "lz4+tcp://127.0.0.1:0")
}

fn bind_random(sock: *mut c_void, endpoint: &str) -> CString {
    let bind_addr = CString::new(endpoint).unwrap();
    assert_eq!(
        zmq_bind(sock, bind_addr.as_ptr()),
        0,
        "bind {endpoint} failed, errno={}",
        omq_zmq::zmq_errno(),
    );
    last_endpoint(sock)
}

pub(crate) fn last_endpoint(sock: *mut c_void) -> CString {
    let mut buf = [0u8; 256];
    let mut len = buf.len();
    assert_eq!(
        zmq_getsockopt(
            sock,
            ZMQ_LAST_ENDPOINT,
            buf.as_mut_ptr().cast(),
            &raw mut len,
        ),
        0,
        "ZMQ_LAST_ENDPOINT failed, errno={}",
        omq_zmq::zmq_errno(),
    );
    CStr::from_bytes_until_nul(&buf[..len]).unwrap().to_owned()
}
