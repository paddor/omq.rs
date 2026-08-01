//! libzmq parity tests for null socket handles.

use std::ffi::c_void;
use std::mem::size_of;

use omq_zmq::{
    zmq_bind, zmq_close, zmq_connect, zmq_disconnect, zmq_getsockopt, zmq_join, zmq_leave,
    zmq_setsockopt, zmq_socket, zmq_socket_monitor, zmq_unbind,
};

const ZMQ_PAIR: i32 = 0;
const ZMQ_SNDHWM: i32 = 23;
const ZMQ_EVENT_ALL: i32 = 0xFFFF;
const ZMQ_ENOTSOCK: i32 = 156_384_721;

#[test]
fn socket_null_context_returns_efault() {
    assert!(zmq_socket(std::ptr::null_mut::<c_void>(), ZMQ_PAIR).is_null());
    assert_eq!(omq_zmq::zmq_errno(), libc::EFAULT);
}

#[test]
fn null_socket_ops_return_enotsock() {
    let sock = std::ptr::null_mut::<c_void>();
    let endpoint = c"inproc://socket-null";
    let group = c"group";
    let hwm = 100i32;
    let mut out = 0i32;
    let mut out_size = size_of::<i32>();

    assert_eq!(zmq_close(sock), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    assert_eq!(
        zmq_setsockopt(
            sock,
            ZMQ_SNDHWM,
            (&hwm as *const i32).cast(),
            size_of::<i32>(),
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    assert_eq!(
        zmq_getsockopt(
            sock,
            ZMQ_SNDHWM,
            (&mut out as *mut i32).cast(),
            &mut out_size,
        ),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    assert_eq!(
        zmq_socket_monitor(sock, endpoint.as_ptr(), ZMQ_EVENT_ALL),
        -1
    );
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);

    assert_eq!(zmq_bind(sock, endpoint.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
    assert_eq!(zmq_connect(sock, endpoint.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
    assert_eq!(zmq_unbind(sock, endpoint.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
    assert_eq!(zmq_disconnect(sock, endpoint.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
    assert_eq!(zmq_join(sock, group.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
    assert_eq!(zmq_leave(sock, group.as_ptr()), -1);
    assert_eq!(omq_zmq::zmq_errno(), ZMQ_ENOTSOCK);
}
