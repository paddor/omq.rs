//! `ZMQ::SOCKOPT_FD` as a PHP stream, and fds of PHP streams for polling.

use std::ffi::{c_int, c_void};

use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::errors::{socket_error, socket_misuse};

const ZMQ_FD: c_int = 14;

unsafe extern "C" {
    fn omq_php_fd_to_stream(fd: c_int, out: *mut Zval) -> c_int;
    fn omq_php_stream_to_fd(value: *const Zval) -> c_int;
}

/// The socket's readiness fd, wrapped in a read-only PHP stream for event
/// loops. The stream owns a duplicate, so closing it leaves the socket intact.
pub(crate) fn socket_fd_stream(socket: *mut c_void) -> PhpResult<Zval> {
    let mut fd: c_int = -1;
    let mut len = size_of::<c_int>();
    if omq_zmq::zmq_getsockopt(socket, ZMQ_FD, (&raw mut fd).cast(), &raw mut len) != 0 {
        return Err(socket_error("Failed to get socket option"));
    }

    let mut stream = Zval::new();
    // SAFETY: `stream` is a valid zval for the shim to overwrite; `fd` is a
    // live descriptor the shim only duplicates.
    if unsafe { omq_php_fd_to_stream(fd, &raw mut stream) } != 0 {
        return Err(socket_misuse("Failed to wrap ZMQ::SOCKOPT_FD in a stream"));
    }

    Ok(stream)
}

/// The fd of a PHP stream resource, for `ZMQPoll`.
pub(crate) fn stream_fd(value: &Zval) -> Option<c_int> {
    // SAFETY: the shim only reads the zval and checks its type first.
    let fd = unsafe { omq_php_stream_to_fd(value) };
    (fd >= 0).then_some(fd)
}
