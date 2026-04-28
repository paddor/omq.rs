//! Context: lightweight Socket factory. Most of the libzmq Context
//! semantics (term, IO threads) don't apply to our model; we keep the
//! type to satisfy `ctx = zmq.Context(); sock = ctx.socket(zmq.PUSH)`.

use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::constants;
use crate::error::map_err;
use crate::socket::Socket;
use crate::socket_async::AsyncSocket;

fn map_socket_type(st: i32) -> PyResult<omq_compio::SocketType> {
    Ok(match st {
        constants::PAIR => omq_compio::SocketType::Pair,
        constants::PUB => omq_compio::SocketType::Pub,
        constants::SUB => omq_compio::SocketType::Sub,
        constants::REQ => omq_compio::SocketType::Req,
        constants::REP => omq_compio::SocketType::Rep,
        constants::DEALER => omq_compio::SocketType::Dealer,
        constants::ROUTER => omq_compio::SocketType::Router,
        constants::PULL => omq_compio::SocketType::Pull,
        constants::PUSH => omq_compio::SocketType::Push,
        constants::XPUB => omq_compio::SocketType::XPub,
        constants::XSUB => omq_compio::SocketType::XSub,
        other => {
            return Err(map_err(omq_proto::error::Error::InvalidEndpoint(format!(
                "unknown socket type {other}"
            ))));
        }
    })
}

#[pyclass(module = "pyomq._native")]
pub struct Context;

#[pymethods]
impl Context {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        let _ = io_threads; // libzmq legacy; unused on our runtime model
        Context
    }

    /// Construct a new socket of the given libzmq type code.
    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<Socket> {
        let _ = py;
        Ok(Socket::new(map_socket_type(socket_type)?))
    }

    /// pyzmq calls this `term`; older code calls `destroy`.
    fn term(&self) {}
    fn destroy(&self) {}

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }
}

/// `pyomq.asyncio.Context`. Hands out `AsyncSocket` instances. The
/// instance itself has no state - it's a factory the way `Context`
/// is in pyzmq's `zmq.asyncio`.
#[pyclass(module = "pyomq._native")]
pub struct AsyncContext;

#[pymethods]
impl AsyncContext {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        let _ = io_threads;
        AsyncContext
    }

    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<AsyncSocket> {
        let _ = py;
        Ok(AsyncSocket::new(map_socket_type(socket_type)?))
    }

    fn term(&self) {}
    fn destroy(&self) {}

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }
}
