//! Context: per-runtime Socket factory. Each Context owns a dedicated
//! tokio runtime on a background thread. `term()` shuts it down.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::constants;
use crate::error::map_err;
use crate::runtime::ContextInner;
use crate::socket::Socket;
use crate::socket_async::AsyncSocket;

fn map_socket_type(socket_type_id: i32) -> PyResult<omq_tokio::SocketType> {
    Ok(match socket_type_id {
        constants::PAIR => omq_tokio::SocketType::Pair,
        constants::PUB => omq_tokio::SocketType::Pub,
        constants::SUB => omq_tokio::SocketType::Sub,
        constants::REQ => omq_tokio::SocketType::Req,
        constants::REP => omq_tokio::SocketType::Rep,
        constants::DEALER => omq_tokio::SocketType::Dealer,
        constants::ROUTER => omq_tokio::SocketType::Router,
        constants::PULL => omq_tokio::SocketType::Pull,
        constants::PUSH => omq_tokio::SocketType::Push,
        constants::XPUB => omq_tokio::SocketType::XPub,
        constants::XSUB => omq_tokio::SocketType::XSub,
        constants::STREAM => omq_tokio::SocketType::Stream,
        constants::SERVER => omq_tokio::SocketType::Server,
        constants::CLIENT => omq_tokio::SocketType::Client,
        constants::RADIO => omq_tokio::SocketType::Radio,
        constants::DISH => omq_tokio::SocketType::Dish,
        constants::GATHER => omq_tokio::SocketType::Gather,
        constants::SCATTER => omq_tokio::SocketType::Scatter,
        constants::PEER => omq_tokio::SocketType::Peer,
        constants::CHANNEL => omq_tokio::SocketType::Channel,
        other => {
            return Err(map_err(omq_proto::error::Error::InvalidEndpoint(format!(
                "unknown socket type {other}"
            ))));
        }
    })
}

#[pyclass(module = "pyomq._native")]
pub struct Context {
    pub(crate) ctx: Arc<ContextInner>,
}

#[pymethods]
impl Context {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        Context {
            ctx: ContextInner::new(io_threads.max(1) as usize),
        }
    }

    #[staticmethod]
    fn shadow_async(context: PyRef<'_, AsyncContext>) -> Self {
        Context {
            ctx: context.ctx.clone(),
        }
    }

    /// Construct a new socket of the given libzmq type code.
    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<Socket> {
        let _ = py;
        Ok(Socket::new(self.ctx.clone(), map_socket_type(socket_type)?))
    }

    /// pyzmq calls this `term`; older code calls `destroy`.
    fn term(&self) {
        self.ctx.term();
    }
    fn destroy(&self) {
        self.ctx.term();
    }

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
        self.ctx.term();
        false
    }
}

/// `pyomq.asyncio.Context`. Hands out `AsyncSocket` instances.
#[pyclass(module = "pyomq._native")]
pub struct AsyncContext {
    pub(crate) ctx: Arc<ContextInner>,
}

#[pymethods]
impl AsyncContext {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        AsyncContext {
            ctx: ContextInner::new(io_threads.max(1) as usize),
        }
    }

    #[staticmethod]
    fn shadow_sync(context: PyRef<'_, Context>) -> Self {
        AsyncContext {
            ctx: context.ctx.clone(),
        }
    }

    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<AsyncSocket> {
        let _ = py;
        Ok(AsyncSocket::new(
            self.ctx.clone(),
            map_socket_type(socket_type)?,
        ))
    }

    fn term(&self) {
        self.ctx.term();
    }
    fn destroy(&self) {
        self.ctx.term();
    }

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
        self.ctx.term();
        false
    }
}
