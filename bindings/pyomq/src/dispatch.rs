//! Sync ↔ async dispatch glue, factored out of `socket.rs` and
//! `socket_async.rs`. Most non-I/O-shape methods (bind, connect,
//! unbind, disconnect, subscribe, unsubscribe, join, leave) just
//! materialise the underlying socket, hand a closure to the compio
//! runtime, and translate the result. Without these helpers the two
//! files repeated the same 6-line pattern eight times each.

use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use omq_proto::error::Error as PError;
use pyo3::prelude::*;

use crate::error::map_err;
use crate::runtime::{self, MissingSocket};
use crate::socket::SocketInner;

/// Sync version: drive a `Result<()>`-returning op on the compio
/// thread, blocking the caller. Releases the GIL across the trip.
pub(crate) fn sync_unit<F, Fut>(
    inner: &Arc<SocketInner>,
    py: Python<'_>,
    op: F,
) -> PyResult<()>
where
    F: FnOnce(Rc<omq_compio::Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), PError>> + 'static,
{
    let id = inner.ensure_id()?;
    py.allow_threads(|| runtime::with_socket(id, op))
        .map_err(|_: MissingSocket| map_err(PError::Closed))
        .and_then(|r| r.map_err(map_err))
}

/// Async version: drive the same shape of op via an asyncio.Future
/// bridged on the compio runtime.
pub(crate) fn async_unit<'py, F, Fut>(
    inner: &Arc<SocketInner>,
    py: Python<'py>,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: FnOnce(Rc<omq_compio::Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), PError>> + 'static,
{
    let id = inner.ensure_id()?;
    runtime::compio_future_into_py(py, move || async move {
        match runtime::with_socket_async(id, op).await {
            Ok(Ok(())) => Python::with_gil(|py| Ok(py.None())),
            Ok(Err(e)) => Err(map_err(e)),
            Err(_) => Err(map_err(PError::Closed)),
        }
    })
}
