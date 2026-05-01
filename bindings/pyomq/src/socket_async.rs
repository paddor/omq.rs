//! Async (`asyncio`) Socket wrapper.
//!
//! Mirrors the methods on `Socket` but each call returns a Python
//! awaitable (`asyncio.Future`) instead of blocking. Reuses the same
//! `SocketInner` so the materialised omq Socket, send/recv queues,
//! pumps, sndbuf, and rxbuf are shared transparently with the sync
//! `Socket` constructor (you don't have to pick async at construction
//! time - it's per-method).
//!
//! Each async method:
//!   1. Builds the work synchronously (parse endpoint, encode message,
//!      etc.) on the calling Python thread.
//!   2. Hands a `Future` to `runtime::compio_future_into_py`, which
//!      spawns it on the compio runtime and bridges completion back
//!      to the asyncio event loop via `loop.call_soon_threadsafe`.

use std::sync::Arc;

use bytes::Bytes;
use omq_proto::error::Error as PError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyType};

use crate::conversions;
use crate::dispatch;
use crate::error::map_err;
use crate::runtime;
use crate::socket::SocketInner;

#[pyclass(module = "pyomq._native")]
pub struct AsyncSocket {
    pub(crate) inner: Arc<SocketInner>,
}

impl AsyncSocket {
    pub fn new(socket_type: omq_compio::SocketType) -> Self {
        Self { inner: SocketInner::new(socket_type) }
    }

    pub fn socket_type(&self) -> omq_compio::SocketType {
        self.inner.socket_type
    }
}

#[pymethods]
impl AsyncSocket {
    fn bind<'py>(&self, py: Python<'py>, endpoint: &str) -> PyResult<Bound<'py, PyAny>> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::async_unit(&self.inner, py, |s| async move { s.bind(ep).await })
    }

    fn connect<'py>(&self, py: Python<'py>, endpoint: &str) -> PyResult<Bound<'py, PyAny>> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::async_unit(&self.inner, py, |s| async move { s.connect(ep).await })
    }

    fn unbind<'py>(&self, py: Python<'py>, endpoint: &str) -> PyResult<Bound<'py, PyAny>> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::async_unit(&self.inner, py, |s| async move { s.unbind(ep).await })
    }

    fn disconnect<'py>(&self, py: Python<'py>, endpoint: &str) -> PyResult<Bound<'py, PyAny>> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::async_unit(&self.inner, py, |s| async move { s.disconnect(ep).await })
    }

    #[pyo3(signature = (payload, flags = 0))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = conversions::bytes_from_pyany(payload)?;
        let Some(msg) = self.inner.build_or_buffer(bytes, flags) else {
            // SNDMORE: queued, return resolved-immediately future.
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| Ok(py.None()))
            });
        };
        let send_tx = self.inner.send_tx_clone()?;
        runtime::compio_future_into_py(py, move || async move {
            match send_tx.send_async(msg).await {
                Ok(()) => Python::with_gil(|py| Ok(py.None())),
                Err(_) => Err(map_err(PError::Closed)),
            }
        })
    }

    #[pyo3(signature = (parts, flags = 0))]
    fn send_multipart<'py>(
        &self,
        py: Python<'py>,
        parts: &Bound<'py, PyAny>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        let send_tx = self.inner.send_tx_clone()?;
        runtime::compio_future_into_py(py, move || async move {
            match send_tx.send_async(msg).await {
                Ok(()) => Python::with_gil(|py| Ok(py.None())),
                Err(_) => Err(map_err(PError::Closed)),
            }
        })
    }

    #[pyo3(signature = (flags = 0))]
    fn recv<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| {
                    Ok(PyBytes::new_bound(py, &head).into_any().unbind())
                })
            });
        }
        let recv_rx = self.inner.recv_rx_clone()?;
        let inner = self.inner.clone();
        runtime::compio_future_into_py(py, move || async move {
            match recv_rx.recv_async().await {
                Ok(msg) => {
                    let mut parts: Vec<Bytes> = msg
                        .into_parts()
                        .into_iter()
                        .map(|p| p.coalesce())
                        .collect();
                    let head = if parts.is_empty() {
                        Bytes::new()
                    } else {
                        parts.remove(0)
                    };
                    if !parts.is_empty() {
                        inner.store_rxbuf(parts);
                    }
                    Python::with_gil(|py| {
                        Ok(PyBytes::new_bound(py, &head).into_any().unbind())
                    })
                }
                Err(_) => Err(map_err(PError::Closed)),
            }
        })
    }

    #[pyo3(signature = (flags = 0))]
    fn recv_multipart<'py>(
        &self,
        py: Python<'py>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| {
                    let parts: Vec<Bound<'_, PyBytes>> = leftover
                        .into_iter()
                        .map(|b| PyBytes::new_bound(py, &b))
                        .collect();
                    Ok(PyList::new_bound(py, parts).into_any().unbind())
                })
            });
        }
        let recv_rx = self.inner.recv_rx_clone()?;
        runtime::compio_future_into_py(py, move || async move {
            match recv_rx.recv_async().await {
                Ok(msg) => Python::with_gil(|py| {
                    Ok(conversions::parts_to_pylist(py, msg).into_any().unbind())
                }),
                Err(_) => Err(map_err(PError::Closed)),
            }
        })
    }

    fn subscribe<'py>(&self, py: Python<'py>, prefix: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::async_unit(&self.inner, py, |s| async move { s.subscribe(bytes).await })
    }

    fn unsubscribe<'py>(&self, py: Python<'py>, prefix: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::async_unit(&self.inner, py, |s| async move { s.unsubscribe(bytes).await })
    }

    fn join<'py>(&self, py: Python<'py>, group: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::async_unit(&self.inner, py, |s| async move { s.join(bytes).await })
    }

    fn leave<'py>(&self, py: Python<'py>, group: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::async_unit(&self.inner, py, |s| async move { s.leave(bytes).await })
    }

    /// Return an `asyncio.Future` that resolves to a list of connection-
    /// status dicts (one per live peer).
    fn connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let id = self.inner.ensure_id()?;
        runtime::compio_future_into_py(py, move || async move {
            let statuses = match runtime::with_socket_async(id, |s| async move {
                s.connections().await.unwrap_or_default()
            })
            .await
            {
                Ok(v) => v,
                Err(_) => vec![],
            };
            Python::with_gil(|py| {
                let dicts: Vec<PyObject> = statuses
                    .iter()
                    .map(|cs| crate::socket::connection_status_to_dict(py, cs))
                    .collect::<PyResult<_>>()?;
                Ok(pyo3::types::PyList::new_bound(py, dicts).into_any().unbind())
            })
        })
    }

    /// Return an `asyncio.Future` that resolves to a connection-status
    /// dict for `connection_id`, or `None` if no such peer is connected.
    fn connection_info<'py>(&self, py: Python<'py>, connection_id: u64) -> PyResult<Bound<'py, PyAny>> {
        let id = self.inner.ensure_id()?;
        runtime::compio_future_into_py(py, move || async move {
            let status = match runtime::with_socket_async(id, move |s| async move {
                s.connection_info(connection_id).await.ok().flatten()
            })
            .await
            {
                Ok(v) => v,
                Err(_) => None,
            };
            Python::with_gil(|py| match status {
                Some(cs) => crate::socket::connection_status_to_dict(py, &cs),
                None => Ok(py.None()),
            })
        })
    }

    /// Return a `Monitor` for this socket. Non-async: subscribing is
    /// instantaneous (just a flume channel allocation on the compio thread).
    fn monitor(&self, py: Python<'_>) -> PyResult<crate::socket::Monitor> {
        let id = self.inner.ensure_id()?;
        let stream = py.allow_threads(|| {
            runtime::with_socket(id, |s| async move { s.monitor() }).map_err(|_| ())
        });
        let stream = stream.map_err(|()| map_err(PError::Closed))?;
        let (rx, lagged) = stream.into_raw();
        Ok(crate::socket::Monitor { rx, lagged })
    }

    /// Sync setsockopt (returning None directly) - matches pyzmq's
    /// async API which keeps setsockopt synchronous since it's not I/O.
    fn setsockopt(
        &self,
        py: Python<'_>,
        option: i32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        crate::options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(
        &self,
        py: Python<'py>,
        option: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::options::getsockopt(self.inner.as_ref(), py, option)
    }

    #[pyo3(signature = (_linger=None))]
    fn close<'py>(&self, py: Python<'py>, _linger: Option<i64>) -> PyResult<Bound<'py, PyAny>> {
        let m = self.inner.take_materialized();
        runtime::compio_future_into_py(py, move || async move {
            if let Some(m) = m {
                runtime::destroy_socket_local(m.id);
            }
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();
        runtime::compio_future_into_py(py, move || async move {
            Python::with_gil(|py| {
                let py_self = AsyncSocket { inner };
                Ok(Py::new(py, py_self)?.into_any())
            })
        })
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Bound<'py, PyType>>,
        exc_val: Option<Bound<'py, PyAny>>,
        exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_, _, _) = (exc_type, exc_val, exc_tb);
        self.close(py, None)
    }
}
