//! Async (`asyncio`) Socket wrapper.
//!
//! The Python-side `pyomq.asyncio.Socket` drives send/recv through
//! direct methods that never leave the calling thread:
//!
//! - **Send**: `_send_direct` pushes into the send yring (EAGAIN if
//!   full). The send pump relays to the omq Socket on the tokio thread.
//! - **Recv**: `_try_recv` pops from the recv yring (returns `None` if
//!   empty). `_recv_fd` returns a dup'd Unix readiness fd that becomes
//!   readable when the recv pump pushes a message. The Python asyncio
//!   wrapper registers this fd with `loop.add_reader` to wake the coroutine.
//!
//! Control-plane ops (bind, connect, subscribe, ...) use the sync
//! dispatch helpers (block on a tokio oneshot).

use std::sync::Arc;

use bytes::Bytes;
use omq_proto::error::Error as PError;

use crate::error::timeout_err;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};

use crate::conversions;
use crate::dispatch;
use crate::error::map_err;
use crate::frame::Frame;
use crate::runtime::ContextInner;
use crate::socket::SocketInner;

#[pyclass(module = "pyomq._native")]
pub struct AsyncSocket {
    pub(crate) inner: Arc<SocketInner>,
}

impl AsyncSocket {
    pub(crate) fn new(ctx: Arc<ContextInner>, socket_type: omq_tokio::SocketType) -> Self {
        Self {
            inner: SocketInner::new(ctx, socket_type),
        }
    }

    pub fn socket_type(&self) -> omq_tokio::SocketType {
        self.inner.socket_type
    }
}

#[pymethods]
impl AsyncSocket {
    fn socket_id(&self) -> PyResult<u64> {
        self.inner.ensure_id()
    }

    fn bind(&self, py: Python<'_>, endpoint: &str) -> PyResult<String> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_string(&self.inner, py, |s| async move {
            s.bind(ep).await.map(|bound| bound.to_string())
        })
    }

    fn connect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.connect(ep).await })
    }

    fn unbind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.unbind(ep).await })
    }

    fn disconnect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.disconnect(ep).await })
    }

    // ── Send (sync push into yring) ─────────────────────────────────

    #[pyo3(signature = (payload, flags = 0, copy = true))]
    fn send(&self, payload: &Bound<'_, PyAny>, flags: i32, copy: bool) -> PyResult<()> {
        let routing_id = conversions::routing_id_from_pyany(payload);
        let bytes = conversions::bytes_from_pyany(payload, copy)?;
        let Some(mut msg) = self.inner.build_or_buffer(bytes, flags) else {
            return Ok(());
        };
        if routing_id != 0 {
            msg = msg.with_routing_id(routing_id);
        }
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard.as_ref().unwrap();
        let mut prod = materialized.send_prod.lock().unwrap();
        match prod.push_and_flush(msg) {
            Ok(_) => Ok(()),
            Err(_) => Err(timeout_err()),
        }
    }

    #[pyo3(signature = (parts, flags = 0, copy = true))]
    fn send_multipart(&self, parts: &Bound<'_, PyAny>, flags: i32, copy: bool) -> PyResult<()> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts, copy)?;
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard.as_ref().unwrap();
        let mut prod = materialized.send_prod.lock().unwrap();
        match prod.push_and_flush(msg) {
            Ok(_) => Ok(()),
            Err(_) => Err(timeout_err()),
        }
    }

    // ── Recv (try-poll + readiness fd for async notification) ────────

    #[pyo3(name = "_try_recv")]
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return Ok(PyBytes::new(py, &head).into_any());
        }
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = materialized.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            materialized.recv_space.notify_changed();
            let mut parts: Vec<Bytes> = msg.iter().collect();
            let head = if parts.is_empty() {
                Bytes::new()
            } else {
                parts.remove(0)
            };
            if !parts.is_empty() {
                self.inner.store_rxbuf(parts);
            }
            Ok(PyBytes::new(py, &head).into_any())
        } else {
            Ok(py.None().bind(py).clone())
        }
    }

    #[pyo3(name = "_try_recv_frame")]
    fn try_recv_frame<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some((head, more)) = self.inner.pop_rxbuf_head_with_more() {
            return Ok(Bound::new(py, Frame::from_bytes_more(head, more))?.into_any());
        }
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = materialized.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            materialized.recv_space.notify_changed();
            let routing_id = msg.routing_id().unwrap_or(0);
            let mut parts: Vec<Bytes> = msg.iter().collect();
            let head = if parts.is_empty() {
                Bytes::new()
            } else {
                parts.remove(0)
            };
            let more = !parts.is_empty();
            if more {
                self.inner.store_rxbuf(parts);
            }
            Ok(Bound::new(py, Frame::from_bytes_more_routing(head, more, routing_id))?.into_any())
        } else {
            Ok(py.None().bind(py).clone())
        }
    }

    #[pyo3(name = "_try_recv_multipart")]
    fn try_recv_multipart<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            return Ok(
                PyList::new(py, leftover.into_iter().map(|b| PyBytes::new(py, &b)))?.into_any(),
            );
        }
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = materialized.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            materialized.recv_space.notify_changed();
            Ok(conversions::parts_to_pylist(py, msg)?.into_any())
        } else {
            Ok(py.None().bind(py).clone())
        }
    }

    #[pyo3(name = "_try_recv_multipart_frames")]
    fn try_recv_multipart_frames<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            return Ok(conversions::frames_to_pylist(py, leftover)?.into_any());
        }
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = materialized.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            materialized.recv_space.notify_changed();
            Ok(conversions::message_to_frame_list(py, msg)?.into_any())
        } else {
            Ok(py.None().bind(py).clone())
        }
    }

    #[cfg(unix)]
    #[pyo3(name = "_recv_fd")]
    fn recv_fd(&self) -> PyResult<i32> {
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| crate::error::map_err(PError::Closed))?;
        let recv_ready = materialized.recv_ready.clone();
        let owned_fd = recv_ready
            .dup_fd()
            .map_err(|e| crate::error::map_err(PError::Io(e)))?;
        recv_ready.park_begin();
        use std::os::fd::IntoRawFd;
        Ok(owned_fd.into_raw_fd())
    }

    #[cfg(not(unix))]
    #[pyo3(name = "_recv_fd")]
    fn recv_fd(&self) -> PyResult<i32> {
        Err(crate::error::map_err(omq_proto::error::Error::Protocol(
            "_recv_fd() is only available on Unix platforms".to_string(),
        )))
    }

    #[cfg(unix)]
    #[pyo3(name = "_send_fd")]
    fn send_fd(&self) -> PyResult<i32> {
        self.inner.materialize()?;
        let materialized_guard = self.inner.materialized.read().unwrap();
        let materialized = materialized_guard
            .as_ref()
            .ok_or_else(|| crate::error::map_err(PError::Closed))?;
        let send_ready = materialized.send_ready.clone();
        let owned_fd = send_ready
            .dup_fd()
            .map_err(|e| crate::error::map_err(PError::Io(e)))?;
        send_ready.park_begin();
        use std::os::fd::IntoRawFd;
        Ok(owned_fd.into_raw_fd())
    }

    #[cfg(not(unix))]
    #[pyo3(name = "_send_fd")]
    fn send_fd(&self) -> PyResult<i32> {
        Err(crate::error::map_err(omq_proto::error::Error::Protocol(
            "_send_fd() is only available on Unix platforms".to_string(),
        )))
    }

    #[cfg(windows)]
    #[pyo3(name = "_set_wakeup_hooks")]
    #[pyo3(signature = (recv_async = None, recv_event = None, send_async = None, send_event = None))]
    fn set_wakeup_hooks(
        &self,
        recv_async: Option<&Bound<'_, PyAny>>,
        recv_event: Option<&Bound<'_, PyAny>>,
        send_async: Option<&Bound<'_, PyAny>>,
        send_event: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.inner.materialize()?;
        self.inner.set_wakeup_hooks(
            recv_async.map(|cb| cb.clone().unbind()),
            recv_event.map(|event| event.clone().unbind()),
            send_async.map(|cb| cb.clone().unbind()),
            send_event.map(|event| event.clone().unbind()),
        );
        Ok(())
    }

    #[cfg(windows)]
    #[pyo3(name = "_set_wakeup_modes")]
    #[pyo3(signature = (recv_mode = None, send_mode = None))]
    fn set_wakeup_modes(&self, recv_mode: Option<u32>, send_mode: Option<u32>) -> PyResult<()> {
        self.inner.materialize()?;
        self.inner.set_wakeup_modes(recv_mode, send_mode);
        Ok(())
    }

    #[cfg(windows)]
    #[pyo3(name = "_clear_wakeup_modes")]
    #[pyo3(signature = (recv_mode = None, send_mode = None))]
    fn clear_wakeup_modes(&self, recv_mode: Option<u32>, send_mode: Option<u32>) -> PyResult<()> {
        self.inner.clear_wakeup_modes(recv_mode, send_mode);
        Ok(())
    }

    #[cfg(windows)]
    #[pyo3(name = "_mark_recv_drain_complete")]
    fn mark_recv_drain_complete(&self) -> PyResult<()> {
        self.inner.mark_recv_wakeup_drain_complete();
        Ok(())
    }

    #[cfg(windows)]
    #[pyo3(name = "_mark_send_drain_complete")]
    fn mark_send_drain_complete(&self) -> PyResult<()> {
        self.inner.mark_send_wakeup_drain_complete();
        Ok(())
    }

    // ── Subscriptions / groups (sync) ───────────────────────────────

    fn subscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.subscribe(bytes).await })
    }

    fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::sync_unit(
            &self.inner,
            py,
            |s| async move { s.unsubscribe(bytes).await },
        )
    }

    fn join(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.join(bytes).await })
    }

    fn leave(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.leave(bytes).await })
    }

    // ── Introspection (sync) ────────────────────────────────────────

    fn connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let statuses: Vec<omq_tokio::ConnectionStatus> = py.detach(|| {
            ctx.with_socket(&sock, |s| async move {
                s.connections().await.unwrap_or_default()
            })
        });
        // Temporary allocation to be able to propagate errors
        let temp = statuses
            .iter()
            .map(|cs| crate::socket::connection_status_to_dict(py, cs))
            .collect::<PyResult<Vec<Bound<'py, PyDict>>>>()?;
        PyList::new(py, temp)
    }

    fn connection_info<'py>(
        &self,
        py: Python<'py>,
        connection_id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let status: Option<omq_tokio::ConnectionStatus> = py.detach(|| {
            ctx.with_socket(&sock, move |s| async move {
                s.connection_info(connection_id).await.ok().flatten()
            })
        });
        match status {
            Some(cs) => crate::socket::connection_status_to_dict(py, &cs).map(|d| d.into_any()),
            None => Ok(py.None().bind(py).clone()),
        }
    }

    fn monitor(&self, py: Python<'_>) -> PyResult<crate::socket::Monitor> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let stream = py.detach(|| ctx.with_socket(&sock, |s| async move { s.monitor() }));
        Ok(crate::socket::Monitor::from_stream(&self.inner.ctx, stream))
    }

    // ── Options ─────────────────────────────────────────────────────

    fn setsockopt(&self, py: Python<'_>, option: i32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(&self, py: Python<'py>, option: i32) -> PyResult<Bound<'py, PyAny>> {
        crate::options::getsockopt(self.inner.as_ref(), py, option)
    }

    #[cfg(feature = "curve")]
    fn set_curve_auth(&self, auth: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::auth::set_curve_auth_impl(&self.inner, auth)
    }

    #[cfg(feature = "plain")]
    /// Configure PLAIN server admission before first socket use.
    ///
    /// Pass an iterable of exact `(username, password)` pairs or a callable
    /// receiving `PeerInfo`. An empty iterable rejects every client. PLAIN
    /// authenticates clients but does not encrypt traffic.
    fn set_plain_auth(&self, auth: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::auth::set_plain_auth_impl(&self.inner, auth)
    }

    // ── Lifecycle ───────────────────────────────────────────────────

    #[pyo3(signature = (linger=None))]
    fn close(&self, py: Python<'_>, linger: Option<i64>) -> PyResult<()> {
        let linger = self.inner.close_linger(linger);
        let m = self.inner.take_materialized();
        let Some(m) = m else {
            return Ok(());
        };
        let ctx = self.inner.ctx.clone();
        py.detach(|| {
            ctx.destroy_socket(m.socket, m.send_prod, m.send_pump, m.recv_pump, linger);
        });
        Ok(())
    }

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyType>>,
        exc_val: Option<Bound<'_, PyAny>>,
        exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        let (_, _, _) = (exc_type, exc_val, exc_tb);
        let _ = self.close(py, None);
        false
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyType>>,
        exc_val: Option<Bound<'_, PyAny>>,
        exc_tb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let (_, _, _) = (exc_type, exc_val, exc_tb);
        self.close(py, None)
    }
}
