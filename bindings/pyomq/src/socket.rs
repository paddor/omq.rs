//! Sync `Socket` Python class and the shared inner state used by both
//! the sync and async wrappers.
//!
//! Hot path is queue-relayed: every `send` / `recv` is a single
//! `flume::Sender::try_send` / `Receiver::try_recv` on a per-socket
//! bounded channel. Two pump tasks on the compio thread bridge the
//! channels to the actual `omq_compio::Socket`'s async send/recv. No
//! cross-thread runtime hop on the hot path; per-call overhead is the
//! flume push (~50-150 ns) plus PyO3 boundary cost.
//!
//! GIL handling on the sync path: the fast path (queue not full / not
//! empty) does NOT call `Python::allow_threads`. Releasing the GIL
//! costs two atomics and a memory barrier (~30-100 ns) - the same
//! order as the queue push itself. On the unblocked path that's pure
//! overhead and other Python threads only stall for the brief ring-
//! buffer push. The slow path (Full / Empty) does release the GIL
//! because it can block for milliseconds.

use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use flume::{RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError};
use omq_proto::error::Error as PError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyType};

use crate::conversions;
use crate::error::{map_err, timeout_err};
use crate::options;
use crate::runtime;

/// Per-socket scratchpad for SNDMORE-style multipart construction.
#[derive(Default)]
pub(crate) struct SendBuffer {
    pub parts: Vec<Bytes>,
}

/// State that exists once the underlying omq Socket is materialized.
/// Held inside `Mutex<Option<...>>` so close() can drop it from `&self`.
pub(crate) struct Materialized {
    pub id: u64,
    pub send_tx: flume::Sender<omq_compio::Message>,
    pub recv_rx: flume::Receiver<omq_compio::Message>,
}

/// Shared state for sync (`Socket`) and async (`AsyncSocket`) wrappers.
/// Both pyclasses hold an `Arc<SocketInner>` and route I/O through the
/// helpers below.
pub(crate) struct SocketInner {
    pub socket_type: omq_compio::SocketType,
    pub overlay: Mutex<options::Overlay>,
    pub sndbuf: Mutex<SendBuffer>,
    pub rxbuf: Mutex<Vec<Bytes>>,
    pub materialized: Mutex<Option<Materialized>>,
}

impl SocketInner {
    pub fn new(socket_type: omq_compio::SocketType) -> Arc<Self> {
        let opts = omq_compio::Options::default();
        let overlay = options::Overlay::from_options(&opts);
        Arc::new(Self {
            socket_type,
            overlay: Mutex::new(overlay),
            sndbuf: Mutex::new(SendBuffer::default()),
            rxbuf: Mutex::new(Vec::new()),
            materialized: Mutex::new(None),
        })
    }

    pub fn parse_endpoint(s: &str) -> PyResult<omq_compio::Endpoint> {
        omq_compio::Endpoint::from_str(s).map_err(map_err)
    }

    /// Build the underlying omq Socket + queues + pumps on first I/O.
    pub fn materialize(&self) -> PyResult<()> {
        let mut slot = self.materialized.lock().unwrap();
        if slot.is_some() {
            return Ok(());
        }
        let opts = self.overlay.lock().unwrap().to_options();
        let (send_tx, send_rx) = match opts.send_hwm {
            Some(n) => flume::bounded::<omq_compio::Message>(n.max(1) as usize),
            None => flume::unbounded(),
        };
        let (recv_tx, recv_rx) = match opts.recv_hwm {
            Some(n) => flume::bounded::<omq_compio::Message>(n.max(1) as usize),
            None => flume::unbounded(),
        };
        let st = self.socket_type;
        let id = runtime::materialize(st, opts, send_rx, recv_tx);
        *slot = Some(Materialized { id, send_tx, recv_rx });
        Ok(())
    }

    pub fn ensure_id(&self) -> PyResult<u64> {
        self.materialize()?;
        Ok(self.materialized.lock().unwrap().as_ref().unwrap().id)
    }

    pub fn send_tx_clone(&self) -> PyResult<flume::Sender<omq_compio::Message>> {
        self.materialize()?;
        Ok(self.materialized.lock().unwrap().as_ref().unwrap().send_tx.clone())
    }

    pub fn recv_rx_clone(&self) -> PyResult<flume::Receiver<omq_compio::Message>> {
        self.materialize()?;
        Ok(self.materialized.lock().unwrap().as_ref().unwrap().recv_rx.clone())
    }

    /// Push `bytes` onto the SNDMORE buffer. Returns `Some(msg)` if the
    /// caller flushes (non-SNDMORE flag), `None` if buffered.
    pub fn build_or_buffer(&self, bytes: Bytes, flags: i32) -> Option<omq_compio::Message> {
        if flags & crate::constants::SNDMORE != 0 {
            self.sndbuf.lock().unwrap().parts.push(bytes);
            return None;
        }
        let mut buf = self.sndbuf.lock().unwrap();
        let mut m = omq_compio::Message::new();
        for p in buf.parts.drain(..) {
            m.push_part(omq_proto::message::Payload::from_bytes(p));
        }
        m.push_part(omq_proto::message::Payload::from_bytes(bytes));
        Some(m)
    }

    /// Pop the head of any leftover RCVMORE frames; `Some` when one
    /// exists and the caller should return it instead of pulling from
    /// the recv channel.
    pub fn pop_rxbuf_head(&self) -> Option<Bytes> {
        let mut rx = self.rxbuf.lock().unwrap();
        if rx.is_empty() {
            None
        } else {
            Some(rx.remove(0))
        }
    }

    /// Take all leftover RCVMORE frames at once (used by recv_multipart).
    pub fn take_rxbuf(&self) -> Vec<Bytes> {
        let mut rx = self.rxbuf.lock().unwrap();
        std::mem::take(&mut *rx)
    }

    pub fn store_rxbuf(&self, parts: Vec<Bytes>) {
        *self.rxbuf.lock().unwrap() = parts;
    }

    pub fn rxbuf_lock(&self) -> MutexGuard<'_, Vec<Bytes>> {
        self.rxbuf.lock().unwrap()
    }

    /// Drop the materialized state on close. Pumps see Disconnected on
    /// the next op and exit; the registry entry is removed separately.
    pub fn take_materialized(&self) -> Option<Materialized> {
        self.materialized.lock().unwrap().take()
    }
}

#[pyclass(module = "pyomq._native")]
pub struct Socket {
    pub(crate) inner: Arc<SocketInner>,
}

impl Socket {
    pub fn from_inner(inner: Arc<SocketInner>) -> Self {
        Self { inner }
    }

    pub fn new(socket_type: omq_compio::SocketType) -> Self {
        Self { inner: SocketInner::new(socket_type) }
    }

    pub fn socket_type(&self) -> omq_compio::SocketType {
        self.inner.socket_type
    }

    pub(crate) fn ensure_id(&self) -> PyResult<u64> {
        self.inner.ensure_id()
    }
}

#[pymethods]
impl Socket {
    fn bind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| runtime::with_socket(id, move |s| async move { s.bind(ep).await }))
            .map_err(missing)
            .and_then(|r| r.map_err(map_err))
    }

    fn connect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| runtime::with_socket(id, move |s| async move { s.connect(ep).await }))
            .map_err(missing)
            .and_then(|r| r.map_err(map_err))
    }

    fn unbind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| runtime::with_socket(id, move |s| async move { s.unbind(ep).await }))
            .map_err(missing)
            .and_then(|r| r.map_err(map_err))
    }

    fn disconnect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| {
            runtime::with_socket(id, move |s| async move { s.disconnect(ep).await })
        })
        .map_err(missing)
        .and_then(|r| r.map_err(map_err))
    }

    #[pyo3(signature = (payload, flags = 0))]
    fn send(&self, py: Python<'_>, payload: &Bound<'_, PyAny>, flags: i32) -> PyResult<()> {
        let bytes = conversions::bytes_from_pyany(payload)?;
        let Some(msg) = self.inner.build_or_buffer(bytes, flags) else {
            return Ok(());
        };
        self.send_message(py, msg)
    }

    #[pyo3(signature = (parts, flags = 0))]
    fn send_multipart(
        &self,
        py: Python<'_>,
        parts: &Bound<'_, PyAny>,
        flags: i32,
    ) -> PyResult<()> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        self.send_message(py, msg)
    }

    #[pyo3(signature = (flags = 0))]
    fn recv<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyBytes>> {
        let _ = flags;
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return Ok(PyBytes::new_bound(py, &head));
        }
        let msg = self.recv_message(py)?;
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
            self.inner.store_rxbuf(parts);
        }
        Ok(PyBytes::new_bound(py, &head))
    }

    #[pyo3(signature = (flags = 0))]
    fn recv_multipart<'py>(
        &self,
        py: Python<'py>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyList>> {
        let _ = flags;
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            let parts: Vec<Bound<'py, PyBytes>> = leftover
                .into_iter()
                .map(|b| PyBytes::new_bound(py, &b))
                .collect();
            return Ok(PyList::new_bound(py, parts));
        }
        let msg = self.recv_message(py)?;
        Ok(conversions::parts_to_pylist(py, msg))
    }

    fn subscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let view: &[u8] = prefix.extract()?;
        let bytes = Bytes::copy_from_slice(view);
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| {
            runtime::with_socket(id, move |s| async move { s.subscribe(bytes).await })
        })
        .map_err(missing)
        .and_then(|r| r.map_err(map_err))
    }

    fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let view: &[u8] = prefix.extract()?;
        let bytes = Bytes::copy_from_slice(view);
        let id = self.inner.ensure_id()?;
        py.allow_threads(|| {
            runtime::with_socket(id, move |s| async move { s.unsubscribe(bytes).await })
        })
        .map_err(missing)
        .and_then(|r| r.map_err(map_err))
    }

    fn setsockopt(
        &self,
        py: Python<'_>,
        option: i32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(
        &self,
        py: Python<'py>,
        option: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        options::getsockopt(self.inner.as_ref(), py, option)
    }

    fn close(&self, py: Python<'_>, _linger: Option<i64>) -> PyResult<()> {
        let m = self.inner.take_materialized();
        let Some(m) = m else { return Ok(()); };
        py.allow_threads(|| runtime::destroy_socket(m.id));
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
}

impl Socket {
    fn send_message(
        &self,
        py: Python<'_>,
        msg: omq_compio::Message,
    ) -> PyResult<()> {
        let send_tx = self.inner.send_tx_clone()?;
        match send_tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(_)) => Err(map_err(PError::Closed)),
            Err(TrySendError::Full(msg)) => {
                let timeout = self.inner.overlay.lock().unwrap().sndtimeo;
                py.allow_threads(|| match timeout {
                    Some(t) => match send_tx.send_timeout(msg, t) {
                        Ok(()) => Ok(()),
                        Err(SendTimeoutError::Timeout(_)) => Err(timeout_err()),
                        Err(SendTimeoutError::Disconnected(_)) => {
                            Err(map_err(PError::Closed))
                        }
                    },
                    None => match send_tx.send(msg) {
                        Ok(()) => Ok(()),
                        Err(SendError(_)) => Err(map_err(PError::Closed)),
                    },
                })
            }
        }
    }

    fn recv_message(&self, py: Python<'_>) -> PyResult<omq_compio::Message> {
        let recv_rx = self.inner.recv_rx_clone()?;
        match recv_rx.try_recv() {
            Ok(m) => Ok(m),
            Err(TryRecvError::Disconnected) => Err(map_err(PError::Closed)),
            Err(TryRecvError::Empty) => {
                let timeout = self.inner.overlay.lock().unwrap().rcvtimeo;
                py.allow_threads(|| match timeout {
                    Some(t) => match recv_rx.recv_timeout(t) {
                        Ok(m) => Ok(m),
                        Err(RecvTimeoutError::Timeout) => Err(timeout_err()),
                        Err(RecvTimeoutError::Disconnected) => {
                            Err(map_err(PError::Closed))
                        }
                    },
                    None => match recv_rx.recv() {
                        Ok(m) => Ok(m),
                        Err(RecvError::Disconnected) => Err(map_err(PError::Closed)),
                    },
                })
            }
        }
    }
}

fn missing(_: runtime::MissingSocket) -> PyErr {
    map_err(PError::Closed)
}

#[allow(dead_code)]
fn _need_for_compat(_d: Duration) {}
