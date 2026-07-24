//! Sync `Socket` Python class and the shared inner state used by both
//! the sync and async wrappers.
//!
//! The synchronous wrapper uses `omq_tokio::blocking::Socket` directly.
//! The asyncio wrapper retains its yring relay and eventfd adapter.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::TrySendError;
use omq_proto::error::Error as PError;
use omq_tokio::MonitorEvent;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};
use tokio::task::JoinHandle;

use crate::conversions;
use crate::dispatch;
use crate::error::{map_err, timeout_err};
use crate::notify::ReadinessSignal;
use crate::options;
use crate::runtime::ContextInner;

/// Per-socket scratchpad for SNDMORE-style multipart construction.
#[derive(Default)]
pub(crate) struct SendBuffer {
    pub parts: Vec<Bytes>,
}

use std::sync::atomic::AtomicU32;

static FORK_GEN: AtomicU32 = AtomicU32::new(0);
static PARENT_FORK_GEN: AtomicU32 = AtomicU32::new(0);
static FORKED: AtomicBool = AtomicBool::new(false);
#[cfg(unix)]
static ATFORK_REGISTERED: std::sync::Once = std::sync::Once::new();

#[cfg(unix)]
extern "C" fn atfork_child() {
    FORKED.store(true, Ordering::Relaxed);
    FORK_GEN.fetch_add(1, Ordering::Relaxed);
}

#[cfg(unix)]
extern "C" fn atfork_parent() {
    // The parent's runtime remains valid, but sockets materialized before
    // fork need one safe receive to resynchronize with child-side peers.
    PARENT_FORK_GEN.fetch_add(1, Ordering::Relaxed);
}

#[cfg(unix)]
pub(crate) fn register_atfork() {
    ATFORK_REGISTERED.call_once(|| unsafe {
        libc::pthread_atfork(Some(atfork_parent), None, Some(atfork_child));
    });
}

#[cfg(not(unix))]
pub(crate) fn register_atfork() {
    // No-op on non-Unix platforms
}

/// State that exists once the underlying omq Socket is materialized.
/// Held inside `Mutex<Option<...>>` so close() can drop it from `&self`.
pub(crate) struct Materialized {
    pub id: u64,
    fork_gen: u32,
    pub socket: Arc<omq_tokio::Socket>,
    pub send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    pub recv_cons: Mutex<yring::Consumer<omq_tokio::Message>>,
    pub recv_ready: Arc<ReadinessSignal>,
    pub send_ready: Arc<ReadinessSignal>,
    pub recv_space: Arc<omq_tokio::engine::StateSignal>,
    pub send_pump: JoinHandle<()>,
    pub recv_pump: JoinHandle<()>,
}

pub(crate) struct BlockingMaterialized {
    pub id: u64,
    fork_gen: u32,
    pub socket: omq_tokio::blocking::Socket,
}

/// Shared state for sync (`Socket`) and async (`AsyncSocket`) wrappers.
/// Both pyclasses hold an `Arc<SocketInner>` and route I/O through the
/// helpers below.
pub(crate) struct SocketInner {
    pub ctx: Arc<ContextInner>,
    pub socket_type: omq_tokio::SocketType,
    pub overlay: Mutex<options::Overlay>,
    pub subscriptions: Mutex<Vec<Bytes>>,
    pub endpoints: Mutex<Vec<(omq_tokio::Endpoint, bool)>>,
    has_tcp_endpoint: AtomicBool,
    parent_fork_gen: AtomicU32,
    post_fork: AtomicBool,
    pub sndbuf: Mutex<SendBuffer>,
    pub rxbuf: Mutex<Vec<Bytes>>,
    pub rxmsgs: Mutex<Vec<omq_tokio::Message>>,
    pub materialized: std::sync::RwLock<Option<Materialized>>,
    pub blocking_materialized: std::sync::RwLock<Option<BlockingMaterialized>>,
    closed: AtomicBool,
}

impl SocketInner {
    pub fn new(ctx: Arc<ContextInner>, socket_type: omq_tokio::SocketType) -> Arc<Self> {
        let opts = omq_tokio::Options::default();
        let overlay = options::Overlay::from_options(&opts);
        Arc::new(Self {
            ctx,
            socket_type,
            overlay: Mutex::new(overlay),
            subscriptions: Mutex::new(Vec::new()),
            endpoints: Mutex::new(Vec::new()),
            has_tcp_endpoint: AtomicBool::new(false),
            parent_fork_gen: AtomicU32::new(PARENT_FORK_GEN.load(Ordering::Relaxed)),
            post_fork: AtomicBool::new(FORKED.load(Ordering::Relaxed)),
            sndbuf: Mutex::new(SendBuffer::default()),
            rxbuf: Mutex::new(Vec::new()),
            rxmsgs: Mutex::new(Vec::new()),
            materialized: std::sync::RwLock::new(None),
            blocking_materialized: std::sync::RwLock::new(None),
            closed: AtomicBool::new(false),
        })
    }

    pub fn parse_endpoint(s: &str) -> PyResult<omq_tokio::Endpoint> {
        omq_tokio::Endpoint::from_str(s).map_err(map_err)
    }

    /// Build the underlying omq Socket + queues + pumps on first I/O.
    /// After fork, the parent's pump tasks and runtime are dead in the
    /// child. The `pthread_atfork` child handler increments `FORK_GEN`;
    /// we detect the mismatch here and re-materialize.
    pub fn materialize(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(map_err(omq_proto::error::Error::Closed));
        }
        let fork_gen = FORK_GEN.load(Ordering::Relaxed);
        {
            let slot = self.materialized.read().unwrap();
            if slot.as_ref().is_some_and(|m| m.fork_gen == fork_gen) {
                return Ok(());
            }
        }
        let mut slot = self.materialized.write().unwrap();
        if slot.as_ref().is_some_and(|m| m.fork_gen == fork_gen) {
            return Ok(());
        }
        *slot = None;
        let opts = self.overlay.lock().unwrap().to_options()?;
        let send_cap = opts.send_hwm.max(1) as usize;
        let recv_cap = opts.recv_hwm.max(1) as usize;
        let (send_prod, send_cons) = yring::async_spsc(send_cap);
        let (recv_prod, recv_cons) = yring::spsc(recv_cap);
        let recv_ready = Arc::new(ReadinessSignal::new());
        let send_ready = Arc::new(ReadinessSignal::new());
        let recv_space = Arc::new(omq_tokio::engine::StateSignal::new());
        let socket_type = self.socket_type;
        let (id, socket, send_pump, recv_pump) = self.ctx.materialize(
            socket_type,
            opts,
            send_cons,
            recv_prod,
            recv_ready.clone(),
            send_ready.clone(),
            recv_space.clone(),
        )?;
        *slot = Some(Materialized {
            id,
            fork_gen,
            socket,
            send_prod: Mutex::new(send_prod),
            recv_cons: Mutex::new(recv_cons),
            recv_ready,
            send_ready,
            recv_space,
            send_pump,
            recv_pump,
        });
        Ok(())
    }

    pub fn ensure_id(&self) -> PyResult<u64> {
        let fork_gen = FORK_GEN.load(Ordering::Relaxed);
        if let Some(m) = self.blocking_materialized.read().unwrap().as_ref()
            && m.fork_gen == fork_gen
        {
            return Ok(m.id);
        }
        self.materialize()?;
        Ok(self.materialized.read().unwrap().as_ref().unwrap().id)
    }

    pub fn materialize_blocking(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(map_err(omq_proto::error::Error::Closed));
        }
        let fork_gen = FORK_GEN.load(Ordering::Relaxed);
        if self
            .blocking_materialized
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|m| m.fork_gen == fork_gen)
        {
            return Ok(());
        }
        let mut slot = self.blocking_materialized.write().unwrap();
        if slot.as_ref().is_some_and(|m| m.fork_gen == fork_gen) {
            return Ok(());
        }
        // The inherited blocking socket owns the parent's runtime. Dropping
        // it after fork would try to join threads that do not exist here.
        if let Some(stale) = slot.take() {
            std::mem::forget(stale);
        }
        let opts = self.overlay.lock().unwrap().to_options()?;
        let (id, socket) = self.ctx.materialize_blocking(self.socket_type, opts)?;
        for (endpoint, is_bind) in self.endpoints.lock().unwrap().clone() {
            if is_bind {
                socket.bind(endpoint).map_err(map_err)?;
            } else {
                socket.connect(endpoint).map_err(map_err)?;
            }
        }
        *slot = Some(BlockingMaterialized {
            id,
            fork_gen,
            socket,
        });
        Ok(())
    }

    pub fn ensure_blocking_socket(&self) -> PyResult<omq_tokio::blocking::Socket> {
        self.materialize_blocking()?;
        Ok(self
            .blocking_materialized
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .socket
            .clone())
    }

    pub fn ensure_blocking_id(&self) -> PyResult<u64> {
        self.materialize_blocking()?;
        Ok(self
            .blocking_materialized
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .id)
    }

    /// Return the Arc<Socket> after materializing. Used by dispatch.
    pub fn ensure_socket(&self) -> PyResult<Arc<omq_tokio::Socket>> {
        self.materialize()?;
        Ok(self
            .materialized
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .socket
            .clone())
    }

    /// Push `bytes` onto the SNDMORE buffer. Returns `Some(msg)` if the
    /// caller flushes (non-SNDMORE flag), `None` if buffered.
    pub fn build_or_buffer(&self, bytes: Bytes, flags: i32) -> Option<omq_tokio::Message> {
        if flags & crate::constants::SNDMORE != 0 {
            self.sndbuf.lock().unwrap().parts.push(bytes);
            return None;
        }
        let mut buf = self.sndbuf.lock().unwrap();
        if buf.parts.is_empty() {
            Some(omq_tokio::Message::single(bytes))
        } else {
            let parts: Vec<Bytes> = buf.parts.drain(..).chain(std::iter::once(bytes)).collect();
            Some(omq_tokio::Message::multipart(parts))
        }
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

    /// Drop the materialized state on close. Pumps see Disconnected on
    /// the next op and exit; the registry entry is removed separately.
    pub fn take_materialized(&self) -> Option<Materialized> {
        self.closed.store(true, Ordering::Relaxed);
        let materialized = self.materialized.write().unwrap().take();
        if let Some(ref state) = materialized {
            state.recv_ready.force_wake();
            state.send_ready.force_wake();
        }
        materialized
    }

    pub fn take_blocking_materialized(&self) -> Option<BlockingMaterialized> {
        self.closed.store(true, Ordering::Relaxed);
        self.blocking_materialized.write().unwrap().take()
    }

    pub fn close_linger(&self, linger: Option<i64>) -> Option<Duration> {
        linger.map_or_else(
            || self.overlay.lock().unwrap().linger,
            options::duration_from_millis,
        )
    }

    #[cfg(windows)]
    pub fn set_wakeup_hooks(
        &self,
        recv_async: Option<Py<PyAny>>,
        recv_event: Option<Py<PyAny>>,
        send_async: Option<Py<PyAny>>,
        send_event: Option<Py<PyAny>>,
    ) {
        let materialized_guard = self.materialized.read().unwrap();
        if let Some(materialized) = materialized_guard.as_ref() {
            materialized
                .recv_ready
                .set_wakeup_hooks(recv_async, recv_event);
            materialized
                .send_ready
                .set_wakeup_hooks(send_async, send_event);
        }
    }

    #[cfg(windows)]
    pub fn set_wakeup_modes(&self, recv_mode: Option<u32>, send_mode: Option<u32>) {
        let materialized_guard = self.materialized.read().unwrap();
        if let Some(materialized) = materialized_guard.as_ref() {
            if let Some(mode) = recv_mode {
                materialized.recv_ready.set_wakeup_mode(mode);
            }
            if let Some(mode) = send_mode {
                materialized.send_ready.set_wakeup_mode(mode);
            }
        }
    }

    #[cfg(windows)]
    pub fn clear_wakeup_modes(&self, recv_mode: Option<u32>, send_mode: Option<u32>) {
        let materialized_guard = self.materialized.read().unwrap();
        if let Some(materialized) = materialized_guard.as_ref() {
            if let Some(mode) = recv_mode {
                materialized.recv_ready.clear_wakeup_mode(mode);
            }
            if let Some(mode) = send_mode {
                materialized.send_ready.clear_wakeup_mode(mode);
            }
        }
    }

    #[cfg(windows)]
    pub fn mark_recv_wakeup_drain_complete(&self) {
        let materialized_guard = self.materialized.read().unwrap();
        if let Some(materialized) = materialized_guard.as_ref() {
            materialized.recv_ready.mark_drain_complete();
        }
    }

    #[cfg(windows)]
    pub fn mark_send_wakeup_drain_complete(&self) {
        let materialized_guard = self.materialized.read().unwrap();
        if let Some(materialized) = materialized_guard.as_ref() {
            materialized.send_ready.mark_drain_complete();
        }
    }
}

/// Monitor event stream returned by `Socket.monitor()`. Delivers
/// `MonitorEvent` dicts (or raises on lag / close). Thread-safe:
/// the underlying flume channel is `Send + Sync`, so `Monitor` can
/// be passed between Python threads.
#[pyclass(module = "pyomq._native")]
pub struct Monitor {
    rx: flume::Receiver<MonitorEvent>,
    lagged: Arc<AtomicU64>,
}

impl Monitor {
    pub(crate) fn from_stream(
        ctx: &Arc<ContextInner>,
        mut stream: omq_tokio::MonitorStream,
    ) -> Self {
        let (tx, rx) = flume::unbounded();
        let lagged = Arc::new(AtomicU64::new(0));
        let lagged2 = lagged.clone();
        ctx.runtime_handle()
            .expect("pyomq: context terminated")
            .spawn(async move {
                loop {
                    match stream.recv().await {
                        Ok(ev) => {
                            if tx.send(ev).is_err() {
                                break;
                            }
                        }
                        Err(omq_tokio::MonitorRecvError::Lagged(n)) => {
                            lagged2.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            });
        Self { rx, lagged }
    }
}

fn monitor_event_to_dict<'py>(py: Python<'py>, ev: &MonitorEvent) -> PyResult<Bound<'py, PyAny>> {
    let d = PyDict::new(py);
    match ev {
        MonitorEvent::Listening { endpoint } => {
            d.set_item("event", "listening")?;
            d.set_item("endpoint", endpoint.to_string())?;
        }
        MonitorEvent::Accepted {
            endpoint,
            connection_id,
            ..
        } => {
            d.set_item("event", "accepted")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", connection_id)?;
        }
        MonitorEvent::Connected {
            endpoint,
            connection_id,
            ..
        } => {
            d.set_item("event", "connected")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", connection_id)?;
        }
        MonitorEvent::HandshakeSucceeded { endpoint, peer } => {
            d.set_item("event", "handshake_succeeded")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
            if let Some(id) = &peer.peer_identity {
                d.set_item("peer_identity", PyBytes::new(py, id))?
            }
        }
        MonitorEvent::HandshakeFailed {
            endpoint, reason, ..
        } => {
            d.set_item("event", "handshake_failed")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("reason", reason.as_str())?;
        }
        MonitorEvent::ConnectDelayed {
            endpoint, attempt, ..
        } => {
            d.set_item("event", "connect_delayed")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("attempt", attempt)?;
        }
        MonitorEvent::Disconnected { endpoint, peer, .. } => {
            d.set_item("event", "disconnected")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
        }
        MonitorEvent::PeerCommand { endpoint, peer, .. } => {
            d.set_item("event", "peer_command")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
        }
        MonitorEvent::Closed => {
            d.set_item("event", "closed")?;
        }
        _ => {
            d.set_item("event", "unknown")?;
        }
    }
    Ok(d.into_any())
}

#[pymethods]
impl Monitor {
    /// Receive the next monitor event. Blocks until an event arrives.
    ///
    /// Raises `zmq.Again` on timeout (if `timeout_ms >= 0`).
    /// Returns a dict with at minimum `{"event": "<name>"}` plus
    /// event-specific keys (`endpoint`, `connection_id`, etc.).
    #[pyo3(signature = (timeout_ms = -1))]
    fn recv<'py>(&self, py: Python<'py>, timeout_ms: i64) -> PyResult<Bound<'py, PyAny>> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            let d = PyDict::new(py);
            d.set_item("event", "lagged")?;
            d.set_item("count", n)?;
            return Ok(d.into_any());
        }
        let ev = py.detach(|| {
            if timeout_ms < 0 {
                self.rx.recv().map_err(|_| ())
            } else {
                self.rx
                    .recv_timeout(Duration::from_millis(timeout_ms as u64))
                    .map_err(|_| ())
            }
        });
        match ev {
            Ok(ev) => monitor_event_to_dict(py, &ev),
            Err(()) => Err(timeout_err()),
        }
    }

    /// Try to receive without blocking. Raises `zmq.Again` if no event
    /// is available.
    fn recv_nowait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            let d = PyDict::new(py);
            d.set_item("event", "lagged")?;
            d.set_item("count", n)?;
            return Ok(d.into_any());
        }
        match self.rx.try_recv() {
            Ok(ev) => monitor_event_to_dict(py, &ev),
            Err(_) => Err(timeout_err()),
        }
    }
}

pub(crate) fn connection_status_to_dict<'py>(
    py: Python<'py>,
    cs: &omq_tokio::ConnectionStatus,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("connection_id", cs.connection_id)?;
    d.set_item("endpoint", cs.endpoint.to_string())?;
    d.set_item("identity", PyBytes::new(py, &cs.identity))?;
    Ok(d)
}

#[pyclass(module = "pyomq._native")]
pub struct Socket {
    pub(crate) inner: Arc<SocketInner>,
}

impl Socket {
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
impl Socket {
    fn socket_id(&self) -> PyResult<u64> {
        self.inner.ensure_blocking_id()
    }

    fn bind(&self, py: Python<'_>, endpoint: &str) -> PyResult<String> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let bound = dispatch::blocking_string(&self.inner, py, move |s| {
            let bound = s.bind(ep.clone())?;
            Ok(bound.to_string())
        })?;
        if let Ok(endpoint) = SocketInner::parse_endpoint(&bound) {
            if endpoint.to_string().starts_with("tcp://") {
                self.inner.has_tcp_endpoint.store(true, Ordering::Release);
            }
            self.inner.endpoints.lock().unwrap().push((endpoint, true));
        }
        Ok(bound)
    }

    fn connect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        let subscriptions = self.inner.subscriptions.lock().unwrap().clone();
        let recorded_ep = ep.clone();
        dispatch::blocking_unit(&self.inner, py, move |s| {
            s.connect(ep)?;
            for prefix in subscriptions {
                s.subscribe(prefix)?;
            }
            Ok(())
        })?;
        let is_tcp = recorded_ep.to_string().starts_with("tcp://");
        self.inner
            .endpoints
            .lock()
            .unwrap()
            .push((recorded_ep, false));
        if is_tcp {
            self.inner.has_tcp_endpoint.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn unbind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::blocking_unit(&self.inner, py, move |s| s.unbind(ep))
    }

    fn disconnect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::blocking_unit(&self.inner, py, move |s| s.disconnect(ep))
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
    fn send_multipart(&self, py: Python<'_>, parts: &Bound<'_, PyAny>, flags: i32) -> PyResult<()> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        self.send_message(py, msg)
    }

    #[pyo3(signature = (flags = 0))]
    fn recv<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyBytes>> {
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return Ok(PyBytes::new(py, &head));
        }
        let msg = if flags & crate::constants::NOBLOCK != 0 {
            self.try_recv_message()?
        } else {
            self.recv_message(py)?
        };
        let mut parts: Vec<Bytes> = msg.iter().collect();
        let head = if parts.is_empty() {
            Bytes::new()
        } else {
            parts.remove(0)
        };
        if !parts.is_empty() {
            self.inner.store_rxbuf(parts);
        }
        Ok(PyBytes::new(py, &head))
    }

    #[pyo3(signature = (flags = 0))]
    fn recv_multipart<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyList>> {
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            return PyList::new(py, leftover.into_iter().map(|b| PyBytes::new(py, &b)));
        }
        let msg = if flags & crate::constants::NOBLOCK != 0 {
            self.try_recv_message()?
        } else {
            self.recv_message(py)?
        };
        conversions::parts_to_pylist(py, msg)
    }

    fn subscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::blocking_unit(&self.inner, py, move |s| s.subscribe(bytes))
    }

    fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::blocking_unit(&self.inner, py, move |s| s.unsubscribe(bytes))
    }

    fn join(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::blocking_unit(&self.inner, py, move |s| s.join(bytes))
    }

    fn leave(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::blocking_unit(&self.inner, py, move |s| s.leave(bytes))
    }

    /// Return a list of dicts describing every live peer connection.
    fn connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let statuses: Vec<omq_tokio::ConnectionStatus> = py.detach(|| {
            self.inner
                .ensure_blocking_socket()
                .map(|s| s.connections().unwrap_or_default())
        })?;
        // Temporary allocation to be able to propagate errors
        let temp = statuses
            .iter()
            .map(|cs| crate::socket::connection_status_to_dict(py, cs))
            .collect::<PyResult<Vec<Bound<'py, PyDict>>>>()?;
        PyList::new(py, temp)
    }

    /// Return a dict for the peer with the given `connection_id`, or
    /// `None` if no such peer is currently connected.
    fn connection_info<'py>(
        &self,
        py: Python<'py>,
        connection_id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let status: Option<omq_tokio::ConnectionStatus> = py.detach(|| {
            self.inner
                .ensure_blocking_socket()
                .ok()
                .and_then(|s| s.connection_info(connection_id).ok().flatten())
        });
        match status {
            Some(cs) => connection_status_to_dict(py, &cs).map(|d| d.into_any()),
            None => Ok(py.None().bind(py).clone()),
        }
    }

    /// Return a `Monitor` that delivers connection-lifecycle events for
    /// this socket. Multiple monitors can be active simultaneously.
    fn monitor(&self, _py: Python<'_>) -> PyResult<Monitor> {
        let sock = self.inner.ensure_blocking_socket()?;
        let stream = sock.monitor();
        Ok(Monitor::from_stream(&self.inner.ctx, stream))
    }

    fn setsockopt(&self, py: Python<'_>, option: i32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(&self, py: Python<'py>, option: i32) -> PyResult<Bound<'py, PyAny>> {
        options::getsockopt(self.inner.as_ref(), py, option)
    }

    #[cfg(feature = "curve")]
    fn set_curve_auth(&self, auth: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::auth::set_curve_auth_impl(&self.inner, auth)
    }

    #[pyo3(signature = (linger=None))]
    fn close(&self, py: Python<'_>, linger: Option<i64>) -> PyResult<()> {
        let linger = self.inner.close_linger(linger);
        if let Some(m) = self.inner.take_blocking_materialized() {
            py.detach(|| m.socket.close_with_linger(linger))
                .map_err(map_err)?;
        }
        if let Some(m) = self.inner.take_materialized() {
            let ctx = self.inner.ctx.clone();
            py.detach(|| {
                ctx.destroy_socket(m.socket, m.send_prod, m.send_pump, m.recv_pump, linger)
            });
        }
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
    fn send_message(&self, py: Python<'_>, msg: omq_tokio::Message) -> PyResult<()> {
        let sock = self.inner.ensure_blocking_socket()?;
        let timeout = self.inner.overlay.lock().unwrap().sndtimeo;
        let post_fork = self.inner.post_fork.swap(false, Ordering::AcqRel);
        if post_fork {
            let tcp = self.inner.has_tcp_endpoint.load(Ordering::Acquire);
            py.detach(|| {
                if tcp {
                    let _ = sock.wait_connected(1, Duration::from_secs(1));
                }
                std::thread::sleep(Duration::from_millis(100));
            });
        }

        match sock.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Closed) => Err(map_err(PError::Closed)),
            Err(TrySendError::Error(e)) => Err(map_err(e)),
            Err(TrySendError::Full(msg)) => py.detach(|| match timeout {
                None => sock.send(msg).map_err(map_err),
                Some(timeout) => {
                    let deadline = Instant::now() + timeout;
                    let mut msg = msg;
                    loop {
                        match sock.try_send(msg) {
                            Ok(()) => return Ok(()),
                            Err(TrySendError::Full(returned)) => msg = returned,
                            Err(TrySendError::Closed) => return Err(map_err(PError::Closed)),
                            Err(TrySendError::Error(e)) => return Err(map_err(e)),
                        }
                        if Instant::now() >= deadline {
                            return Err(timeout_err());
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            }),
        }
    }

    fn recv_message(&self, py: Python<'_>) -> PyResult<omq_tokio::Message> {
        let cached = {
            let mut msgs = self.inner.rxmsgs.lock().unwrap();
            if msgs.is_empty() {
                None
            } else {
                Some(msgs.remove(0))
            }
        };
        if let Some(msg) = cached {
            return Ok(msg);
        }
        let sock = self.inner.ensure_blocking_socket()?;
        let timeout = self.inner.overlay.lock().unwrap().rcvtimeo;
        let parent_fork = self.inner.parent_fork_gen.load(Ordering::Acquire)
            != PARENT_FORK_GEN.load(Ordering::Acquire);
        if parent_fork {
            self.inner
                .parent_fork_gen
                .store(PARENT_FORK_GEN.load(Ordering::Acquire), Ordering::Release);
        }
        let post_fork_recv = self.inner.post_fork.load(Ordering::Acquire)
            || FORKED.load(Ordering::Acquire)
            || parent_fork;
        let tcp = self.inner.has_tcp_endpoint.load(Ordering::Acquire);
        if matches!(self.inner.socket_type, omq_tokio::SocketType::Pull) && post_fork_recv && tcp {
            let wait = timeout.unwrap_or(Duration::from_secs(1));
            let _ = sock.wait_connected(1, wait);
            let _ = sock.connections();
        }
        // Probe the backend pipe while holding the GIL. This is safe for
        // sockets with any mix of inproc, ipc, and tcp endpoints.
        if !post_fork_recv {
            match sock.try_recv() {
                Ok(msg) => return Ok(msg),
                Err(omq_proto::error::Error::Closed) => {
                    return Err(map_err(omq_proto::error::Error::Closed));
                }
                Err(_) => {}
            }
        }
        py.detach(|| match timeout {
            None if !post_fork_recv => sock.recv().map_err(map_err),
            None => loop {
                match sock.try_recv() {
                    Ok(msg) => {
                        self.inner.post_fork.store(false, Ordering::Release);
                        break Ok(msg);
                    }
                    Err(omq_proto::error::Error::Closed) => {
                        break Err(map_err(omq_proto::error::Error::Closed));
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(1)),
                }
            },
            Some(timeout) => {
                let deadline = Instant::now() + timeout;
                loop {
                    match sock.try_recv() {
                        Ok(msg) => return Ok(msg),
                        Err(omq_proto::error::Error::Closed) => {
                            return Err(map_err(omq_proto::error::Error::Closed));
                        }
                        Err(_) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => return Err(timeout_err()),
                    }
                }
            }
        })
    }

    fn try_recv_message(&self) -> PyResult<omq_tokio::Message> {
        let cached = {
            let mut msgs = self.inner.rxmsgs.lock().unwrap();
            if msgs.is_empty() {
                None
            } else {
                Some(msgs.remove(0))
            }
        };
        if let Some(msg) = cached {
            return Ok(msg);
        }
        let sock = self.inner.ensure_blocking_socket()?;
        sock.try_recv().map_err(map_err)
    }
}
