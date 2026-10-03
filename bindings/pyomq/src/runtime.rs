//! Per-context tokio runtime on a dedicated background thread.
//!
//! Each `ContextInner` owns an `omq_tokio::Context` which manages
//! the tokio runtime and background thread. `term()` shuts it down
//! (aborts all pumps, drops the handle).
//!
//! Python async wrappers call native try_send and drain external receive sinks
//! directly where eligible. Bounded workers retain pre-ready/backpressured
//! sends and fallback receives. Synchronous wrappers use the native blocking
//! API. Driver tasks and control operations still run on the context runtime.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use omq_tokio::Socket as InnerSocket;
use pyo3::prelude::*;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::notify::ReadinessSignal;

struct RuntimeState {
    pid: u32,
    ctx: omq_tokio::Context,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

static GLOBAL_RECV_SIGNAL: Mutex<Option<(u32, Arc<ReadinessSignal>)>> = Mutex::new(None);

/// Process-global recv signal for `wait_any`. Recv pumps from all
/// contexts signal this after pushing a message; `wait_any` parks on it.
/// Recreated after fork (PID guard).
pub(crate) fn global_recv_signal() -> Arc<ReadinessSignal> {
    let mut guard = GLOBAL_RECV_SIGNAL.lock().unwrap();
    let pid = std::process::id();
    if let Some((cached_pid, signal)) = guard.as_ref()
        && *cached_pid == pid
    {
        return signal.clone();
    }
    let signal = Arc::new(ReadinessSignal::new());
    *guard = Some((pid, signal.clone()));
    signal
}

/// Allocate the next socket id. Strictly monotonic; never recycled.
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) struct ContextInner {
    io_threads: usize,
    state: Mutex<Option<RuntimeState>>,
    terminated: AtomicBool,
    owns_runtime: bool,
}

impl ContextInner {
    pub fn new(io_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            io_threads: io_threads.max(1),
            state: Mutex::new(None),
            terminated: AtomicBool::new(false),
            owns_runtime: true,
        })
    }

    pub fn from_shared_context(ctx: omq_tokio::Context) -> Arc<Self> {
        let io_threads = ctx.io_threads().max(1);
        Arc::new(Self {
            io_threads,
            state: Mutex::new(Some(RuntimeState {
                pid: std::process::id(),
                ctx,
            })),
            terminated: AtomicBool::new(false),
            owns_runtime: false,
        })
    }

    pub fn share_key(&self) -> PyResult<u128> {
        Ok(self.runtime_context()?.share_key())
    }

    pub fn from_share_key(share_key: u128) -> PyResult<Arc<Self>> {
        let ctx = omq_tokio::Context::from_share_key(share_key)
            .ok_or_else(|| crate::error::map_err(omq_proto::error::Error::Closed))?;
        Ok(Self::from_shared_context(ctx))
    }

    fn ensure_runtime(&self) -> PyResult<Handle> {
        if self.terminated.load(Ordering::Acquire) {
            return Err(crate::error::map_err(omq_proto::error::Error::Closed));
        }
        let mut guard = self.state.lock().unwrap();
        let pid = std::process::id();
        if let Some(rt) = guard.as_ref()
            && rt.pid == pid
        {
            if rt.ctx.is_terminated() {
                return Err(crate::error::map_err(omq_proto::error::Error::Closed));
            }
            return Ok(rt.ctx.handle().clone());
        }
        if !self.owns_runtime {
            return Err(crate::error::map_err(omq_proto::error::Error::Closed));
        }
        let ctx = omq_tokio::Context::with_config(omq_tokio::ContextConfig {
            io_threads: self.io_threads,
        });
        let handle = ctx.handle().clone();
        if let Some(stale) = guard.take() {
            // In a forked child the inherited runtime threads no longer
            // exist. Do not drop the context and try to join them.
            std::mem::forget(stale);
        }
        *guard = Some(RuntimeState { pid, ctx });
        Ok(handle)
    }

    pub fn can_drive_runtime(&self) -> bool {
        if self.terminated.load(Ordering::Acquire) {
            return false;
        }
        self.state
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|rt| rt.pid == std::process::id() && !rt.ctx.is_terminated())
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
            || self
                .state
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|rt| rt.ctx.is_terminated())
    }

    pub fn runtime_handle(&self) -> PyResult<Handle> {
        self.ensure_runtime()
    }

    pub fn runtime_context(&self) -> PyResult<omq_tokio::Context> {
        let _ = self.ensure_runtime()?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .as_ref()
            .expect("runtime initialized")
            .ctx
            .clone())
    }

    /// Build a socket using omq-tokio's native blocking adapter.
    pub fn materialize_blocking(
        &self,
        socket_type: omq_tokio::SocketType,
        options: omq_tokio::Options,
    ) -> PyResult<(u64, omq_tokio::blocking::Socket)> {
        let ctx = self.runtime_context()?;
        let handle = ctx.handle().clone();
        let (otx, orx) = flume::bounded(1);
        handle.spawn(async move {
            let id = next_id();
            let _ = otx.send((id, ctx.blocking_socket(socket_type, options)));
        });
        Ok(Python::attach(|py| {
            py.detach(|| orx.recv().expect("pyomq: runtime dropped result"))
        }))
    }

    /// Spawn a Send future on the tokio runtime and block until it completes.
    pub fn spawn_blocking<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = self.runtime_handle().expect("pyomq: context terminated");
        let (otx, orx) = flume::bounded::<T>(1);
        handle.spawn(async move {
            let out = fut.await;
            let _ = otx.send(out);
        });
        Python::attach(|py| py.detach(|| orx.recv().expect("pyomq: runtime dropped result")))
    }

    /// Build a socket on the tokio thread, spawn per-socket send/recv pumps,
    /// and return the socket Arc and its id.
    #[expect(clippy::too_many_arguments, clippy::type_complexity)]
    pub fn materialize(
        &self,
        socket_type: omq_tokio::SocketType,
        options: omq_tokio::Options,
        send_queue: Arc<crate::send::SendQueue>,
        send_cons: yring::AsyncConsumer<omq_tokio::Message>,
        send_ready: Arc<ReadinessSignal>,
        mut recv_prod: yring::Producer<omq_tokio::Message>,
        recv_ready: Arc<ReadinessSignal>,
        recv_space: Arc<omq_tokio::engine::StateSignal>,
        recv_config: Option<Arc<omq_tokio::engine::RecvSinkConfig>>,
    ) -> PyResult<(u64, Arc<InnerSocket>, JoinHandle<()>, JoinHandle<()>)> {
        let ctx = self.runtime_context()?;
        let handle = ctx.handle().clone();
        let (otx, orx) = flume::bounded(1);
        let recv_all_signal = global_recv_signal();
        handle.spawn(async move {
            let id = next_id();
            let sock = Arc::new(match recv_config {
                Some(config) => ctx.socket_with_recv_sink_config(socket_type, options, config),
                None => ctx.socket(socket_type, options),
            });

            let send_socket = sock.clone();
            let send_recv_ready = recv_ready.clone();
            let send_all_ready = recv_all_signal.clone();
            let send_pump = tokio::spawn(async move {
                futures::pin_mut!(send_cons);
                let mut count = 0;
                let mut bytes = 0;
                while let Some(message) = futures::StreamExt::next(&mut send_cons).await {
                    bytes += message.byte_len();
                    count += 1;
                    let _ = send_socket.send(message).await;
                    send_queue.complete();
                    if matches!(
                        send_socket.socket_type(),
                        omq_tokio::SocketType::Req | omq_tokio::SocketType::Rep
                    ) {
                        send_recv_ready.signal();
                        send_all_ready.signal();
                    }
                    if count >= 256 || bytes >= 1024 * 1024 {
                        count = 0;
                        bytes = 0;
                        tokio::task::yield_now().await;
                    }
                }
                send_ready.signal();
            });
            let recv_socket = sock.clone();
            let recv_pump = tokio::spawn(async move {
                let mut count = 0;
                let mut bytes = 0;
                while let Ok(msg) = recv_socket.recv_for_external_recv().await {
                    bytes += msg.byte_len();
                    count += 1;
                    let mut pending_msg = msg;
                    loop {
                        match recv_prod.push(pending_msg) {
                            Ok(()) => {
                                if matches!(
                                    recv_prod.flush_and_check(),
                                    yring::FlushResult::Flushed {
                                        was_empty: true,
                                        ..
                                    }
                                ) {
                                    recv_ready.signal();
                                    recv_all_signal.signal();
                                }
                                break;
                            }
                            Err(returned) => {
                                pending_msg = returned;
                                let seen = recv_space.generation();
                                let changed = recv_space.changed_after(seen);
                                tokio::pin!(changed);
                                match recv_prod.push(pending_msg) {
                                    Ok(()) => {
                                        if matches!(
                                            recv_prod.flush_and_check(),
                                            yring::FlushResult::Flushed {
                                                was_empty: true,
                                                ..
                                            }
                                        ) {
                                            recv_ready.signal();
                                            recv_all_signal.signal();
                                        }
                                        break;
                                    }
                                    Err(returned2) => {
                                        pending_msg = returned2;
                                        changed.await;
                                    }
                                }
                            }
                        }
                    }
                    if count >= 256 || bytes >= 1024 * 1024 {
                        count = 0;
                        bytes = 0;
                        tokio::task::yield_now().await;
                    }
                }
            });

            let _ = otx.send((id, sock, send_pump, recv_pump));
        });
        Ok(Python::attach(|py| {
            py.detach(|| orx.recv().expect("pyomq: runtime dropped result"))
        }))
    }

    /// Close a socket: drain the send yring, then close with linger.
    ///
    /// If the context is already terminated, the runtime is gone and
    /// spawned tasks were aborted. Just drop the socket.
    pub fn destroy_socket(
        &self,
        materialized: crate::socket::Materialized,
        linger: Option<Duration>,
    ) {
        let crate::socket::Materialized {
            socket: sock,
            send_queue,
            send_pump,
            recv_pump,
            recv_wakeup,
            send_wakeup,
            ..
        } = materialized;
        recv_pump.abort();
        if let Some(wakeup) = recv_wakeup {
            wakeup.abort();
        }
        if let Some(wakeup) = send_wakeup {
            wakeup.abort();
        }
        send_queue.close();
        let handle = match self.runtime_handle() {
            Ok(h) => h,
            Err(_) => return,
        };
        let (otx, orx) = flume::bounded(1);
        handle.spawn(async move {
            let started = tokio::time::Instant::now();
            let _ = recv_pump.await;
            let mut send_pump = send_pump;
            match linger {
                Some(Duration::ZERO) => {
                    send_pump.abort();
                    let _ = send_pump.await;
                }
                Some(limit) => {
                    if tokio::time::timeout(limit, &mut send_pump).await.is_err() {
                        send_pump.abort();
                        let _ = send_pump.await;
                    }
                }
                None => {
                    let _ = send_pump.await;
                }
            }
            let linger = linger.map(|limit| limit.saturating_sub(started.elapsed()));
            let s = Arc::try_unwrap(sock).unwrap_or_else(|arc| (*arc).clone_shared());
            let _ = s.close_with_linger(linger).await;
            let _ = otx.send(());
        });
        let _ = orx.recv();
    }

    /// Run an async op against a socket and return the result.
    pub fn with_socket<F, Fut, T>(&self, sock: &Arc<InnerSocket>, op: F) -> T
    where
        F: FnOnce(Arc<InnerSocket>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let s = sock.clone();
        self.spawn_blocking(op(s))
    }

    /// Shut down this context's runtime. Delegates to
    /// `omq_tokio::Context::term()` which cancels the runtime's
    /// shutdown token and joins the background thread.
    pub fn term(&self) {
        self.terminated.store(true, Ordering::Release);
        let state = self.state.lock().unwrap().take();
        if let Some(s) = state {
            if self.owns_runtime && s.pid == std::process::id() {
                s.ctx.term();
            } else {
                drop_or_forget_foreign(s);
            }
        }
    }

    /// Bridge a Rust future to a Python `asyncio.Future`.
    pub fn tokio_future_into_py<'py, F>(
        &self,
        py: Python<'py>,
        fut: F,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        use pyo3::prelude::*;

        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        let py_future = event_loop.call_method0("create_future")?;
        let loop_handle: Py<PyAny> = event_loop.clone().unbind().into_any();
        let future_handle: Py<PyAny> = py_future.clone().unbind().into_any();

        self.runtime_handle()?.spawn(async move {
            let result = fut.await;
            Python::attach(|gil| {
                let loop_obj = loop_handle.bind(gil);
                let fut_obj = future_handle.bind(gil);
                let _ = match result {
                    Ok(value) => {
                        let setter = fut_obj.getattr("set_result")?;
                        loop_obj.call_method1("call_soon_threadsafe", (setter, value))
                    }
                    Err(e) => {
                        let setter = fut_obj.getattr("set_exception")?;
                        loop_obj.call_method1("call_soon_threadsafe", (setter, e.into_value(gil)))
                    }
                };
                PyResult::<()>::Ok(())
            })
            .ok();
        });

        Ok(py_future)
    }
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        if let Some(s) = self.state.get_mut().unwrap().take() {
            if self.owns_runtime && s.pid == std::process::id() {
                s.ctx.term();
            } else {
                drop_or_forget_foreign(s);
            }
        }
    }
}

fn drop_or_forget_foreign(state: RuntimeState) {
    if state.pid == std::process::id() {
        drop(state);
    } else {
        std::mem::forget(state);
    }
}

/// Forward messages using the native blocking sockets. The synchronous
/// Python proxy runs in its caller's thread, so this loop may block there.
#[allow(dead_code)]
pub fn blocking_proxy(
    fe_inner: Arc<crate::socket::SocketInner>,
    be_inner: Arc<crate::socket::SocketInner>,
    cap_inner: Option<Arc<crate::socket::SocketInner>>,
    ctrl_inner: Option<Arc<crate::socket::SocketInner>>,
) {
    let Ok(fe) = fe_inner.ensure_blocking_socket() else {
        return;
    };
    let Ok(be) = be_inner.ensure_blocking_socket() else {
        return;
    };
    let cap = cap_inner
        .as_ref()
        .and_then(|inner| inner.ensure_blocking_socket().ok());
    let ctrl = ctrl_inner
        .as_ref()
        .and_then(|inner| inner.ensure_blocking_socket().ok());

    let (tx, rx) = flume::unbounded();
    for (side, socket) in [(0_u8, fe.clone_shared()), (1, be.clone_shared())] {
        let tx = tx.clone();
        std::thread::spawn(move || {
            while let Ok(msg) = socket.recv() {
                if tx.send((side, msg)).is_err() {
                    break;
                }
            }
        });
    }
    if let Some(socket) = ctrl.as_ref().map(omq_tokio::blocking::Socket::clone_shared) {
        let tx = tx.clone();
        std::thread::spawn(move || {
            while let Ok(msg) = socket.recv() {
                if tx.send((2, msg)).is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);

    while let Ok((side, msg)) = rx.recv() {
        if side == 2 {
            let command: Vec<u8> = msg.iter().next().unwrap_or_default().to_vec();
            match command.as_slice() {
                b"TERMINATE" | b"KILL" => return,
                b"PAUSE" => loop {
                    let Ok((_, msg)) = rx.recv() else { return };
                    let command: Vec<u8> = msg.iter().next().unwrap_or_default().to_vec();
                    if command == b"RESUME" {
                        break;
                    }
                    if command == b"TERMINATE" || command == b"KILL" {
                        return;
                    }
                },
                _ => {}
            }
        } else if side == 0 {
            if let Some(capture) = &cap {
                let _ = capture.send(msg.clone());
            }
            if be.send(msg).is_err() {
                return;
            }
        } else {
            if let Some(capture) = &cap {
                let _ = capture.send(msg.clone());
            }
            if fe.send(msg).is_err() {
                return;
            }
        }
    }
}

pub fn proxy_handles(
    ctx: &Arc<ContextInner>,
    fe: omq_tokio::blocking::Socket,
    be: omq_tokio::blocking::Socket,
    cap: Option<omq_tokio::blocking::Socket>,
    ctrl: Option<omq_tokio::blocking::Socket>,
) -> omq_proto::error::Result<omq_tokio::proxy::ProxyExit> {
    let mut proxy = omq_tokio::Proxy::new(fe.into_async(), be.into_async());
    if let Some(cap) = cap {
        proxy = proxy.capture(cap.into_async());
    }
    if let Some(ctrl) = ctrl {
        proxy = proxy.control(ctrl.into_async());
    }
    ctx.spawn_blocking(async move { proxy.run().await })
}

/// Block the calling thread until at least one of the given sockets has
/// an inbound message ready (or until `timeout_ms` elapses).
pub fn wait_any(
    sockets: Vec<(u64, Arc<crate::socket::SocketInner>)>,
    timeout_ms: Option<u64>,
) -> Vec<u64> {
    if sockets.is_empty() {
        return vec![];
    }

    let poll_ready = |sockets: &[(u64, Arc<crate::socket::SocketInner>)]| -> Vec<u64> {
        sockets
            .iter()
            .filter(|(_, inner)| {
                if !inner.rxbuf.lock().unwrap().is_empty() {
                    return true;
                }
                if !inner.rxmsgs.lock().unwrap().is_empty() {
                    return true;
                }
                let materialized_guard = inner.materialized.read().unwrap();
                if let Some(materialized) = materialized_guard.as_ref() {
                    let mut consumers = materialized.recv_cons.lock().unwrap();
                    consumers.refresh(materialized.recv_config.as_ref());
                    consumers.has_data()
                } else {
                    drop(materialized_guard);
                    let Ok(sock) = inner.ensure_blocking_socket() else {
                        return false;
                    };
                    match sock.into_async().try_recv_for_external_recv() {
                        Ok(msg) => {
                            inner.rxmsgs.lock().unwrap().push(msg);
                            true
                        }
                        Err(_) => false,
                    }
                }
            })
            .map(|(id, _)| *id)
            .collect()
    };

    let ready = poll_ready(&sockets);
    if !ready.is_empty() {
        return ready;
    }

    let recv_signal = global_recv_signal();
    let deadline = timeout_ms.and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms)));

    recv_signal.park_begin();
    let ready = poll_ready(&sockets);
    if !ready.is_empty() {
        recv_signal.park_end();
        return ready;
    }

    loop {
        let wait_dur = match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    recv_signal.park_end();
                    return vec![];
                }
                d - now
            }
            None => Duration::from_millis(100),
        };

        // Native blocking sockets have thread wakeups, not a readiness fd.
        // Poll their try-recv path periodically while retaining the
        // readiness-fd fast path for asyncio sockets.
        recv_signal.wait_timeout(wait_dur.min(Duration::from_millis(10)));

        let ready = poll_ready(&sockets);
        if !ready.is_empty() {
            recv_signal.park_end();
            return ready;
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            recv_signal.park_end();
            return vec![];
        }
    }
}
