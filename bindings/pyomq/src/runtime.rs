//! Per-context tokio runtime on a dedicated background thread.
//!
//! Each `ContextInner` owns an `omq_tokio::Context` which manages
//! the tokio runtime and background thread. `term()` shuts it down
//! (aborts all pumps, drops the handle).
//!
//! omq-tokio::Socket is Send + Sync, so Python-side wrappers hold an
//! Arc<Socket> directly in SocketInner. However, the socket's internal
//! driver tasks (ConnectionDriver, actor loop) are spawned via
//! tokio::spawn and need the tokio scheduler actively polling to make
//! progress. Python threads have no tokio runtime context, so they
//! cannot call socket.send()/recv() directly.
//!
//! Asyncio sockets use a yring relay. Synchronous sockets use the
//! blocking API from `omq_tokio`, which owns its receive pipe and IO
//! thread directly.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
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
}

impl ContextInner {
    pub fn new(io_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            io_threads: io_threads.max(1),
            state: Mutex::new(None),
            terminated: AtomicBool::new(false),
        })
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
            return Ok(rt.ctx.handle().clone());
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

    pub fn runtime_handle(&self) -> PyResult<Handle> {
        self.ensure_runtime()
    }

    /// Build a socket using omq-tokio's native blocking adapter.
    pub fn materialize_blocking(
        &self,
        socket_type: omq_tokio::SocketType,
        options: omq_tokio::Options,
    ) -> PyResult<(u64, omq_tokio::blocking::Socket)> {
        let handle = self.ensure_runtime()?;
        let ctx = self
            .state
            .lock()
            .unwrap()
            .as_ref()
            .expect("runtime initialized")
            .ctx
            .clone();
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
        send_cons: yring::AsyncConsumer<omq_tokio::Message>,
        mut recv_prod: yring::Producer<omq_tokio::Message>,
        recv_ready: Arc<ReadinessSignal>,
        send_ready: Arc<ReadinessSignal>,
        recv_space: Arc<omq_tokio::engine::StateSignal>,
    ) -> PyResult<(u64, Arc<InnerSocket>, JoinHandle<()>, JoinHandle<()>)> {
        let handle = self.ensure_runtime()?;
        let (otx, orx) = flume::bounded(1);
        let recv_all_signal = global_recv_signal();
        handle.spawn(async move {
            let id = next_id();
            let sock = Arc::new(InnerSocket::new(socket_type, options));

            const SEND_YIELD_INTERVAL: u32 = 256;
            let send_socket = sock.clone();
            let send_pump = tokio::spawn(async move {
                futures::pin_mut!(send_cons);
                let mut batch = 0u32;
                while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
                    let _ = send_socket.send(msg).await;
                    send_cons.as_mut().get_mut().release();
                    send_ready.signal();
                    batch += 1;
                    if batch >= SEND_YIELD_INTERVAL {
                        batch = 0;
                        tokio::task::yield_now().await;
                    }
                }
                send_ready.signal();
            });

            let recv_socket = sock.clone();
            let recv_pump = tokio::spawn(async move {
                while let Ok(msg) = recv_socket.recv().await {
                    let mut pending_msg = msg;
                    loop {
                        match recv_prod.push(pending_msg) {
                            Ok(()) => {
                                recv_prod.flush();
                                recv_ready.signal();
                                recv_all_signal.signal();
                                break;
                            }
                            Err(returned) => {
                                pending_msg = returned;
                                let seen = recv_space.generation();
                                let changed = recv_space.changed_after(seen);
                                tokio::pin!(changed);
                                match recv_prod.push(pending_msg) {
                                    Ok(()) => {
                                        recv_prod.flush();
                                        recv_ready.signal();
                                        recv_all_signal.signal();
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
        sock: Arc<InnerSocket>,
        send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
        send_pump: JoinHandle<()>,
        recv_pump: JoinHandle<()>,
        linger: Option<Duration>,
    ) {
        recv_pump.abort();
        drop(send_prod);
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
            let s = Arc::try_unwrap(sock).unwrap_or_else(|arc| (*arc).clone());
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
            s.ctx.term();
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
            s.ctx.term();
        }
    }
}

#[allow(dead_code)]
fn drain_recv_ring(inner: &Arc<crate::socket::SocketInner>) -> Vec<omq_tokio::Message> {
    let materialized_guard = inner.materialized.read().unwrap();
    let Some(materialized) = materialized_guard.as_ref() else {
        return vec![];
    };
    let mut cons = materialized.recv_cons.lock().unwrap();
    let mut msgs = Vec::new();
    while let Some(msg) = cons.prefetch_and_pop() {
        msgs.push(msg);
    }
    if !msgs.is_empty() {
        materialized.recv_space.notify_changed();
    }
    msgs
}

#[allow(dead_code)]
fn push_to_capture(cap: &Arc<crate::socket::SocketInner>, msg: &omq_tokio::Message) {
    let copy = omq_tokio::Message::multipart(msg.iter());
    let materialized_guard = cap.materialized.read().unwrap();
    if let Some(materialized) = materialized_guard.as_ref() {
        let mut prod = materialized.send_prod.lock().unwrap();
        let _ = prod.push_and_flush(copy);
    }
}

/// Run a forwarding proxy between two sockets on the tokio thread.
#[allow(dead_code)]
pub fn proxy(
    ctx: &Arc<ContextInner>,
    fe_inner: Arc<crate::socket::SocketInner>,
    be_inner: Arc<crate::socket::SocketInner>,
    cap_inner: Option<Arc<crate::socket::SocketInner>>,
    ctrl_inner: Option<Arc<crate::socket::SocketInner>>,
) {
    let fe_materialized_guard = fe_inner.materialized.read().unwrap();
    let fe_materialized = fe_materialized_guard.as_ref().unwrap();
    fe_materialized.send_pump.abort();
    fe_materialized.recv_pump.abort();
    let fe_sock = fe_materialized.socket.clone();
    drop(fe_materialized_guard);

    let be_materialized_guard = be_inner.materialized.read().unwrap();
    let be_materialized = be_materialized_guard.as_ref().unwrap();
    be_materialized.send_pump.abort();
    be_materialized.recv_pump.abort();
    let be_sock = be_materialized.socket.clone();
    drop(be_materialized_guard);

    let ctrl_sock = ctrl_inner.as_ref().map(|ctrl| {
        let materialized_guard = ctrl.materialized.read().unwrap();
        let materialized = materialized_guard.as_ref().unwrap();
        materialized.send_pump.abort();
        materialized.recv_pump.abort();
        materialized.socket.clone()
    });

    let fe_drained = drain_recv_ring(&fe_inner);
    let be_drained = drain_recv_ring(&be_inner);

    ctx.spawn_blocking(async move {
        for msg in fe_drained {
            if let Some(ref cap) = cap_inner {
                push_to_capture(cap, &msg);
            }
            if be_sock.send(msg).await.is_err() {
                return;
            }
        }
        for msg in be_drained {
            if let Some(ref cap) = cap_inner {
                push_to_capture(cap, &msg);
            }
            if fe_sock.send(msg).await.is_err() {
                return;
            }
        }

        proxy_loop(&fe_sock, &be_sock, &cap_inner, &ctrl_sock).await;
    });
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
    for (side, socket) in [(0_u8, fe.clone()), (1, be.clone())] {
        let tx = tx.clone();
        std::thread::spawn(move || {
            while let Ok(msg) = socket.recv() {
                if tx.send((side, msg)).is_err() {
                    break;
                }
            }
        });
    }
    if let Some(socket) = ctrl.clone() {
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

#[allow(dead_code)]
const PROXY_BATCH: usize = 64;

#[allow(dead_code)]
async fn proxy_drain_and_forward(
    from: &Arc<InnerSocket>,
    to: &Arc<InnerSocket>,
    first: omq_tokio::Message,
    cap: &Option<Arc<crate::socket::SocketInner>>,
) -> bool {
    if let Some(c) = cap {
        push_to_capture(c, &first);
    }
    if to.send(first).await.is_err() {
        return false;
    }
    for _ in 1..PROXY_BATCH {
        let Ok(msg) = from.try_recv() else { break };
        if let Some(c) = cap {
            push_to_capture(c, &msg);
        }
        if to.send(msg).await.is_err() {
            return false;
        }
    }
    true
}

#[allow(dead_code)]
async fn proxy_loop(
    fe: &Arc<InnerSocket>,
    be: &Arc<InnerSocket>,
    cap: &Option<Arc<crate::socket::SocketInner>>,
    ctrl: &Option<Arc<InnerSocket>>,
) {
    loop {
        enum Action {
            FeToBe(omq_tokio::Message),
            BeToFe(omq_tokio::Message),
            Control(omq_tokio::Message),
            Done,
        }

        let action = if let Some(ctrl_sock) = ctrl {
            futures::select! {
                msg = fe.recv().fuse() => match msg {
                    Ok(m) => Action::FeToBe(m),
                    Err(_) => Action::Done,
                },
                msg = be.recv().fuse() => match msg {
                    Ok(m) => Action::BeToFe(m),
                    Err(_) => Action::Done,
                },
                msg = ctrl_sock.recv().fuse() => match msg {
                    Ok(m) => Action::Control(m),
                    Err(_) => Action::Done,
                },
            }
        } else {
            futures::select! {
                msg = fe.recv().fuse() => match msg {
                    Ok(m) => Action::FeToBe(m),
                    Err(_) => Action::Done,
                },
                msg = be.recv().fuse() => match msg {
                    Ok(m) => Action::BeToFe(m),
                    Err(_) => Action::Done,
                },
            }
        };

        match action {
            Action::FeToBe(msg) => {
                if !proxy_drain_and_forward(fe, be, msg, cap).await {
                    return;
                }
            }
            Action::BeToFe(msg) => {
                if !proxy_drain_and_forward(be, fe, msg, cap).await {
                    return;
                }
            }
            Action::Control(msg) => {
                let cmd: Vec<u8> = msg.iter().next().unwrap_or_default().to_vec();
                match cmd.as_slice() {
                    b"TERMINATE" | b"KILL" => return,
                    b"PAUSE" => {
                        if let Some(ctrl_sock) = ctrl {
                            loop {
                                let Ok(m) = ctrl_sock.recv().await else {
                                    return;
                                };
                                let c: Vec<u8> = m.iter().next().unwrap_or_default().to_vec();
                                match c.as_slice() {
                                    b"RESUME" => break,
                                    b"TERMINATE" | b"KILL" => return,
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Action::Done => return,
        }
    }
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
                    let cons = materialized.recv_cons.lock().unwrap();
                    !cons.is_empty()
                } else {
                    drop(materialized_guard);
                    let Ok(sock) = inner.ensure_blocking_socket() else {
                        return false;
                    };
                    match sock.try_recv() {
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
    let deadline = timeout_ms.map(|ms| std::time::Instant::now() + Duration::from_millis(ms));

    recv_signal.park_begin();
    let ready = poll_ready(&sockets);
    if !ready.is_empty() {
        recv_signal.park_end();
        return ready;
    }

    loop {
        let wait_dur = match deadline {
            Some(d) => {
                let now = std::time::Instant::now();
                if now >= d {
                    recv_signal.park_end();
                    return vec![];
                }
                d - now
            }
            None => Duration::from_millis(100),
        };

        // Native blocking sockets have thread wakeups, not an eventfd.
        // Poll their try-recv path periodically while retaining the
        // eventfd fast path for asyncio sockets.
        recv_signal.wait_timeout(wait_dur.min(Duration::from_millis(10)));

        let ready = poll_ready(&sockets);
        if !ready.is_empty() {
            recv_signal.park_end();
            return ready;
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            recv_signal.park_end();
            return vec![];
        }
    }
}
