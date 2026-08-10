//! Socket handle and `zmq_socket`/close/bind/connect/unbind/disconnect.

use std::collections::VecDeque;
use std::ffi::{CStr, c_int, c_void};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use std::net::{IpAddr, Ipv6Addr};

use bytes::Bytes;
use omq_tokio::SocketType;
use omq_tokio::endpoint::{Endpoint, Host};
use omq_tokio::engine::StateSignal;

use std::os::raw::c_char;

use crate::context::{OmqContext, next_socket_id};
use crate::error::{ETERM, fail, map_omq_err, set_errno};
use crate::notify::{NotifyHandle, PlatformNotifyHandle, RecvNotify};
use crate::opts::{LingerSetting, SocketOverlay};

/// Rewrite `Host::Wildcard` to IPv6 unspecified (::) for dual-stack bind.
fn ipv6_rewrite_wildcard(ep: Endpoint) -> Endpoint {
    match ep {
        Endpoint::Tcp {
            host: Host::Wildcard,
            port,
        } => Endpoint::Tcp {
            host: Host::Ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            port,
        },
        other => other,
    }
}

// Default channel capacity (matches default HWM).
pub(crate) const DEFAULT_HWM: usize = 1000;

/// Dual yring consumers for the recv path.
#[derive(Debug)]
pub(crate) struct RecvConsumers {
    /// Filled directly by the first peer's `ConnectionDriver`.
    pub fast: yring::Consumer<omq_tokio::engine::RecvItem>,
    /// Filled by the recv pump task (fallback for second+ peers).
    pub pump: yring::Consumer<omq_tokio::engine::RecvItem>,
}

#[expect(dead_code)]
#[derive(Debug)]
pub(crate) struct OmqSocket {
    pub id: u64,
    pub ctx: Arc<OmqContext>,
    pub socket_type: SocketType,
    pub overlay: Mutex<SocketOverlay>,
    pub sndtimeo_ms: AtomicI64,
    pub rcvtimeo_ms: AtomicI64,
    /// Accumulator for SNDMORE multipart assembly.
    pub send_accum: crate::local_cell::LocalCell<Vec<Bytes>>,
    /// Lock-free inproc bypass (sender half). Set once during connect;
    /// accessed only from the `zmq_send` caller thread (ZMQ's single-thread
    /// contract per socket).
    pub bypass_send: crate::local_cell::LocalCell<Option<crate::inproc_bypass::BypassSend>>,
    /// Lock-free inproc bypass (receiver half).
    pub bypass_recv: crate::local_cell::LocalCell<Option<crate::inproc_bypass::BypassRecv>>,
    /// Leftover frames from a multipart recv (RCVMORE).
    pub recv_drain: Mutex<VecDeque<Bytes>>,
    /// True when `recv_drain` is non-empty. Checked without the lock so the
    /// mutex is skipped entirely on the common single-frame recv path.
    pub drain_nonempty: AtomicBool,
    /// Dual yring consumers. `fast` is filled directly by the first
    /// peer's `ConnectionDriver` (bypasses `async_channel` + recv pump).
    /// `pump` is filled by the recv pump task for second+ peers.
    /// Accessed only from the `zmq_recv` caller thread.
    pub recv_cons: crate::local_cell::LocalCell<Option<RecvConsumers>>,
    /// The inner omq-tokio socket. Send+Sync, stored directly.
    pub inner: std::sync::OnceLock<Arc<omq_tokio::Socket>>,
    /// Backpressure: recv pump waits on this when the recv ring is full.
    pub recv_space: std::sync::OnceLock<Arc<StateSignal>>,
    /// Shared config for recycling the recv fast yring on peer churn.
    pub recv_sink_config: std::sync::OnceLock<Arc<omq_tokio::engine::RecvSinkConfig>>,
    pub last_endpoint: Mutex<Option<String>>,
    pub notify: Arc<PlatformNotifyHandle>,
    pub bound_or_connected: AtomicBool,
    pub connect_count: AtomicUsize,
    pub recv_pump: std::sync::OnceLock<tokio::task::JoinHandle<()>>,
}

/// Map ZMQ socket-type integer to `SocketType`.
fn map_socket_type(t: c_int) -> Option<SocketType> {
    match t {
        0 => Some(SocketType::Pair),
        1 => Some(SocketType::Pub),
        2 => Some(SocketType::Sub),
        3 => Some(SocketType::Req),
        4 => Some(SocketType::Rep),
        5 => Some(SocketType::Dealer),
        6 => Some(SocketType::Router),
        7 => Some(SocketType::Pull),
        8 => Some(SocketType::Push),
        9 => Some(SocketType::XPub),
        10 => Some(SocketType::XSub),
        11 => Some(SocketType::Stream),
        12 => Some(SocketType::Server),
        13 => Some(SocketType::Client),
        14 => Some(SocketType::Radio),
        15 => Some(SocketType::Dish),
        16 => Some(SocketType::Gather),
        17 => Some(SocketType::Scatter),
        19 => Some(SocketType::Peer),
        20 => Some(SocketType::Channel),
        _ => None,
    }
}

/// Run an async op against the socket's inner Arc and return the result.
/// Spawns the future on the context's tokio runtime and blocks the
/// calling thread until completion.
pub(crate) fn with_socket<F, Fut, T>(
    ctx: &Arc<OmqContext>,
    inner: &Arc<omq_tokio::Socket>,
    op: F,
) -> Result<T, ()>
where
    F: FnOnce(Arc<omq_tokio::Socket>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (otx, orx) = flume::bounded::<T>(1);
    let s = inner.clone();
    let Some(handle) = ctx.handle() else {
        return Err(());
    };
    handle.spawn(async move {
        let out = op(s).await;
        let _ = otx.send(out);
    });
    orx.recv().map_err(|_| ())
}

fn is_bypass_eligible(a: SocketType, b: SocketType) -> bool {
    matches!(
        (a, b),
        (SocketType::Push, SocketType::Pull) | (SocketType::Pull, SocketType::Push)
    )
}

fn zero_io_inproc_supported(sock: &OmqSocket, endpoint: &Endpoint) -> bool {
    sock.ctx.zero_io_threads()
        && matches!(endpoint, Endpoint::Inproc { .. })
        && matches!(sock.socket_type, SocketType::Push | SocketType::Pull)
}

fn try_install_bypass(sender: &Arc<OmqSocket>, receiver: &Arc<OmqSocket>) {
    let capacity = {
        let Ok(s_ov) = sender.overlay.lock() else {
            return;
        };
        let Ok(r_ov) = receiver.overlay.lock() else {
            return;
        };
        let shwm = s_ov.send_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
        let rhwm = r_ov.recv_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
        shwm.min(rhwm).max(16)
    };

    let recv_notify = receiver.notify.recv_notifier();

    // Byte ring capacity: enough for `capacity` messages at a generous
    // average size. Rounded up to a power of two internally.
    let byte_ring_cap = capacity * 1024;
    let (bsend, brecv) = crate::inproc_bypass::create_bypass(byte_ring_cap, recv_notify);
    // SAFETY: bypass setup happens from bind/connect plumbing and must not
    // bind either socket's later app-facing owner thread.
    *unsafe { sender.bypass_send.get_unchecked() } = Some(bsend);
    // SAFETY: same setup phase as above.
    *unsafe { receiver.bypass_recv.get_unchecked() } = Some(brecv);
}

/// Register an inproc bind. If there are pending connectors, install
/// bypass pipes for eligible pairs.
fn register_inproc_bind(sock: &Arc<OmqSocket>, name: &str) {
    let ctx = &sock.ctx;
    let Ok(mut binds) = ctx.inproc_binds.lock() else {
        return;
    };
    binds.insert(name.to_owned(), Arc::downgrade(sock));
    drop(binds);

    let Ok(mut waiting) = ctx.inproc_waiting.lock() else {
        return;
    };
    let waiters = waiting.remove(name).unwrap_or_default();
    drop(waiting);
    for w in waiters {
        if let Some(connector) = w.upgrade()
            && is_bypass_eligible(connector.socket_type, sock.socket_type)
        {
            let (sender, receiver) = if connector.socket_type == SocketType::Push {
                (&connector, sock)
            } else {
                (sock, &connector)
            };
            try_install_bypass(sender, receiver);
        }
    }
}

/// Register an inproc connect. If the binder exists, install bypass.
fn register_inproc_connect(sock: &Arc<OmqSocket>, name: &str) {
    let ctx = &sock.ctx;
    let binder = ctx
        .inproc_binds
        .lock()
        .ok()
        .and_then(|g| g.get(name).and_then(std::sync::Weak::upgrade));

    if let Some(binder) = binder {
        if is_bypass_eligible(sock.socket_type, binder.socket_type) {
            let (sender, receiver) = if sock.socket_type == SocketType::Push {
                (sock, &binder)
            } else {
                (&binder, sock)
            };
            try_install_bypass(sender, receiver);
        }
    } else if let Ok(mut waiting) = ctx.inproc_waiting.lock() {
        waiting
            .entry(name.to_owned())
            .or_default()
            .push(Arc::downgrade(sock));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket(ctx_ptr: *mut c_void, type_int: c_int) -> *mut c_void {
    if ctx_ptr.is_null() {
        set_errno(libc::EFAULT);
        return std::ptr::null_mut();
    }
    // SAFETY: caller must pass a valid context pointer from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    if ctx.is_effectively_terminated() {
        set_errno(ETERM);
        return std::ptr::null_mut();
    }
    let Some(socket_type) = map_socket_type(type_int) else {
        set_errno(libc::EINVAL);
        return std::ptr::null_mut();
    };
    if ctx.socket_count.load(Ordering::Relaxed) >= ctx.max_sockets.load(Ordering::Relaxed) {
        set_errno(libc::EMFILE);
        return std::ptr::null_mut();
    }

    let Some(notify) = crate::notify::create_notify() else {
        set_errno(libc::EMFILE);
        return std::ptr::null_mut();
    };
    let id = next_socket_id();

    let overlay = SocketOverlay {
        ipv6: ctx.ipv6.load(Ordering::Acquire),
        linger: if ctx.blocky.load(Ordering::Acquire) {
            LingerSetting::Unset
        } else {
            LingerSetting::Finite(std::time::Duration::ZERO)
        },
        ..SocketOverlay::default()
    };

    let sock = Arc::new(OmqSocket {
        id,
        ctx: ctx.clone(),
        socket_type,
        overlay: Mutex::new(overlay),
        sndtimeo_ms: AtomicI64::new(-1),
        rcvtimeo_ms: AtomicI64::new(-1),
        send_accum: crate::local_cell::LocalCell::new(Vec::new()),
        bypass_send: crate::local_cell::LocalCell::new(None),
        bypass_recv: crate::local_cell::LocalCell::new(None),
        recv_drain: Mutex::new(VecDeque::new()),
        drain_nonempty: AtomicBool::new(false),
        recv_cons: crate::local_cell::LocalCell::new(None),
        inner: std::sync::OnceLock::new(),
        recv_space: std::sync::OnceLock::new(),
        recv_sink_config: std::sync::OnceLock::new(),
        last_endpoint: Mutex::new(None),
        notify,
        bound_or_connected: AtomicBool::new(false),
        connect_count: AtomicUsize::new(0),
        recv_pump: std::sync::OnceLock::new(),
    });
    sock.ctx.socket_opened();
    sock.ctx.register_socket(&sock);

    Box::into_raw(Box::new(sock)).cast()
}

/// Materialize the omq-tokio socket on the io thread with current overlay
/// options, then start the recv pump. Called once on first bind/connect
/// so that options set between `zmq_socket` and first bind/connect take
/// effect.
pub(crate) fn ensure_materialized(sock: &Arc<OmqSocket>) -> bool {
    if sock.ctx.io_context().is_none() {
        return false;
    }
    // CAS guarantees exactly one thread wins the materialization race.
    if sock
        .bound_or_connected
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        // Another thread already materialized. Spin until inner is set.
        while sock.inner.get().is_none() {
            std::hint::spin_loop();
        }
        return true;
    }
    let Ok(overlay) = sock.overlay.lock() else {
        return false;
    };
    let opts = overlay
        .to_options()
        .workload_profile(omq_tokio::options::WorkloadProfile::Throughput);
    let recv_hwm = overlay.recv_hwm.unwrap_or(DEFAULT_HWM as u32) as usize;
    drop(overlay);

    let socket_type = sock.socket_type;

    let recv_notify = sock.notify.recv_notifier();
    let cap = recv_hwm.max(16);
    let (fast_prod, fast_cons) = yring::spsc(cap);
    let (mut pump_prod, pump_cons) = yring::spsc(cap);
    // SAFETY: materialization happens before user recv access and must not
    // bind the later app-facing owner thread.
    *unsafe { sock.recv_cons.get_unchecked() } = Some(RecvConsumers {
        fast: fast_cons,
        pump: pump_cons,
    });

    let recv_space = Arc::new(StateSignal::new());
    let _ = sock.recv_space.set(recv_space.clone());

    // Build the RecvSink::Yring for the driver's direct fast path.
    let signal_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || recv_notify.signal());
    let recv_sink = omq_tokio::engine::RecvSink::Yring(omq_tokio::engine::YringSink {
        producer: fast_prod,
        signal: Box::new({
            let f = signal_cb.clone();
            move || f()
        }),
        space: recv_space.clone(),
    });
    let recv_sink_cfg = Arc::new(omq_tokio::engine::RecvSinkConfig::new(
        recv_sink,
        signal_cb,
        recv_space.clone(),
        cap,
    ));
    let _ = sock.recv_sink_config.set(recv_sink_cfg.clone());

    // Enter the tokio runtime context so that Socket::new (which calls
    // tokio::spawn internally for its driver task) and the recv pump
    // spawn below are scheduled on the context's runtime.
    let Some(ctx) = sock.ctx.io_context().cloned() else {
        return false;
    };
    let handle = ctx.handle().clone();
    let _guard = handle.enter();

    let inner = Arc::new(ctx.socket_with_recv_sink_config(socket_type, opts, recv_sink_cfg));

    // Recv pump: relay from async_channel into the pump yring.
    // Handles second+ peers whose drivers push to async_channel.
    // For the single-peer case, the async_channel stays empty
    // and this task idles.
    let s_recv = inner.clone();
    let recv_pump = tokio::spawn(async move {
        while let Ok(msg) = s_recv.recv().await {
            push_to_pump(&mut pump_prod, msg, recv_notify, &recv_space).await;
        }
    });

    let _ = sock.inner.set(inner);
    let _ = sock.recv_pump.set(recv_pump);
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_close(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    // SAFETY: sock_ptr came from Box::into_raw in zmq_socket; reclaiming ownership.
    let arc = unsafe { *Box::from_raw(sock_ptr.cast::<Arc<OmqSocket>>()) };

    if let Some(h) = arc.recv_pump.get() {
        h.abort();
    }
    // SAFETY: `zmq_close` reclaims the socket pointer, so no later user access
    // can legally race this cleanup.
    *unsafe { arc.bypass_send.get_unchecked() } = None;
    // SAFETY: same close-time exclusive ownership as above.
    *unsafe { arc.bypass_recv.get_unchecked() } = None;
    let linger = arc
        .overlay
        .lock()
        .map_or(Some(std::time::Duration::ZERO), |overlay| {
            overlay.effective_linger()
        });
    let close_socket = arc.inner.get().map(|inner| inner.as_ref().clone());

    // Enter the tokio runtime context so that dropping OmqSocket (and
    // the Arc<omq_tokio::Socket> it holds) doesn't panic from missing
    // reactor.
    let Some(handle) = arc.ctx.handle().cloned() else {
        arc.notify.close();
        arc.ctx.socket_closed();
        drop(arc);
        return 0;
    };
    if let Some(socket) = close_socket {
        let ctx = arc.ctx.clone();
        ctx.linger_started();
        handle.spawn(async move {
            let _ = socket.close_with_linger(linger).await;
            ctx.linger_finished();
        });
    }
    let _guard = handle.enter();

    arc.notify.close();
    arc.ctx.socket_closed();
    drop(arc);
    0
}

/// Validate `sock_ptr` and `addr`, parse `addr` as a `CStr` and then as an
/// `Endpoint`. Returns the socket reference, owned address string, and
/// parsed endpoint on success. Also checks the context terminated flag.
///
/// # Safety
///
/// `sock_ptr` must be a valid `*mut Arc<OmqSocket>` or null (returns EFAULT).
/// `addr` must be a valid C string pointer or null (returns EFAULT).
unsafe fn parse_endpoint_args<'a>(
    sock_ptr: *mut c_void,
    addr: *const c_char,
) -> Result<(&'a Arc<OmqSocket>, String, Endpoint), c_int> {
    if sock_ptr.is_null() {
        return Err(crate::error::ENOTSOCK);
    }
    if addr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: caller guarantees sock_ptr is a valid socket from zmq_socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.is_effectively_terminated() {
        return Err(ETERM);
    }
    // SAFETY: addr is non-null (checked above); caller guarantees a valid C string.
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();

    let Ok(endpoint) = omq_tokio::Endpoint::from_str(&addr_str) else {
        return Err(libc::EINVAL);
    };
    Ok((sock, addr_str, endpoint))
}

/// Validate `sock_ptr` and `cstr`, parse `cstr` as a UTF-8 `CStr`.
/// Does **not** parse an endpoint or check the terminated flag (join/leave
/// operate on group names, not transport addresses).
///
/// # Safety
///
/// `sock_ptr` must be a valid `*mut Arc<OmqSocket>` or null.
/// `cstr` must be a valid C string pointer or null.
unsafe fn parse_group_args<'a>(
    sock_ptr: *mut c_void,
    cstr: *const c_char,
) -> Result<(&'a Arc<OmqSocket>, Bytes), c_int> {
    if sock_ptr.is_null() {
        return Err(crate::error::ENOTSOCK);
    }
    if cstr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: caller guarantees sock_ptr is a valid socket from zmq_socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    // SAFETY: cstr is non-null (checked above); caller guarantees a valid C string.
    let Ok(g) = unsafe { CStr::from_ptr(cstr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    Ok((sock, Bytes::copy_from_slice(g.as_bytes())))
}

/// Map the two-level `Result<Result<T, omq Error>, ()>` returned by
/// `with_socket` to a C return code (0 on success).
fn result_to_rc<T>(result: &Result<Result<T, omq_tokio::error::Error>, ()>) -> c_int {
    match result {
        Ok(Ok(_)) => 0,
        Ok(Err(e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_bind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, mut endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    // ZMQ_IPV6: rewrite wildcard bind to IPv6 unspecified (dual-stack).
    let Ok(ov) = sock.overlay.lock() else {
        return fail(ETERM);
    };
    if ov.ipv6 {
        endpoint = ipv6_rewrite_wildcard(endpoint);
    }
    drop(ov);

    if sock.ctx.zero_io_threads() {
        if !zero_io_inproc_supported(sock, &endpoint) {
            return fail(libc::ENOTSUP);
        }
        if let Endpoint::Inproc { .. } = endpoint {
            register_inproc_bind(sock, &addr_str);
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = Some(addr_str);
            }
            return 0;
        }
    }

    if !ensure_materialized(sock) {
        return fail(ETERM);
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        s.bind(endpoint.clone()).await?;
        let resolved = s.last_bound_endpoint().map(|ep| ep.to_string());
        Ok::<_, omq_tokio::error::Error>(resolved)
    });

    match result {
        Ok(Ok(resolved)) => {
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = resolved.or(Some(addr_str.clone()));
            }
            if addr_str.starts_with("inproc://") {
                register_inproc_bind(sock, &addr_str);
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    if sock.ctx.zero_io_threads() {
        if !zero_io_inproc_supported(sock, &endpoint) {
            return fail(libc::ENOTSUP);
        }
        if let Endpoint::Inproc { .. } = endpoint {
            register_inproc_connect(sock, &addr_str);
            sock.connect_count.fetch_add(1, Ordering::AcqRel);
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = Some(addr_str);
            }
            return 0;
        }
    }

    if !ensure_materialized(sock) {
        return fail(ETERM);
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        s.connect(endpoint).await
    });

    match result {
        Ok(Ok(())) => {
            sock.connect_count.fetch_add(1, Ordering::AcqRel);
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = Some(addr_str.clone());
            }
            if addr_str.starts_with("inproc://") {
                register_inproc_connect(sock, &addr_str);
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_unbind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, _addr_str, endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        s.unbind(endpoint).await
    });

    result_to_rc(&result)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, _addr_str, endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        s.disconnect(endpoint).await
    });

    match result {
        Ok(Ok(())) => {
            let _ = sock
                .connect_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    Some(n.saturating_sub(1))
                });
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect_peer(sock_ptr: *mut c_void, _addr: *const libc::c_char) -> u32 {
    if sock_ptr.is_null() {
        set_errno(crate::error::ENOTSOCK);
        return 0;
    }
    set_errno(crate::error::ENOTSUP);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect_peer(sock_ptr: *mut c_void, _routing_id: u32) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_join(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = match unsafe { parse_group_args(sock_ptr, group) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    ensure_materialized(sock);
    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let result = with_socket(&sock.ctx, inner, move |s| async move { s.join(g).await });
    result_to_rc(&result)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_leave(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = match unsafe { parse_group_args(sock_ptr, group) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    ensure_materialized(sock);
    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let result = with_socket(&sock.ctx, inner, move |s| async move { s.leave(g).await });
    result_to_rc(&result)
}

/// Start monitoring events on `sock`, publishing them as two-frame messages
/// on an inproc PAIR socket bound to `addr`.
///
/// Frame layout (libzmq v1 monitor protocol):
///   frame 1: `event_id` (`u16` LE) + value (`i32` LE) = 6 bytes
///   frame 2: endpoint string (UTF-8)
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor(
    sock_ptr: *mut c_void,
    addr: *const libc::c_char,
    events: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    // addr == NULL means stop monitoring.
    if addr.is_null() {
        return 0;
    }
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    // SAFETY: addr is non-null (checked above); caller guarantees a valid C string.
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let events_mask = events as u16;

    ensure_materialized(sock);

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let Some(monitor_ctx) = sock.ctx.io_context().cloned() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        let mut stream = s.monitor();

        // Create a PAIR socket for publishing events, bind it to addr.
        let pair = std::sync::Arc::new(
            monitor_ctx.socket(omq_tokio::SocketType::Pair, omq_tokio::Options::default()),
        );
        let ep = omq_tokio::Endpoint::from_str(&addr_str)
            .map_err(|e| omq_tokio::error::Error::InvalidEndpoint(e.to_string()))?;
        pair.bind(ep).await?;

        tokio::spawn(async move {
            while let Ok(ev) = stream.recv().await {
                let Some((event_id, endpoint)) = monitor_event_to_zmq(&ev) else {
                    continue;
                };
                if events_mask != 0xFFFF && (events_mask & event_id) == 0 {
                    continue;
                }
                let msg = monitor_frame(event_id, 0, &endpoint);
                if pair.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok::<_, omq_tokio::error::Error>(())
    });

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(()) => fail(ETERM),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor_versioned(
    sock_ptr: *mut c_void,
    addr: *const libc::c_char,
    events: u64,
    event_version: c_int,
    _type: c_int,
) -> c_int {
    if event_version != 1 || events > c_int::MAX as u64 {
        return fail(crate::error::ENOTSUP);
    }
    zmq_socket_monitor(sock_ptr, addr, events as c_int)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor_pipes_stats(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_get_peer_state(
    socket: *mut c_void,
    _routing_id: *const c_void,
    _routing_id_size: usize,
) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

async fn push_to_pump(
    prod: &mut yring::Producer<omq_tokio::engine::RecvItem>,
    msg: omq_tokio::Message,
    recv_notify: RecvNotify,
    space: &StateSignal,
) {
    let flush_signal = |prod: &mut yring::Producer<omq_tokio::engine::RecvItem>| {
        if let yring::FlushResult::Flushed {
            was_empty: true, ..
        } = prod.flush_and_check()
        {
            recv_notify.signal();
        }
    };
    let mut m = msg;
    loop {
        match prod.push(omq_tokio::engine::RecvItem::new(m)) {
            Ok(()) => {
                flush_signal(prod);
                return;
            }
            Err(returned) => {
                m = returned.into_message();
                let seen = space.generation();
                let changed = space.changed_after(seen);
                tokio::pin!(changed);
                match prod.push(omq_tokio::engine::RecvItem::new(m)) {
                    Ok(()) => {
                        flush_signal(prod);
                        return;
                    }
                    Err(returned2) => {
                        m = returned2.into_message();
                        changed.await;
                    }
                }
            }
        }
    }
}

fn monitor_event_to_zmq(ev: &omq_tokio::MonitorEvent) -> Option<(u16, String)> {
    use omq_tokio::MonitorEvent;
    match ev {
        MonitorEvent::Listening { endpoint } => Some((0x0008, endpoint.to_string())),
        MonitorEvent::Accepted { endpoint, .. } => Some((0x0020, endpoint.to_string())),
        MonitorEvent::Connected { endpoint, .. } => Some((0x0001, endpoint.to_string())),
        MonitorEvent::ConnectDelayed { endpoint, .. } => Some((0x0002, endpoint.to_string())),
        MonitorEvent::HandshakeSucceeded { endpoint, .. } => Some((0x1000, endpoint.to_string())),
        MonitorEvent::HandshakeFailed { endpoint, .. } => Some((0x2000, endpoint.to_string())),
        MonitorEvent::Disconnected { endpoint, .. } => Some((0x0200, endpoint.to_string())),
        MonitorEvent::Closed => Some((0x0400, String::new())),
        _ => None,
    }
}

fn monitor_frame(event_id: u16, value: i32, endpoint: &str) -> omq_tokio::Message {
    let mut header = [0u8; 6];
    header[0..2].copy_from_slice(&event_id.to_le_bytes());
    header[2..6].copy_from_slice(&value.to_le_bytes());
    omq_tokio::Message::multipart([
        bytes::Bytes::copy_from_slice(&header),
        bytes::Bytes::copy_from_slice(endpoint.as_bytes()),
    ])
}
