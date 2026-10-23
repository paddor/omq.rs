//! Socket handle and `zmq_socket`/close/bind/connect/unbind/disconnect.

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
use crate::opts::{LingerSetting, MechanismOverlay, SocketOverlay};

mod monitor;
mod state;

pub(crate) use state::{RecvState, SendState};

pub use monitor::{
    zmq_socket_get_peer_state, zmq_socket_monitor, zmq_socket_monitor_pipes_stats,
    zmq_socket_monitor_versioned,
};

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
    pub fast: yring::Consumer<omq_tokio::Message>,
    /// Filled by the recv pump task (fallback for second+ peers).
    pub pump: yring::Consumer<omq_tokio::Message>,
    prefer_pump: bool,
}

impl RecvConsumers {
    #[expect(
        clippy::len_zero,
        reason = "is_empty registers waiters; this snapshot only selects the next source"
    )]
    pub(crate) fn try_pop(&mut self) -> Option<(omq_tokio::Message, bool)> {
        // This length snapshot selects the next source only. Failed pops
        // still register both rings' empty waiters before the caller parks.
        if self.prefer_pump && self.pump.len() > 0 {
            self.prefer_pump = false;
            return self.pump.prefetch_and_pop_with_full();
        }
        if let Some(message) = self.fast.prefetch_and_pop_with_full() {
            self.prefer_pump = true;
            return Some(message);
        }
        self.prefer_pump = false;
        self.pump.prefetch_and_pop_with_full()
    }

    pub(crate) fn refresh(&mut self, config: Option<&Arc<omq_tokio::engine::RecvSinkConfig>>) {
        if self.fast.is_disconnected()
            && let Some(config) = config
            && let Some(consumer) = config.try_take_pending_consumer()
        {
            self.fast = consumer;
        }
    }
}

#[derive(Debug)]
pub(crate) struct OmqSocket {
    pub id: u64,
    pub ctx: Arc<OmqContext>,
    pub socket_type: SocketType,
    pub overlay: Mutex<SocketOverlay>,
    pub sndtimeo_ms: AtomicI64,
    pub rcvtimeo_ms: AtomicI64,
    pub send: SendState,
    pub recv: RecvState,
    /// The inner omq-tokio socket. Send+Sync, stored directly.
    pub inner: std::sync::OnceLock<Arc<omq_tokio::Socket>>,
    pub last_endpoint: Mutex<Option<String>>,
    pub notify: Arc<PlatformNotifyHandle>,
    /// True for the REP socket bound to the reserved ZAP endpoint.
    pub(crate) zap_handler: AtomicBool,
    pub bound_or_connected: AtomicBool,
    pub connect_count: AtomicUsize,
}

impl OmqSocket {
    pub(crate) fn recv_has_data(&self) -> bool {
        // SAFETY: libzmq sockets are accessed by at most one application thread.
        let Some(consumers) = unsafe { self.recv.consumers.get() }.as_mut() else {
            return false;
        };
        consumers.refresh(self.recv.sink_config.get());
        !consumers.fast.is_empty() || !consumers.pump.is_empty()
    }

    fn allow_thread_migration(&self) {
        self.send.allow_thread_migration();
        self.recv.allow_thread_migration();
    }
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

/// Run an async op against the socket's inner Arc on the context runtime and
/// block the calling thread until completion. Fails with `ETERM` when the
/// context shuts down first and `EDEADLK` on an OMQ IO thread.
pub(crate) fn with_socket<F, Fut, T>(
    ctx: &Arc<OmqContext>,
    inner: &Arc<omq_tokio::Socket>,
    op: F,
) -> Result<T, c_int>
where
    F: FnOnce(Arc<omq_tokio::Socket>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    crate::blocking::run(ctx, op(inner.clone()))
}

/// Create a socket handle of the requested type in a context; return null with errno on
/// failure.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket(ctx_ptr: *mut c_void, type_int: c_int) -> *mut c_void {
    let (ctx, socket_type) = match socket_preconditions(ctx_ptr, type_int) {
        Ok(checked) => checked,
        Err(errno) => {
            set_errno(errno);
            return std::ptr::null_mut();
        }
    };

    let Some(notify) = crate::notify::create_notify() else {
        set_errno(libc::EMFILE);
        return std::ptr::null_mut();
    };
    let id = next_socket_id();

    // Context-level ZMQ_IPV6 and ZMQ_BLOCKY seed the per-socket defaults;
    // non-blocky contexts default new sockets to zero linger like libzmq.
    let overlay = SocketOverlay {
        ipv6: ctx.ipv6.load(Ordering::Acquire),
        linger: if ctx.blocky.load(Ordering::Acquire) {
            LingerSetting::Unset
        } else {
            LingerSetting::Finite(std::time::Duration::ZERO)
        },
        ..SocketOverlay::default()
    };
    let queue_without_ready_peer = !overlay.immediate;

    let sock = Arc::new(OmqSocket {
        id,
        ctx: ctx.clone(),
        socket_type,
        overlay: Mutex::new(overlay),
        sndtimeo_ms: AtomicI64::new(-1),
        rcvtimeo_ms: AtomicI64::new(-1),
        send: SendState::new(queue_without_ready_peer),
        recv: RecvState::new(),
        inner: std::sync::OnceLock::new(),
        last_endpoint: Mutex::new(None),
        notify,
        zap_handler: AtomicBool::new(false),
        bound_or_connected: AtomicBool::new(false),
        connect_count: AtomicUsize::new(0),
    });
    sock.ctx.socket_opened();
    sock.ctx.register_socket(&sock);

    crate::handle::socket_into_raw(sock)
}

/// Validate the context handle, its state, the socket type, and the
/// context's socket limit, in that errno order.
fn socket_preconditions<'a>(
    ctx_ptr: *mut c_void,
    type_int: c_int,
) -> Result<(&'a Arc<OmqContext>, SocketType), c_int> {
    if ctx_ptr.is_null() {
        return Err(libc::EFAULT);
    }
    // SAFETY: C callers pass null or a pointer from this API.
    let Ok(ctx) = (unsafe { crate::handle::context(ctx_ptr) }) else {
        return Err(libc::EFAULT);
    };
    if ctx.is_effectively_terminated() {
        return Err(ETERM);
    }
    let Some(socket_type) = map_socket_type(type_int) else {
        return Err(libc::EINVAL);
    };
    if ctx.socket_count.load(Ordering::Relaxed) >= ctx.max_sockets.load(Ordering::Relaxed) {
        return Err(libc::EMFILE);
    }
    Ok((ctx, socket_type))
}

/// Opts a socket into externally serialized migration between application threads.
#[unsafe(no_mangle)]
pub extern "C" fn omq_socket_allow_thread_migration(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    // SAFETY: C callers pass null or a pointer from this API.
    let socket = match unsafe { crate::handle::socket(sock_ptr) } {
        Ok(socket) => socket,
        Err(errno) => return fail(errno),
    };
    socket.allow_thread_migration();
    0
}

/// ZAP-related overlay values, snapshotted under the overlay lock.
struct ZapSettings {
    mechanism: MechanismOverlay,
    domain: String,
    enforce: bool,
    identity: Bytes,
}

/// Overlay values read under one lock when the backend socket is created.
struct MaterializeSettings {
    opts: omq_tokio::Options,
    recv_hwm: usize,
    queue_without_ready_peer: bool,
    zap: ZapSettings,
}

impl MaterializeSettings {
    fn from_overlay(overlay: &SocketOverlay) -> Self {
        Self {
            opts: overlay.to_options(),
            recv_hwm: omq_tokio::options::queue_capacity(
                overlay.recv_hwm.unwrap_or(DEFAULT_HWM as u32),
            ),
            queue_without_ready_peer: !overlay.immediate,
            zap: ZapSettings {
                mechanism: overlay.mechanism.clone(),
                domain: overlay.zap_domain.clone(),
                enforce: overlay.zap_enforce_domain,
                identity: overlay.identity.clone(),
            },
        }
    }
}

/// Route server-side authentication through the context's ZAP handler when
/// the mechanism and domain call for it. Returns whether ZAP is in use.
fn configure_zap_authentication(
    sock: &Arc<OmqSocket>,
    opts: &mut omq_tokio::Options,
    zap_settings: ZapSettings,
) -> Result<bool, c_int> {
    let ZapSettings {
        mechanism,
        domain: zap_domain,
        enforce: zap_enforce,
        identity,
    } = zap_settings;

    let server_mechanism = matches!(
        mechanism,
        MechanismOverlay::Null
            | MechanismOverlay::PlainServer
            | MechanismOverlay::CurveServer { .. }
    );
    if zap_enforce && zap_domain.is_empty() && server_mechanism {
        return Err(libc::EINVAL);
    }

    // PLAIN servers always use ZAP; NULL and CURVE servers only with a domain.
    let needs_zap = matches!(mechanism, MechanismOverlay::PlainServer)
        || (!zap_domain.is_empty()
            && matches!(
                mechanism,
                MechanismOverlay::Null | MechanismOverlay::CurveServer { .. }
            ));
    if !needs_zap {
        return Ok(false);
    }
    if zap_domain.is_empty() {
        return Err(libc::EINVAL);
    }

    let zap = Arc::clone(&sock.ctx.zap);
    let timeout = opts
        .handshake_timeout
        .unwrap_or(std::time::Duration::from_secs(30));
    let authenticator = omq_tokio::Authenticator::new_with_result(move |peer| {
        let query = crate::zap::ZapQuery {
            domain: &zap_domain,
            address: peer.peer_address.as_deref().unwrap_or(""),
            identity: &identity,
            peer,
        };
        zap.authorize(&query, timeout)
    });
    let setup = std::mem::replace(&mut opts.mechanism, omq_tokio::MechanismSetup::Null);
    opts.mechanism = match setup {
        omq_tokio::MechanismSetup::Null => omq_tokio::MechanismSetup::NullServer { authenticator },
        omq_tokio::MechanismSetup::PlainServer { .. } => {
            omq_tokio::MechanismSetup::PlainServer { authenticator }
        }
        omq_tokio::MechanismSetup::CurveServer {
            our_keypair,
            mut options,
        } => {
            options.authenticator = Some(authenticator);
            omq_tokio::MechanismSetup::CurveServer {
                our_keypair,
                options,
            }
        }
        other => other,
    };
    Ok(true)
}

/// Materialize the omq-tokio socket on the io thread with current overlay
/// options, then start the recv pump. Called once on first bind/connect
/// so that options set between `zmq_socket` and first bind/connect take
/// effect.
pub(crate) fn ensure_materialized(sock: &Arc<OmqSocket>) -> Result<(), c_int> {
    if sock.zap_handler.load(Ordering::Acquire) {
        return Err(libc::EINVAL);
    }
    let Some(ctx) = sock.ctx.io_context().cloned() else {
        return Err(ETERM);
    };
    if sock.inner.get().is_some() {
        return Ok(());
    }

    let MaterializeSettings {
        mut opts,
        recv_hwm,
        queue_without_ready_peer,
        zap,
    } = {
        let overlay = sock.overlay.lock().map_err(|_| ETERM)?;
        MaterializeSettings::from_overlay(&overlay)
    };
    let needs_zap = configure_zap_authentication(sock, &mut opts, zap)?;
    opts.validate().map_err(|_| libc::EINVAL)?;

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
        return Ok(());
    }
    // SAFETY: materialization runs before hot-path send ownership and freezes
    // bind/connect-scoped options for this socket.
    *unsafe { sock.send.queue_without_ready_peer.get_unchecked() } = queue_without_ready_peer;

    let socket_type = sock.socket_type;

    let cap = recv_hwm.max(16);
    let (fast_prod, fast_cons) = yring::spsc(cap);
    let (pump_prod, pump_cons) = yring::spsc(cap);
    // SAFETY: materialization happens before user recv access and must not
    // bind the later app-facing owner thread.
    *unsafe { sock.recv.consumers.get_unchecked() } = Some(RecvConsumers {
        fast: fast_cons,
        pump: pump_cons,
        prefer_pump: false,
    });

    let recv_space = Arc::new(StateSignal::new());
    let _ = sock.recv.space.set(recv_space.clone());

    let recv_sink_cfg = build_recv_sink(sock, cap, needs_zap, fast_prod, recv_space.clone());
    let _ = sock.recv.sink_config.set(recv_sink_cfg.clone());

    // Enter the tokio runtime context so that Socket::new (which calls
    // tokio::spawn internally for its driver task) and the recv pump
    // spawn below are scheduled on the context's runtime.
    let handle = ctx.handle().clone();
    let _guard = handle.enter();

    let inner = Arc::new(ctx.socket_for_libzmq(socket_type, opts, Some(recv_sink_cfg)));
    let recv_pump = spawn_recv_pump(sock, &inner, pump_prod, recv_space);

    let _ = sock.inner.set(inner);
    let _ = sock.recv.pump.set(recv_pump);
    Ok(())
}

/// Build the direct receive sink. ZAP-authenticated sockets use a shared
/// MPSC queue so every peer's authentication properties stay attached to
/// its messages; the normal hot path retains the SPSC yring.
fn build_recv_sink(
    sock: &Arc<OmqSocket>,
    cap: usize,
    needs_zap: bool,
    fast_prod: yring::Producer<omq_tokio::Message>,
    recv_space: Arc<StateSignal>,
) -> Arc<omq_tokio::engine::RecvSinkConfig> {
    let notify = sock.notify.clone();
    let signal_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || notify.signal_recv());

    let recv_sink = if needs_zap {
        let (sink, receiver) = omq_tokio::engine::RecvSink::authenticated(cap, signal_cb.clone());
        // SAFETY: like recv_cons, this is set before any user recv access.
        *unsafe { sock.recv.authenticated.get_unchecked() } = Some(receiver);
        sink
    } else {
        omq_tokio::engine::RecvSink::Yring(omq_tokio::engine::YringSink {
            producer: fast_prod,
            signal: Box::new({
                let f = signal_cb.clone();
                move || f()
            }),
            space: recv_space.clone(),
        })
    };

    Arc::new(omq_tokio::engine::RecvSinkConfig::new(
        recv_sink, signal_cb, recv_space, cap,
    ))
}

/// Relay complete transport items into the pump yring. Request/reply
/// validation and admission belong to the application receive path.
/// Handles peers whose drivers push to the internal receive queues.
/// For the single-peer case, the `async_channel` stays empty
/// and this task idles. Must run inside the context runtime.
fn spawn_recv_pump(
    sock: &Arc<OmqSocket>,
    inner: &Arc<omq_tokio::Socket>,
    mut pump_prod: yring::Producer<omq_tokio::Message>,
    recv_space: Arc<StateSignal>,
) -> tokio::task::JoinHandle<()> {
    let s_recv = inner.clone();
    let pump_notify = sock.notify.clone();
    tokio::spawn(async move {
        let recv_notify = pump_notify.recv_notifier();
        while let Ok(msg) = s_recv.recv_for_external_recv().await {
            push_to_pump(&mut pump_prod, msg, recv_notify, &recv_space).await;
        }
    })
}

/// Close the socket handle; accepted output follows its configured linger policy.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_close(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    // SAFETY: C callers pass null or a pointer from this API; on success
    // close owns the socket and the handle is gone.
    let arc = match unsafe { crate::handle::take_socket(sock_ptr) } {
        Ok(socket) => socket,
        Err(errno) => return fail(errno),
    };

    // A full direct receive ring may have a driver waiting for space.
    // Drop its consumer before signaling so the driver observes closure.
    // SAFETY: close has exclusive socket access; the last recv may have run
    // on a different thread before ownership returned to this caller.
    let consumers = unsafe { arc.recv.consumers.get_unchecked() }.take();
    drop(consumers);
    if let Some(space) = arc.recv.space.get() {
        space.notify_changed();
    }

    if arc.zap_handler.swap(false, Ordering::AcqRel) {
        arc.ctx.zap.unbind(arc.id);
    }

    if let Some(h) = arc.recv.pump.get() {
        h.abort();
    }
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
        if matches!(linger, Some(std::time::Duration::ZERO)) {
            drop(handle.spawn(async move {
                let _ = socket.close_with_linger(linger).await;
            }));
        } else {
            let work = arc.ctx.begin_work();
            drop(handle.spawn(async move {
                let _ = socket.close_with_linger(linger).await;
                drop(work);
            }));
        }
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
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = unsafe { crate::handle::socket(sock_ptr) }?;
    if sock.ctx.is_effectively_terminated() {
        return Err(ETERM);
    }
    // SAFETY: addr is non-null (checked above); caller guarantees a valid C string.
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();

    // DART's buffer ownership API is currently native Rust only. Cargo
    // feature unification must not silently expose it through the C facade.
    if addr_str.starts_with("dart://") {
        return Err(libc::EINVAL);
    }

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
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = unsafe { crate::handle::socket(sock_ptr) }?;
    // SAFETY: cstr is non-null (checked above); caller guarantees a valid C string.
    let Ok(g) = unsafe { CStr::from_ptr(cstr) }.to_str() else {
        return Err(libc::EINVAL);
    };
    Ok((sock, Bytes::copy_from_slice(g.as_bytes())))
}

/// Map the two-level `Result<Result<T, omq Error>, errno>` returned by
/// `with_socket` to a C return code (0 on success).
fn result_to_rc<T>(result: &Result<Result<T, omq_tokio::error::Error>, c_int>) -> c_int {
    match result {
        Ok(Ok(_)) => 0,
        Ok(Err(e)) => fail(map_omq_err(e)),
        Err(errno) => fail(*errno),
    }
}

/// Read codec parameters for this operation even after backend materialization.
/// Validate both the requested overlay mechanism and the backend's actual
/// mechanism; the latter is checked by the native endpoint setup.
fn compression_options_for_endpoint(
    overlay: &SocketOverlay,
    endpoint: &Endpoint,
) -> Result<Option<omq_tokio::CompressionOptions>, c_int> {
    if omq_tokio::CompressionKind::for_endpoint(endpoint).is_none() {
        return Ok(None);
    }
    let options = overlay.to_options();
    omq_tokio::CompressionKind::for_endpoint_with_mechanism(endpoint, &options.mechanism)
        .map_err(|_| libc::EINVAL)?;
    options.validate().map_err(|_| libc::EINVAL)?;
    Ok(Some(omq_tokio::CompressionOptions::from(&options)))
}

/// Bind a null-terminated endpoint URI; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_bind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, mut endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    if addr_str == crate::zap::ENDPOINT {
        if let Err(error) = sock.ctx.zap.bind(sock) {
            return fail(error);
        }
        sock.zap_handler.store(true, Ordering::Release);
        sock.bound_or_connected.store(true, Ordering::Release);
        if let Ok(mut ep) = sock.last_endpoint.lock() {
            *ep = Some(addr_str);
        }
        return 0;
    }
    if sock.zap_handler.load(Ordering::Acquire) {
        return fail(libc::EINVAL);
    }

    // ZMQ_IPV6: rewrite wildcard bind to IPv6 unspecified (dual-stack).
    let Ok(ov) = sock.overlay.lock() else {
        return fail(ETERM);
    };
    if ov.ipv6 {
        endpoint = ipv6_rewrite_wildcard(endpoint);
    }
    let compression = match compression_options_for_endpoint(&ov, &endpoint) {
        Ok(compression) => compression,
        Err(error) => return fail(error),
    };
    drop(ov);

    if let Err(error) = ensure_materialized(sock) {
        return fail(error);
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        if let Some(compression) = compression {
            s.bind_with_compression_options(endpoint.clone(), compression)
                .await?;
        } else {
            s.bind(endpoint.clone()).await?;
        }
        let resolved = s.last_bound_endpoint().map(|ep| ep.to_string());
        Ok::<_, omq_tokio::error::Error>(resolved)
    });

    match result {
        Ok(Ok(resolved)) => {
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = resolved.or(Some(addr_str));
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(errno) => fail(errno),
    }
}

/// Register a null-terminated endpoint URI for connection and automatic reconnection.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    if addr_str == crate::zap::ENDPOINT {
        return fail(libc::EINVAL);
    }
    if sock.zap_handler.load(Ordering::Acquire) {
        return fail(libc::EINVAL);
    }

    let compression = {
        let Ok(overlay) = sock.overlay.lock() else {
            return fail(ETERM);
        };
        match compression_options_for_endpoint(&overlay, &endpoint) {
            Ok(compression) => compression,
            Err(error) => return fail(error),
        }
    };

    if let Err(error) = ensure_materialized(sock) {
        return fail(error);
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        if let Some(compression) = compression {
            s.connect_with_compression_options(endpoint, compression)
                .await
        } else {
            s.connect(endpoint).await
        }
    });

    match result {
        Ok(Ok(())) => {
            sock.connect_count.fetch_add(1, Ordering::AcqRel);
            if let Ok(mut ep) = sock.last_endpoint.lock() {
                *ep = Some(addr_str);
            }
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(errno) => fail(errno),
    }
}

/// Remove a bound endpoint by its null-terminated URI; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_unbind(sock_ptr: *mut c_void, addr: *const libc::c_char) -> c_int {
    let (sock, addr_str, endpoint) = match unsafe { parse_endpoint_args(sock_ptr, addr) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };

    if addr_str == crate::zap::ENDPOINT && sock.zap_handler.swap(false, Ordering::AcqRel) {
        sock.ctx.zap.unbind(sock.id);
        sock.bound_or_connected.store(false, Ordering::Release);
        if let Ok(mut endpoint) = sock.last_endpoint.lock() {
            *endpoint = None;
        }
        return 0;
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        s.unbind(endpoint).await
    });

    result_to_rc(&result)
}

/// Remove a connected endpoint and stop retries; return zero or -1 with errno.
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
            // Atomic::try_update is unstable on MSRV 1.93.
            #[allow(deprecated)]
            let _ = sock
                .connect_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    Some(n.saturating_sub(1))
                });
            0
        }
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(errno) => fail(errno),
    }
}

/// Unsupported peer-connect entry point; return zero with ENOTSUP for a valid socket.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_connect_peer(sock_ptr: *mut c_void, _addr: *const libc::c_char) -> u32 {
    if sock_ptr.is_null() {
        set_errno(crate::error::ENOTSOCK);
        return 0;
    }
    set_errno(crate::error::ENOTSUP);
    0
}

/// Unsupported peer-disconnect entry point; return -1 with ENOTSUP for a valid socket.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_disconnect_peer(sock_ptr: *mut c_void, _routing_id: u32) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

/// Join a null-terminated DISH group; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_join(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = match unsafe { parse_group_args(sock_ptr, group) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    if let Err(error) = ensure_materialized(sock) {
        return fail(error);
    }
    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let result = with_socket(&sock.ctx, inner, move |s| async move { s.join(g).await });
    result_to_rc(&result)
}

/// Leave a null-terminated DISH group; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_leave(sock_ptr: *mut c_void, group: *const libc::c_char) -> c_int {
    let (sock, g) = match unsafe { parse_group_args(sock_ptr, group) } {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    if let Err(error) = ensure_materialized(sock) {
        return fail(error);
    }
    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let result = with_socket(&sock.ctx, inner, move |s| async move { s.leave(g).await });
    result_to_rc(&result)
}

async fn push_to_pump(
    prod: &mut yring::Producer<omq_tokio::Message>,
    msg: omq_tokio::Message,
    recv_notify: RecvNotify,
    space: &StateSignal,
) {
    let flush_signal = |prod: &mut yring::Producer<omq_tokio::Message>| {
        if let yring::FlushResult::Flushed {
            was_empty: true, ..
        } = prod.flush_and_check()
        {
            recv_notify.signal();
        }
    };
    let mut m = msg;
    loop {
        match prod.push(m) {
            Ok(()) => {
                flush_signal(prod);
                return;
            }
            Err(returned) => {
                m = returned;
                let seen = space.generation();
                let changed = space.changed_after(seen);
                tokio::pin!(changed);
                match prod.push(m) {
                    Ok(()) => {
                        flush_signal(prod);
                        return;
                    }
                    Err(returned2) => {
                        m = returned2;
                        changed.await;
                    }
                }
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
