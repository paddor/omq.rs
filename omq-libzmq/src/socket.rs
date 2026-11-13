//! Socket handle, `zmq_socket` and `zmq_close`; endpoint calls live in
//! `endpoint`, backend creation in `materialize`.

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use omq_tokio::SocketType;

use crate::context::{OmqContext, next_socket_id};
use crate::error::{ETERM, c_try, fail, map_omq_err, set_errno};
use crate::notify::{NotifyHandle, PlatformNotifyHandle};
use crate::opts::{LingerSetting, SocketOverlay};

mod endpoint;
mod materialize;
mod monitor;
mod state;

pub use endpoint::{
    zmq_bind, zmq_connect, zmq_connect_peer, zmq_disconnect, zmq_disconnect_peer, zmq_join,
    zmq_leave, zmq_unbind,
};
pub(crate) use materialize::{RecvConsumers, ensure_materialized, materialize_for_io};
pub(crate) use state::{EndpointState, RecvState, SendState};

pub use monitor::{
    zmq_socket_get_peer_state, zmq_socket_monitor, zmq_socket_monitor_pipes_stats,
    zmq_socket_monitor_versioned,
};

// Default channel capacity (matches default HWM).
pub(crate) const DEFAULT_HWM: usize = 1000;

#[derive(Debug)]
pub(crate) struct OmqSocket {
    pub id: u64,
    pub ctx: Arc<OmqContext>,
    pub socket_type: SocketType,
    pub overlay: Mutex<SocketOverlay>,
    pub send: SendState,
    pub recv: RecvState,
    /// The inner omq-tokio socket. Send+Sync, stored directly.
    pub inner: std::sync::OnceLock<Arc<omq_tokio::Socket>>,
    pub endpoints: EndpointState,
    pub notify: Arc<PlatformNotifyHandle>,
    /// True for the REP socket bound to the reserved ZAP endpoint.
    pub(crate) zap_handler: AtomicBool,
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

    /// True for the REP socket bound to the reserved ZAP endpoint.
    #[inline]
    pub(crate) fn is_zap_handler(&self) -> bool {
        self.zap_handler.load(Ordering::Acquire)
    }
}

/// libzmq socket type codes (`ZMQ_PAIR` .. `ZMQ_CHANNEL`; 18 is unused).
const SOCKET_TYPES: [(c_int, SocketType); 20] = [
    (0, SocketType::Pair),
    (1, SocketType::Pub),
    (2, SocketType::Sub),
    (3, SocketType::Req),
    (4, SocketType::Rep),
    (5, SocketType::Dealer),
    (6, SocketType::Router),
    (7, SocketType::Pull),
    (8, SocketType::Push),
    (9, SocketType::XPub),
    (10, SocketType::XSub),
    (11, SocketType::Stream),
    (12, SocketType::Server),
    (13, SocketType::Client),
    (14, SocketType::Radio),
    (15, SocketType::Dish),
    (16, SocketType::Gather),
    (17, SocketType::Scatter),
    (19, SocketType::Peer),
    (20, SocketType::Channel),
];

/// Map a libzmq socket type code to `SocketType`.
fn map_socket_type(code: c_int) -> Option<SocketType> {
    SOCKET_TYPES
        .iter()
        .find(|&&(candidate, _)| candidate == code)
        .map(|&(_, socket_type)| socket_type)
}

/// libzmq's `ZMQ_TYPE` code; `None` for omq-only socket types.
pub(crate) fn socket_type_code(socket_type: SocketType) -> Option<c_int> {
    SOCKET_TYPES
        .iter()
        .find(|&&(_, candidate)| candidate == socket_type)
        .map(|&(code, _)| code)
}

/// Run an async op against the socket's inner Arc on the context runtime and
/// block the calling thread until completion. Fails with `ETERM` when the
/// context shuts down first, `EDEADLK` on an OMQ IO thread, and the mapped
/// errno when the op fails.
pub(crate) fn run_on_socket<F, Fut, T>(
    sock: &OmqSocket,
    inner: &Arc<omq_tokio::Socket>,
    op: F,
) -> Result<T, c_int>
where
    F: FnOnce(Arc<omq_tokio::Socket>) -> Fut,
    Fut: std::future::Future<Output = omq_tokio::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    crate::blocking::run(&sock.ctx, op(inner.clone()))?.map_err(|error| map_omq_err(&error))
}

/// Materialize the backend socket and borrow it.
pub(crate) fn materialized(sock: &Arc<OmqSocket>) -> Result<&Arc<omq_tokio::Socket>, c_int> {
    ensure_materialized(sock)?;
    sock.inner.get().ok_or(ETERM)
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
        ipv6: ctx.options.ipv6.load(Ordering::Acquire),
        linger: if ctx.options.blocky.load(Ordering::Acquire) {
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
        send: SendState::new(queue_without_ready_peer),
        recv: RecvState::new(),
        inner: std::sync::OnceLock::new(),
        endpoints: EndpointState::new(),
        notify,
        zap_handler: AtomicBool::new(false),
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
    if ctx.socket_count.load(Ordering::Relaxed) >= ctx.options.max_sockets.load(Ordering::Relaxed) {
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
    let socket = c_try!(unsafe { crate::handle::socket(sock_ptr) });
    socket.allow_thread_migration();
    0
}

/// Close the socket handle; accepted output follows its configured linger policy.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_close(sock_ptr: *mut c_void) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API; on success
    // close owns the socket and the handle is gone.
    let arc = c_try!(unsafe { crate::handle::take_socket(sock_ptr) });

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
        // A lingering close keeps context termination waiting until it ends.
        let work =
            (!matches!(linger, Some(std::time::Duration::ZERO))).then(|| arc.ctx.begin_work());
        drop(handle.spawn(async move {
            let _ = socket.close_with_linger(linger).await;
            drop(work);
        }));
    }
    let _guard = handle.enter();

    arc.notify.close();
    arc.ctx.socket_closed();
    drop(arc);
    0
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
