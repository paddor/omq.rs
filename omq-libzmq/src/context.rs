//! Context: owns a tokio runtime on a background thread.

use std::ffi::c_int;

use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use tokio::runtime::Handle;

use crate::notify::NotifyHandle;

/// Per-context: lazily-created `omq_tokio::Context` and ZMQ state.
pub(crate) struct OmqContext {
    pub(crate) ctx: OnceLock<omq_tokio::Context>,
    pub(crate) configured_io_threads: AtomicI32,
    pub terminated: Arc<AtomicBool>,
    pub socket_count: AtomicI32,
    linger_count: AtomicI32,
    socket_notify: (Mutex<()>, Condvar),
    sockets: Mutex<Vec<Weak<crate::socket::OmqSocket>>>,
    pub max_sockets: AtomicI32,
    pub max_msg_size: AtomicI64,
    pub ipv6: AtomicBool,
    pub blocky: AtomicBool,
    pub zero_copy_recv: AtomicBool,
    /// Zmq-layer inproc registry. Maps inproc name to the bound `OmqSocket`.
    pub(crate) inproc_binds: Mutex<FxHashMap<String, std::sync::Weak<crate::socket::OmqSocket>>>,
    /// Pending inproc connect requests waiting for a bind.
    pub(crate) inproc_waiting:
        Mutex<FxHashMap<String, Vec<std::sync::Weak<crate::socket::OmqSocket>>>>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_socket_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

impl OmqContext {
    fn new(n_io_threads: usize) -> Arc<Self> {
        let n = n_io_threads;
        Arc::new(Self {
            ctx: OnceLock::new(),
            configured_io_threads: AtomicI32::new(i32::try_from(n).unwrap_or(i32::MAX)),
            terminated: Arc::new(AtomicBool::new(false)),
            socket_count: AtomicI32::new(0),
            linger_count: AtomicI32::new(0),
            socket_notify: (Mutex::new(()), Condvar::new()),
            sockets: Mutex::new(Vec::new()),
            max_sockets: AtomicI32::new(1023),
            max_msg_size: AtomicI64::new(-1),
            ipv6: AtomicBool::new(false),
            blocky: AtomicBool::new(true),
            zero_copy_recv: AtomicBool::new(true),
            inproc_binds: Mutex::new(FxHashMap::default()),
            inproc_waiting: Mutex::new(FxHashMap::default()),
        })
    }

    pub(crate) fn handle(&self) -> Option<&Handle> {
        self.ctx.get().map(omq_tokio::Context::handle)
    }

    pub(crate) fn io_context(&self) -> Option<&omq_tokio::Context> {
        let n = self.configured_io_threads.load(Ordering::Acquire);
        (n > 0).then(|| {
            self.ctx.get_or_init(|| {
                omq_tokio::Context::with_config(omq_tokio::ContextConfig {
                    io_threads: n as usize,
                })
            })
        })
    }

    pub(crate) fn zero_io_threads(&self) -> bool {
        self.configured_io_threads.load(Ordering::Acquire) == 0
    }

    pub(crate) fn socket_opened(&self) {
        self.socket_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn register_socket(&self, sock: &Arc<crate::socket::OmqSocket>) {
        if let Ok(mut sockets) = self.sockets.lock() {
            sockets.retain(|s| s.strong_count() > 0);
            sockets.push(Arc::downgrade(sock));
        }
    }

    pub(crate) fn socket_closed(&self) {
        let prev = self.socket_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            let (_, cvar) = &self.socket_notify;
            cvar.notify_all();
        }
    }

    pub(crate) fn linger_started(&self) {
        self.linger_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn linger_finished(&self) {
        let prev = self.linger_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            let (_, cvar) = &self.socket_notify;
            cvar.notify_all();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.terminated.store(true, Ordering::Release);
        let notifies = self
            .sockets
            .lock()
            .map(|mut sockets| {
                sockets.retain(|s| s.strong_count() > 0);
                sockets
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|s| s.notify.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for notify in notifies {
            notify.signal_recv();
            notify.signal_send();
        }
        let (_, cvar) = &self.socket_notify;
        cvar.notify_all();
    }
}

impl std::fmt::Debug for OmqContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqContext")
            .field("ctx", &self.ctx)
            .field("terminated", &self.terminated.load(Ordering::Relaxed))
            .field("socket_count", &self.socket_count.load(Ordering::Relaxed))
            .field("linger_count", &self.linger_count.load(Ordering::Relaxed))
            .field("max_sockets", &self.max_sockets.load(Ordering::Relaxed))
            .field("max_msg_size", &self.max_msg_size.load(Ordering::Relaxed))
            .field("ipv6", &self.ipv6.load(Ordering::Relaxed))
            .field("blocky", &self.blocky.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// Context handle: Box<Arc<OmqContext>> cast to *mut c_void.

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_new() -> *mut libc::c_void {
    let arc = OmqContext::new(1);
    Box::into_raw(Box::new(arc)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_init(io_threads: c_int) -> *mut libc::c_void {
    let n = io_threads.max(0) as usize;
    let arc = OmqContext::new(n);
    Box::into_raw(Box::new(arc)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_shutdown(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    ctx.shutdown();
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_term(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: ctx_ptr came from Box::into_raw in zmq_ctx_new; reclaiming ownership.
    let arc = unsafe { *Box::from_raw(ctx_ptr.cast::<Arc<OmqContext>>()) };

    // Signal termination to all io threads and wake blocking socket calls.
    arc.shutdown();

    // Wait until all sockets are closed.
    {
        let (lock, cvar) = &arc.socket_notify;
        let Ok(guard) = lock.lock() else {
            return crate::error::fail(crate::error::ETERM);
        };
        if cvar
            .wait_while(guard, |()| {
                arc.socket_count.load(Ordering::Acquire) > 0
                    || arc.linger_count.load(Ordering::Acquire) > 0
            })
            .is_err()
        {
            return crate::error::fail(crate::error::ETERM);
        }
    }

    if let Some(ctx) = arc.ctx.get() {
        ctx.term();
    }
    drop(arc);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_destroy(ctx_ptr: *mut libc::c_void) -> c_int {
    zmq_ctx_term(ctx_ptr)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_term(ctx_ptr: *mut libc::c_void) -> c_int {
    zmq_ctx_term(ctx_ptr)
}

const ZMQ_IO_THREADS: c_int = 1;
const ZMQ_MAX_SOCKETS: c_int = 2;
const ZMQ_SOCKET_LIMIT: c_int = 3;
const ZMQ_MAX_MSGSZ: c_int = 5;
const ZMQ_MSG_T_SIZE: c_int = 6;
const ZMQ_ZERO_COPY_RECV: c_int = 10;
const ZMQ_IPV6_CTX: c_int = 42;
const ZMQ_BLOCKY: c_int = 70;

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set(ctx_ptr: *mut libc::c_void, option: c_int, value: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => {
            if value < 0 || ctx.socket_count.load(Ordering::Acquire) != 0 || ctx.ctx.get().is_some()
            {
                return crate::error::fail(libc::EINVAL);
            }
            ctx.configured_io_threads.store(value, Ordering::Release);
        }
        ZMQ_MAX_SOCKETS => {
            if value < 0 {
                return crate::error::fail(libc::EINVAL);
            }
            ctx.max_sockets.store(value, Ordering::Relaxed);
        }
        ZMQ_MAX_MSGSZ => {
            ctx.max_msg_size.store(i64::from(value), Ordering::Relaxed);
        }
        ZMQ_IPV6_CTX => {
            ctx.ipv6.store(value != 0, Ordering::Release);
        }
        ZMQ_BLOCKY => {
            ctx.blocky.store(value != 0, Ordering::Release);
        }
        ZMQ_ZERO_COPY_RECV => {
            ctx.zero_copy_recv.store(value != 0, Ordering::Release);
        }
        ZMQ_SOCKET_LIMIT => {}
        _ => return crate::error::fail(libc::EINVAL),
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get(ctx_ptr: *mut libc::c_void, option: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => ctx.configured_io_threads.load(Ordering::Acquire),
        ZMQ_MAX_SOCKETS | ZMQ_SOCKET_LIMIT => ctx.max_sockets.load(Ordering::Relaxed),
        ZMQ_MAX_MSGSZ => ctx.max_msg_size.load(Ordering::Relaxed) as c_int,
        ZMQ_MSG_T_SIZE => c_int::try_from(crate::msg::ZMQ_MSG_T_SIZE).unwrap_or(c_int::MAX),
        ZMQ_ZERO_COPY_RECV => c_int::from(ctx.zero_copy_recv.load(Ordering::Acquire)),
        ZMQ_IPV6_CTX => c_int::from(ctx.ipv6.load(Ordering::Acquire)),
        ZMQ_BLOCKY => c_int::from(ctx.blocky.load(Ordering::Acquire)),
        _ => crate::error::fail(libc::EINVAL),
    }
}
