//! Context: owns a tokio runtime on a background thread.

use std::ffi::{c_int, c_void};
#[cfg(unix)]
use std::sync::Once;

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
    pub(crate) zap: Arc<crate::zap::ZapService>,
    owns_io_context: bool,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(unix)]
static PROCESS_GUARDS: Once = Once::new();

#[cfg(unix)]
fn install_process_guards() {
    PROCESS_GUARDS.call_once(|| {
        // Rust binaries ignore SIGPIPE during std startup. C/Lua hosts loading
        // omq-libzmq as a cdylib do not get that guard.
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        }
    });
}

#[cfg(not(unix))]
fn install_process_guards() {}

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
            zap: Arc::new(crate::zap::ZapService::default()),
            owns_io_context: true,
        })
    }

    fn from_io_context(ctx: omq_tokio::Context) -> Arc<Self> {
        let out = Arc::new(Self {
            ctx: OnceLock::new(),
            configured_io_threads: AtomicI32::new(i32::try_from(ctx.io_threads()).unwrap_or(1)),
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
            zap: Arc::new(crate::zap::ZapService::default()),
            owns_io_context: false,
        });
        out.ctx
            .set(ctx)
            .expect("new imported context should be empty");
        out
    }

    pub(crate) fn handle(&self) -> Option<&Handle> {
        if self.is_effectively_terminated() {
            return None;
        }
        self.ctx.get().map(omq_tokio::Context::handle)
    }

    pub(crate) fn io_context(&self) -> Option<&omq_tokio::Context> {
        if self.is_effectively_terminated() {
            return None;
        }
        let n = self.configured_io_threads.load(Ordering::Acquire);
        if n <= 0 {
            return None;
        }
        let ctx = self.ctx.get_or_init(|| {
            omq_tokio::Context::with_config(omq_tokio::ContextConfig {
                io_threads: n as usize,
            })
        });
        (!ctx.is_terminated()).then_some(ctx)
    }

    pub(crate) fn is_effectively_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
            || self
                .ctx
                .get()
                .is_some_and(omq_tokio::Context::is_terminated)
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
        self.zap.shutdown();
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
    install_process_guards();
    let arc = OmqContext::new(1);
    Box::into_raw(Box::new(arc)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_init(io_threads: c_int) -> *mut libc::c_void {
    install_process_guards();
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

    if arc.owns_io_context
        && let Some(ctx) = arc.ctx.get()
    {
        ctx.term();
    }
    drop(arc);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_ctx_share_key(
    ctx_ptr: *mut c_void,
    key_hi: *mut u64,
    key_lo: *mut u64,
) -> c_int {
    if ctx_ptr.is_null() || key_hi.is_null() || key_lo.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from this library.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    if ctx.is_effectively_terminated() {
        return crate::error::fail(crate::error::ETERM);
    }
    let Some(io_ctx) = ctx.io_context() else {
        return crate::error::fail(libc::ENOTSUP);
    };
    let key = io_ctx.share_key();
    // SAFETY: output pointers were checked non-null above.
    unsafe {
        *key_hi = (key >> 64) as u64;
        *key_lo = key as u64;
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_ctx_from_share_key(key_hi: u64, key_lo: u64) -> *mut c_void {
    install_process_guards();
    let key = (u128::from(key_hi) << 64) | u128::from(key_lo);
    let Some(ctx) = omq_tokio::Context::from_share_key(key) else {
        let _ = crate::error::fail(libc::EINVAL);
        return std::ptr::null_mut();
    };
    Box::into_raw(Box::new(OmqContext::from_io_context(ctx))).cast()
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
const ZMQ_THREAD_NAME_PREFIX: c_int = 9;
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

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set_ext(
    ctx_ptr: *mut libc::c_void,
    option: c_int,
    optval: *const c_void,
    optvallen: usize,
) -> c_int {
    if option == ZMQ_THREAD_NAME_PREFIX {
        if optval.is_null() && optvallen > 0 {
            return crate::error::fail(libc::EFAULT);
        }
        return 0;
    }
    if optval.is_null() || optvallen < std::mem::size_of::<c_int>() {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null and at least sizeof(int) bytes (checked above).
    let value = unsafe { std::ptr::read_unaligned(optval.cast::<c_int>()) };
    zmq_ctx_set(ctx_ptr, option, value)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get_ext(
    ctx_ptr: *mut libc::c_void,
    option: c_int,
    optval: *mut c_void,
    optvallen: *mut usize,
) -> c_int {
    if option == ZMQ_THREAD_NAME_PREFIX {
        return write_bytes(optval, optvallen, b"");
    }
    let value = zmq_ctx_get(ctx_ptr, option);
    if value == -1 && option != ZMQ_MAX_MSGSZ {
        return -1;
    }
    write_i32(optval, optvallen, value)
}

fn write_i32(optval: *mut c_void, optvallen: *mut usize, value: c_int) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    if avail < std::mem::size_of::<c_int>() {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: output buffer is non-null and large enough (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        std::ptr::write_unaligned(optval.cast::<c_int>(), value);
        *optvallen = std::mem::size_of::<c_int>();
    }
    0
}

fn write_bytes(optval: *mut c_void, optvallen: *mut usize, bytes: &[u8]) -> c_int {
    if optval.is_null() || optvallen.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: optvallen is non-null (checked above).
    let avail = unsafe { *optvallen };
    let needed = bytes.len() + 1;
    if avail < needed {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: output buffer is non-null and large enough (checked above).
    unsafe {
        std::ptr::write_bytes(optval.cast::<u8>(), 0, avail);
        if !bytes.is_empty() {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), optval.cast::<u8>(), bytes.len());
        }
        *optvallen = needed;
    }
    0
}
