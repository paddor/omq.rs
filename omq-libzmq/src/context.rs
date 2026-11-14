//! Context: owns a tokio runtime on a background thread.

use std::ffi::{c_int, c_void};
#[cfg(unix)]
use std::sync::Once;

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use omq_tokio::engine::StateSignal;
use tokio::runtime::Handle;

use crate::error::c_try;
use crate::notify::NotifyHandle;
use crate::opts::OptOut;

/// Per-context: lazily-created `omq_tokio::Context` and ZMQ state.
pub(crate) struct OmqContext {
    pub(crate) ctx: OnceLock<omq_tokio::Context>,
    pub terminated: Arc<AtomicBool>,
    shutdown_signal: StateSignal,
    pub socket_count: AtomicI32,
    work_count: AtomicI32,
    socket_notify: (Mutex<()>, Condvar),
    sockets: Mutex<Vec<Weak<crate::socket::OmqSocket>>>,
    pub(crate) options: ContextOptions,
    pub(crate) zap: Arc<crate::zap::ZapService>,
    owns_io_context: bool,
}

/// Values set through `zmq_ctx_set` and `zmq_ctx_set_ext`.
pub(crate) struct ContextOptions {
    pub(crate) configured_io_threads: AtomicI32,
    pub(crate) max_sockets: AtomicI32,
    pub(crate) max_msg_size: AtomicI64,
    pub(crate) ipv6: AtomicBool,
    pub(crate) blocky: AtomicBool,
    pub(crate) zero_copy_recv: AtomicBool,
    thread_name_prefix: Mutex<Option<String>>,
}

impl Default for ContextOptions {
    /// libzmq defaults.
    fn default() -> Self {
        Self {
            configured_io_threads: AtomicI32::new(1),
            max_sockets: AtomicI32::new(1023),
            max_msg_size: AtomicI64::new(-1),
            ipv6: AtomicBool::new(false),
            blocky: AtomicBool::new(true),
            zero_copy_recv: AtomicBool::new(true),
            thread_name_prefix: Mutex::new(None),
        }
    }
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
    fn build(io_threads: i32, owns_io_context: bool) -> Self {
        Self {
            ctx: OnceLock::new(),
            terminated: Arc::new(AtomicBool::new(false)),
            shutdown_signal: StateSignal::new(),
            socket_count: AtomicI32::new(0),
            work_count: AtomicI32::new(0),
            socket_notify: (Mutex::new(()), Condvar::new()),
            sockets: Mutex::new(Vec::new()),
            options: ContextOptions {
                configured_io_threads: AtomicI32::new(io_threads),
                ..ContextOptions::default()
            },
            zap: Arc::new(crate::zap::ZapService::default()),
            owns_io_context,
        }
    }

    fn new(n_io_threads: usize) -> Arc<Self> {
        let io_threads = i32::try_from(n_io_threads).unwrap_or(i32::MAX);
        Arc::new(Self::build(io_threads, true))
    }

    fn from_io_context(ctx: omq_tokio::Context) -> Arc<Self> {
        let io_threads = i32::try_from(ctx.io_threads()).unwrap_or(1);
        let out = Arc::new(Self::build(io_threads, false));
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

    /// Runtime handle for closing a socket. Unlike `handle`, it is still
    /// returned after `zmq_ctx_shutdown`: the runtime lives until
    /// `zmq_ctx_term` has seen every socket and lingering close end.
    pub(crate) fn close_handle(&self) -> Option<&Handle> {
        self.ctx
            .get()
            .filter(|ctx| !ctx.is_terminated())
            .map(omq_tokio::Context::handle)
    }

    pub(crate) fn io_context(&self) -> Option<&omq_tokio::Context> {
        if self.is_effectively_terminated() {
            return None;
        }
        // ZMQ_IO_THREADS=0 still runs one IO thread for connection setup,
        // the control plane, and receive paths that need a relay task.
        let n = self
            .options
            .configured_io_threads
            .load(Ordering::Acquire)
            .max(1);
        let thread_name_prefix = self
            .options
            .thread_name_prefix
            .lock()
            .expect("thread name prefix poisoned");
        let ctx = self.ctx.get_or_init(|| {
            let config = omq_tokio::ContextConfig {
                io_threads: n as usize,
            };
            match thread_name_prefix.as_deref() {
                Some(prefix) => omq_tokio::Context::with_config_and_name(config, prefix),
                None => omq_tokio::Context::with_config(config),
            }
        });
        (!ctx.is_terminated()).then_some(ctx)
    }

    fn set_thread_name_prefix(&self, prefix: String) -> Result<(), ()> {
        let mut stored = self
            .options
            .thread_name_prefix
            .lock()
            .expect("thread name prefix poisoned");
        if self.socket_count.load(Ordering::Acquire) != 0 || self.ctx.get().is_some() {
            return Err(());
        }
        *stored = Some(prefix);
        Ok(())
    }

    fn thread_name_prefix(&self) -> String {
        self.options
            .thread_name_prefix
            .lock()
            .expect("thread name prefix poisoned")
            .clone()
            .unwrap_or_default()
    }

    pub(crate) fn is_effectively_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
            || self
                .ctx
                .get()
                .is_some_and(omq_tokio::Context::is_terminated)
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
            self.notify_socket_change();
        }
    }

    /// Hold `zmq_ctx_term` until the returned guard drops.
    pub(crate) fn begin_work(self: &Arc<Self>) -> WorkGuard {
        self.work_count.fetch_add(1, Ordering::Relaxed);
        WorkGuard(self.clone())
    }

    fn notify_socket_change(&self) {
        let (lock, cvar) = &self.socket_notify;
        // Serialize with the predicate-to-wait transition in zmq_ctx_term.
        let _guard = lock.lock().expect("context socket wait poisoned");
        cvar.notify_all();
    }

    pub(crate) fn shutdown(&self) {
        self.terminated.store(true, Ordering::Release);
        self.shutdown_signal.notify_changed();
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
        self.notify_socket_change();
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        self.shutdown_signal
            .wait_until(|| self.is_effectively_terminated())
            .await;
    }
}

impl std::fmt::Debug for OmqContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqContext")
            .field("ctx", &self.ctx)
            .field("terminated", &self.terminated.load(Ordering::Relaxed))
            .field("socket_count", &self.socket_count.load(Ordering::Relaxed))
            .field("work_count", &self.work_count.load(Ordering::Relaxed))
            .field(
                "max_sockets",
                &self.options.max_sockets.load(Ordering::Relaxed),
            )
            .field(
                "max_msg_size",
                &self.options.max_msg_size.load(Ordering::Relaxed),
            )
            .field("ipv6", &self.options.ipv6.load(Ordering::Relaxed))
            .field("blocky", &self.options.blocky.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Background work that `zmq_ctx_term` waits for: a lingering close or an
/// async send whose callback has not run. The guard lives in the task, so
/// runtime teardown that drops the task also ends the work.
#[derive(Debug)]
pub(crate) struct WorkGuard(Arc<OmqContext>);

impl Drop for WorkGuard {
    fn drop(&mut self) {
        let prev = self.0.work_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.0.notify_socket_change();
        }
    }
}

// Context handles are tagged; see `handle.rs`.

/// Create a context handle with one IO thread; close it with `zmq_ctx_term`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_new() -> *mut libc::c_void {
    install_process_guards();
    let arc = OmqContext::new(1);
    crate::handle::context_into_raw(arc)
}

/// Create a context with the requested IO thread count; legacy alias for context creation.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_init(io_threads: c_int) -> *mut libc::c_void {
    install_process_guards();
    let n = io_threads.max(0) as usize;
    let arc = OmqContext::new(n);
    crate::handle::context_into_raw(arc)
}

/// Stop context activity and wake blocked socket calls without releasing the context handle.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_shutdown(ctx_ptr: *mut libc::c_void) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API.
    let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
    ctx.shutdown();
    0
}

/// Shut down the context, wait for sockets and background work, and release its handle.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_term(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if omq_tokio::Context::on_io_thread() {
        // Term waits for work that may need this thread.
        return crate::error::fail(libc::EDEADLK);
    }
    // SAFETY: the handle stays valid until this call frees it below. Other
    // threads may still pass it to zmq_socket or zmq_ctx_get while term
    // waits; they must see ETERM.
    let arc = c_try!(unsafe { crate::handle::context(ctx_ptr) }).clone();

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
                    || arc.work_count.load(Ordering::Acquire) > 0
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
    // SAFETY: libzmq forbids using the handle after zmq_ctx_term returns, so
    // this is its final owner.
    drop(unsafe { crate::handle::take_context(ctx_ptr) });
    0
}

/// Write the context's process-local 128-bit IO runtime sharing key as two u64 words.
#[unsafe(no_mangle)]
pub extern "C" fn omq_ctx_share_key(
    ctx_ptr: *mut c_void,
    key_hi: *mut u64,
    key_lo: *mut u64,
) -> c_int {
    if ctx_ptr.is_null() || key_hi.is_null() || key_lo.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: C callers pass null or a pointer from this API.
    let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
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

/// Create a context handle sharing an existing process-local IO runtime; null indicates an
/// invalid key.
#[unsafe(no_mangle)]
pub extern "C" fn omq_ctx_from_share_key(key_hi: u64, key_lo: u64) -> *mut c_void {
    install_process_guards();
    let key = (u128::from(key_hi) << 64) | u128::from(key_lo);
    let Some(ctx) = omq_tokio::Context::from_share_key(key) else {
        let _ = crate::error::fail(libc::EINVAL);
        return std::ptr::null_mut();
    };
    crate::handle::context_into_raw(OmqContext::from_io_context(ctx))
}

/// Terminate and release a context; alias for `zmq_ctx_term`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_destroy(ctx_ptr: *mut libc::c_void) -> c_int {
    zmq_ctx_term(ctx_ptr)
}

/// Terminate and release a context; legacy alias for `zmq_ctx_term`.
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

/// Set an integer context option; return zero on success or -1 with errno on failure.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set(ctx_ptr: *mut libc::c_void, option: c_int, value: c_int) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API.
    let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
    match option {
        ZMQ_IO_THREADS => {
            if value < 0 || ctx.socket_count.load(Ordering::Acquire) != 0 || ctx.ctx.get().is_some()
            {
                return crate::error::fail(libc::EINVAL);
            }
            ctx.options
                .configured_io_threads
                .store(value, Ordering::Release);
        }
        ZMQ_MAX_SOCKETS => {
            if value < 0 {
                return crate::error::fail(libc::EINVAL);
            }
            ctx.options.max_sockets.store(value, Ordering::Relaxed);
        }
        ZMQ_MAX_MSGSZ => {
            ctx.options
                .max_msg_size
                .store(i64::from(value), Ordering::Relaxed);
        }
        ZMQ_IPV6_CTX => {
            ctx.options.ipv6.store(value != 0, Ordering::Release);
        }
        ZMQ_BLOCKY => {
            ctx.options.blocky.store(value != 0, Ordering::Release);
        }
        ZMQ_ZERO_COPY_RECV => {
            ctx.options
                .zero_copy_recv
                .store(value != 0, Ordering::Release);
        }
        ZMQ_THREAD_NAME_PREFIX => {
            if ctx.set_thread_name_prefix(value.to_string()).is_err() {
                return crate::error::fail(libc::EINVAL);
            }
        }
        ZMQ_SOCKET_LIMIT => {}
        _ => return crate::error::fail(libc::EINVAL),
    }
    0
}

/// Read an integer context option; return -1 with errno on failure.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get(ctx_ptr: *mut libc::c_void, option: c_int) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API.
    let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
    match option {
        ZMQ_IO_THREADS => ctx.options.configured_io_threads.load(Ordering::Acquire),
        ZMQ_MAX_SOCKETS | ZMQ_SOCKET_LIMIT => ctx.options.max_sockets.load(Ordering::Relaxed),
        ZMQ_MAX_MSGSZ => ctx.options.max_msg_size.load(Ordering::Relaxed) as c_int,
        ZMQ_MSG_T_SIZE => c_int::try_from(crate::msg::ZMQ_MSG_T_SIZE).unwrap_or(c_int::MAX),
        ZMQ_ZERO_COPY_RECV => c_int::from(ctx.options.zero_copy_recv.load(Ordering::Acquire)),
        ZMQ_THREAD_NAME_PREFIX => ctx.thread_name_prefix().parse().unwrap_or(0),
        ZMQ_IPV6_CTX => c_int::from(ctx.options.ipv6.load(Ordering::Acquire)),
        ZMQ_BLOCKY => c_int::from(ctx.options.blocky.load(Ordering::Acquire)),
        _ => crate::error::fail(libc::EINVAL),
    }
}

/// Set a context option from a caller-owned byte buffer; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set_ext(
    ctx_ptr: *mut libc::c_void,
    option: c_int,
    optval: *const c_void,
    optvallen: usize,
) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if option == ZMQ_THREAD_NAME_PREFIX {
        if optval.is_null() {
            return crate::error::fail(libc::EFAULT);
        }
        if optvallen == 0 || optvallen > 16 {
            return crate::error::fail(libc::EINVAL);
        }
        // SAFETY: optval is non-null and optvallen describes the input bytes.
        let bytes = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
        let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
        if bytes.is_empty() || bytes.contains(&0) {
            return crate::error::fail(libc::EINVAL);
        }
        let Ok(prefix) = std::str::from_utf8(bytes) else {
            return crate::error::fail(libc::EINVAL);
        };
        // SAFETY: C callers pass null or a pointer from this API.
        let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
        return match ctx.set_thread_name_prefix(prefix.to_owned()) {
            Ok(()) => 0,
            Err(()) => crate::error::fail(libc::EINVAL),
        };
    }
    if optval.is_null() || optvallen < std::mem::size_of::<c_int>() {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: optval is non-null and at least sizeof(int) bytes (checked above).
    let value = unsafe { std::ptr::read_unaligned(optval.cast::<c_int>()) };
    zmq_ctx_set(ctx_ptr, option, value)
}

/// Write a context option into caller storage and update its length; return zero or -1 with
/// errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get_ext(
    ctx_ptr: *mut libc::c_void,
    option: c_int,
    optval: *mut c_void,
    optvallen: *mut usize,
) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if option == ZMQ_THREAD_NAME_PREFIX {
        // SAFETY: C callers pass null or a pointer from this API.
        let ctx = c_try!(unsafe { crate::handle::context(ctx_ptr) });
        let out = OptOut::new(optval, optvallen);
        c_try!(out.padded_string(ctx.thread_name_prefix().as_bytes()));
        return 0;
    }
    let value = zmq_ctx_get(ctx_ptr, option);
    if value == -1 && option != ZMQ_MAX_MSGSZ {
        return -1;
    }
    c_try!(OptOut::new(optval, optvallen).i32(value));
    0
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn padded_string_zeroes_the_rest_of_the_buffer() {
        let mut buf = [0xFF_u8; 8];
        let mut len = buf.len();
        let rc = OptOut::new(buf.as_mut_ptr().cast(), &raw mut len).padded_string(b"omq");
        assert_eq!(rc, Ok(()));
        assert_eq!(len, 4);
        assert_eq!(&buf, b"omq\0\0\0\0\0");

        let mut small = 3;
        let rc = OptOut::new(buf.as_mut_ptr().cast(), &raw mut small).padded_string(b"omq");
        assert_eq!(rc, Err(libc::EINVAL));
        assert_eq!(small, 3);
        let rc = OptOut::new(buf.as_mut_ptr().cast(), std::ptr::null_mut()).padded_string(b"omq");
        assert_eq!(rc, Err(libc::EFAULT));
    }

    #[test]
    fn final_close_and_work_wake_a_waiter_between_check_and_wait() {
        for work in [false, true] {
            let context = OmqContext::new(1);
            let mut work_guard = None;
            let counter = if work {
                work_guard = Some(context.begin_work());
                &context.work_count
            } else {
                context.socket_opened();
                &context.socket_count
            };
            let (lock, cvar) = &context.socket_notify;
            let guard = lock.lock().unwrap();
            assert_eq!(counter.load(Ordering::Acquire), 1);

            let closing = context.clone();
            let thread = std::thread::spawn(move || {
                if let Some(work_guard) = work_guard {
                    drop(work_guard);
                } else {
                    closing.socket_closed();
                }
            });
            // Force the last completion after the predicate check, while
            // this thread still owns the condition variable's mutex.
            let deadline = Instant::now() + Duration::from_secs(5);
            while counter.load(Ordering::Acquire) != 0 {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            let (guard, waited) = cvar.wait_timeout(guard, Duration::from_secs(5)).unwrap();
            assert!(!waited.timed_out(), "last completion wake was lost");
            drop(guard);
            thread.join().unwrap();
        }
    }

    fn shared_context(owner: *mut c_void) -> *mut c_void {
        let (mut hi, mut lo) = (0, 0);
        assert_eq!(omq_ctx_share_key(owner, &raw mut hi, &raw mut lo), 0);
        let shared = omq_ctx_from_share_key(hi, lo);
        assert!(!shared.is_null());
        shared
    }

    #[test]
    fn runtime_teardown_under_a_lingering_close_releases_term() {
        let owner = zmq_ctx_new();
        let shared = shared_context(owner);
        let push = crate::zmq_socket(shared, 8);
        assert_eq!(
            crate::zmq_connect(push, c"inproc://linger-runtime-teardown".as_ptr()),
            0
        );
        // Queued for a peer that never binds, so the default infinite linger
        // keeps the close task alive until the owner's runtime drops it.
        assert_eq!(crate::zmq_send(push, b"x".as_ptr().cast(), 1, 0), 1);
        assert_eq!(crate::zmq_close(push), 0);
        assert_eq!(zmq_ctx_term(owner), 0);

        let shared = shared as usize;
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done_tx.send(zmq_ctx_term(shared as *mut c_void));
        });
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)), Ok(0));
    }
}
