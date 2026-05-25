//! Context: owns N io threads, each running a compio runtime.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_int;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use omq_compio::Socket as InnerSocket;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct IoThread {
    submit: flume::Sender<Job>,
    // Keep the JoinHandle so the thread is joined when the context drops.
    #[allow(dead_code)]
    handle: Option<thread::JoinHandle<()>>,
}

/// Per-context: one omq runtime per io thread.
pub(crate) struct OmqContext {
    pub(crate) io_threads: Vec<IoThread>,
    pub next_thread: AtomicUsize,
    pub terminated: Arc<AtomicBool>,
    pub socket_count: AtomicI32,
    socket_notify: (Mutex<()>, Condvar),
    pub max_sockets: AtomicI32,
    pub max_msg_size: AtomicI64,
    /// Zmq-layer inproc registry. Maps inproc name to the bound `OmqSocket`.
    /// Used to install bypass pipes when both sides are present.
    pub(crate) inproc_binds: Mutex<HashMap<String, std::sync::Weak<crate::socket::OmqSocket>>>,
    /// Pending inproc connect requests waiting for a bind.
    pub(crate) inproc_waiting:
        Mutex<HashMap<String, Vec<std::sync::Weak<crate::socket::OmqSocket>>>>,
}

thread_local! {
    /// Io-thread-local registry: socket id -> Rc<InnerSocket>.
    pub(crate) static REG: RefCell<HashMap<u64, Rc<InnerSocket>>> =
        RefCell::new(HashMap::new());
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_socket_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn build_compio_runtime() -> std::io::Result<compio::runtime::Runtime> {
    use omq_compio::ProactorBuilderExt;

    let mut runtime_builder = compio::runtime::RuntimeBuilder::new();
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor.with_omq_buffer_pool();
    if let Ok(raw) = std::env::var("OMQ_SQPOLL_IDLE_MS")
        && let Ok(ms) = raw.parse::<u64>()
    {
        proactor.sqpoll_idle(std::time::Duration::from_millis(ms));
    }
    runtime_builder.with_proactor(proactor);
    runtime_builder.build()
}

impl OmqContext {
    #[allow(clippy::arc_with_non_send_sync)]
    fn new(n_io_threads: usize) -> Arc<Self> {
        let n = n_io_threads.max(1);
        let terminated = Arc::new(AtomicBool::new(false));
        let mut io_threads = Vec::with_capacity(n);
        for i in 0..n {
            let (tx, rx) = flume::unbounded::<Job>();
            let name = format!("omq-libzmq-io-{i}");
            let handle = thread::Builder::new()
                .name(name)
                .spawn(move || {
                    let rt = build_compio_runtime().expect("omq-libzmq: compio runtime");
                    rt.block_on(async move {
                        while let Ok(job) = rx.recv_async().await {
                            job();
                        }
                    });
                })
                .expect("omq-libzmq: spawn io thread");
            io_threads.push(IoThread {
                submit: tx,
                handle: Some(handle),
            });
        }
        Arc::new(Self {
            io_threads,
            next_thread: AtomicUsize::new(0),
            terminated,
            socket_count: AtomicI32::new(0),
            socket_notify: (Mutex::new(()), Condvar::new()),
            max_sockets: AtomicI32::new(1023),
            max_msg_size: AtomicI64::new(-1),
            inproc_binds: Mutex::new(HashMap::new()),
            inproc_waiting: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn assign_thread(&self) -> usize {
        let n = self.io_threads.len();
        self.next_thread.fetch_add(1, Ordering::Relaxed) % n
    }

    pub(crate) fn submit(&self, thread_idx: usize, job: Job) {
        let _ = self.io_threads[thread_idx].submit.send(job);
    }

    pub(crate) fn socket_opened(&self) {
        self.socket_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn socket_closed(&self) {
        let prev = self.socket_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            let (_, cvar) = &self.socket_notify;
            cvar.notify_all();
            // Drop the lock before notify returns.
        }
    }
}

impl std::fmt::Debug for OmqContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqContext")
            .field("io_threads", &self.io_threads.len())
            .field("terminated", &self.terminated.load(Ordering::Relaxed))
            .field("socket_count", &self.socket_count.load(Ordering::Relaxed))
            .field("max_sockets", &self.max_sockets.load(Ordering::Relaxed))
            .field("max_msg_size", &self.max_msg_size.load(Ordering::Relaxed))
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
pub extern "C" fn zmq_init(_io_threads: c_int) -> *mut libc::c_void {
    zmq_ctx_new()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_shutdown(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // Borrow without consuming.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    ctx.terminated.store(true, Ordering::Release);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_term(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // Consume the Box so we can drop it properly.
    let arc = unsafe { *Box::from_raw(ctx_ptr.cast::<Arc<OmqContext>>()) };

    // Signal termination to all io threads.
    arc.terminated.store(true, Ordering::Release);

    // Wait until all sockets are closed.
    {
        let (lock, cvar) = &arc.socket_notify;
        let guard = lock.lock().unwrap();
        let _guard = cvar
            .wait_while(guard, |()| arc.socket_count.load(Ordering::Acquire) > 0)
            .unwrap();
    }

    // Drop the submit channels so each io-thread's recv_async returns Err.
    // We need mutable access; Arc strong count should be 1 here but to be
    // safe we use a separate channel-drop step.
    // We can't mutate through Arc; instead we just drop the Arc which drops
    // the IoThread vec, closing all the senders.
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
const ZMQ_IPV6_CTX: c_int = 42;

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set(ctx_ptr: *mut libc::c_void, option: c_int, value: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => {
            // io threads already running; setting this is a no-op
        }
        ZMQ_MAX_SOCKETS => {
            ctx.max_sockets.store(value, Ordering::Relaxed);
        }
        ZMQ_MAX_MSGSZ => {
            ctx.max_msg_size.store(i64::from(value), Ordering::Relaxed);
        }
        #[allow(clippy::match_same_arms)]
        ZMQ_SOCKET_LIMIT | ZMQ_IPV6_CTX => {}
        _ => return crate::error::fail(libc::EINVAL),
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get(ctx_ptr: *mut libc::c_void, option: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => c_int::try_from(ctx.io_threads.len()).unwrap_or(c_int::MAX),
        ZMQ_MAX_SOCKETS | ZMQ_SOCKET_LIMIT => ctx.max_sockets.load(Ordering::Relaxed),
        ZMQ_MAX_MSGSZ => ctx.max_msg_size.load(Ordering::Relaxed) as c_int,
        ZMQ_IPV6_CTX => 0,
        _ => crate::error::fail(libc::EINVAL),
    }
}
