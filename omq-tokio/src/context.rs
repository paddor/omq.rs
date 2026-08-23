//! Runtime-owning context for omq sockets.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rustc_hash::FxHashMap;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use omq_proto::options::Options;
use omq_proto::proto::SocketType;

use crate::Socket;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Configuration for a [`Context`] that owns its own tokio runtime.
///
/// ```
/// use omq_tokio::ContextConfig;
///
/// // 4 IO threads (4 independent `current_thread` runtimes).
/// // Multi-IO contexts add an internal runtime for socket control work.
/// let cfg = ContextConfig { io_threads: 4 };
///
/// // Read from OMQ_IO_THREADS env var, default 1.
/// let cfg = ContextConfig::from_env();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ContextConfig {
    /// Number of data-plane IO threads. Each IO thread runs an independent
    /// `current_thread` tokio runtime on its own OS thread. Contexts with more
    /// than one IO thread also use one uncounted runtime for socket control
    /// work. Zero disables owned runtimes and uses the caller's active runtime
    /// instead. Default: 1.
    pub io_threads: usize,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self { io_threads: 1 }
    }
}

impl ContextConfig {
    /// Read configuration from environment variables.
    ///
    /// - `OMQ_IO_THREADS`: number of IO threads (default 1).
    pub fn from_env() -> Self {
        let io_threads = std::env::var("OMQ_IO_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        Self { io_threads }
    }

    /// Build a single `current_thread` tokio
    /// [`Runtime`](tokio::runtime::Runtime). Useful for benchmarks
    /// that need direct `rt.block_on()` without the `Context`
    /// background-thread overhead.
    pub fn build_runtime(self) -> tokio::runtime::Runtime {
        build_current_thread_runtime()
    }
}

// ---- IO thread pool (private) ------------------------------------------------

struct IoThread {
    handle: Handle,
    load: AtomicUsize,
}

struct IoThreadPool {
    threads: Vec<IoThread>,
    primary_job_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<BoxFuture>>>,
    cancel: CancellationToken,
    joins: Mutex<Vec<Option<thread::JoinHandle<()>>>>,
}

impl IoThreadPool {
    fn new(n: usize) -> Arc<Self> {
        assert!(n >= 1);
        let cancel = CancellationToken::new();
        // Multi-IO contexts reserve runtime 0 for socket actors and blocking
        // control calls. The configured count applies to data runtimes.
        let runtime_count = if n > 1 { n + 1 } else { 1 };
        let mut threads = Vec::with_capacity(runtime_count);
        let mut joins = Vec::with_capacity(runtime_count);
        let mut primary_job_tx = None;

        for i in 0..runtime_count {
            let (handle_tx, handle_rx) = mpsc::channel::<Handle>();
            let cancel_i = cancel.clone();

            let join = if i == 0 {
                let (job_tx, mut job_rx) = tokio::sync::mpsc::unbounded_channel::<BoxFuture>();
                primary_job_tx = Some(job_tx);
                thread::Builder::new()
                    .name("omq-io-0".into())
                    .spawn(move || {
                        let rt = build_current_thread_runtime();
                        let _ = handle_tx.send(rt.handle().clone());
                        rt.block_on(async move {
                            loop {
                                let mut fut = tokio::select! {
                                    biased;
                                    () = cancel_i.cancelled() => break,
                                    job = job_rx.recv() => match job {
                                        Some(fut) => fut,
                                        None => break,
                                    },
                                };
                                tokio::select! {
                                    biased;
                                    () = cancel_i.cancelled() => break,
                                    () = fut.as_mut() => {}
                                }
                            }
                        });
                    })
                    .expect("omq: failed to spawn primary IO thread")
            } else {
                let name = format!("omq-io-{i}");
                thread::Builder::new()
                    .name(name)
                    .spawn(move || {
                        let rt = build_current_thread_runtime();
                        let _ = handle_tx.send(rt.handle().clone());
                        rt.block_on(cancel_i.cancelled());
                    })
                    .expect("omq: failed to spawn IO thread")
            };

            let handle = handle_rx.recv().expect("omq: runtime handle");
            threads.push(IoThread {
                handle,
                load: AtomicUsize::new(0),
            });
            joins.push(Some(join));
        }

        Arc::new(Self {
            threads,
            primary_job_tx: Mutex::new(primary_job_tx),
            cancel,
            joins: Mutex::new(joins),
        })
    }

    fn primary_handle(&self) -> &Handle {
        &self.threads[0].handle
    }

    fn thread_count(&self) -> usize {
        self.threads.len() - self.data_thread_offset()
    }

    fn data_thread_offset(&self) -> usize {
        usize::from(self.threads.len() > 1)
    }

    fn assign_thread(&self) -> usize {
        // Return a logical data-thread index. Multi-IO runtime 0 stays
        // reserved for socket actors and binding control calls.
        let offset = self.data_thread_offset();
        let best = self
            .threads
            .get(offset..)
            .expect("data-thread offset is in bounds")
            .iter()
            .enumerate()
            .min_by_key(|(_, t)| t.load.load(Ordering::Relaxed))
            .map_or(0, |(i, _)| i);
        self.threads[best + offset]
            .load
            .fetch_add(1, Ordering::Relaxed);
        best
    }

    fn release_thread(&self, index: usize) {
        self.threads[index + self.data_thread_offset()]
            .load
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn shutdown(&self) {
        self.cancel.cancel();
        *self.primary_job_tx.lock().expect("job_tx poisoned") = None;
        let mut joins = self.joins.lock().expect("joins poisoned");
        for j in joins.iter_mut() {
            if let Some(handle) = j.take() {
                let _ = handle.join();
            }
        }
    }
}

impl std::fmt::Debug for IoThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoThreadPool")
            .field("thread_count", &self.thread_count())
            .finish_non_exhaustive()
    }
}

// ---- IoPoolHandle (pub(crate)) -----------------------------------------------

/// Handle to the IO thread pool for spawning tasks on specific IO
/// threads. When the inner pool is `None`, all spawning uses bare
/// `tokio::spawn()` (single-thread or borrowed-runtime mode).
#[derive(Clone, Debug)]
pub(crate) struct IoPoolHandle {
    pool: Option<Arc<IoThreadPool>>,
}

impl IoPoolHandle {
    pub(crate) fn none() -> Self {
        Self { pool: None }
    }

    /// Spawn a future on the primary IO thread (index 0).
    pub(crate) fn spawn_primary<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future<Output: Send> + Send + 'static,
    {
        match &self.pool {
            None => tokio::spawn(fut),
            Some(pool) => pool.threads[0].handle.spawn(fut),
        }
    }

    /// Spawn a future on a specific IO thread.
    pub(crate) fn spawn_on<F>(&self, index: usize, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future<Output: Send> + Send + 'static,
    {
        match &self.pool {
            None => tokio::spawn(fut),
            Some(pool) => pool.threads[index + pool.data_thread_offset()]
                .handle
                .spawn(fut),
        }
    }

    /// Whether data-plane tasks run on owned runtimes separate from the
    /// primary socket runtime.
    pub(crate) fn has_dedicated_io_threads(&self) -> bool {
        self.pool
            .as_ref()
            .is_some_and(|pool| pool.data_thread_offset() != 0)
    }

    /// Pick the least-loaded IO thread, increment its load, return
    /// the thread index.
    pub(crate) fn assign_thread(&self) -> usize {
        match &self.pool {
            None => 0,
            Some(pool) => pool.assign_thread(),
        }
    }

    /// Decrement load on a thread (peer removed).
    pub(crate) fn release_thread(&self, index: usize) {
        if let Some(pool) = &self.pool {
            pool.release_thread(index);
        }
    }

    /// Number of IO threads.
    pub(crate) fn thread_count(&self) -> usize {
        match &self.pool {
            None => 1,
            Some(pool) => pool.thread_count(),
        }
    }
}

// ---- Context -----------------------------------------------------------------

/// A runtime context for omq sockets.
///
/// # Owned runtime (default)
///
/// `Context::new()` and `Context::with_config()` spawn dedicated OS
/// threads, each running an independent `current_thread` tokio runtime.
/// The configured count controls data-plane runtimes. Multi-IO contexts add
/// one internal runtime for socket actors and blocking control calls.
/// The user does not need tokio in their own `Cargo.toml` for OMQ IO work.
///
/// ```no_run
/// use omq_tokio::{Context, SocketType, Options, Message};
///
/// # async fn example() {
/// let ctx = Context::new();
/// let sock = ctx.socket(SocketType::Push, Options::default());
/// sock.bind("tcp://*:5555".parse().unwrap()).await.unwrap();
/// sock.send(Message::from("hello")).await.unwrap();
/// # }
/// ```
///
/// In a plain `fn main()`, either use [`blocking_socket`](Self::blocking_socket)
/// or [`block_on`](Self::block_on) as a small executor helper.
///
/// # Embedded in an existing runtime
///
/// `Context::current()` wraps the caller's active tokio runtime.
/// No background thread is spawned.
///
/// ```no_run
/// use omq_tokio::{Context, SocketType, Options};
///
/// # async fn example() {
/// let ctx = Context::current();
/// let sock = ctx.socket(SocketType::Pull, Options::default());
/// let msg = sock.recv().await.unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Context {
    inner: Arc<ContextCore>,
}

enum RuntimeOwnership {
    Owned { pool: Arc<IoThreadPool> },
    Borrowed { handle: Handle },
}

/// Shared context core.
///
/// A `ContextCore` owns the runtime, the per-context `inproc://`
/// namespace, and the language-neutral share key used by bindings.
pub struct ContextCore {
    ownership: RuntimeOwnership,
    owner_pid: u32,
    io_threads: usize,
    terminated: AtomicBool,
    share_key: u128,
    inproc_registry: Arc<crate::transport::inproc::InprocRegistry>,
}

impl std::fmt::Debug for ContextCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextCore")
            .field("io_threads", &self.io_threads)
            .field("owner_pid", &self.owner_pid)
            .field("share_key", &format_args!("{:032x}", self.share_key))
            .field(
                "owned",
                &matches!(self.ownership, RuntimeOwnership::Owned { .. }),
            )
            .field("terminated", &self.terminated.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

static NEXT_CONTEXT_KEY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static CONTEXT_KEY_PREFIX: LazyLock<u64> = LazyLock::new(|| {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos ^ (u64::from(std::process::id()) << 32)
});
static CONTEXT_REGISTRY: LazyLock<Mutex<FxHashMap<u128, Weak<ContextCore>>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

fn next_context_key() -> u128 {
    let low = NEXT_CONTEXT_KEY.fetch_add(1, Ordering::Relaxed);
    (u128::from(*CONTEXT_KEY_PREFIX) << 64) | u128::from(low)
}

fn register_context_core(core: &Arc<ContextCore>) {
    let mut registry = CONTEXT_REGISTRY.lock().expect("context registry poisoned");
    registry.retain(|_, weak| weak.strong_count() > 0);
    registry.insert(core.share_key, Arc::downgrade(core));
}

fn unregister_context_core(share_key: u128) {
    if let Ok(mut registry) = CONTEXT_REGISTRY.lock() {
        registry.remove(&share_key);
    }
}

impl ContextCore {
    fn new(ownership: RuntimeOwnership, io_threads: usize) -> Arc<Self> {
        let core = Arc::new(Self {
            ownership,
            owner_pid: std::process::id(),
            io_threads,
            terminated: AtomicBool::new(false),
            share_key: next_context_key(),
            inproc_registry: Arc::new(crate::transport::inproc::InprocRegistry::new()),
        });
        register_context_core(&core);
        core
    }

    fn is_owner_process(&self) -> bool {
        self.owner_pid == std::process::id()
    }

    fn primary_handle(&self) -> &Handle {
        match &self.ownership {
            RuntimeOwnership::Owned { pool } => pool.primary_handle(),
            RuntimeOwnership::Borrowed { handle } => handle,
        }
    }

    fn io_pool_handle(&self) -> IoPoolHandle {
        match &self.ownership {
            RuntimeOwnership::Owned { pool } => IoPoolHandle {
                pool: Some(pool.clone()),
            },
            RuntimeOwnership::Borrowed { .. } => IoPoolHandle::none(),
        }
    }

    /// Language-neutral opaque key for importing this context in another
    /// binding loaded into the same native library image.
    pub fn share_key(&self) -> u128 {
        self.share_key
    }
}

impl Context {
    /// Create a context with 1 data-plane IO thread (`current_thread` runtime
    /// on a dedicated OS thread). This is the libzmq-like default. Multi-IO
    /// contexts use an additional internal control runtime.
    pub fn new() -> Self {
        Self::with_config(ContextConfig::default())
    }

    /// Create a context with custom configuration.
    ///
    /// Each data-plane IO thread runs an independent `current_thread` tokio
    /// runtime on its own OS thread. Connections are pinned to an IO thread
    /// for life (least-loaded assignment at connect/accept time). Contexts
    /// with more than one IO thread add an internal control runtime. With zero
    /// IO threads, this is equivalent to [`Context::current`] and requires an
    /// active tokio runtime.
    pub fn with_config(config: ContextConfig) -> Self {
        if config.io_threads == 0 {
            return Self::current();
        }
        let io_threads = config.io_threads;
        let pool = IoThreadPool::new(io_threads);
        Self {
            inner: ContextCore::new(RuntimeOwnership::Owned { pool }, io_threads),
        }
    }

    /// Wrap the caller's active tokio runtime. No background thread is
    /// spawned; the context borrows the existing runtime.
    ///
    /// [`block_on()`](Self::block_on) panics on a borrowed context.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context.
    pub fn current() -> Self {
        let handle =
            Handle::try_current().expect("Context::current() called outside a tokio runtime");
        Self {
            inner: ContextCore::new(RuntimeOwnership::Borrowed { handle }, 0),
        }
    }

    /// Return the opaque `u128` key for this context core.
    ///
    /// The key is only meaningful inside the current process and native
    /// library image. Importing it creates another `Context` handle to the
    /// same runtime and `inproc://` namespace.
    pub fn share_key(&self) -> u128 {
        self.inner.share_key()
    }

    /// Import a context by a key previously returned by [`share_key`](Self::share_key).
    pub fn from_share_key(share_key: u128) -> Option<Self> {
        let core = {
            let mut registry = CONTEXT_REGISTRY.lock().ok()?;
            let core = registry.get(&share_key).and_then(Weak::upgrade);
            if core.is_none() {
                registry.remove(&share_key);
            }
            core?
        };
        if !core.is_owner_process() || core.terminated.load(Ordering::Acquire) {
            return None;
        }
        Some(Self { inner: core })
    }

    #[doc(hidden)]
    pub fn core(&self) -> Arc<ContextCore> {
        self.inner.clone()
    }

    #[doc(hidden)]
    pub fn from_core(core: Arc<ContextCore>) -> Option<Self> {
        if !core.is_owner_process() || core.terminated.load(Ordering::Acquire) {
            return None;
        }
        Some(Self { inner: core })
    }

    /// Create a blocking socket on this context's runtime.
    ///
    /// Each method blocks the calling thread via
    /// [`block_on`](Self::block_on). For async usage, use
    /// [`socket()`](Self::socket).
    ///
    /// # Panics
    ///
    /// Panics on a borrowed context ([`Context::current()`]).
    pub fn blocking_socket(
        &self,
        socket_type: SocketType,
        options: Options,
    ) -> crate::blocking::Socket {
        assert!(
            self.io_threads() > 0,
            "blocking_socket() requires at least one owned IO thread"
        );
        crate::blocking::Socket::new(self.socket(socket_type, options), self.clone())
    }

    /// Create an async socket on this context's runtime.
    pub fn socket(&self, socket_type: SocketType, options: Options) -> Socket {
        assert!(
            !self.inner.terminated.load(Ordering::Acquire),
            "Context::socket() called on a terminated context"
        );
        assert!(
            self.inner.is_owner_process(),
            "Context::socket() called on a context inherited across fork"
        );
        let _guard = self.inner.primary_handle().enter();
        let io_pool = self.inner.io_pool_handle();
        Socket::new_with_io_pool(
            socket_type,
            options,
            &io_pool,
            self.inner.inproc_registry.clone(),
        )
    }

    #[doc(hidden)]
    pub fn socket_with_recv_sink_config(
        &self,
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Socket {
        assert!(
            !self.inner.terminated.load(Ordering::Acquire),
            "Context::socket_with_recv_sink_config() called on a terminated context"
        );
        assert!(
            self.inner.is_owner_process(),
            "Context::socket_with_recv_sink_config() called on a context inherited across fork"
        );
        let _guard = self.inner.primary_handle().enter();
        let io_pool = self.inner.io_pool_handle();
        Socket::new_with_recv_sink_config_and_io_pool(
            socket_type,
            options,
            config,
            &io_pool,
            self.inner.inproc_registry.clone(),
        )
    }

    /// Run a future on this context's runtime, blocking the calling
    /// thread until it completes. The future runs inline on the
    /// primary IO thread with the same priority as spawned driver tasks.
    /// If the caller already has an async runtime, await socket futures
    /// directly instead.
    ///
    /// # Panics
    ///
    /// Panics if the context was created with [`Context::current()`]
    /// (the caller is already async; just `.await` directly).
    pub fn block_on<F, T>(&self, f: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let RuntimeOwnership::Owned { ref pool } = self.inner.ownership else {
            panic!(
                "Context::block_on() is not available on a borrowed context \
                 (created with Context::current())"
            );
        };
        assert!(
            !self.inner.terminated.load(Ordering::Acquire),
            "Context::block_on() called on a terminated context"
        );
        assert!(
            self.inner.is_owner_process(),
            "Context::block_on() called on a context inherited across fork"
        );
        let guard = pool.primary_job_tx.lock().expect("job_tx poisoned");
        let job_tx = guard
            .as_ref()
            .expect("Context::block_on() called on a terminated context");
        let (result_tx, result_rx) = mpsc::channel();
        let fut: BoxFuture = Box::pin(async move {
            let result = f.await;
            let _ = result_tx.send(result);
        });
        job_tx.send(fut).expect("omq: context runtime exited");
        drop(guard);
        result_rx
            .recv()
            .expect("omq: context runtime exited unexpectedly")
    }

    /// Return the tokio runtime handle for the primary IO thread.
    pub fn handle(&self) -> &Handle {
        self.inner.primary_handle()
    }

    /// Number of IO threads. Returns 0 for a borrowed context
    /// ([`Context::current()`]).
    pub fn io_threads(&self) -> usize {
        self.inner.io_threads
    }

    /// Whether this context core has been terminated.
    pub fn is_terminated(&self) -> bool {
        self.inner.terminated.load(Ordering::Acquire)
    }

    /// Shut down this context's runtime. All spawned driver tasks are
    /// aborted and the background threads exit.
    ///
    /// No-op for a borrowed context ([`Context::current()`]).
    /// No-op if already terminated.
    pub fn term(&self) {
        if self.inner.terminated.swap(true, Ordering::AcqRel) {
            return;
        }
        unregister_context_core(self.inner.share_key);
        if !self.inner.is_owner_process() {
            return;
        }
        if let RuntimeOwnership::Owned { ref pool } = self.inner.ownership {
            pool.shutdown();
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ContextCore {
    fn drop(&mut self) {
        unregister_context_core(self.share_key);
        if !self.is_owner_process() {
            return;
        }
        if let RuntimeOwnership::Owned { ref pool } = self.ownership {
            pool.shutdown();
        }
    }
}

fn build_current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("omq: failed to build current_thread runtime")
}

#[cfg(test)]
mod tests {
    use super::IoThreadPool;

    #[test]
    fn single_io_thread_shares_control_runtime() {
        let pool = IoThreadPool::new(1);
        assert_eq!(pool.threads.len(), 1);
        assert_eq!(pool.thread_count(), 1);
        assert_eq!(pool.assign_thread(), 0);
        pool.shutdown();
    }

    #[test]
    fn configured_count_excludes_control_runtime() {
        let pool = IoThreadPool::new(3);
        assert_eq!(pool.threads.len(), 4);
        assert_eq!(pool.thread_count(), 3);
        assert_eq!(pool.assign_thread(), 0);
        assert_eq!(pool.assign_thread(), 1);
        assert_eq!(pool.assign_thread(), 2);
        pool.shutdown();
    }
}
