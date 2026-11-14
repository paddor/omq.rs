//! Runtime-owning context for omq sockets.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak, mpsc};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use rustc_hash::FxHashMap;
use tokio::runtime::Handle;

use omq_proto::options::Options;
use omq_proto::proto::SocketType;

use crate::Socket;
use crate::socket::handle::{Build, Placement};

mod io_pool;

use io_pool::IoThreadPool;
pub(crate) use io_pool::{IoPoolHandle, IoThreadLease};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

thread_local! {
    static OWNED_IO_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

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
/// sock.bind("tcp://*:5555").await.unwrap();
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
    network_config: Mutex<(crate::NetworkRateLimits, bool)>,
    network: OnceLock<Arc<Vec<crate::network_limit::Lane>>>,
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
            network_config: Mutex::new((crate::NetworkRateLimits::default(), false)),
            network: OnceLock::new(),
            inproc_registry: Arc::new(crate::transport::inproc::InprocRegistry::new()),
        });

        register_context_core(&core);
        core
    }

    fn freeze_network(&self) {
        let mut config = self
            .network_config
            .lock()
            .expect("context network settings");

        if config.1 {
            return;
        }

        config.1 = true;
        if !config.0.enabled() {
            return;
        }

        let RuntimeOwnership::Owned { pool } = &self.ownership else {
            unreachable!()
        };

        let allocator = crate::network_limit::Allocator::new(config.0, pool.threads.len());
        let lanes = Arc::new(allocator.lanes());

        for (thread, lane) in pool.threads.iter().zip(lanes.iter()) {
            thread.handle.spawn(lane.clone().run(pool.cancel.clone()));
        }

        pool.primary_handle()
            .spawn(allocator.run(pool.cancel.clone()));
        self.network
            .set(lanes)
            .expect("network settings frozen once");
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

    pub(crate) fn io_pool_handle(&self) -> IoPoolHandle {
        match &self.ownership {
            RuntimeOwnership::Owned { pool } => IoPoolHandle {
                pool: Some(pool.clone()),
                network: self.network.get().cloned(),
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
    /// Set aggregate network byte rates before creating the first socket.
    ///
    /// Requires an OMQ-owned runtime when either direction is limited. Limits
    /// are shared by all handles importing this context. Setup traffic is
    /// exempt; transport headers below the OMQ stream/datagram are not counted.
    ///
    /// # Errors
    /// Returns a configuration error for zero rate/burst, borrowed runtimes,
    /// terminated contexts, or changes after the first socket is created.
    pub fn set_network_rate_limits(&self, limits: crate::NetworkRateLimits) -> crate::Result<()> {
        limits.validate()?;
        // Check ownership before touching a mutex that may have been locked
        // by a vanished thread in the process that called fork().
        if self.is_terminated() || !self.inner.is_owner_process() {
            return Err(crate::Error::Config(
                "network limits require a live context owned by this process".into(),
            ));
        }

        let mut config = self
            .inner
            .network_config
            .lock()
            .expect("context network settings");

        if config.1 {
            return Err(crate::Error::Config(
                "network limits must be configured before creating sockets".into(),
            ));
        }

        if limits.enabled() && self.io_threads() == 0 {
            return Err(crate::Error::Config(
                "network limits require an OMQ-owned runtime".into(),
            ));
        }

        config.0 = limits;
        Ok(())
    }

    /// Return the context's configured aggregate network byte rates.
    pub fn network_rate_limits(&self) -> crate::NetworkRateLimits {
        assert!(
            self.inner.is_owner_process(),
            "network_rate_limits() called on a context inherited across fork"
        );

        self.inner
            .network_config
            .lock()
            .expect("context network settings")
            .0
    }

    #[cfg(all(test, feature = "quic"))]
    pub(crate) fn io_pool_handle_for_test(&self) -> IoPoolHandle {
        self.inner.io_pool_handle()
    }

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
        Self::with_config_inner(config, None)
    }

    /// Create a context with one IO thread and a custom context name.
    ///
    /// For example, the name `orders` produces the background thread name
    /// `orders/IO/0`.
    pub fn with_name(name: impl Into<String>) -> Self {
        Self::with_config_and_name(ContextConfig::default(), name)
    }

    /// Create a context with custom configuration and a custom context name.
    ///
    /// Data thread names use `<name>/IO/<index>`. Multi-IO contexts also use
    /// `<name>/Ctrl` for the internal control runtime. Without an explicit
    /// name, contexts receive process-local names such as `OMQ0` and `OMQ1`.
    /// Names are shortened at UTF-8 boundaries to fit Linux's 15-byte thread
    /// name limit while retaining the role and IO index.
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty or contains a NUL byte.
    pub fn with_config_and_name(config: ContextConfig, name: impl Into<String>) -> Self {
        let name = name.into();
        assert!(
            !name.is_empty() && !name.contains('\0'),
            "context name must be non-empty and contain no NUL byte"
        );

        Self::with_config_inner(config, Some(name))
    }

    fn with_config_inner(config: ContextConfig, context_name: Option<String>) -> Self {
        if config.io_threads == 0 {
            return Self::current();
        }

        let io_threads = config.io_threads;
        let pool = IoThreadPool::new_with_context_name(io_threads, context_name);
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
        let build = Build::native(None);
        self.new_socket("Context::socket()", socket_type, options, build)
    }

    #[doc(hidden)]
    pub fn socket_with_recv_sink_config(
        &self,
        socket_type: SocketType,
        options: Options,
        config: Arc<crate::engine::RecvSinkConfig>,
    ) -> Socket {
        let build = Build::native(Some(config));
        let caller = "Context::socket_with_recv_sink_config()";
        self.new_socket(caller, socket_type, options, build)
    }

    /// Construct a binding socket with libzmq send and polling semantics.
    #[doc(hidden)]
    pub fn socket_for_libzmq(
        &self,
        socket_type: SocketType,
        options: Options,
        recv_sink_config: Option<Arc<crate::engine::RecvSinkConfig>>,
    ) -> Socket {
        let build = Build {
            recv_sink_config,
            compatibility: crate::routing::SendCompatibility::Libzmq,
        };

        self.new_socket("Context::socket_for_libzmq()", socket_type, options, build)
    }

    /// Construct a blocking binding socket with libzmq send semantics.
    #[doc(hidden)]
    pub fn blocking_socket_for_libzmq(
        &self,
        socket_type: SocketType,
        options: Options,
    ) -> crate::blocking::Socket {
        assert!(
            self.io_threads() > 0,
            "blocking_socket_for_libzmq() requires at least one owned IO thread"
        );

        crate::blocking::Socket::new(
            self.socket_for_libzmq(socket_type, options, None),
            self.clone(),
        )
    }

    /// Panic unless this context is live and owned by this process.
    fn assert_usable(&self, caller: &str) {
        assert!(
            !self.inner.terminated.load(Ordering::Acquire),
            "{caller} called on a terminated context"
        );
        assert!(
            self.inner.is_owner_process(),
            "{caller} called on a context inherited across fork"
        );
    }

    /// Build a socket on the primary runtime once the context is checked
    /// and its network limits are frozen.
    fn new_socket(
        &self,
        caller: &str,
        socket_type: SocketType,
        options: Options,
        build: Build,
    ) -> Socket {
        self.assert_usable(caller);
        self.inner.freeze_network();
        let _guard = self.inner.primary_handle().enter();
        let placement = Placement {
            io_pool: &self.inner.io_pool_handle(),
            inproc_registry: self.inner.inproc_registry.clone(),
        };

        Socket::new_in(socket_type, options, build, placement)
    }

    /// Run a future on this context's runtime, blocking the calling
    /// thread until it completes. The future runs inline on the primary IO
    /// thread, concurrently with other callers' futures.
    /// If the caller already has an async runtime, await socket futures
    /// directly instead.
    ///
    /// # Panics
    ///
    /// Panics if the context was created with [`Context::current()`]
    /// (the caller is already async; just `.await` directly). Panics if
    /// called on this context's own primary IO thread, for example from
    /// inside another `block_on` future: the call would wait for a future
    /// that only the waiting thread can run.
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

        self.assert_usable("Context::block_on()");
        assert!(
            !pool.on_primary_thread(),
            "Context::block_on() called on the context's own primary IO thread; \
             await the future instead"
        );
        let guard = pool.primary_job_tx.lock().expect("job_tx poisoned");
        let job_tx = guard
            .as_ref()
            .expect("Context::block_on() called on a terminated context");
        let (result_tx, result_rx) = mpsc::channel();
        let fut: BoxFuture = Box::pin(async move {
            // The primary IO thread polls this job inline and also runs
            // the context's sockets. A panic unwinds the caller instead.
            let result = std::panic::AssertUnwindSafe(f).catch_unwind().await;
            let _ = result_tx.send(result);
        });
        job_tx.send(fut).expect("omq: context runtime exited");
        drop(guard);

        match result_rx
            .recv()
            .expect("omq: context runtime exited unexpectedly")
        {
            Ok(output) => output,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Return the tokio runtime handle for the primary IO thread.
    pub fn handle(&self) -> &Handle {
        self.inner.primary_handle()
    }

    /// Whether the calling thread runs an owned context's IO runtime.
    /// A blocking wait there can need work that only this thread runs.
    pub fn on_io_thread() -> bool {
        OWNED_IO_THREAD.get()
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
        .on_thread_start(block_sigpipe)
        .build()
        .expect("omq: failed to build current_thread runtime")
}

/// Keeps SIGPIPE pending on this thread, so writes to a reset peer fail with
/// `EPIPE` instead. Rust binaries ignore SIGPIPE, but library hosts such as Go
/// keep the default action on non-Go threads, which terminates the process.
#[cfg(unix)]
fn block_sigpipe() {
    use nix::sys::signal::{SigSet, SigmaskHow, Signal, pthread_sigmask};

    let mut set = SigSet::empty();
    set.add(Signal::SIGPIPE);
    pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&set), None).expect("omq: failed to block SIGPIPE");
}

#[cfg(not(unix))]
fn block_sigpipe() {}

#[cfg(test)]
mod tests;
