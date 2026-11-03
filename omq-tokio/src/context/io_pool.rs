//! Owned IO runtimes: one `current_thread` runtime per OS thread, plus
//! load-balanced thread leases for driver tasks.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::{BoxFuture, OWNED_IO_THREAD, block_sigpipe, build_current_thread_runtime};

static NEXT_CONTEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

pub(super) struct IoThread {
    pub(super) handle: Handle,
    pub(super) load: AtomicUsize,
}

pub(super) struct IoThreadPool {
    pub(super) threads: Vec<IoThread>,
    pub(super) primary_job_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<BoxFuture>>>,
    pub(super) cancel: CancellationToken,
    joins: Mutex<Vec<Option<thread::JoinHandle<()>>>>,
}

impl IoThreadPool {
    pub(super) fn new_with_context_name(n: usize, context_name: Option<String>) -> Arc<Self> {
        assert!(n >= 1);
        let cancel = CancellationToken::new();
        let id = NEXT_CONTEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
        let context_name = context_name
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| format!("OMQ{id}"));
        // Multi-IO contexts reserve runtime 0 for socket actors and blocking
        // control calls. The configured count applies to data runtimes.
        let runtime_count = if n > 1 { n + 1 } else { 1 };
        let context_name = fit_context_name(&context_name, n);
        let mut threads = Vec::with_capacity(runtime_count);
        let mut joins = Vec::with_capacity(runtime_count);
        let mut primary_job_tx = None;

        for i in 0..runtime_count {
            let (handle_tx, handle_rx) = mpsc::channel::<Handle>();
            let thread_name = background_thread_name(context_name, runtime_count, i);

            let join = if i == 0 {
                let (join, job_tx) = spawn_primary_thread(thread_name, cancel.clone(), handle_tx);
                primary_job_tx = Some(job_tx);
                join
            } else {
                spawn_data_thread(thread_name, cancel.clone(), handle_tx)
            };

            // Each thread reports its runtime handle once the runtime is up.
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

    pub(super) fn primary_handle(&self) -> &Handle {
        &self.threads[0].handle
    }

    pub(super) fn thread_count(&self) -> usize {
        self.threads.len() - self.data_thread_offset()
    }

    fn data_thread_offset(&self) -> usize {
        usize::from(self.threads.len() > 1)
    }

    pub(super) fn assign_thread(&self) -> usize {
        self.assign_thread_preferred(0)
    }

    fn assign_thread_preferred(&self, preferred: usize) -> usize {
        // Return a logical data-thread index. Multi-IO runtime 0 stays
        // reserved for socket actors and binding control calls.
        let offset = self.data_thread_offset();
        let best = self
            .threads
            .get(offset..)
            .expect("data-thread offset is in bounds")
            .iter()
            .enumerate()
            .min_by_key(|(index, t)| (t.load.load(Ordering::Relaxed), *index != preferred))
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

    pub(super) fn shutdown(&self) {
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

/// Runtime 0 also runs jobs submitted through the returned sender.
fn spawn_primary_thread(
    name: String,
    cancel: CancellationToken,
    handle_tx: mpsc::Sender<Handle>,
) -> (
    thread::JoinHandle<()>,
    tokio::sync::mpsc::UnboundedSender<BoxFuture>,
) {
    let (job_tx, mut job_rx) = tokio::sync::mpsc::unbounded_channel::<BoxFuture>();

    let join = thread::Builder::new()
        .name(name)
        .spawn(move || {
            OWNED_IO_THREAD.set(true);
            block_sigpipe();
            let rt = build_current_thread_runtime();
            let _ = handle_tx.send(rt.handle().clone());
            rt.block_on(async move {
                // Jobs from different callers run concurrently, so
                // one long wait cannot delay another caller's bind.
                // Cancellation drops every unfinished job.
                let mut jobs = FuturesUnordered::new();
                loop {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => break,
                        job = job_rx.recv() => match job {
                            Some(fut) => jobs.push(fut),
                            None => break,
                        },
                        Some(()) = jobs.next(), if !jobs.is_empty() => {}
                    }
                }
            });
        })
        .expect("omq: failed to spawn primary IO thread");

    (join, job_tx)
}

/// Data runtimes only drive spawned tasks until the context is canceled.
fn spawn_data_thread(
    name: String,
    cancel: CancellationToken,
    handle_tx: mpsc::Sender<Handle>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            OWNED_IO_THREAD.set(true);
            block_sigpipe();
            let rt = build_current_thread_runtime();
            let _ = handle_tx.send(rt.handle().clone());
            rt.block_on(cancel.cancelled());
        })
        .expect("omq: failed to spawn IO thread")
}

fn background_thread_name(prefix: &str, runtime_count: usize, index: usize) -> String {
    if runtime_count > 1 && index == 0 {
        format!("{prefix}/Ctrl")
    } else {
        let io_index = index.saturating_sub(usize::from(runtime_count > 1));
        format!("{prefix}/IO/{io_index}")
    }
}

pub(super) fn fit_context_name(name: &str, io_threads: usize) -> &str {
    let max_io_suffix = format!("/IO/{}", io_threads - 1).len();
    let max_suffix = if io_threads > 1 {
        max_io_suffix.max("/Ctrl".len())
    } else {
        max_io_suffix
    };
    truncate_utf8(name, 15usize.saturating_sub(max_suffix))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    &value[..end]
}

impl std::fmt::Debug for IoThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoThreadPool")
            .field("thread_count", &self.thread_count())
            .finish_non_exhaustive()
    }
}

/// Handle to the IO thread pool for spawning tasks on specific IO
/// threads. When the inner pool is `None`, all spawning uses bare
/// `tokio::spawn()` (single-thread or borrowed-runtime mode).
#[derive(Clone, Debug)]
pub(crate) struct IoPoolHandle {
    pub(super) pool: Option<Arc<IoThreadPool>>,
    pub(super) network: Option<Arc<Vec<crate::network_limit::Lane>>>,
}

/// One load reservation owned by the driver future, including before its
/// first poll. Task completion, abortion, and setup failure release it once.
#[derive(Debug)]
pub(crate) struct IoThreadLease {
    pool: Option<Arc<IoThreadPool>>,
    index: usize,
}

impl IoThreadLease {
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

impl Drop for IoThreadLease {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.release_thread(self.index);
        }
    }
}

impl IoPoolHandle {
    pub(crate) fn none() -> Self {
        Self {
            pool: None,
            network: None,
        }
    }

    pub(crate) fn network_on(&self, index: usize) -> crate::network_limit::Lane {
        let offset = self
            .pool
            .as_ref()
            .map_or(0, |pool| pool.data_thread_offset());
        self.network
            .as_ref()
            .map_or_else(Default::default, |lanes| lanes[index + offset].clone())
    }

    pub(crate) fn network_primary(&self) -> crate::network_limit::Lane {
        self.network
            .as_ref()
            .map_or_else(Default::default, |lanes| lanes[0].clone())
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

    /// Spawn a future on the leased IO thread. The future owns the lease,
    /// so the thread's load reservation lasts as long as the task.
    pub(crate) fn spawn_leased<F>(
        &self,
        lease: IoThreadLease,
        fut: F,
    ) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future<Output: Send> + Send + 'static,
    {
        self.spawn_on(lease.index(), async move {
            let _lease = lease;
            fut.await
        })
    }

    /// Whether tasks run on a runtime owned by OMQ.
    pub(crate) fn owns_runtime(&self) -> bool {
        self.pool.is_some()
    }

    /// Whether data-plane tasks run on owned runtimes separate from the
    /// primary socket runtime.
    pub(crate) fn has_dedicated_io_threads(&self) -> bool {
        self.pool
            .as_ref()
            .is_some_and(|pool| pool.data_thread_offset() != 0)
    }

    fn lease(&self, index: usize) -> IoThreadLease {
        IoThreadLease {
            pool: self.pool.clone(),
            index,
        }
    }

    /// Reserve the least-loaded IO thread for one driver future.
    pub(crate) fn reserve_thread(&self) -> IoThreadLease {
        let index = match &self.pool {
            None => 0,
            Some(pool) => pool.assign_thread(),
        };
        self.lease(index)
    }

    /// Prefer the transport's reactor when it is among the least-loaded lanes.
    #[cfg(feature = "quic")]
    pub(crate) fn reserve_thread_preferred(&self, preferred: usize) -> IoThreadLease {
        let index = self
            .pool
            .as_ref()
            .map_or(0, |pool| pool.assign_thread_preferred(preferred));
        self.lease(index)
    }

    /// Reserve data IO thread `index` regardless of its current load.
    #[cfg(any(feature = "quic", target_os = "linux"))]
    pub(crate) fn reserve_thread_at(&self, index: usize) -> IoThreadLease {
        if let Some(pool) = &self.pool {
            pool.threads[index + pool.data_thread_offset()]
                .load
                .fetch_add(1, Ordering::Relaxed);
        }
        self.lease(index)
    }

    /// Alive tasks per owned runtime, control runtime first when present.
    #[cfg(all(test, feature = "quic"))]
    pub(crate) fn alive_tasks(&self) -> Vec<usize> {
        self.pool.as_ref().map_or_else(Vec::new, |pool| {
            pool.threads
                .iter()
                .map(|thread| thread.handle.metrics().num_alive_tasks())
                .collect()
        })
    }

    #[cfg(all(test, feature = "quic"))]
    pub(crate) fn io_loads(&self) -> Vec<usize> {
        self.pool.as_ref().map_or_else(Vec::new, |pool| {
            pool.threads[pool.data_thread_offset()..]
                .iter()
                .map(|thread| thread.load.load(Ordering::Relaxed))
                .collect()
        })
    }

    /// Number of IO threads.
    pub(crate) fn thread_count(&self) -> usize {
        match &self.pool {
            None => 1,
            Some(pool) => pool.thread_count(),
        }
    }
}
