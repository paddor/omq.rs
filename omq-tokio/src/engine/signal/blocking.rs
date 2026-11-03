//! Blocking-thread wakeups: parked receive callers, deferred user wakers,
//! and receive cancellation.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex, Weak};

use smallvec::SmallVec;

#[derive(Debug)]
struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

thread_local! {
    // Every call wakes this same OS thread. Retain no future or socket state.
    static BLOCKING_UNPARK: Arc<Unpark> = Arc::new(Unpark(std::thread::current()));
}

/// Poll `future` on the calling thread and park it between wakeups.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let unpark = BLOCKING_UNPARK
        .try_with(Arc::clone)
        // A later TLS destructor may still need to perform a blocking call.
        .unwrap_or_else(|_| Arc::new(Unpark(std::thread::current())));
    let waker = std::task::Waker::from(unpark);
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

/// Registered OS threads awaiting socket data. Ready receives never register.
///
/// Each call owns its waiter. A producer wakes every armed caller once; one
/// caller cannot replace another clone's thread or consume its wake token.
#[derive(Debug)]
pub(crate) struct BlockingSignal {
    active: AtomicUsize,
    waiters: Mutex<Vec<Arc<BlockingThread>>>,
}

#[derive(Debug)]
struct BlockingThread {
    target: WakeTarget,
    armed: AtomicBool,
}

#[derive(Debug)]
enum WakeTarget {
    /// A parked receive. One wake per arm.
    Thread(std::thread::Thread),
    /// A foreign readiness waiter. Woken on every publication.
    Waker(std::task::Waker),
}

struct CachedBlockingThread {
    signal: Weak<BlockingSignal>,
    state: Arc<BlockingThread>,
}

thread_local! {
    // Weak signals retain no socket resources. Thread exit releases cached
    // waiters; new registrations prune sockets that have already closed.
    static BLOCKING_THREADS: RefCell<Vec<CachedBlockingThread>> = const { RefCell::new(Vec::new()) };
}

fn blocking_thread(signal: &Arc<BlockingSignal>) -> Arc<BlockingThread> {
    let new = || {
        Arc::new(BlockingThread {
            target: WakeTarget::Thread(std::thread::current()),
            armed: AtomicBool::new(true),
        })
    };
    BLOCKING_THREADS
        .try_with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(cached) = cache.iter().find(|cached| {
                cached.signal.as_ptr() == Arc::as_ptr(signal)
                // Active guards and the signal's list own other references.
                // A nested call must get a separate waiter.
                && Arc::strong_count(&cached.state) == 1
            }) {
                return cached.state.clone();
            }
            cache.retain(|cached| cached.signal.strong_count() != 0);
            let state = new();
            cache.push(CachedBlockingThread {
                signal: Arc::downgrade(signal),
                state: state.clone(),
            });
            state
        })
        .unwrap_or_else(|_| new()) // Thread-local teardown is outside steady state.
}

/// Receive guards this thread holds, and user wakers deferred until the
/// outermost one releases.
struct ReceiveGuards {
    held: Cell<usize>,
    pending: Cell<bool>,
    deferred: RefCell<Vec<Arc<BlockingThread>>>,
}

thread_local! {
    static RECEIVE_GUARDS: ReceiveGuards = const {
        ReceiveGuards {
            held: Cell::new(0),
            pending: Cell::new(false),
            deferred: RefCell::new(Vec::new()),
        }
    };
}

/// A receive guard was acquired on this thread.
#[inline]
pub(crate) fn enter_receive_guard() {
    let _ = RECEIVE_GUARDS.try_with(|guards| guards.held.set(guards.held.get() + 1));
}

/// A receive guard was released. The outermost release runs deferred wakers.
#[inline]
pub(crate) fn leave_receive_guard() {
    let wakers = RECEIVE_GUARDS.try_with(|guards| {
        let held = guards.held.get().saturating_sub(1);
        guards.held.set(held);
        (held == 0 && guards.pending.replace(false))
            .then(|| std::mem::take(&mut *guards.deferred.borrow_mut()))
    });
    if let Ok(Some(wakers)) = wakers {
        run_wakers(wakers);
    }
}

fn defer_wakers(wakers: &mut SmallVec<[Arc<BlockingThread>; 4]>) -> bool {
    RECEIVE_GUARDS
        .try_with(|guards| {
            if guards.held.get() == 0 {
                return false;
            }
            guards.deferred.borrow_mut().extend(wakers.drain(..));
            guards.pending.set(true);
            true
        })
        .unwrap_or(false)
}

fn run_wakers(wakers: impl IntoIterator<Item = Arc<BlockingThread>>) {
    for waiter in wakers {
        if let WakeTarget::Waker(waker) = &waiter.target {
            waker.wake_by_ref();
        }
    }
}

pub(crate) struct BlockingWaiter<'a> {
    signal: &'a BlockingSignal,
    state: Arc<BlockingThread>,
}

impl BlockingSignal {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            waiters: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn register(self: &Arc<Self>) -> BlockingWaiter<'_> {
        let state = blocking_thread(self);
        state.armed.store(true, Ordering::SeqCst);
        let mut waiters = self.waiters.lock().unwrap();
        waiters.push(state.clone());
        self.active.fetch_add(1, Ordering::SeqCst);
        BlockingWaiter {
            signal: self,
            state,
        }
    }

    /// Publication precedes this call. Registration precedes the receiver's
    /// recheck. The fences prevent both sides from missing one another.
    #[inline]
    pub(crate) fn wake(&self) {
        fence(Ordering::SeqCst);
        self.wake_after_fence();
    }

    /// Publication must precede a `SeqCst` fence before calling this method.
    /// A preceding `DataSignal::mark` already provides that fence.
    #[inline]
    pub(crate) fn wake_after_fence(&self) {
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut wakers = SmallVec::<[Arc<BlockingThread>; 4]>::new();
        {
            let waiters = self.waiters.lock().unwrap();
            for waiter in waiters.iter() {
                match &waiter.target {
                    WakeTarget::Thread(thread) => {
                        if waiter.armed.swap(false, Ordering::AcqRel) {
                            thread.unpark();
                        }
                    }
                    WakeTarget::Waker(_) => wakers.push(waiter.clone()),
                }
            }
        }
        // User wakers can reenter registration or panic. Keep them outside
        // the waiter lock, including their clone/drop callbacks. Under a
        // receive guard they wait until the guard releases, so a waker may
        // also reenter receive.
        if wakers.is_empty() || defer_wakers(&mut wakers) {
            return;
        }
        run_wakers(wakers);
    }

    /// Wake `waker` on every publication until the registration drops.
    /// The caller rechecks its queue after registering.
    pub(crate) fn register_waker(self: &Arc<Self>, waker: std::task::Waker) -> WakerRegistration {
        let state = Arc::new(BlockingThread {
            target: WakeTarget::Waker(waker),
            armed: AtomicBool::new(true),
        });
        self.waiters.lock().unwrap().push(state.clone());
        self.active.fetch_add(1, Ordering::SeqCst);
        WakerRegistration {
            signal: self.clone(),
            state,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_waiter(&self, thread: std::thread::ThreadId) -> bool {
        self.waiters.lock().unwrap().iter().any(|waiter| {
            matches!(&waiter.target, WakeTarget::Thread(t) if t.id() == thread)
                && waiter.armed.load(Ordering::Acquire)
        })
    }
}

impl BlockingWaiter<'_> {
    pub(crate) fn park(&self) {
        if self.state.armed.load(Ordering::Acquire) {
            std::thread::park();
        }
    }

    pub(crate) fn park_timeout(&self, timeout: std::time::Duration) {
        if self.state.armed.load(Ordering::Acquire) {
            std::thread::park_timeout(timeout);
        }
    }

    #[inline]
    pub(crate) fn prepare_sleep(&self) {
        // Order registration before the queue recheck, including rearming an
        // existing waiter after a spurious wake or another clone's drain.
        self.state.armed.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }
}

impl Drop for BlockingWaiter<'_> {
    fn drop(&mut self) {
        self.signal.unregister(&self.state);
    }
}

/// Owned [`BlockingSignal::register_waker`] registration.
#[derive(Debug)]
pub(crate) struct WakerRegistration {
    signal: Arc<BlockingSignal>,
    state: Arc<BlockingThread>,
}

impl Drop for WakerRegistration {
    fn drop(&mut self) {
        self.signal.unregister(&self.state);
    }
}

impl BlockingSignal {
    fn unregister(&self, state: &Arc<BlockingThread>) {
        let mut waiters = self.waiters.lock().unwrap();
        waiters.retain(|waiter| !Arc::ptr_eq(waiter, state));
        self.active.fetch_sub(1, Ordering::Release);
    }
}

/// Cancellation handle for blocking receive calls.
#[derive(Debug)]
pub struct BlockingRecvCancel {
    canceled: AtomicBool,
    registered: AtomicBool,
    thread: Mutex<Option<std::thread::Thread>>,
}

impl BlockingRecvCancel {
    /// Create a cancel handle in the active state.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            canceled: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            thread: Mutex::new(None),
        }
    }

    /// Cancel current and future receive waits.
    #[inline]
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
        if let Some(thread) = self.thread.lock().unwrap().clone() {
            thread.unpark();
        }
    }

    /// Returns whether this handle has been canceled.
    #[inline]
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn register(&self, thread: &std::thread::Thread) {
        *self.thread.lock().unwrap() = Some(thread.clone());
        self.registered.store(true, Ordering::Release);
        if self.is_canceled() {
            thread.unpark();
        }
    }

    /// Register the current OS thread once for repeated cancelable receives.
    ///
    /// This avoids per-call registration when a foreign binding owns the
    /// blocking socket thread.
    pub fn register_current_thread_once(&self) {
        if self
            .registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let thread = std::thread::current();
        *self.thread.lock().unwrap() = Some(thread.clone());
        if self.is_canceled() {
            thread.unpark();
        }
    }

    #[inline]
    fn unregister(&self) {
        *self.thread.lock().unwrap() = None;
        self.registered.store(false, Ordering::Release);
    }
}

impl Default for BlockingRecvCancel {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct BlockingRecvCancelGuard<'a> {
    pub(crate) cancel: &'a BlockingRecvCancel,
}

impl Drop for BlockingRecvCancelGuard<'_> {
    fn drop(&mut self) {
        self.cancel.unregister();
    }
}

#[cfg(test)]
mod tests;
