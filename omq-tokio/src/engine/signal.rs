//! Coalesced data-available notification.
//!
//! Implements the libzmq ypipe signaling discipline: one wake per
//! batch, not one wake per push. Used by `PeerTransmitSlot`, `SendPipe`,
//! send pipes, transmit slots, and fan-out lane workers.
//!
//! Protocol:
//! - **Producer** calls [`DataSignal::mark`] after each push. Only the
//!   idle-to-pending transition fires `notify_one`; additional marks stay
//!   coalesced.
//! - **Consumer** calls [`DataSignal::begin_drain`] before draining, then
//!   [`DataSignal::clear_after`] after draining. A mark that races with
//!   the drain moves the signal to `DIRTY`, so a stale empty read still
//!   rearms the signal.
//! - A mark that finds the signal already pending skips its wake. Its item
//!   must then be visible to the consumer's next drain. That is a
//!   store-then-load on each side (item then state, state then item), so
//!   both sides need a sequentially consistent fence: `mark` before reading
//!   the state, `begin_drain` after writing it. Release and acquire alone
//!   let both loads read stale values, and the consumer parks with the item
//!   queued (`tests/loom_signal.rs`).

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex, Weak};

use smallvec::SmallVec;
use tokio::sync::Notify;

fn new_notify() -> Notify {
    let notify = Notify::new();
    // Tokio's waiter mutex allocates on first use on some platforms.
    // Register and remove an empty waiter while the signal is private.
    {
        let mut waiter = std::pin::pin!(notify.notified());
        waiter.as_mut().enable();
    }
    notify
}

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

const IDLE: u8 = 0;
const PENDING: u8 = 1;
const DRAINING: u8 = 2;
const DIRTY: u8 = 3;

/// Coalesced data-available notification.
#[derive(Debug)]
pub(crate) struct DataSignal {
    #[cfg(feature = "dart")]
    pub(crate) dart_admission: std::sync::OnceLock<omq_proto::dart::AdmissionCounter>,
    #[cfg(feature = "dart")]
    endpoint: std::sync::OnceLock<Arc<DataSignal>>,
    close_progress: std::sync::OnceLock<Arc<StateSignal>>,
    state: AtomicU8,
    /// Async consumers parked in [`Self::ready`].
    waiters: AtomicUsize,
    notify: Notify,
}

/// Counts one parked [`DataSignal::ready`] caller until it wakes or drops.
struct ParkedWaiter<'a>(&'a AtomicUsize);

impl Drop for ParkedWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl DataSignal {
    #[cfg(feature = "dart")]
    pub(crate) fn forward_to(&self, endpoint: Arc<Self>) {
        assert!(
            endpoint.endpoint.get().is_none(),
            "DART wake forwarding has one level"
        );
        let _ = self.endpoint.set(endpoint);
    }

    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "dart")]
            dart_admission: std::sync::OnceLock::new(),
            #[cfg(feature = "dart")]
            endpoint: std::sync::OnceLock::new(),
            close_progress: std::sync::OnceLock::new(),
            state: AtomicU8::new(IDLE),
            waiters: AtomicUsize::new(0),
            notify: new_notify(),
        }
    }

    pub(crate) fn watch_idle(&self, progress: &Arc<StateSignal>) {
        let existing = self.close_progress.get_or_init(|| progress.clone());
        debug_assert!(Arc::ptr_eq(existing, progress));
    }

    pub(crate) fn notify_close_progress(&self) {
        if let Some(progress) = self.close_progress.get() {
            progress.notify_changed();
        }
    }

    /// Producer: mark data available.
    /// Wakes one waiter only on the idle-to-pending transition.
    #[inline]
    pub(crate) fn mark(&self) {
        #[cfg(feature = "dart")]
        if let Some(endpoint) = self.endpoint.get() {
            endpoint.mark();
        }
        // Order the caller's publication before the state read. See the
        // module docs.
        fence(Ordering::SeqCst);
        let state = self.state.load(Ordering::Acquire);
        if !matches!(state, PENDING | DIRTY) {
            self.mark_pending(state);
        }
    }

    fn mark_pending(&self, mut state: u8) {
        loop {
            match state {
                IDLE => match self.state.compare_exchange_weak(
                    IDLE,
                    PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.notify.notify_one();
                        return;
                    }
                    Err(next) => state = next,
                },
                PENDING | DIRTY => return,
                DRAINING => match self.state.compare_exchange_weak(
                    DRAINING,
                    DIRTY,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(next) => state = next,
                },
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    /// Consumer: enter a drain pass.
    #[inline]
    pub(crate) fn begin_drain(&self) {
        if self.state.load(Ordering::Acquire) == PENDING {
            let _ =
                self.state
                    .compare_exchange(PENDING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
        }
        // Order the state write before the drain's reads. See the module
        // docs.
        fence(Ordering::SeqCst);
    }

    /// Consumer: clear after draining.
    ///
    /// If the source is non-empty, or any producer marked during the
    /// drain, re-fire the signal.
    /// Returns `true` when this call fired a wake.
    #[inline]
    pub(crate) fn clear_after(&self, is_empty: bool) -> bool {
        if !is_empty {
            return self.rearm();
        }

        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                DRAINING => match self.state.compare_exchange_weak(
                    DRAINING,
                    IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        if let Some(progress) = self.close_progress.get() {
                            progress.notify_changed();
                        }
                        return false;
                    }
                    Err(next) => state = next,
                },
                DIRTY => return self.rearm(),
                PENDING | IDLE => return false,
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    #[inline]
    fn rearm(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                PENDING => return false,
                IDLE | DRAINING | DIRTY => match self.state.compare_exchange_weak(
                    state,
                    PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.notify.notify_one();
                        return true;
                    }
                    Err(next) => state = next,
                },
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    /// Consumer self-reschedule: fire `notify_one` unconditionally.
    ///
    /// Use when the consumer knows data remains (e.g. budget exhausted)
    /// and needs to wake itself on the next select iteration. Unlike
    /// `mark()`, this does not check `pending` because the consumer
    /// has not cleared it (the slot is non-empty).
    #[inline]
    pub(crate) fn reschedule(&self) {
        self.notify.notify_one();
    }

    /// Wake all waiters unconditionally. Shutdown / `mark_dead` path.
    pub(crate) fn wake_all(&self) {
        self.state.store(PENDING, Ordering::Release);
        self.notify.notify_waiters();
        if let Some(progress) = self.close_progress.get() {
            progress.notify_changed();
        }
    }

    /// Wait until data is marked pending.
    ///
    /// This remains ready after a previous `Notified` future was woken
    /// and then dropped by a `select!` branch losing the race.
    pub(crate) async fn ready(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_idle() {
            return;
        }
        // Count this waiter before the final check. A consumer that later
        // sees pending data in `hand_off` then also sees this waiter.
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let _parked = ParkedWaiter(&self.waiters);
        if self.state.load(Ordering::SeqCst) != IDLE {
            return;
        }
        notified.await;
    }

    /// Consumer: call after a wake from [`Self::ready`] led to taking data.
    ///
    /// One mark wakes one waiter, and a signal left pending coalesces later
    /// marks. With concurrent async consumers, the woken one must pass the
    /// wake on while data may remain, or the others stay parked behind it.
    #[inline]
    pub(crate) fn hand_off(&self) {
        fence(Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) != 0 && !self.is_idle() {
            self.notify.notify_one();
        }
    }

    #[inline]
    pub(crate) fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == IDLE
    }

    #[cfg(test)]
    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

/// Stateful "something changed" signal.
///
/// Unlike a bare [`Notify`], every wake bumps a generation counter.
/// Waiters capture the generation, enable their waiter, re-check caller
/// state, then await only if nothing changed meanwhile.
#[derive(Debug)]
pub struct StateSignal {
    #[cfg(feature = "dart")]
    endpoints: std::sync::OnceLock<Mutex<Arc<Vec<Weak<DataSignal>>>>>,
    generation: AtomicU64,
    notify: Notify,
}

/// Completes once any of `signals` changes after this call returns.
/// Generations are read before returning, so a probe made after this call
/// cannot miss a change. Never completes for an empty set. Up to four
/// signals wait without allocating, as senders do on every full-pipe park.
pub(crate) fn any_changed(signals: impl IntoIterator<Item = Arc<StateSignal>>) -> Changes {
    Changes(
        signals
            .into_iter()
            .map(|signal| {
                let seen = signal.generation();
                (signal, seen)
            })
            .collect(),
    )
}

/// Generations captured by [`any_changed`].
#[derive(Debug)]
pub(crate) struct Changes(SmallVec<[(Arc<StateSignal>, u64); 4]>);

impl Changes {
    /// Wait until any captured signal moved past its generation.
    pub(crate) async fn wait(self) {
        let signals = self.0;
        match signals.as_slice() {
            [] => std::future::pending().await,
            [(a, seen_a)] => a.changed_after(*seen_a).await,
            [(a, seen_a), (b, seen_b)] => {
                tokio::select! {
                    () = a.changed_after(*seen_a) => {}
                    () = b.changed_after(*seen_b) => {}
                }
            }
            [(a, seen_a), (b, seen_b), (c, seen_c)] => {
                tokio::select! {
                    () = a.changed_after(*seen_a) => {}
                    () = b.changed_after(*seen_b) => {}
                    () = c.changed_after(*seen_c) => {}
                }
            }
            [(a, seen_a), (b, seen_b), (c, seen_c), (d, seen_d)] => {
                tokio::select! {
                    () = a.changed_after(*seen_a) => {}
                    () = b.changed_after(*seen_b) => {}
                    () = c.changed_after(*seen_c) => {}
                    () = d.changed_after(*seen_d) => {}
                }
            }
            many => {
                let mut waits: futures::stream::FuturesUnordered<_> = many
                    .iter()
                    .map(|(signal, seen)| signal.changed_after(*seen))
                    .collect();
                futures::StreamExt::next(&mut waits).await;
            }
        }
    }
}

impl StateSignal {
    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&self, signal: &Arc<DataSignal>) {
        let mut subscribers = self
            .endpoints
            .get_or_init(|| Mutex::new(Arc::new(Vec::new())))
            .lock()
            .expect("DART space subscribers");
        let endpoints = Arc::make_mut(&mut subscribers);
        endpoints.retain(|endpoint| endpoint.strong_count() != 0);
        if !endpoints
            .iter()
            .any(|endpoint| endpoint.as_ptr() == Arc::as_ptr(signal))
        {
            endpoints.push(Arc::downgrade(signal));
        }
    }

    /// Create a signal with generation zero.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "dart")]
            endpoints: std::sync::OnceLock::new(),
            generation: AtomicU64::new(0),
            notify: new_notify(),
        }
    }

    /// Return the current notification generation.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Advance the generation and wake registered waiters.
    #[inline]
    pub fn notify_changed(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
        #[cfg(feature = "dart")]
        if let Some(endpoints) = self.endpoints.get() {
            // Snapshot once per publication; registration copies only while
            // a wake holds the old list. Notify can invoke user callbacks.
            let endpoints = Arc::clone(&endpoints.lock().expect("DART space subscribers"));
            for endpoint in endpoints.iter() {
                if let Some(signal) = endpoint.upgrade() {
                    signal.mark();
                }
            }
        }
    }

    /// Wait until the notification generation differs from `seen`.
    pub async fn changed_after(&self, seen: u64) {
        if self.generation() != seen {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.generation() != seen {
            return;
        }
        notified.await;
    }

    /// Wait on the calling OS thread until caller state or generation changes.
    pub fn wait_until_blocking(&self, ready: impl FnMut() -> bool) {
        block_on(self.wait_until(ready));
    }

    /// Wait until the predicate holds or a registered state change occurs.
    pub async fn wait_until(&self, mut ready: impl FnMut() -> bool) {
        loop {
            if ready() {
                return;
            }
            let seen = self.generation();
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if ready() || self.generation() != seen {
                return;
            }
            notified.await;
        }
    }
}

impl Default for StateSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
