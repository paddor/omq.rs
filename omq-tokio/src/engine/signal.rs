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

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Poll `future` on the calling thread and park it between wakeups.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);

    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
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
    thread: std::thread::Thread,
    armed: AtomicBool,
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

    pub(crate) fn register(&self) -> BlockingWaiter<'_> {
        let state = Arc::new(BlockingThread {
            thread: std::thread::current(),
            armed: AtomicBool::new(true),
        });
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
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        for waiter in self.waiters.lock().unwrap().iter() {
            if waiter.armed.swap(false, Ordering::AcqRel) {
                waiter.thread.unpark();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn has_waiter(&self, thread: std::thread::ThreadId) -> bool {
        self.waiters
            .lock()
            .unwrap()
            .iter()
            .any(|waiter| waiter.thread.id() == thread && waiter.armed.load(Ordering::Acquire))
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
        let mut waiters = self.signal.waiters.lock().unwrap();
        waiters.retain(|waiter| !Arc::ptr_eq(waiter, &self.state));
        self.signal.active.fetch_sub(1, Ordering::Release);
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
    state: AtomicU8,
    notify: Notify,
}

impl DataSignal {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(IDLE),
            notify: Notify::new(),
        }
    }

    /// Producer: mark data available.
    /// Wakes one waiter only on the idle-to-pending transition.
    #[inline]
    pub(crate) fn mark(&self) {
        // Order the caller's publication before the state read. See the
        // module docs.
        fence(Ordering::SeqCst);
        let mut state = self.state.load(Ordering::Acquire);
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
                    Ok(_) => return false,
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
        notified.await;
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
    generation: AtomicU64,
    notify: Notify,
}

impl StateSignal {
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn notify_changed(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

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
mod tests {
    use std::sync::Arc;

    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn first_mark_wakes() {
        let sig = Arc::new(DataSignal::new());
        let s = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_secs(1), s.notified()).await });
        tokio::task::yield_now().await;
        sig.mark();
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn second_mark_coalesces() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        sig.mark();
        timeout(Duration::from_secs(1), sig.ready())
            .await
            .expect("pending signal should be ready");

        sig.begin_drain();
        sig.clear_after(true);
        let s = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_millis(20), s.ready()).await });
        assert!(
            handle.await.unwrap().is_err(),
            "no wake after clear without new mark",
        );
    }

    #[tokio::test]
    async fn rearm_fires_when_nonempty() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        let s = sig.clone();
        let _ = timeout(Duration::from_secs(1), s.notified()).await;
        sig.begin_drain();
        assert!(sig.clear_after(false));

        let s2 = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_secs(1), s2.notified()).await });
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn rearm_silent_when_empty() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        let s = sig.clone();
        let _ = timeout(Duration::from_secs(1), s.notified()).await;
        sig.begin_drain();
        assert!(!sig.clear_after(true));

        let s2 = sig.clone();
        let handle =
            tokio::spawn(async move { timeout(Duration::from_millis(20), s2.notified()).await });
        assert!(
            handle.await.unwrap().is_err(),
            "rearm with is_empty=true must not wake",
        );
    }

    #[tokio::test]
    async fn clear_after_rearms_when_marked_during_drain_even_if_empty_stale() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        sig.begin_drain();
        sig.mark();
        assert!(sig.clear_after(true));
        timeout(Duration::from_secs(1), sig.ready())
            .await
            .expect("dirty drain should preserve readiness");
    }

    #[tokio::test]
    async fn wake_all_wakes_multiple() {
        let sig = Arc::new(DataSignal::new());
        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = sig.clone();
            handles.push(tokio::spawn(async move {
                timeout(Duration::from_secs(1), s.notified()).await
            }));
        }
        tokio::task::yield_now().await;
        sig.wake_all();
        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }
    }

    #[tokio::test]
    async fn ready_observes_pending_after_cancelled_waiter() {
        let sig = DataSignal::new();
        let mut notified = Box::pin(sig.notified());
        notified.as_mut().enable();

        sig.mark();
        drop(notified);

        timeout(Duration::from_secs(1), sig.ready())
            .await
            .expect("pending flag should keep readiness visible");
    }

    #[tokio::test]
    async fn state_signal_observes_change_after_waiter_creation() {
        let sig = StateSignal::new();
        let seen = sig.generation();
        let wait = sig.changed_after(seen);
        tokio::pin!(wait);
        sig.notify_changed();
        timeout(Duration::from_secs(1), wait)
            .await
            .expect("generation change should wake waiter");
    }

    #[tokio::test]
    async fn state_signal_observes_change_before_await() {
        let sig = StateSignal::new();
        let seen = sig.generation();
        sig.notify_changed();
        timeout(Duration::from_secs(1), sig.changed_after(seen))
            .await
            .expect("generation change should be stateful");
    }
}
