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

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering, fence};
#[cfg(feature = "dart")]
use std::sync::{Mutex, Weak};

use smallvec::SmallVec;
use tokio::sync::Notify;

mod blocking;

pub use blocking::BlockingRecvCancel;
pub(crate) use blocking::{
    BlockingRecvCancelGuard, BlockingSignal, WakerRegistration, block_on, enter_receive_guard,
    leave_receive_guard,
};

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
