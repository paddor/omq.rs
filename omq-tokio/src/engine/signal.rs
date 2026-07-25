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

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use tokio::sync::Notify;

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
        if self.state.load(Ordering::Acquire) != PENDING {
            return;
        }
        let _ = self
            .state
            .compare_exchange(PENDING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
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
        if self.state.load(Ordering::Acquire) != IDLE {
            return;
        }
        notified.await;
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
