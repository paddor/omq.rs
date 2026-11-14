//! Skip polling wait sources that have not asked to be polled.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

/// Gate for wait sources that a `select!` loop polls every turn but that
/// rarely become ready. The sources register one stable private waker.
/// After a poll in which they all returned `Pending`, later polls skip them
/// until that waker fires, so an idle turn costs one atomic load instead of
/// the sources' own registration.
///
/// Every source must stay registered across `Pending` polls and wake through
/// the waker of its latest poll, as tokio's `Notified` and `AtomicWaker` do.
pub(crate) struct WakeGate {
    wake: Arc<GateWake>,
    waker: Waker,
    /// The task waker last stored in `wake`.
    task: Option<Waker>,
    /// The last poll left every source registered with `waker`.
    parked: bool,
}

#[derive(Debug, Default)]
struct GateWake {
    fired: AtomicBool,
    task: Mutex<Option<Waker>>,
}

impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Set the flag before reading the task waker. `poll` stores the task
        // waker before reading the flag, so one of them sees the other.
        self.fired.store(true, Ordering::Release);

        if let Some(task) = self.task.lock().expect("gate task poisoned").as_ref() {
            task.wake_by_ref();
        }
    }
}

impl WakeGate {
    pub(crate) fn new() -> Self {
        let wake = Arc::new(GateWake::default());
        Self {
            waker: Waker::from(wake.clone()),
            wake,
            task: None,
            parked: false,
        }
    }

    /// Poll the sources through `poll` unless they are parked and unwoken.
    pub(crate) fn poll<T>(
        &mut self,
        cx: &mut Context<'_>,
        poll: impl FnOnce(&mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        if !self
            .task
            .as_ref()
            .is_some_and(|task| task.will_wake(cx.waker()))
        {
            let task = cx.waker().clone();
            *self.wake.task.lock().expect("gate task poisoned") = Some(task.clone());
            self.task = Some(task);
        }

        // Acquire pairs with the wake's release.
        if self.parked && !self.wake.fired.load(Ordering::Acquire) {
            return Poll::Pending;
        }

        // A wake racing with this poll either sets the flag again or made its
        // source ready before it fired.
        self.wake.fired.store(false, Ordering::Relaxed);
        let result = poll(&mut Context::from_waker(&self.waker));
        // A ready source may hold more: it registers again only when empty.
        self.parked = result.is_pending();

        result
    }

    /// The sources were polled with another waker: poll them next time.
    pub(crate) fn reopen(&mut self) {
        self.parked = false;
    }
}

impl Default for WakeGate {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WakeGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WakeGate")
            .field("wake", &self.wake)
            .field("parked", &self.parked)
            .finish_non_exhaustive()
    }
}

/// A long-lived future behind a [`WakeGate`].
pub(crate) struct WokenGate<F> {
    future: F,
    gate: WakeGate,
}

impl<F> WokenGate<F> {
    pub(crate) fn new(future: F) -> Self {
        Self {
            future,
            gate: WakeGate::new(),
        }
    }
}

impl<F> WokenGate<Pin<&mut F>> {
    /// Replace the completed inner future.
    pub(crate) fn set(&mut self, future: F) {
        self.future.set(future);
        self.gate.reopen();
    }
}

impl<F: Future + Unpin> Future for WokenGate<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let Self { future, gate } = &mut *self;
        gate.poll(cx, |cx| Pin::new(future).poll(cx))
    }
}

impl<F> fmt::Debug for WokenGate<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WokenGate")
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
