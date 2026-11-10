use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

/// Waker that counts its wakes.
#[derive(Debug, Default)]
pub(crate) struct WakeCounter(AtomicUsize);

impl WakeCounter {
    /// Wakes so far.
    pub(crate) fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    /// Wakes so far, then reset the count to zero.
    pub(crate) fn take(&self) -> usize {
        self.0.swap(0, Ordering::AcqRel)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Release pairs with the Acquire reads: a test that sees a wake
        // also sees what the waking thread published before it.
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// A fresh wake counter and a waker that bumps it.
pub(crate) fn counting_waker() -> (Arc<WakeCounter>, Waker) {
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    (counter, waker)
}

/// Poll `future` once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}
