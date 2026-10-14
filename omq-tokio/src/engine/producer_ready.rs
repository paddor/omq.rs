//! Shared readiness for serialized inbox producers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use fanring::{mpsc, teardown::Coordinated};

/// The caller serializes producer access. A ready probe that cancels a
/// pending ring waiter must notify the inbox's other capacity waiters.
#[inline]
pub(super) fn ready<T>(
    sender: &mut mpsc::Sender<T, Coordinated>,
    waiting: &AtomicBool,
    waker: &Waker,
    notify_ready: impl FnOnce(),
) -> Result<bool, ()> {
    // No waiter needs registration or cancellation on the usual ready path.
    if !waiting.load(Ordering::Acquire) && !sender.is_disconnected() && !sender.is_full() {
        return Ok(true);
    }
    wait_ready(sender, waiting, waker, notify_ready)
}

#[cold]
fn wait_ready<T>(
    sender: &mut mpsc::Sender<T, Coordinated>,
    waiting: &AtomicBool,
    waker: &Waker,
    notify_ready: impl FnOnce(),
) -> Result<bool, ()> {
    let was_waiting = waiting.swap(true, Ordering::AcqRel);
    match sender.poll_ready(&mut Context::from_waker(waker)) {
        Poll::Pending => return Ok(false),
        Poll::Ready(Err(_)) => return Err(()),
        Poll::Ready(Ok(())) => {}
    }
    waiting.store(false, Ordering::Release);
    if was_waiting {
        notify_ready();
    }
    Ok(true)
}
