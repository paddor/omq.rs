//! Readiness callback: the push form of `ZMQ_FD`.
//!
//! A registered callback runs at most once per arm, where the receive
//! descriptor is signaled for the application: input arrived, a blocked
//! send may proceed, or the context is terminating. Registration and each
//! `ZMQ_EVENTS` read arm it, as they consume a `ZMQ_FD` wake.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering, fence};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// C callback for `omq_socket_set_ready_callback`.
pub(crate) type OMQReadyCallback = extern "C" fn(*mut c_void);

/// A callback with its userdata.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Registered {
    pub(crate) callback: OMQReadyCallback,
    pub(crate) userdata: *mut c_void,
}

// SAFETY: the caller of `omq_socket_set_ready_callback` allows the callback
// to run with its userdata on any thread.
unsafe impl Send for Registered {}

#[derive(Debug, Default)]
pub(crate) struct ReadyCallback {
    /// A callback is registered. Written and read only by socket calls,
    /// which the application serializes.
    registered: AtomicBool,
    /// The next signal runs the callback.
    armed: AtomicBool,
    /// Held while the callback runs, so clearing waits for a call in flight.
    slot: Mutex<Option<Registered>>,
}

impl ReadyCallback {
    fn slot(&self) -> MutexGuard<'_, Option<Registered>> {
        // The callback is `extern "C"` and cannot unwind into the lock.
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replace the callback and arm it; `None` clears it. When this returns,
    /// no call of a replaced callback is in flight.
    pub(crate) fn set(&self, registered: Option<Registered>) {
        let mut slot = self.slot();
        *slot = registered;
        // Relaxed: socket calls are serialized, and `fire` reads the slot
        // under the lock.
        self.registered.store(slot.is_some(), Ordering::Relaxed);
        self.armed.store(slot.is_some(), Ordering::Relaxed);
        drop(slot);
        fence(Ordering::SeqCst);
    }

    /// Arm before the caller checks readiness: a later signal runs the
    /// callback.
    pub(crate) fn arm(&self) {
        // Relaxed: set by an earlier socket call on the serialized owner.
        if !self.registered.load(Ordering::Relaxed) {
            return;
        }
        self.armed.store(true, Ordering::Relaxed);
        // Pairs with the fence in `fire`: either the signal sees the arm, or
        // the caller's readiness check sees the state the signal reports.
        fence(Ordering::SeqCst);
    }

    /// The receive descriptor was signaled: run the callback if armed.
    pub(crate) fn fire(&self) {
        // Pairs with the fence in `arm`; orders the producer's queue update
        // before the `armed` load.
        fence(Ordering::SeqCst);
        // Relaxed: the fences order it; the slot lock orders the callback.
        if !self.armed.load(Ordering::Relaxed) || !self.armed.swap(false, Ordering::Relaxed) {
            return;
        }
        let slot = self.slot();
        if let Some(registered) = *slot {
            (registered.callback)(registered.userdata);
        }
    }
}

#[cfg(test)]
mod tests;
