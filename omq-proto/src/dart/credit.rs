//! Shared admission and returned storage accounting. Publication wakes belong
//! to the runtime; the counters themselves perform no I/O.

#[cfg(all(loom, target_pointer_width = "64"))]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(all(loom, target_pointer_width = "64")))]
use std::sync::atomic::{AtomicUsize, Ordering};

const CLOSED: usize = 1 << (usize::BITS - 1);

/// One finite admission budget spanning queued and unacknowledged messages.
#[derive(Debug)]
pub struct AdmissionCounter {
    state: AtomicUsize,
    capacity: usize,
}

impl AdmissionCounter {
    /// Create an empty admission budget.
    ///
    /// # Panics
    /// Panics if capacity is zero or uses the highest bit of `usize`.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity < CLOSED);
        Self {
            state: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Reserve up to `requested` slots; return zero after closure.
    pub fn acquire(&self, requested: usize) -> usize {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CLOSED != 0 {
                return 0;
            }
            let count = requested.min(self.capacity - state);
            if count == 0 {
                return 0;
            }
            match self.state.compare_exchange_weak(
                state,
                state + count,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return count,
                Err(next) => state = next,
            }
        }
    }

    /// Return previously acquired slots.
    ///
    /// # Panics
    /// Panics if `count` exceeds the number of acquired slots.
    pub fn release(&self, count: usize) {
        let previous = self.state.fetch_sub(count, Ordering::AcqRel);
        assert!(previous & !CLOSED >= count);
    }

    /// Return unreserved slots, or zero after closure.
    pub fn available(&self) -> usize {
        let state = self.state.load(Ordering::Acquire);
        if state & CLOSED != 0 {
            0
        } else {
            self.capacity - state
        }
    }

    /// Permanently prevent further acquisitions.
    pub fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }
    /// Whether admission has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }
}

/// Publish only after reusable storage has entered its free list. A drain
/// racing a return observes it now or on the next wake, never twice.
#[derive(Debug, Default)]
pub struct CreditCounter(AtomicUsize);

impl CreditCounter {
    /// Publish credits after their reusable storage has been returned.
    pub fn publish(&self, count: usize) {
        self.0.fetch_add(count, Ordering::Release);
    }
    /// Atomically take all published credits, leaving the counter empty.
    pub fn take(&self) -> usize {
        self.0.swap(0, Ordering::AcqRel)
    }
}
