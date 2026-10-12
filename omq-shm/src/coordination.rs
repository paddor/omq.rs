//! Production atomic transitions, also compiled against Loom in the test crate.
#![forbid(unsafe_code)]

use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};
use crate::sync::{Arc, Mutex};

pub(crate) const STATE_BITS: u64 = 3;
pub(crate) const GENERATION_STEP: u64 = STATE_BITS + 1;
pub(crate) const PENDING: u64 = 1;
pub(crate) const BORROWED: u64 = 2;
pub(crate) const DONE: u64 = 3;
pub(crate) const COORDINATES: usize = 5;

pub(crate) struct LeaseCell<'a> {
    pub(crate) state: &'a AtomicU64,
    pub(crate) closed: &'a AtomicU64,
}

impl LeaseCell<'_> {
    #[inline]
    pub(crate) fn publish(
        &self,
        generation: u64,
        cells: [&AtomicU64; COORDINATES],
        coordinates: [u64; COORDINATES],
    ) -> bool {
        for (cell, value) in cells.iter().zip(coordinates) {
            cell.store(value, Ordering::Relaxed);
        }
        self.state.store(generation | PENDING, Ordering::Release);
        // Pair with close_reader: publication and close cannot both miss.
        fence(Ordering::SeqCst);
        self.closed.load(Ordering::Acquire) != 0 && self.complete(generation, PENDING)
    }

    #[inline]
    pub(crate) fn borrow(&self, generation: u64) -> bool {
        self.closed.load(Ordering::Acquire) == 0
            && self
                .state
                .compare_exchange(
                    generation | PENDING,
                    generation | BORROWED,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    #[inline]
    pub(crate) fn matches(
        cells: [&AtomicU64; COORDINATES],
        coordinates: [u64; COORDINATES],
    ) -> bool {
        cells
            .iter()
            .zip(coordinates)
            .all(|(cell, value)| cell.load(Ordering::Relaxed) == value)
    }

    #[inline]
    pub(crate) fn complete(&self, generation: u64, state: u64) -> bool {
        self.state
            .compare_exchange(
                generation | state,
                generation | DONE,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    #[inline]
    pub(crate) fn cancel_pending(&self) -> bool {
        let value = self.state.load(Ordering::Acquire);
        value & STATE_BITS == PENDING && self.complete(value & !STATE_BITS, PENDING)
    }

    #[inline]
    pub(crate) fn completed(&self, generation: u64) -> bool {
        let value = self.state.load(Ordering::Acquire);
        value & !STATE_BITS != generation || value & STATE_BITS == DONE
    }
}

pub(crate) fn close_reader(closed: &AtomicU64) {
    closed.store(1, Ordering::Release);
    fence(Ordering::SeqCst);
}

pub(crate) struct CreditSignal<'a> {
    pub(crate) sequence: &'a AtomicU32,
    pub(crate) parked: &'a AtomicU32,
    pub(crate) stopped: &'a AtomicBool,
}

impl CreditSignal<'_> {
    #[inline]
    pub(crate) fn notify(&self) -> bool {
        self.sequence.fetch_add(1, Ordering::Release);
        self.parked.swap(0, Ordering::SeqCst) != 0
    }

    #[inline]
    pub(crate) fn arm(&self, sequence: u32) -> bool {
        self.parked.swap(1, Ordering::SeqCst);
        !self.stopped.load(Ordering::Acquire) && self.sequence.load(Ordering::Acquire) == sequence
    }
}

pub(crate) struct CarrierState<'a> {
    pub(crate) write: &'a AtomicU64,
    pub(crate) read: &'a AtomicU64,
    pub(crate) imported: &'a AtomicU64,
    pub(crate) reader_parked: &'a AtomicU32,
    pub(crate) writer_parked: &'a AtomicU32,
    pub(crate) capacity: usize,
}

pub(crate) struct Window {
    pub(crate) cursor: u64,
    pub(crate) length: usize,
}

impl CarrierState<'_> {
    #[inline]
    pub(crate) fn read_window(&self) -> Option<Window> {
        let read = self.read.load(Ordering::Relaxed);
        let write = self.write.load(Ordering::Acquire);
        let length = write.wrapping_sub(read);
        (length <= self.capacity as u64).then_some(Window {
            cursor: read,
            length: length as usize,
        })
    }

    #[inline]
    pub(crate) fn write_window(&self) -> Option<Window> {
        let write = self.write.load(Ordering::Relaxed);
        let read = self.read.load(Ordering::Acquire);
        let length = write.wrapping_sub(read);
        (length <= self.capacity as u64).then(|| Window {
            cursor: write,
            length: self.capacity - length as usize,
        })
    }

    #[inline]
    pub(crate) fn publish(&self, cursor: u64) -> bool {
        self.write.store(cursor, Ordering::Release);
        self.reader_parked.swap(0, Ordering::SeqCst) != 0
    }

    #[inline]
    pub(crate) fn return_bytes(&self, cursor: u64) -> bool {
        self.reader_parked.store(0, Ordering::Relaxed);
        self.read.store(cursor, Ordering::Release);
        self.notify_writer()
    }

    #[inline]
    pub(crate) fn arm_reader(&self, polling: bool) -> Option<bool> {
        // RMW acquires a concurrent publisher that has already disarmed us.
        self.reader_parked
            .swap(u32::from(!polling), Ordering::SeqCst);
        self.read_window().map(|window| window.length != 0)
    }

    #[inline]
    pub(crate) fn park_writer(&self) {
        self.writer_parked.swap(1, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn notify_writer(&self) -> bool {
        self.writer_parked.swap(0, Ordering::SeqCst) != 0
    }

    #[inline]
    pub(crate) fn acknowledge_imports(&self, count: u64) -> bool {
        self.imported.store(count, Ordering::Release);
        self.notify_writer()
    }

    #[inline]
    pub(crate) fn imported(&self) -> u64 {
        self.imported.load(Ordering::Acquire)
    }
}

pub(crate) fn unique_release<T>(owner: Arc<T>, gate: &Mutex<()>) -> Option<Arc<T>> {
    if Arc::strong_count(&owner) != 1 {
        let _guard = gate.lock().expect("view release");
        if Arc::strong_count(&owner) != 1 {
            drop(owner);
            return None;
        }
    }
    Some(owner)
}
