//! Mutable receive ownership. A single handle uses an atomic transfer;
//! multiple handles also serialize through a mutex. The transfer remains
//! necessary because concurrent calls can borrow the same socket handle.

use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard, Weak};

use crossbeam_utils::atomic::AtomicCell;

pub(crate) struct ReceiveCell<T> {
    available: AtomicCell<ManuallyDrop<Option<Box<T>>>>,
    shared: Mutex<()>,
    handles: Weak<()>,
}

impl<T> fmt::Debug for ReceiveCell<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveCell")
            .field("shared", &self.shared)
            .field("handles", &self.handles)
            .finish_non_exhaustive()
    }
}

impl<T> ReceiveCell<T> {
    pub(crate) fn new(value: T, handles: Weak<()>) -> Self {
        assert!(AtomicCell::<ManuallyDrop<Option<Box<T>>>>::is_lock_free());
        let available = AtomicCell::new(ManuallyDrop::new(Some(Box::new(value))));
        Self {
            available,
            shared: Mutex::new(()),
            handles,
        }
    }

    pub(crate) fn set_handles(&mut self, handles: Weak<()>) {
        self.handles = handles;
    }

    #[expect(
        clippy::inline_always,
        reason = "Measured scalar receives avoid out-of-line ownership transfers"
    )]
    #[inline(always)]
    pub(crate) fn lock(&self) -> ReceiveGuard<'_, T> {
        let shared = (self.handles.strong_count() > 1)
            .then(|| self.shared.lock().expect("receive ownership poisoned"));
        let value = loop {
            if let Some(value) = ManuallyDrop::into_inner(self.available.take()) {
                break value;
            }
            // A single handle may still have overlapping calls through &self,
            // or a clone may have appeared while its prior call owned the state.
            std::thread::yield_now();
        };
        ReceiveGuard {
            cell: self,
            value: Some(value),
            serialization: shared,
        }
    }

    #[inline]
    pub(crate) fn try_lock(&self) -> Option<ReceiveGuard<'_, T>> {
        let shared = if self.handles.strong_count() > 1 {
            Some(self.shared.try_lock().ok()?)
        } else {
            None
        };
        Some(ReceiveGuard {
            cell: self,
            value: Some(ManuallyDrop::into_inner(self.available.take())?),
            serialization: shared,
        })
    }
}

impl<T> Drop for ReceiveCell<T> {
    fn drop(&mut self) {
        // The cell's exclusive destructor owns any value still available.
        drop(ManuallyDrop::into_inner(
            std::mem::take(&mut self.available).into_inner(),
        ));
    }
}

#[derive(Debug)]
pub(crate) struct ReceiveGuard<'a, T> {
    cell: &'a ReceiveCell<T>,
    value: Option<Box<T>>,
    serialization: Option<MutexGuard<'a, ()>>,
}

impl<T> Deref for ReceiveGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value.as_deref().expect("owned receive state")
    }
}

impl<T> DerefMut for ReceiveGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_deref_mut().expect("owned receive state")
    }
}

impl<T> Drop for ReceiveGuard<'_, T> {
    #[expect(
        clippy::inline_always,
        reason = "Measured scalar receives avoid out-of-line ownership transfers"
    )]
    #[inline(always)]
    fn drop(&mut self) {
        // Publish ownership before releasing the optional shared-handle lock.
        // This guard owns the only value, so the slot is empty. ManuallyDrop
        // lets AtomicCell use a release store instead of exchanging and
        // dropping the known-empty previous value. ReceiveCell drops storage.
        self.cell
            .available
            .store(ManuallyDrop::new(self.value.take()));
        drop(self.serialization.take());
    }
}

#[cfg(test)]
mod tests {
    use super::ReceiveCell;
    use std::sync::Arc;

    #[test]
    fn only_multiple_handles_acquire_the_mutex() {
        let handle = Arc::new(());
        let cell = ReceiveCell::new(0, Arc::downgrade(&handle));
        let mut owned = cell.lock();
        assert!(owned.serialization.is_none());
        *owned += 1;
        let clone = handle.clone();
        assert!(cell.try_lock().is_none());
        drop(owned);
        let mut shared = cell.lock();
        assert!(shared.serialization.is_some());
        assert_eq!(*shared, 1);
        *shared += 1;
        drop(shared);
        drop(clone);
        let owned = cell.lock();
        assert!(owned.serialization.is_none());
        assert_eq!(*owned, 2);
    }

    #[test]
    fn overlapping_calls_on_one_handle_transfer_exclusive_ownership() {
        let handle = Arc::new(());
        let cell = ReceiveCell::new(0, Arc::downgrade(&handle));
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let cell = &cell;
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        let mut owned = cell.lock();
                        assert!(owned.serialization.is_none());
                        *owned += 1;
                    }
                });
            }
        });
        assert_eq!(*cell.lock(), 20_000);
    }

    #[test]
    fn unwind_returns_single_handle_ownership() {
        let handle = Arc::new(());
        let cell = ReceiveCell::new(0, Arc::downgrade(&handle));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            *cell.lock() = 7;
            let _owned = cell.lock();
            panic!("receive interrupted");
        }));
        assert!(result.is_err());
        assert_eq!(*cell.lock(), 7);
    }
}
