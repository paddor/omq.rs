use std::cell::UnsafeCell;
#[cfg(debug_assertions)]
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};

/// Single-threaded interior mutability.
///
/// Debug builds wrap `UnsafeCell<T>` behind an owner-thread check unless an
/// embedding explicitly promises externally serialized thread migration.
/// Release builds trust the same caller invariant without a hot-path check.
pub(crate) struct LocalCell<T> {
    value: UnsafeCell<T>,
    #[cfg(debug_assertions)]
    owner: OnceLock<std::thread::ThreadId>,
    #[cfg(debug_assertions)]
    allow_thread_migration: AtomicBool,
}

// SAFETY: ZMQ sockets are not thread-safe. Callers must serialize all access.
// Debug builds enforce one fixed thread unless migration is explicitly enabled.
unsafe impl<T: Send> Sync for LocalCell<T> {}

impl<T> LocalCell<T> {
    pub(crate) fn new(val: T) -> Self {
        Self {
            value: UnsafeCell::new(val),
            #[cfg(debug_assertions)]
            owner: OnceLock::new(),
            #[cfg(debug_assertions)]
            allow_thread_migration: AtomicBool::new(false),
        }
    }

    pub(crate) fn allow_thread_migration(&self) {
        #[cfg(debug_assertions)]
        self.allow_thread_migration.store(true, Ordering::Release);
    }

    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(crate) unsafe fn get(&self) -> &mut T {
        #[cfg(debug_assertions)]
        {
            if !self.allow_thread_migration.load(Ordering::Acquire) {
                let current = std::thread::current().id();
                let owner = self.owner.get_or_init(|| current);
                assert_eq!(
                    *owner, current,
                    "LocalCell accessed from multiple socket threads"
                );
            }
        }

        // SAFETY: caller upholds the socket access serialization invariant.
        unsafe { &mut *self.value.get() }
    }

    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_unchecked(&self) -> &mut T {
        // SAFETY: caller guarantees setup or teardown has exclusive access and
        // must not bind the runtime owner thread for later app-facing access.
        unsafe { &mut *self.value.get() }
    }
}

impl<T> std::fmt::Debug for LocalCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCell").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
