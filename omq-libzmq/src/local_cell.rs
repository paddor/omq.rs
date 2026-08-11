use std::cell::UnsafeCell;
#[cfg(debug_assertions)]
use std::sync::OnceLock;

/// Single-threaded interior mutability.
///
/// Debug builds wrap `UnsafeCell<T>` behind an owner-thread check. Release
/// builds trust libzmq's one-thread-per-socket contract and avoid the check
/// on the hot path.
pub(crate) struct LocalCell<T> {
    value: UnsafeCell<T>,
    #[cfg(debug_assertions)]
    owner: OnceLock<std::thread::ThreadId>,
}

// SAFETY: ZMQ sockets are not thread-safe. Callers must access each socket
// from one application thread at a time. Debug builds enforce this.
unsafe impl<T: Send> Sync for LocalCell<T> {}

impl<T> LocalCell<T> {
    pub(crate) fn new(val: T) -> Self {
        Self {
            value: UnsafeCell::new(val),
            #[cfg(debug_assertions)]
            owner: OnceLock::new(),
        }
    }

    #[inline]
    #[expect(clippy::mut_from_ref)]
    pub(crate) unsafe fn get(&self) -> &mut T {
        #[cfg(debug_assertions)]
        {
            let current = std::thread::current().id();
            let owner = self.owner.get_or_init(|| current);
            assert_eq!(
                *owner, current,
                "LocalCell accessed from multiple socket threads"
            );
        }
        // SAFETY: caller upholds the socket single-thread access invariant.
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
mod tests {
    use std::sync::Arc;

    use super::LocalCell;

    #[test]
    fn permits_repeated_access_from_owner_thread() {
        let cell = LocalCell::new(1usize);

        // SAFETY: test accesses the cell from one thread.
        *unsafe { cell.get() } += 1;
        // SAFETY: same owner thread as above.
        assert_eq!(*unsafe { cell.get() }, 2);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn rejects_access_from_second_thread() {
        let cell = Arc::new(LocalCell::new(1usize));

        // SAFETY: this binds the owner to the current test thread.
        assert_eq!(*unsafe { cell.get() }, 1);

        let other = Arc::clone(&cell);
        let result = std::thread::spawn(move || {
            // SAFETY: this intentionally violates the owner-thread contract
            // to verify the guard panics before returning a mutable reference.
            let _ = unsafe { other.get() };
        })
        .join();

        assert!(result.is_err());
    }
}
