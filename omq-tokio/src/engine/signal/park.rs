//! Parking for threads that block on a socket.
//!
//! On Apple platforms std parks a thread on a dispatch semaphore that the
//! thread keeps for life. Its Mach port does not survive `fork()`, so a
//! thread that parked before a fork traps in the child the next time it
//! parks. There each thread parks on a mutex and condition variable
//! instead, replaced when the process id changes. Elsewhere std's parking
//! survives a fork and is used directly.

/// Parks the thread that created it until another thread unparks it.
/// Like std's parking, an unpark before the park is kept, and a park may
/// return early.
#[derive(Debug, Clone)]
pub(crate) struct Parker(imp::Parker);

impl Parker {
    /// The calling thread's parker.
    pub(crate) fn current() -> Self {
        Self(imp::Parker::current())
    }

    /// Block until unparked. Only the thread that created this parker
    /// parks on it.
    pub(crate) fn park(&self) {
        self.0.park();
    }

    /// Block until unparked or `timeout` passed.
    pub(crate) fn park_timeout(&self, timeout: std::time::Duration) {
        self.0.park_timeout(timeout);
    }

    pub(crate) fn unpark(&self) {
        self.0.unpark();
    }

    /// False in a forked child for a parker the parent created: park on
    /// [`Parker::current`] instead.
    pub(crate) fn is_current(&self) -> bool {
        self.0.is_current()
    }

    #[cfg(test)]
    pub(crate) fn thread_id(&self) -> std::thread::ThreadId {
        self.0.thread_id()
    }
}

#[cfg(not(target_vendor = "apple"))]
mod imp {
    #[derive(Debug, Clone)]
    pub(super) struct Parker(std::thread::Thread);

    impl Parker {
        pub(super) fn current() -> Self {
            Self(std::thread::current())
        }

        pub(super) fn park(&self) {
            debug_assert_eq!(self.0.id(), std::thread::current().id());
            std::thread::park();
        }

        pub(super) fn park_timeout(&self, timeout: std::time::Duration) {
            debug_assert_eq!(self.0.id(), std::thread::current().id());
            std::thread::park_timeout(timeout);
        }

        pub(super) fn unpark(&self) {
            self.0.unpark();
        }

        #[expect(clippy::unused_self, reason = "the Apple parker checks its process")]
        pub(super) fn is_current(&self) -> bool {
            true
        }

        #[cfg(test)]
        pub(super) fn thread_id(&self) -> std::thread::ThreadId {
            self.0.id()
        }
    }
}

#[cfg(target_vendor = "apple")]
mod imp {
    use std::cell::RefCell;
    use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

    #[derive(Debug)]
    struct State {
        /// The process that created it. A forked child makes its own: a
        /// thread that vanished in the fork may have held `notified`.
        pid: u32,
        notified: Mutex<bool>,
        unparked: Condvar,
        #[cfg(test)]
        thread: std::thread::ThreadId,
    }

    thread_local! {
        static CURRENT: RefCell<Option<Arc<State>>> = const { RefCell::new(None) };
    }

    #[derive(Debug, Clone)]
    pub(super) struct Parker(Arc<State>);

    impl Parker {
        pub(super) fn current() -> Self {
            let pid = std::process::id();
            let new = || {
                Arc::new(State {
                    pid,
                    notified: Mutex::new(false),
                    unparked: Condvar::new(),
                    #[cfg(test)]
                    thread: std::thread::current().id(),
                })
            };

            let state = CURRENT
                .try_with(|current| {
                    let mut current = current.borrow_mut();
                    match &*current {
                        Some(state) if state.pid == pid => state.clone(),
                        _ => current.insert(new()).clone(),
                    }
                })
                // During thread-local teardown a call parks on its own.
                .unwrap_or_else(|_| new());

            Self(state)
        }

        fn notified(&self) -> MutexGuard<'_, bool> {
            self.0
                .notified
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        }

        pub(super) fn park(&self) {
            let mut notified = self.notified();
            while !*notified {
                notified = self
                    .0
                    .unparked
                    .wait(notified)
                    .unwrap_or_else(PoisonError::into_inner);
            }

            *notified = false;
        }

        pub(super) fn park_timeout(&self, timeout: std::time::Duration) {
            let mut notified = self.notified();
            if !*notified {
                notified = self
                    .0
                    .unparked
                    .wait_timeout(notified, timeout)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }

            *notified = false;
        }

        pub(super) fn unpark(&self) {
            *self.notified() = true;
            self.0.unparked.notify_one();
        }

        pub(super) fn is_current(&self) -> bool {
            self.0.pid == std::process::id()
        }

        #[cfg(test)]
        pub(super) fn thread_id(&self) -> std::thread::ThreadId {
            self.0.thread
        }
    }
}
