use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(windows)]
use pyo3::prelude::*;
#[cfg(windows)]
use pyo3::types::PyAny;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub(crate) use unix::EventFdSignal as UnixSignal;
#[cfg(windows)]
pub(crate) use windows::WindowsSignal;

/// Internal backend for the socket wakeup primitive.
///
/// The shared `ReadinessSignal` facade exposes the generic park/wake interface;
/// the backend carries the platform-specific wake transport.
#[cfg(unix)]
type SignalBackend = UnixSignal;

#[cfg(windows)]
type SignalBackend = WindowsSignal;

/// Platform-agnostic readiness signal for async socket wake-up paths.
///
/// The `parking` flag avoids syscalls on the hot path. The consumer
/// sets it before sleeping; the producer only writes to the backend
/// wake transport when it sees the flag.
pub(crate) struct ReadinessSignal {
    parking: AtomicBool,
    backend: SignalBackend,
}

impl ReadinessSignal {
    pub fn new() -> Self {
        Self {
            parking: AtomicBool::new(false),
            backend: SignalBackend::new(),
        }
    }

    pub fn signal(&self) {
        #[cfg(unix)]
        {
            self.backend.signal(self.parking.load(Ordering::Acquire));
        }
        #[cfg(windows)]
        {
            self.backend.signal(self.parking.load(Ordering::Acquire));
        }
    }

    pub fn force_wake(&self) {
        self.backend.force_wake();
    }

    #[cfg(windows)]
    pub fn mark_drain_complete(&self) {
        self.backend.mark_drain_complete();
    }

    pub fn park_begin(&self) {
        self.parking.store(true, Ordering::Release);
    }

    pub fn park_end(&self) {
        self.parking.store(false, Ordering::Relaxed);
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.backend.wait_timeout(timeout)
    }

    #[cfg(unix)]
    pub fn fd(&self) -> i32 {
        self.backend.fd()
    }

    #[cfg(not(unix))]
    pub fn fd(&self) -> i32 {
        -1
    }

    #[cfg(unix)]
    pub fn dup_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        self.backend.dup_fd()
    }

    #[cfg(windows)]
    pub fn set_wakeup_hooks(
        &self,
        async_callback: Option<Py<PyAny>>,
        sync_event: Option<Py<PyAny>>,
    ) {
        self.backend.set_wakeup_hooks(async_callback, sync_event);
    }

    #[cfg(windows)]
    pub fn set_wakeup_mode(&self, mode: u32) {
        self.backend.set_wakeup_mode(mode);
    }

    #[cfg(windows)]
    pub fn clear_wakeup_mode(&self, mode: u32) {
        self.backend.clear_wakeup_mode(mode);
    }

    /// Permanently arm the signal so wakeups are emitted even when no
    /// thread is currently parked in the wait loop.
    pub fn arm_persistent(&self) {
        self.parking.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::ReadinessSignal;
    use std::sync::atomic::Ordering;

    #[test]
    fn parking_state_tracks_wait_loop() {
        let signal = ReadinessSignal::new();
        signal.park_begin();
        assert!(signal.parking.load(Ordering::Acquire));
        signal.park_end();
        assert!(!signal.parking.load(Ordering::Acquire));
    }
}
