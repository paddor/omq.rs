//! Cross-platform notification primitives for socket signaling and polling.
//!
//! Unix: eventfd (Linux) or pipe pairs. Windows: manual-reset events via
//! `CreateEventW`/`SetEvent`/`ResetEvent`.
//!
//! Three abstractions:
//! - `RecvNotify`: Copy, monomorphic signal/drain for hot paths.
//! - `PollWaiter`: platform-specific multi-socket polling (`poll()` vs
//!   `WaitForMultipleObjects` with 64-handle batching).
//! - `NotifyHandle` trait: dynamic dispatch for setup/teardown.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
pub(crate) mod windows;

#[cfg(unix)]
pub(crate) use unix::RecvNotify;

#[cfg(windows)]
pub(crate) use windows::RecvNotify;

#[cfg(unix)]
pub(crate) use unix::{PollWaiter, check_descriptors};

/// Descriptor items are not polled on Windows.
#[cfg(windows)]
pub(crate) fn check_descriptors(_items: &mut [crate::poll::ZmqPollItem]) -> i32 {
    0
}

#[cfg(windows)]
#[allow(unused_imports)]
pub(crate) use windows::PollWaiter;

#[cfg(unix)]
pub(crate) use unix::UnixNotifyHandle as PlatformNotifyHandle;

#[cfg(windows)]
pub(crate) use windows::WindowsNotifyHandle as PlatformNotifyHandle;

#[allow(dead_code)]
pub(crate) trait NotifyHandle: Send + Sync {
    fn signal_recv(&self);
    fn signal_send(&self);
    fn close(&self);
    fn recv_fd(&self) -> std::os::raw::c_int;
    fn send_fd(&self) -> std::os::raw::c_int;
    fn recv_notifier(&self) -> RecvNotify;
}

/// Wakeup handles for a new socket, or `None` when the OS cannot create
/// them.
pub(crate) fn create_notify() -> Option<std::sync::Arc<PlatformNotifyHandle>> {
    PlatformNotifyHandle::new().map(std::sync::Arc::new)
}
