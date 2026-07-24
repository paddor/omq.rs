use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows::Win32::System::Threading::{CreateEventW, ResetEvent, SetEvent, WaitForSingleObject};

pub(crate) const WAKEUP_MODE_ASYNC: u32 = 1 << 0;
pub(crate) const WAKEUP_MODE_SYNC: u32 = 1 << 1;
const WAKEUP_CALLBACK_IDLE: u8 = 0;
const WAKEUP_CALLBACK_SCHEDULED: u8 = 1 << 0;
const WAKEUP_CALLBACK_PENDING: u8 = 1 << 1;

pub(crate) struct WindowsSignal {
    state: Mutex<WindowsWakeupState>,
}

struct WakeupDispatch {
    async_callback: Option<Py<PyAny>>,
    sync_event: Option<Py<PyAny>>,
    mode: u32,
}

// The only non-native-thread-safe piece here is the Windows waitable event handle,
// and it is fully protected by the mutex in this backend. The surrounding atomics
// are only used for bookkeeping, so the backend is safe to send and share.
unsafe impl Send for WindowsSignal {}
unsafe impl Sync for WindowsSignal {}

struct WindowsWakeupState {
    // The waitable event used by the Windows side to wake the async loop.
    event: HANDLE,
    // Latched wakeup signal: once set, the waiter must consume it before it can
    // go idle again. This is the OS-visible pending bit for the event path.
    pending: AtomicBool,
    // True while the Python-side drain callback is running. Additional wakeups
    // during this window are coalesced into a follow-up callback instead of
    // scheduling a new one immediately.
    draining: AtomicBool,
    // Small state machine for the callback path:
    // - SCHEDULED: a drain callback has been queued
    // - PENDING: another wakeup arrived while a drain was already in progress
    // - IDLE: nothing pending for the callback path
    callback_state: AtomicU8,
    hooks: WakeupHooks,
}

impl WindowsWakeupState {
    fn new() -> Self {
        let event = unsafe { CreateEventW(None, true, false, None) }.expect("CreateEventW failed");
        assert!(!event.is_invalid(), "CreateEventW failed");
        Self {
            event,
            pending: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            callback_state: AtomicU8::new(WAKEUP_CALLBACK_IDLE),
            hooks: WakeupHooks::default(),
        }
    }

    fn set_hooks(&mut self, async_callback: Option<Py<PyAny>>, sync_event: Option<Py<PyAny>>) {
        self.hooks.set(async_callback, sync_event);
    }

    fn set_mode(&mut self, mode: u32) {
        self.hooks.set_mode(mode);
    }

    fn clear_mode(&mut self, mode: u32) {
        self.hooks.clear_mode(mode);
    }

    fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    fn try_schedule_callback(&self) -> bool {
        if self.draining.load(Ordering::Acquire) {
            self.callback_state
                .fetch_or(WAKEUP_CALLBACK_PENDING, Ordering::AcqRel);
            false
        } else {
            self.begin_drain();
            let prev = self
                .callback_state
                .fetch_or(WAKEUP_CALLBACK_SCHEDULED, Ordering::AcqRel);
            if prev & WAKEUP_CALLBACK_SCHEDULED != 0 {
                self.callback_state
                    .fetch_or(WAKEUP_CALLBACK_PENDING, Ordering::AcqRel);
                false
            } else {
                true
            }
        }
    }

    fn finish_callback(&self) -> bool {
        let mut prev = self.callback_state.load(Ordering::Acquire);
        loop {
            let needs_followup = prev & WAKEUP_CALLBACK_PENDING != 0;
            let next = WAKEUP_CALLBACK_IDLE;
            match self.callback_state.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.draining.store(false, Ordering::Release);
                    return needs_followup;
                }
                Err(current) => prev = current,
            }
        }
    }

    fn consume_pending(&self) {
        self.pending.store(false, Ordering::Release);
        unsafe {
            let _ = ResetEvent(self.event);
        }
    }
}

impl Drop for WindowsWakeupState {
    fn drop(&mut self) {
        if !self.event.is_invalid() {
            unsafe {
                let _ = windows::Win32::Foundation::CloseHandle(self.event);
            }
        }
    }
}

impl WindowsSignal {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(WindowsWakeupState::new()),
        }
    }

    pub(crate) fn signal(&self) {
        let should_invoke = {
            let state = self.state.lock().unwrap();
            let already_pending = state.pending.swap(true, Ordering::AcqRel);
            if !already_pending {
                unsafe {
                    let _ = SetEvent(state.event);
                }
            }
            let mode = state.hooks.mode.load(Ordering::Relaxed);
            let callback_enabled =
                mode & WAKEUP_MODE_ASYNC != 0 && state.hooks.async_callback.is_some();
            let sync_enabled = mode & WAKEUP_MODE_SYNC != 0 && state.hooks.sync_event.is_some();
            if callback_enabled {
                state.try_schedule_callback()
            } else {
                sync_enabled
            }
        };

        if should_invoke {
            Python::attach(|py| {
                let dispatch = {
                    let state = self.state.lock().unwrap();
                    let async_callback = state
                        .hooks
                        .async_callback
                        .as_ref()
                        .map(|cb| cb.clone_ref(py));
                    let sync_event = state.hooks.sync_event.as_ref().map(|ev| ev.clone_ref(py));
                    let mode = state.hooks.mode.load(Ordering::Relaxed);
                    WakeupDispatch {
                        async_callback,
                        sync_event,
                        mode,
                    }
                };

                if dispatch.mode & WAKEUP_MODE_ASYNC != 0
                    && let Some(callback) = dispatch.async_callback.as_ref()
                {
                    let _ = callback.call(py, (), None);
                }
                if dispatch.mode & WAKEUP_MODE_SYNC != 0
                    && let Some(event) = dispatch.sync_event.as_ref()
                {
                    let _ = event.call_method0(py, "set");
                }
            });
        }
    }

    pub(crate) fn mark_drain_complete(&self) {
        let rearm = {
            let state = self.state.lock().unwrap();
            state.consume_pending();
            state.finish_callback()
        };
        if rearm {
            self.signal();
        }
    }

    pub(crate) fn force_wake(&self) {
        self.signal();
    }

    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        if state.pending.swap(false, Ordering::AcqRel) {
            return true;
        }
        unsafe {
            let _ = ResetEvent(state.event);
        }
        if state.pending.swap(false, Ordering::AcqRel) {
            return true;
        }
        let handle = state.event;
        drop(state);

        let timeout_ms = timeout.as_millis().min(u32::MAX as u128) as u32;
        match unsafe { WaitForSingleObject(handle, timeout_ms) } {
            WAIT_OBJECT_0 => true,
            WAIT_TIMEOUT => false,
            _ => false,
        }
    }

    pub(crate) fn set_wakeup_hooks(
        &self,
        async_callback: Option<Py<PyAny>>,
        sync_event: Option<Py<PyAny>>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.set_hooks(async_callback, sync_event);
    }

    pub(crate) fn set_wakeup_mode(&self, mode: u32) {
        let pending = {
            let mut state = self.state.lock().unwrap();
            state.set_mode(mode);
            state.pending.load(Ordering::Acquire)
        };
        if pending {
            self.signal();
        }
    }

    pub(crate) fn clear_wakeup_mode(&self, mode: u32) {
        let mut state = self.state.lock().unwrap();
        state.clear_mode(mode);
    }
}

#[derive(Default)]
pub(crate) struct WakeupHooks {
    pub async_callback: Option<Py<PyAny>>,
    pub sync_event: Option<Py<PyAny>>,
    pub mode: std::sync::atomic::AtomicU32,
}

impl WakeupHooks {
    fn set(&mut self, async_callback: Option<Py<PyAny>>, sync_event: Option<Py<PyAny>>) {
        self.async_callback = async_callback;
        self.sync_event = sync_event;
    }

    fn set_mode(&mut self, mode: u32) {
        if mode == 0 {
            self.mode.store(0, Ordering::Relaxed);
        } else {
            self.mode.fetch_or(mode, Ordering::Relaxed);
        }
    }

    fn clear_mode(&mut self, mode: u32) {
        if mode == 0 {
            self.mode.store(0, Ordering::Relaxed);
        } else {
            self.mode.fetch_and(!mode, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_wakeup_waits_for_drain_finish_before_rearming() {
        let signal = WindowsSignal::new();
        let state = signal.state.lock().unwrap();

        assert!(state.try_schedule_callback());
        assert!(!state.try_schedule_callback());
        assert!(state.finish_callback());
    }

    #[test]
    fn completed_async_drain_consumes_pending_wakeup() {
        let signal = WindowsSignal::new();

        {
            let state = signal.state.lock().unwrap();
            state.pending.store(true, Ordering::Release);
            assert!(state.try_schedule_callback());
        }

        signal.mark_drain_complete();

        let state = signal.state.lock().unwrap();
        assert!(!state.pending.load(Ordering::Acquire));
        assert!(!state.draining.load(Ordering::Acquire));
        assert_eq!(
            state.callback_state.load(Ordering::Acquire),
            WAKEUP_CALLBACK_IDLE
        );
    }
}
