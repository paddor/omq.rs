use std::sync::Mutex;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows::Win32::System::Threading::{CreateEventW, ResetEvent, SetEvent, WaitForSingleObject};

use super::{CallbackDispatch, DispatchClaim};

pub(crate) const WAKEUP_MODE_ASYNC: u32 = 1 << 0;
pub(crate) const WAKEUP_MODE_SYNC: u32 = 1 << 1;

pub(crate) struct WindowsSignal {
    state: Mutex<WindowsWakeupState>,
}

struct WakeupDispatch {
    async_callback: Option<Py<PyAny>>,
    sync_event: Option<Py<PyAny>>,
}

// The Windows event handle and all wakeup state are protected by `state`.
unsafe impl Send for WindowsSignal {}
unsafe impl Sync for WindowsSignal {}

struct WindowsWakeupState {
    // The waitable event used by the Windows side to wake the async loop.
    event: HANDLE,
    // Latched wakeup signal: once set, the waiter must consume it before it can
    // go idle again. This is the OS-visible pending bit for the event path.
    pending: bool,
    // A queued or running Python drain owns `Scheduled`. Additional wakeups are
    // coalesced into `ScheduledPending` and cause one follow-up drain.
    callback_dispatch: CallbackDispatch,
    hooks: WakeupHooks,
}

impl WindowsWakeupState {
    fn new() -> Self {
        let event = unsafe { CreateEventW(None, true, false, None) }.expect("CreateEventW failed");
        assert!(!event.is_invalid(), "CreateEventW failed");
        Self {
            event,
            pending: false,
            callback_dispatch: CallbackDispatch::default(),
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

    fn claim_dispatch(&mut self, callback_enabled: bool, sync_enabled: bool) -> DispatchClaim {
        self.callback_dispatch.claim(callback_enabled, sync_enabled)
    }

    fn finish_callback(&mut self) -> bool {
        self.callback_dispatch.finish()
    }

    fn abort_callback(&mut self) {
        self.callback_dispatch.abort();
    }

    fn consume_pending(&mut self) {
        self.pending = false;
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

    pub(crate) fn signal(&self, waiter_armed: bool) {
        let claim = {
            let mut state = self.state.lock().unwrap();
            let mode = state.hooks.mode;
            let callback_enabled =
                mode & WAKEUP_MODE_ASYNC != 0 && state.hooks.async_callback.is_some();
            let sync_enabled = mode & WAKEUP_MODE_SYNC != 0 && state.hooks.sync_event.is_some();
            if !waiter_armed && !callback_enabled && !sync_enabled {
                return;
            }
            if !state.pending {
                state.pending = true;
                unsafe {
                    let _ = SetEvent(state.event);
                }
            }
            state.claim_dispatch(callback_enabled, sync_enabled)
        };

        if !claim.async_callback && !claim.sync_event {
            return;
        }

        let async_dispatched = Python::attach(|py| {
            let dispatch = {
                let state = self.state.lock().unwrap();
                WakeupDispatch {
                    // A claimed dispatch must run even if Python clears the mode
                    // before this producer thread acquires the GIL.
                    async_callback: claim
                        .async_callback
                        .then(|| state.hooks.async_callback.as_ref())
                        .flatten()
                        .map(|cb| cb.clone_ref(py)),
                    sync_event: claim
                        .sync_event
                        .then(|| state.hooks.sync_event.as_ref())
                        .flatten()
                        .map(|ev| ev.clone_ref(py)),
                }
            };

            let async_dispatched = if claim.async_callback {
                dispatch.async_callback.as_ref().is_some_and(|callback| {
                    match callback.call(py, (), None) {
                        Ok(_) => true,
                        Err(error) => {
                            error.write_unraisable(py, Some(callback.bind(py)));
                            false
                        }
                    }
                })
            } else {
                false
            };

            if let Some(event) = dispatch.sync_event.as_ref()
                && let Err(error) = event.call_method0(py, "set")
            {
                error.write_unraisable(py, Some(event.bind(py)));
            }

            async_dispatched
        });

        if claim.async_callback && !async_dispatched {
            // No Python drain will acknowledge this claim. Release it so a
            // later readiness transition can schedule another callback.
            let mut state = self.state.lock().unwrap();
            state.abort_callback();
        }
    }

    pub(crate) fn mark_drain_complete(&self) {
        let rearm = {
            let mut state = self.state.lock().unwrap();
            state.consume_pending();
            state.finish_callback()
        };
        if rearm {
            self.signal(true);
        }
    }

    pub(crate) fn force_wake(&self) {
        self.signal(true);
    }

    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.pending {
            state.pending = false;
            return true;
        }
        unsafe {
            let _ = ResetEvent(state.event);
        }
        if state.pending {
            state.pending = false;
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
            state.pending
        };
        if pending {
            self.signal(true);
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
    pub mode: u32,
}

impl WakeupHooks {
    fn set(&mut self, async_callback: Option<Py<PyAny>>, sync_event: Option<Py<PyAny>>) {
        self.async_callback = async_callback;
        self.sync_event = sync_event;
    }

    fn set_mode(&mut self, mode: u32) {
        if mode == 0 {
            self.mode = 0;
        } else {
            self.mode |= mode;
        }
    }

    fn clear_mode(&mut self, mode: u32) {
        if mode == 0 {
            self.mode = 0;
        } else {
            self.mode &= !mode;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_wakeup_waits_for_drain_finish_before_rearming() {
        let signal = WindowsSignal::new();
        let mut state = signal.state.lock().unwrap();

        assert!(state.claim_dispatch(true, false).async_callback);
        assert!(!state.claim_dispatch(true, false).async_callback);
        assert!(state.finish_callback());
        assert!(state.claim_dispatch(true, false).async_callback);
    }

    #[test]
    fn completed_async_drain_consumes_pending_wakeup() {
        let signal = WindowsSignal::new();

        {
            let mut state = signal.state.lock().unwrap();
            state.pending = true;
            assert!(state.claim_dispatch(true, false).async_callback);
        }

        signal.mark_drain_complete();

        let state = signal.state.lock().unwrap();
        assert!(!state.pending);
    }

    #[test]
    fn unarmed_signal_does_not_latch_pending_wakeup() {
        let signal = WindowsSignal::new();

        signal.signal(false);

        let state = signal.state.lock().unwrap();
        assert!(!state.pending);
    }

    #[test]
    fn armed_signal_latches_pending_wakeup() {
        let signal = WindowsSignal::new();

        signal.signal(true);

        let state = signal.state.lock().unwrap();
        assert!(state.pending);
    }

    #[test]
    fn callback_claim_survives_mode_clear_before_dispatch() {
        let signal = WindowsSignal::new();
        let mut state = signal.state.lock().unwrap();

        state.set_mode(WAKEUP_MODE_ASYNC);
        let claim = state.claim_dispatch(true, false);
        state.clear_mode(WAKEUP_MODE_ASYNC);

        assert!(claim.async_callback);
        assert!(!state.finish_callback());
    }

    #[test]
    fn failed_callback_dispatch_releases_claim_without_losing_wakeup() {
        let signal = WindowsSignal::new();
        let mut state = signal.state.lock().unwrap();

        state.pending = true;
        assert!(state.claim_dispatch(true, false).async_callback);
        assert!(!state.claim_dispatch(true, false).async_callback);
        state.abort_callback();

        assert!(state.pending);
        assert!(state.claim_dispatch(true, false).async_callback);
    }
}
