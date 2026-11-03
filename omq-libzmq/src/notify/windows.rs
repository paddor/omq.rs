use super::NotifyHandle;
use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::System::Threading::{CreateEventW, ResetEvent, SetEvent};

#[derive(Clone, Copy)]
pub(crate) struct RecvNotify {
    event: HANDLE,
}

// SAFETY: HANDLE is an opaque kernel object reference. SetEvent/ResetEvent
// are atomic at the kernel level.
unsafe impl Send for RecvNotify {}
unsafe impl Sync for RecvNotify {}

impl RecvNotify {
    #[inline]
    pub(crate) fn signal(self) {
        // SAFETY: SetEvent takes the handle by value and touches no Rust
        // memory. The owning WindowsNotifyHandle outlives this copy; a
        // null handle (failed CreateEventW) only makes the call fail.
        unsafe {
            let _ = SetEvent(self.event);
        }
    }

    pub(crate) fn drain(self) {
        // SAFETY: as in `signal`; ResetEvent only touches kernel state.
        unsafe {
            let _ = ResetEvent(self.event);
        }
    }

    pub(crate) fn wait_for_readable(self, timeout_ms: std::os::raw::c_int) -> bool {
        use windows::Win32::Foundation::WAIT_OBJECT_0;
        use windows::Win32::System::Threading::WaitForSingleObject;

        let wait_ms = if timeout_ms < 0 {
            u32::MAX
        } else {
            timeout_ms as u32
        };
        // SAFETY: as in `signal`; the wait reads no Rust memory, and a
        // null handle returns WAIT_FAILED.
        let rc = unsafe { WaitForSingleObject(self.event, wait_ms) };
        rc == WAIT_OBJECT_0
    }
}

pub(crate) struct WindowsNotifyHandle {
    recv_event: Option<HANDLE>,
    send_event: Option<HANDLE>,
}

// SAFETY: HANDLE is an opaque kernel object reference; Win32 event
// operations are thread-safe.
unsafe impl Send for WindowsNotifyHandle {}
unsafe impl Sync for WindowsNotifyHandle {}

impl std::fmt::Debug for WindowsNotifyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WindowsNotifyHandle { .. }")
    }
}

impl WindowsNotifyHandle {
    pub(crate) fn new() -> Self {
        // SAFETY: no security attributes and no name, so no pointers are
        // passed. Each call returns a new manual-reset event or an error.
        let recv_event = unsafe { CreateEventW(None, true, false, None) };
        // SAFETY: as above.
        let send_event = unsafe { CreateEventW(None, true, false, None) };
        Self {
            recv_event: recv_event.ok(),
            send_event: send_event.ok(),
        }
    }
}

impl Drop for WindowsNotifyHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.recv_event.take() {
            // SAFETY: `handle` came from CreateEventW and `take()` makes
            // this its only close. No RecvNotify copy outlives `self`.
            unsafe {
                let _ = CloseHandle(handle);
            }
        }
        if let Some(handle) = self.send_event.take() {
            // SAFETY: as above.
            unsafe {
                let _ = CloseHandle(handle);
            }
        }
    }
}

impl NotifyHandle for WindowsNotifyHandle {
    fn signal_recv(&self) {
        if let Some(handle) = self.recv_event {
            // SAFETY: `handle` is an open event owned by `self`; SetEvent
            // touches no Rust memory.
            unsafe {
                let _ = SetEvent(handle);
            }
        }
    }

    fn signal_send(&self) {
        if let Some(handle) = self.send_event {
            // SAFETY: as in `signal_recv`.
            unsafe {
                let _ = SetEvent(handle);
            }
        }
    }

    fn close(&self) {}

    fn recv_fd(&self) -> std::os::raw::c_int {
        -1
    }

    fn send_fd(&self) -> std::os::raw::c_int {
        -1
    }

    fn recv_notifier(&self) -> RecvNotify {
        RecvNotify {
            event: self.recv_event.unwrap_or_default(),
        }
    }
}

/// `WaitForMultipleObjects` accepts at most 64 handles per call.
/// Batch 0 blocks with the user's timeout; batches 1+ poll non-blocking.
/// After the blocking wait, every handle is individually checked with
/// `WaitForSingleObject(h, 0)` so all signaled handles are reported.
pub(crate) struct PollWaiter {
    batches: Vec<Vec<HANDLE>>,
    handle_map: Vec<(usize, libc::c_short)>,
}

impl PollWaiter {
    const BATCH_SIZE: usize = 64;

    pub(crate) fn new(items: &[crate::poll::ZmqPollItem]) -> Self {
        let mut batches: Vec<Vec<HANDLE>> = vec![Vec::new()];
        let mut handle_map = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if item.socket.is_null() {
                continue;
            }

            let Some(sock) = crate::poll::item_socket(item) else {
                continue;
            };

            if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0
                && let Some(handle) = sock.notify.recv_event
                && !handle.is_invalid()
            {
                if batches.last().unwrap().len() >= Self::BATCH_SIZE {
                    batches.push(Vec::new());
                }
                batches.last_mut().unwrap().push(handle);
                handle_map.push((idx, crate::poll::ZMQ_POLLIN as libc::c_short));
            }

            if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0
                && let Some(handle) = sock.notify.send_event
                && !handle.is_invalid()
            {
                if batches.last().unwrap().len() >= Self::BATCH_SIZE {
                    batches.push(Vec::new());
                }
                batches.last_mut().unwrap().push(handle);
                handle_map.push((idx, crate::poll::ZMQ_POLLOUT as libc::c_short));
            }
        }

        batches.retain(|b| !b.is_empty());

        Self {
            batches,
            handle_map,
        }
    }

    pub(crate) fn has_no_handles(&self) -> bool {
        self.batches.is_empty()
    }

    #[expect(dead_code)]
    pub(crate) fn handle_count(&self) -> usize {
        self.handle_map.len()
    }

    pub(crate) fn prepare_for_wait(&mut self) {
        for batch in &self.batches {
            for handle in batch {
                // SAFETY: each handle is an open event of a socket in the
                // poll items; the caller keeps those sockets open for the
                // whole poll. ResetEvent touches no Rust memory.
                unsafe {
                    let _ = ResetEvent(*handle);
                }
            }
        }
    }

    pub(crate) fn wait(
        &mut self,
        timeout_ms: libc::c_long,
        items: &mut [crate::poll::ZmqPollItem],
    ) -> std::os::raw::c_int {
        use windows::Win32::Foundation::WAIT_OBJECT_0;
        use windows::Win32::System::Threading::{WaitForMultipleObjects, WaitForSingleObject};

        if self.batches.is_empty() {
            return 0;
        }

        // Block on the first batch with the user's timeout to sleep
        // until at least one event fires (or timeout expires).
        let wait_timeout = if timeout_ms < 0 {
            u32::MAX
        } else {
            timeout_ms as u32
        };
        // SAFETY: the slice is borrowed for the call and holds 1 to 64
        // (MAXIMUM_WAIT_OBJECTS) open events, as in `prepare_for_wait`.
        unsafe {
            WaitForMultipleObjects(&self.batches[0], false, wait_timeout);
        }

        // Scan every handle individually to find all signaled ones.
        for item in items.iter_mut() {
            item.revents = 0;
        }
        let mut ready_count = 0i32;
        let mut map_idx = 0;
        for batch in &self.batches {
            for handle in batch {
                let (item_idx, event_type) = self.handle_map[map_idx];
                map_idx += 1;
                // SAFETY: open event, as in `prepare_for_wait`; a zero
                // timeout only tests the state.
                let rc = unsafe { WaitForSingleObject(*handle, 0) };
                if rc == WAIT_OBJECT_0 {
                    if items[item_idx].revents == 0 {
                        ready_count += 1;
                    }
                    items[item_idx].revents |= event_type;
                }
            }
        }

        ready_count
    }
}
