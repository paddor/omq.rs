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

#[allow(dead_code)]
pub(crate) trait NotifyHandle: Send + Sync {
    fn signal_recv(&self);
    fn signal_send(&self);
    fn close(&self);
    fn recv_fd(&self) -> std::os::raw::c_int;
    fn send_fd(&self) -> std::os::raw::c_int;
    fn recv_notifier(&self) -> RecvNotify;
}

#[cfg(unix)]
mod unix {
    use super::NotifyHandle;
    #[cfg(target_os = "linux")]
    use crate::socket::DEFAULT_HWM;

    #[derive(Clone, Copy)]
    pub(crate) struct RecvNotify {
        poll_fd: std::os::unix::io::RawFd,
        signal_fd: std::os::unix::io::RawFd,
    }

    // SAFETY: RawFd is just an i32 handle; syscalls are thread-safe.
    unsafe impl Send for RecvNotify {}
    unsafe impl Sync for RecvNotify {}

    impl RecvNotify {
        #[inline]
        pub(crate) fn signal(self) {
            if self.signal_fd < 0 {
                return;
            }
            #[cfg(target_os = "linux")]
            {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically increments.
                unsafe {
                    libc::write(self.signal_fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals.
                unsafe {
                    libc::write(self.signal_fd, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        pub(crate) fn drain(self) {
            if self.poll_fd < 0 {
                return;
            }
            #[cfg(target_os = "linux")]
            {
                let mut buf = 0u64;
                // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                unsafe {
                    libc::read(self.poll_fd, (&raw mut buf).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let mut buf = [0u8; 64];
                loop {
                    // SAFETY: fd is a valid pipe read end; draining signal bytes.
                    let n = unsafe {
                        libc::read(
                            self.poll_fd,
                            buf.as_mut_ptr().cast::<libc::c_void>(),
                            buf.len(),
                        )
                    };
                    if n <= 0 {
                        break;
                    }
                }
            }
        }

        pub(crate) fn wait_for_readable(self, timeout_ms: std::os::raw::c_int) -> bool {
            if self.poll_fd < 0 {
                return false;
            }
            let mut pfd = libc::pollfd {
                fd: self.poll_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: pfd is a valid single-element pollfd array.
            let rc = unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) };
            rc > 0
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) struct UnixNotifyHandle(Option<LinuxEventFd>);
    #[cfg(not(target_os = "linux"))]
    pub(crate) struct UnixNotifyHandle(Option<UnixPipeFd>);

    #[allow(dead_code)]
    struct LinuxEventFd {
        recv_fd: std::os::raw::c_int,
        send_fd: std::os::raw::c_int,
    }

    #[allow(dead_code)]
    struct UnixPipeFd {
        recv_read: std::os::raw::c_int,
        recv_write: std::os::raw::c_int,
        send_read: std::os::raw::c_int,
        send_write: std::os::raw::c_int,
    }

    impl UnixNotifyHandle {
        pub(crate) fn new() -> Option<Self> {
            #[cfg(target_os = "linux")]
            {
                Self::new_linux()
            }
            #[cfg(not(target_os = "linux"))]
            {
                Self::new_pipes()
            }
        }

        #[cfg(target_os = "linux")]
        fn new_linux() -> Option<Self> {
            // Non-semaphore: a single read returns the accumulated count
            // and resets to 0. This allows O(1) drain instead of O(N).
            // SAFETY: eventfd returns a valid fd on success, -1 on failure.
            let recv_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
            if recv_fd < 0 {
                return None;
            }
            // SAFETY: same as above.
            let send_fd = unsafe { libc::eventfd(DEFAULT_HWM as u32, libc::EFD_NONBLOCK) };
            if send_fd < 0 {
                // SAFETY: recv_fd is a valid open fd from the successful eventfd call above.
                unsafe { libc::close(recv_fd) };
                return None;
            }
            Some(Self(Some(LinuxEventFd { recv_fd, send_fd })))
        }

        #[cfg(not(target_os = "linux"))]
        fn new_pipes() -> Option<Self> {
            let mut fds = [-1i32; 2];
            // SAFETY: fds is a valid 2-element array; pipe() writes two fds into it on success.
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                return None;
            }
            // SAFETY: fds[0] and fds[1] are valid fds from the successful pipe() call.
            unsafe {
                libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
            }
            let (recv_read, recv_write) = (fds[0], fds[1]);
            // SAFETY: same as above.
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                // SAFETY: recv_read and recv_write are valid open fds.
                unsafe {
                    libc::close(recv_read);
                    libc::close(recv_write);
                }
                return None;
            }
            // SAFETY: fds[0] and fds[1] are valid fds from the successful pipe() call.
            unsafe {
                libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK);
            }
            let (send_read, send_write) = (fds[0], fds[1]);
            Some(Self(Some(UnixPipeFd {
                recv_read,
                recv_write,
                send_read,
                send_write,
            })))
        }
    }

    impl NotifyHandle for UnixNotifyHandle {
        fn signal_recv(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.0 {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically increments the counter.
                unsafe {
                    libc::write(linux.recv_fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.0 {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals readiness.
                unsafe {
                    libc::write(unix.recv_write, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        fn signal_send(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.0 {
                let val: u64 = 1;
                // SAFETY: fd is a valid eventfd; writing 8 bytes atomically decrements the counter.
                unsafe {
                    libc::write(linux.send_fd, (&raw const val).cast::<libc::c_void>(), 8);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.0 {
                let b: u8 = 1;
                // SAFETY: fd is a valid pipe write end; writing 1 byte signals readiness.
                unsafe {
                    libc::write(unix.send_write, (&raw const b).cast::<libc::c_void>(), 1);
                }
            }
        }

        // Takes &self (behind Arc), so fds can't be invalidated after close.
        // No double-close path exists: called once from zmq_close, then dropped.
        fn close(&self) {
            #[cfg(target_os = "linux")]
            if let Some(linux) = &self.0 {
                // SAFETY: recv_fd and send_fd are valid fds opened by new().
                unsafe {
                    libc::close(linux.recv_fd);
                    libc::close(linux.send_fd);
                }
            }
            #[cfg(not(target_os = "linux"))]
            if let Some(unix) = &self.0 {
                // SAFETY: all fds are valid, opened by new().
                unsafe {
                    libc::close(unix.recv_read);
                    libc::close(unix.recv_write);
                    libc::close(unix.send_read);
                    libc::close(unix.send_write);
                }
            }
        }

        fn recv_fd(&self) -> std::os::raw::c_int {
            #[cfg(target_os = "linux")]
            {
                self.0.as_ref().map_or(-1, |l| l.recv_fd)
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.0.as_ref().map_or(-1, |u| u.recv_read)
            }
        }

        fn send_fd(&self) -> std::os::raw::c_int {
            #[cfg(target_os = "linux")]
            {
                self.0.as_ref().map_or(-1, |l| l.send_fd)
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.0.as_ref().map_or(-1, |u| u.send_read)
            }
        }

        fn recv_notifier(&self) -> RecvNotify {
            #[cfg(target_os = "linux")]
            {
                let fd = self.recv_fd();
                RecvNotify {
                    poll_fd: fd,
                    signal_fd: fd,
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.0.as_ref().map_or(
                    RecvNotify {
                        poll_fd: -1,
                        signal_fd: -1,
                    },
                    |u| RecvNotify {
                        poll_fd: u.recv_read,
                        signal_fd: u.recv_write,
                    },
                )
            }
        }
    }

    impl std::fmt::Debug for UnixNotifyHandle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("UnixNotifyHandle { .. }")
        }
    }

    pub(crate) struct PollWaiter {
        pfds: Vec<libc::pollfd>,
        map: Vec<(usize, libc::c_short)>, // (item_idx, zmq_event_type)
    }

    impl PollWaiter {
        pub(crate) fn new(items: &[crate::poll::ZmqPollItem]) -> Self {
            let mut pfds = Vec::new();
            let mut map = Vec::new();

            for (i, item) in items.iter().enumerate() {
                if !item.socket.is_null() {
                    // SAFETY: socket is non-null (checked above);
                    // caller guarantees valid socket.
                    let sock = unsafe {
                        &*(item
                            .socket
                            .cast::<std::sync::Arc<crate::socket::OmqSocket>>())
                    };

                    if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0 {
                        let fd = sock.notify.recv_fd();
                        if fd >= 0 {
                            pfds.push(libc::pollfd {
                                fd,
                                events: libc::POLLIN,
                                revents: 0,
                            });
                            map.push((i, crate::poll::ZMQ_POLLIN as libc::c_short));
                        }
                    }
                    if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0 {
                        let fd = sock.notify.send_fd();
                        if fd >= 0 {
                            pfds.push(libc::pollfd {
                                fd,
                                events: libc::POLLIN,
                                revents: 0,
                            });
                            map.push((i, crate::poll::ZMQ_POLLOUT as libc::c_short));
                        }
                    }
                } else if item.fd >= 0 {
                    let mut events: libc::c_short = 0;
                    if (item.events & crate::poll::ZMQ_POLLIN as libc::c_short) != 0 {
                        events |= libc::POLLIN as libc::c_short;
                    }
                    if (item.events & crate::poll::ZMQ_POLLOUT as libc::c_short) != 0 {
                        events |= libc::POLLOUT as libc::c_short;
                    }
                    if (item.events & crate::poll::ZMQ_POLLERR as libc::c_short) != 0 {
                        events |= libc::POLLERR as libc::c_short;
                    }
                    pfds.push(libc::pollfd {
                        fd: item.fd,
                        events,
                        revents: 0,
                    });
                    map.push((i, 0));
                }
            }

            Self { pfds, map }
        }

        pub(crate) fn has_no_handles(&self) -> bool {
            self.pfds.is_empty()
        }

        pub(crate) fn prepare_for_wait(&mut self) {
            for (pfd_idx, pfd) in self.pfds.iter_mut().enumerate() {
                if pfd.fd < 0 {
                    continue;
                }
                if self.map[pfd_idx].1 == 0 {
                    continue;
                }
                #[cfg(target_os = "linux")]
                {
                    let mut val = 0u64;
                    // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
                    unsafe { libc::read(pfd.fd, (&raw mut val).cast(), 8) };
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let mut buf = [0u8; 64];
                    loop {
                        // SAFETY: fd is a valid pipe read end; draining signal bytes.
                        let n = unsafe {
                            libc::read(pfd.fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len())
                        };
                        if n <= 0 {
                            break;
                        }
                    }
                }
            }
        }

        pub(crate) fn wait(
            &mut self,
            timeout_ms: libc::c_long,
            items: &mut [crate::poll::ZmqPollItem],
        ) -> std::os::raw::c_int {
            if self.pfds.is_empty() {
                return 0;
            }

            let poll_timeout = if timeout_ms < 0 {
                -1
            } else {
                timeout_ms as std::os::raw::c_int
            };

            // SAFETY: pfds is a valid pollfd array; poll blocks until events or timeout.
            let rc = unsafe {
                libc::poll(
                    self.pfds.as_mut_ptr(),
                    self.pfds.len() as libc::nfds_t,
                    poll_timeout,
                )
            };
            if rc < 0 {
                return crate::error::fail(
                    std::io::Error::last_os_error()
                        .raw_os_error()
                        .unwrap_or(libc::EINTR) as std::os::raw::c_int,
                );
            }
            if rc == 0 {
                return 0;
            }

            // Clear revents before accumulating results
            for item in items.iter_mut() {
                item.revents = 0;
            }

            let mut ready_count = 0i32;
            let mut counted = vec![false; items.len()];
            for (pfd_idx, pfd) in self.pfds.iter().enumerate() {
                if pfd.revents == 0 {
                    continue;
                }
                let (item_idx, zmq_event) = self.map[pfd_idx];

                if zmq_event == 0 {
                    // Raw fd item
                    if (pfd.revents & libc::POLLIN as libc::c_short) != 0 {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLIN as libc::c_short;
                    }
                    if (pfd.revents & libc::POLLOUT as libc::c_short) != 0 {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLOUT as libc::c_short;
                    }
                    if (pfd.revents
                        & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as libc::c_short)
                        != 0
                    {
                        items[item_idx].revents |= crate::poll::ZMQ_POLLERR as libc::c_short;
                    }
                } else {
                    // ZMQ socket item
                    items[item_idx].revents |= zmq_event;
                }

                if items[item_idx].revents != 0 && !counted[item_idx] {
                    counted[item_idx] = true;
                    ready_count += 1;
                }
            }

            ready_count
        }
    }
}

#[cfg(windows)]
pub(crate) mod windows {
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
            unsafe {
                let _ = SetEvent(self.event);
            }
        }

        pub(crate) fn drain(self) {
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
            let recv_event = unsafe { CreateEventW(None, true, false, None) };
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
                unsafe {
                    let _ = CloseHandle(handle);
                }
            }
            if let Some(handle) = self.send_event.take() {
                unsafe {
                    let _ = CloseHandle(handle);
                }
            }
        }
    }

    impl NotifyHandle for WindowsNotifyHandle {
        fn signal_recv(&self) {
            if let Some(handle) = self.recv_event {
                unsafe {
                    let _ = SetEvent(handle);
                }
            }
        }

        fn signal_send(&self) {
            if let Some(handle) = self.send_event {
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

                // SAFETY: socket is non-null (checked above); caller guarantees valid socket.
                let sock = unsafe {
                    &*(item
                        .socket
                        .cast::<std::sync::Arc<crate::socket::OmqSocket>>())
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
}

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn create_notify() -> Option<std::sync::Arc<PlatformNotifyHandle>> {
    #[cfg(unix)]
    {
        unix::UnixNotifyHandle::new().map(std::sync::Arc::new)
    }
    #[cfg(windows)]
    {
        Some(std::sync::Arc::new(windows::WindowsNotifyHandle::new()))
    }
}

#[cfg(unix)]
pub(crate) use unix::RecvNotify;

#[cfg(windows)]
pub(crate) use windows::RecvNotify;

#[cfg(unix)]
pub(crate) use unix::PollWaiter;

#[cfg(windows)]
#[allow(unused_imports)]
pub(crate) use windows::PollWaiter;

#[cfg(unix)]
pub(crate) use unix::UnixNotifyHandle as PlatformNotifyHandle;

#[cfg(windows)]
pub(crate) use windows::WindowsNotifyHandle as PlatformNotifyHandle;

pub(crate) fn has_bypass_data(sock: &crate::socket::OmqSocket) -> bool {
    crate::socket::adopt_pending_bypass_recv(sock);
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let bypass_ptr = &*unsafe { sock.bypass_recv.get() };
    bypass_ptr.as_ref().is_some_and(|br| !br.is_empty())
}
