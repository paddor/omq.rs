//! Unix notification fds: an eventfd per direction on Linux, a nonblocking
//! pipe per direction elsewhere.

use super::NotifyHandle;
#[cfg(target_os = "linux")]
use crate::socket::DEFAULT_HWM;
use std::os::raw::c_int;

/// Signal a notification fd: add one to an eventfd, or write one pipe byte.
#[inline]
fn signal_fd(fd: c_int) {
    #[cfg(target_os = "linux")]
    {
        let val: u64 = 1;
        // SAFETY: fd is a valid eventfd; writing 8 bytes atomically increments.
        unsafe {
            libc::write(fd, (&raw const val).cast::<libc::c_void>(), 8);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let b: u8 = 1;
        // SAFETY: fd is a valid pipe write end; writing 1 byte signals.
        unsafe {
            libc::write(fd, (&raw const b).cast::<libc::c_void>(), 1);
        }
    }
}

/// Reset a notification fd: read the eventfd counter, or drain the pipe.
fn drain_fd(fd: c_int) {
    #[cfg(target_os = "linux")]
    {
        let mut buf = 0u64;
        // SAFETY: fd is a valid eventfd; 8-byte read drains the counter.
        unsafe {
            libc::read(fd, (&raw mut buf).cast::<libc::c_void>(), 8);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: fd is a valid pipe read end; draining signal bytes.
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };
            if n <= 0 {
                break;
            }
        }
    }
}

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

        signal_fd(self.signal_fd);
    }

    pub(crate) fn drain(self) {
        if self.poll_fd < 0 {
            return;
        }

        drain_fd(self.poll_fd);
    }

    pub(crate) fn wait_for_readable(self, timeout_ms: c_int) -> bool {
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

/// Readable (poll) and writable (signal) ends per direction. On Linux one
/// eventfd serves as both ends of a direction.
struct NotifyFds {
    recv_poll: c_int,
    recv_signal: c_int,
    send_poll: c_int,
    send_signal: c_int,
}

impl NotifyFds {
    #[cfg(target_os = "linux")]
    fn open() -> Option<Self> {
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

        Some(Self {
            recv_poll: recv_fd,
            recv_signal: recv_fd,
            send_poll: send_fd,
            send_signal: send_fd,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn open() -> Option<Self> {
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

        Some(Self {
            recv_poll: recv_read,
            recv_signal: recv_write,
            send_poll: fds[0],
            send_signal: fds[1],
        })
    }
}

impl Drop for NotifyFds {
    fn drop(&mut self) {
        let directions = [
            (self.recv_poll, self.recv_signal),
            (self.send_poll, self.send_signal),
        ];

        for (poll, signal) in directions {
            // SAFETY: the fds were opened by `open` and are closed only here.
            unsafe { libc::close(poll) };
            if signal != poll {
                // SAFETY: as above; a shared eventfd was closed once already.
                unsafe { libc::close(signal) };
            }
        }
    }
}

pub(crate) struct UnixNotifyHandle(NotifyFds);

impl UnixNotifyHandle {
    pub(crate) fn new() -> Option<Self> {
        NotifyFds::open().map(Self)
    }
}

impl NotifyHandle for UnixNotifyHandle {
    #[inline]
    fn signal_recv(&self) {
        self.recv_notifier().signal();
    }

    fn signal_send(&self) {
        signal_fd(self.0.send_signal);
    }

    // Tasks and receive callbacks may still signal after socket close.
    // Their Arc owners keep the descriptors open until the final drop.
    fn close(&self) {}

    fn recv_fd(&self) -> c_int {
        self.0.recv_poll
    }

    fn send_fd(&self) -> c_int {
        self.0.send_poll
    }

    #[inline]
    fn recv_notifier(&self) -> RecvNotify {
        RecvNotify {
            poll_fd: self.0.recv_poll,
            signal_fd: self.0.recv_signal,
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
                let Some(sock) = crate::poll::item_socket(item) else {
                    continue;
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

            drain_fd(pfd.fd);
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

                if (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as libc::c_short)
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
