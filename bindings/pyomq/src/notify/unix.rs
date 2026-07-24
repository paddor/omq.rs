use std::time::Duration;

pub(crate) struct EventFdSignal {
    efd: i32,
}

impl EventFdSignal {
    pub(crate) fn new() -> Self {
        let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(efd >= 0, "eventfd creation failed");
        Self { efd }
    }

    pub(crate) fn signal(&self, parked: bool) {
        if parked {
            self.write_eventfd();
        }
    }

    pub(crate) fn force_wake(&self) {
        self.write_eventfd();
    }

    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.efd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let ret = unsafe { libc::poll(&mut pfd, 1, ms) };
        if ret > 0 {
            let mut val: u64 = 0;
            unsafe {
                libc::read(self.efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn fd(&self) -> i32 {
        self.efd
    }

    pub(crate) fn dup_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let fd = unsafe { libc::dup(self.efd) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn write_eventfd(&self) {
        let val: u64 = 1;
        while unsafe { libc::write(self.efd, &val as *const u64 as *const libc::c_void, 8) } < 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}

impl Drop for EventFdSignal {
    fn drop(&mut self) {
        unsafe { libc::close(self.efd) };
    }
}
