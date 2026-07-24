use std::time::Duration;

pub(crate) struct EventFdSignal {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    efd: i32,
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    read_fd: i32,
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    write_fd: i32,
}

impl EventFdSignal {
    pub(crate) fn new() -> Self {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            assert!(efd >= 0, "eventfd creation failed");
            Self { efd }
        }

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            let mut fds = [0; 2];
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            assert!(rc == 0, "pipe creation failed");
            for fd in fds {
                set_nonblocking(fd);
                set_cloexec(fd);
            }
            Self {
                read_fd: fds[0],
                write_fd: fds[1],
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn read_fd(&self) -> i32 {
        self.efd
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn read_fd(&self) -> i32 {
        self.read_fd
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn write_signal(&self) {
        let val: u64 = 1;
        while unsafe { libc::write(self.efd, &val as *const u64 as *const libc::c_void, 8) } < 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn write_signal(&self) {
        let val: u8 = 1;
        while unsafe { libc::write(self.write_fd, &val as *const u8 as *const libc::c_void, 1) } < 0
        {
            let err = std::io::Error::last_os_error().raw_os_error();
            if err != Some(libc::EINTR) {
                break;
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn drain(&self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(self.efd, &mut val as *mut u64 as *mut libc::c_void, 8);
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn drain(&self) {
        let mut buf = [0u8; 64];
        loop {
            let n = unsafe {
                libc::read(
                    self.read_fd,
                    buf.as_mut_ptr().cast::<libc::c_void>(),
                    buf.len(),
                )
            };
            if n > 0 {
                continue;
            }
            let err = std::io::Error::last_os_error().raw_os_error();
            if n < 0 && err == Some(libc::EINTR) {
                continue;
            }
            break;
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn close(&self) {
        unsafe { libc::close(self.efd) };
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn close(&self) {
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn dup_read_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let fd = unsafe { libc::dup(self.efd) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn dup_read_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let fd = unsafe { libc::dup(self.read_fd) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    pub(crate) fn signal(&self, parked: bool) {
        if parked {
            self.write_signal();
        }
    }

    pub(crate) fn force_wake(&self) {
        self.write_signal();
    }

    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.read_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let ret = unsafe { libc::poll(&mut pfd, 1, ms) };
        if ret > 0 {
            self.drain();
            true
        } else {
            false
        }
    }

    pub(crate) fn fd(&self) -> i32 {
        self.read_fd()
    }

    pub(crate) fn dup_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        self.dup_read_fd()
    }
}

impl Drop for EventFdSignal {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_nonblocking(fd: i32) {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0, "fcntl(F_GETFL) failed");
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    assert!(rc == 0, "fcntl(F_SETFL) failed");
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_cloexec(fd: i32) {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0, "fcntl(F_GETFD) failed");
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    assert!(rc == 0, "fcntl(F_SETFD) failed");
}
