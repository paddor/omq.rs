use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct PipeNotify {
    read_fd: RawFd,
    write_fd: RawFd,
    parking: AtomicBool,
}

unsafe impl Send for PipeNotify {}
unsafe impl Sync for PipeNotify {}

impl PipeNotify {
    pub fn new() -> Self {
        let mut fds = [0i32; 2];
        let ret = create_pipe(&mut fds);
        assert!(
            ret == 0,
            "pipe2 failed: {}",
            std::io::Error::last_os_error()
        );
        Self {
            read_fd: fds[0],
            write_fd: fds[1],
            parking: AtomicBool::new(false),
        }
    }

    pub fn notify(&self) {
        if self.parking.swap(false, Ordering::AcqRel) {
            self.write_byte();
        }
    }

    pub fn force_wake(&self) {
        self.write_byte();
    }

    pub fn read_fd(&self) -> RawFd {
        self.read_fd
    }

    pub fn park_begin(&self) {
        self.parking.store(true, Ordering::Release);
    }

    pub fn cancel_park(&self) {
        self.parking.store(false, Ordering::Release);
    }

    pub fn clear(&self) {
        let mut buf = [0u8; 64];
        loop {
            let ret = unsafe {
                libc::read(
                    self.read_fd,
                    buf.as_mut_ptr().cast::<libc::c_void>(),
                    buf.len(),
                )
            };
            if ret > 0 {
                continue;
            }
            if ret < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            break;
        }
    }

    fn write_byte(&self) {
        let val: u8 = 1;
        loop {
            let ret =
                unsafe { libc::write(self.write_fd, (&raw const val).cast::<libc::c_void>(), 1) };
            if ret >= 0 {
                break;
            }
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn create_pipe(fds: &mut [RawFd; 2]) -> i32 {
    unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn create_pipe(fds: &mut [RawFd; 2]) -> i32 {
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if ret != 0 {
        return ret;
    }

    for fd in *fds {
        unsafe {
            libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    0
}

impl Drop for PipeNotify {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }
}
