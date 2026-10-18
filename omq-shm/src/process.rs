//! Owned process descriptors and shared futex waits through safe OS wrappers.
#![forbid(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::process::PidfdFlags;
use rustix::thread::futex;

use crate::mapping::invalid;

#[derive(Debug)]
pub(crate) struct Process {
    pidfd: Option<File>,
    dead: AtomicBool,
    disconnected: AtomicBool,
}

impl Process {
    pub(crate) fn peer(socket: impl AsFd) -> io::Result<Self> {
        let credentials = rustix::net::sockopt::socket_peercred(socket)?;
        if credentials.uid != rustix::process::geteuid() {
            return Err(invalid(
                "SHM requires peers with the same UID and visible PID namespace",
            ));
        }
        // PID reuse can only delay reclamation: the original process must
        // have exited before its PID can name a replacement.
        let (pidfd, dead) = match rustix::process::pidfd_open(credentials.pid, PidfdFlags::empty())
        {
            Ok(fd) => (Some(File::from(fd)), false),
            Err(rustix::io::Errno::SRCH) => (None, true),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            pidfd,
            dead: AtomicBool::new(dead),
            disconnected: AtomicBool::new(false),
        })
    }

    #[cfg(all(test, miri))]
    pub(crate) fn testing() -> Self {
        Self {
            pidfd: None,
            dead: AtomicBool::new(false),
            disconnected: AtomicBool::new(false),
        }
    }

    pub(crate) fn dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
            || (self.disconnected.load(Ordering::Acquire) && self.observe_exit())
    }

    pub(crate) fn observe_exit(&self) -> bool {
        if self.dead.load(Ordering::Acquire) {
            return true;
        }
        let Some(file) = &self.pidfd else {
            return false;
        };
        let mut descriptors = [PollFd::new(file, PollFlags::IN)];
        if rustix::event::poll(
            &mut descriptors,
            Some(&Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
        )
        .is_ok_and(|ready| ready != 0 && descriptors[0].revents().contains(PollFlags::IN))
        {
            self.dead.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub(crate) fn descriptor(&self) -> io::Result<Option<File>> {
        self.pidfd.as_ref().map(File::try_clone).transpose()
    }

    pub(crate) fn disconnect(&self) {
        self.disconnected.store(true, Ordering::Release);
    }
}

/// The peer can write `word`, so a wait never trusts it to bring a wake.
/// Each wait is bounded; callers recheck local state after it returns.
pub(crate) fn futex_wait(word: &AtomicU32, expected: u32) {
    const SLICE: Timespec = Timespec {
        tv_sec: 0,
        tv_nsec: 100_000_000,
    };
    let _ = futex::wait(word, futex::Flags::empty(), expected, Some(&SLICE));
}

pub(crate) fn futex_wake(word: &AtomicU32) {
    let _ = futex::wake(word, futex::Flags::empty(), i32::MAX as u32);
}
