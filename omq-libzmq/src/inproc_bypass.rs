//! Lock-free inproc bypass: connects `zmq_send` and `zmq_recv` directly
//! via a SPSC ring, completely bypassing the io thread for eligible
//! socket types (PUSH/PULL).
//!
//! The bypass is installed when both sides of an inproc connection
//! are present (bind + connect, either order). The sender pushes
//! `Message` into the ring from its C thread; the receiver pops from
//! its C thread. Zero channel crossings, zero io thread involvement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::socket::NotifyFd;

/// Shared state between the sender and receiver halves of an inproc bypass.
pub(crate) struct InprocPipe {
    pub(crate) closed: AtomicBool,
    /// Write end: signaled by the sender when the pipe transitions from
    /// empty to non-empty (eventfd on Linux, pipe write end on macOS).
    pub(crate) recv_signal_fd: std::os::unix::io::RawFd,
    /// Read end: drained by the receiver when the ring becomes empty
    /// (same as `recv_signal_fd` on Linux, pipe read end on macOS).
    pub(crate) recv_drain_fd: std::os::unix::io::RawFd,
}

impl std::fmt::Debug for InprocPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InprocPipe")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Sender half installed on the PUSH socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassSend {
    pub(crate) producer: yring::Producer<omq_compio::Message>,
    pub(crate) pipe: Arc<InprocPipe>,
}

/// Receiver half installed on the PULL socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassRecv {
    pub(crate) consumer: yring::Consumer<omq_compio::Message>,
    pipe: Arc<InprocPipe>,
}

/// Create a bypass pair for an inproc PUSH/PULL connection.
pub(crate) fn create_bypass(
    capacity: usize,
    recv_signal_fd: std::os::unix::io::RawFd,
    recv_drain_fd: std::os::unix::io::RawFd,
) -> (BypassSend, BypassRecv) {
    let (producer, consumer) = yring::spsc(capacity);
    let pipe = Arc::new(InprocPipe {
        closed: AtomicBool::new(false),
        recv_signal_fd,
        recv_drain_fd,
    });
    (
        BypassSend {
            producer,
            pipe: pipe.clone(),
        },
        BypassRecv { consumer, pipe },
    )
}

impl BypassSend {
    /// Push + flush a message. Returns Err(msg) if full.
    /// Signals the receiver's eventfd if the ring was empty before flush.
    #[inline]
    pub(crate) fn push(&mut self, msg: omq_compio::Message) -> Result<(), omq_compio::Message> {
        self.producer.push(msg)?;
        if let yring::FlushResult::Flushed {
            was_empty: true, ..
        } = self.producer.flush()
        {
            NotifyFd::signal_recv(self.pipe.recv_signal_fd);
        }
        Ok(())
    }
}

impl BypassRecv {
    /// Prefetch + pop a message. Returns None if empty.
    /// Drains the recv eventfd when the ring becomes empty so poll
    /// sees the fd as not-readable after all messages are consumed.
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<omq_compio::Message> {
        let msg = self.consumer.prefetch_and_pop();
        if msg.is_some() && self.consumer.is_empty() {
            drain_recv_fd(self.pipe.recv_drain_fd);
        }
        msg
    }
}

#[cfg(target_os = "linux")]
fn drain_recv_fd(fd: std::os::unix::io::RawFd) {
    let mut buf = 0u64;
    unsafe {
        libc::read(fd, (&raw mut buf).cast::<libc::c_void>(), 8);
    }
}

#[cfg(not(target_os = "linux"))]
fn drain_recv_fd(fd: std::os::unix::io::RawFd) {
    let mut buf = [0u8; 64];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };
        if n <= 0 {
            break;
        }
    }
}
