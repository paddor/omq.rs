//! `zmq_poll` -- multiplexed I/O readiness.
//!
//! Three-phase algorithm, repeated until an item is ready or the timeout
//! passes:
//! 1. `check_immediate`: scan yring consumers and send readiness (zero
//!    syscalls).
//! 2. `PollWaiter::wait`: block on OS events (`poll()` on Unix, WFMO on
//!    Windows). A one-shot send wake signals a `POLLOUT` socket's send
//!    eventfd once it may have become writable.
//! 3. `accumulate_buffered`: pick up messages that arrived while blocking and
//!    confirm send readiness.
//!
//! Platform-specific logic lives in `notify.rs`; this file has no `#[cfg]` gates.

use std::ffi::c_int;
use std::sync::Arc;

use crate::consts;
use crate::socket::OmqSocket;

#[cfg(unix)]
pub(crate) type ZmqFd = libc::c_int;
#[cfg(windows)]
pub(crate) type ZmqFd = usize;

pub(crate) const ZMQ_POLLIN: libc::c_short = consts::ZMQ_POLLIN as libc::c_short;
pub(crate) const ZMQ_POLLOUT: libc::c_short = consts::ZMQ_POLLOUT as libc::c_short;
#[allow(dead_code)]
pub(crate) const ZMQ_POLLERR: libc::c_short = consts::ZMQ_POLLERR as libc::c_short;

/// `zmq_pollitem_t` layout compatible with libzmq.
#[repr(C)]
#[derive(Debug)]
pub struct ZmqPollItem {
    pub socket: *mut libc::c_void,
    pub fd: ZmqFd,
    pub events: libc::c_short,
    pub revents: libc::c_short,
}

fn check_immediate(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        item.revents = 0;
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 && recv_ready(sock) {
            item.revents |= ZMQ_POLLIN;
        }
        if (item.events & ZMQ_POLLOUT) != 0 && crate::send_recv::send_ready(sock) {
            item.revents |= ZMQ_POLLOUT;
        }
        if item.revents != 0 {
            ready += 1;
        }
    }
    ready
}

/// Whether a socket has a buffered message or frame to receive.
pub(crate) fn recv_ready(sock: &OmqSocket) -> bool {
    sock.drain_nonempty
        .load(std::sync::atomic::Ordering::Relaxed)
        || sock.recv_has_data()
        || crate::send_recv::authenticated_recv_has_data(sock)
}

/// After a wait: add buffered input, and keep `POLLOUT` only for sockets
/// that are writable. A send eventfd only says readiness may have changed.
fn accumulate_buffered(items: &mut [ZmqPollItem]) {
    for item in items.iter_mut() {
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };
        if (item.events & ZMQ_POLLIN) != 0 && recv_ready(sock) {
            item.revents |= ZMQ_POLLIN;
        }
        if (item.events & ZMQ_POLLOUT) != 0 {
            if crate::send_recv::send_ready(sock) {
                item.revents |= ZMQ_POLLOUT;
            } else {
                item.revents &= !ZMQ_POLLOUT;
            }
        }
    }
}

/// Arm send wakes for `POLLOUT` sockets that are not writable yet.
fn arm_send_wakes(items: &[ZmqPollItem]) -> Vec<crate::send_recv::SendWake> {
    items
        .iter()
        .filter(|item| !item.socket.is_null() && (item.events & ZMQ_POLLOUT) != 0)
        .filter_map(|item| {
            // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
            let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };
            crate::send_recv::arm_send_wake(sock)
        })
        .collect()
}

fn any_socket_terminated(items: &[ZmqPollItem]) -> bool {
    items.iter().any(|item| {
        if item.socket.is_null() {
            return false;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };
        sock.ctx.is_effectively_terminated()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
) -> c_int {
    if nitems < 0 {
        return crate::error::fail(libc::EINVAL);
    }
    if items.is_null() && nitems > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let n = nitems as usize;
    let items_slice = if n == 0 {
        &mut []
    } else {
        // SAFETY: items is non-null (checked above) with nitems elements.
        unsafe { std::slice::from_raw_parts_mut(items, n) }
    };

    if any_socket_terminated(items_slice) {
        return crate::error::fail(crate::error::ETERM);
    }

    let ready = check_immediate(items_slice);
    if ready > 0 || timeout_ms == 0 {
        return ready;
    }

    let deadline = (timeout_ms > 0)
        .then(|| std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64));
    let mut waiter = crate::notify::PollWaiter::new(items_slice);
    if waiter.has_no_handles() {
        if timeout_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
        }
        return 0;
    }

    loop {
        waiter.prepare_for_wait();
        // Armed after draining: a wake that fires from here on stays visible.
        let _send_wakes = arm_send_wakes(items_slice);

        let ready = check_immediate(items_slice);
        if ready > 0 {
            return ready;
        }

        let wait_ms = match deadline {
            None => -1,
            Some(deadline) => {
                let left = deadline.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    return 0;
                }
                // Round up so the final wait reaches the deadline.
                libc::c_long::try_from(left.as_micros().div_ceil(1000)).unwrap_or(libc::c_long::MAX)
            }
        };
        let rc = waiter.wait(wait_ms, items_slice);
        if rc < 0 {
            return rc;
        }
        accumulate_buffered(items_slice);
        let ready = items_slice.iter().filter(|item| item.revents != 0).count();
        if ready > 0 {
            return c_int::try_from(ready).unwrap_or(c_int::MAX);
        }
        if any_socket_terminated(items_slice) {
            return crate::error::fail(crate::error::ETERM);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ppoll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
    sigmask: *const libc::c_void,
) -> c_int {
    if !sigmask.is_null() {
        return crate::error::fail(crate::error::ENOTSUP);
    }
    zmq_poll(items, nitems, timeout_ms)
}
