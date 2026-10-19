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
    /// OMQ socket handle, or null to poll a native descriptor.
    pub socket: *mut libc::c_void,
    /// Native descriptor used when `socket` is null.
    pub fd: ZmqFd,
    /// Requested readiness mask.
    pub events: libc::c_short,
    /// Observed readiness mask written by the poll operation.
    pub revents: libc::c_short,
}

fn check_immediate(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        item.revents = 0;
        let Some(sock) = item_socket(item) else {
            continue;
        };

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
        let Some(sock) = item_socket(item) else {
            continue;
        };
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
        .filter(|item| (item.events & ZMQ_POLLOUT) != 0)
        .filter_map(|item| crate::send_recv::arm_send_wake(item_socket(item)?))
        .collect()
}

fn any_socket_terminated(items: &[ZmqPollItem]) -> bool {
    items
        .iter()
        .filter_map(item_socket)
        .any(|sock| sock.ctx.is_effectively_terminated())
}

/// The socket of a socket item. `zmq_poll` validates every handle first.
pub(crate) fn item_socket<'a>(item: &ZmqPollItem) -> Option<&'a Arc<OmqSocket>> {
    // SAFETY: C callers pass null or a pointer from this API.
    unsafe { crate::handle::socket(item.socket) }.ok()
}

/// Wait for socket or descriptor events, with a millisecond timeout; negative means
/// indefinite.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poll(
    items: *mut ZmqPollItem,
    nitems: c_int,
    timeout_ms: libc::c_long,
) -> c_int {
    let items_slice = match poll_items(items, nitems) {
        Ok(items) => items,
        Err(errno) => return crate::error::fail(errno),
    };

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

        let Some(wait_ms) = wait_budget_ms(deadline) else {
            return 0;
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

/// Borrow the caller's poll items after validating count, pointer, socket
/// handles, and context state, in libzmq's errno order.
fn poll_items<'a>(items: *mut ZmqPollItem, nitems: c_int) -> Result<&'a mut [ZmqPollItem], c_int> {
    if nitems < 0 {
        return Err(libc::EINVAL);
    }
    if items.is_null() && nitems > 0 {
        return Err(libc::EFAULT);
    }

    let n = nitems as usize;
    let items_slice = if n == 0 {
        &mut []
    } else {
        // SAFETY: items is non-null (checked above) with nitems elements.
        unsafe { std::slice::from_raw_parts_mut(items, n) }
    };

    if items_slice
        .iter()
        .any(|item| !item.socket.is_null() && item_socket(item).is_none())
    {
        return Err(crate::error::ENOTSOCK);
    }
    if any_socket_terminated(items_slice) {
        return Err(crate::error::ETERM);
    }
    Ok(items_slice)
}

/// Milliseconds for the next OS wait: -1 without a deadline, `None` once it passed.
fn wait_budget_ms(deadline: Option<std::time::Instant>) -> Option<libc::c_long> {
    let Some(deadline) = deadline else {
        return Some(-1);
    };

    let left = deadline.saturating_duration_since(std::time::Instant::now());
    if left.is_zero() {
        return None;
    }
    // Round up so the final wait reaches the deadline.
    Some(libc::c_long::try_from(left.as_micros().div_ceil(1000)).unwrap_or(libc::c_long::MAX))
}

/// Poll with a millisecond timeout; non-null signal masks are unsupported and return ENOTSUP.
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
