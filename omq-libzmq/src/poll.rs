//! `zmq_poll` -- multiplexed I/O readiness.
//!
//! Three-phase algorithm:
//! 1. `check_immediate`: scan yring consumers and bypass rings (zero syscalls).
//! 2. `PollWaiter::wait`: block on OS events (`poll()` on Unix, WFMO on Windows).
//! 3. `accumulate_buffered`: pick up messages that arrived while blocking.
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

        if (item.events & ZMQ_POLLIN) != 0 {
            crate::socket::adopt_pending_bypass_recv(sock);
            let drain_nonempty = sock
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);
            // SAFETY: libzmq sockets are accessed by at most one application thread.
            let recv_cons_has_data = unsafe { sock.recv_cons.get() }
                .as_ref()
                .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty());
            // SAFETY: same socket-thread invariant as above.
            let bypass_recv_has_data = unsafe { sock.bypass_recv.get() }
                .as_ref()
                .is_some_and(|br| !br.is_empty());
            let authenticated_recv_has_data = crate::send_recv::authenticated_recv_has_data(sock);
            let has_buffered = drain_nonempty
                || recv_cons_has_data
                || bypass_recv_has_data
                || authenticated_recv_has_data;
            if has_buffered {
                item.revents |= ZMQ_POLLIN;
            }
        }
        if (item.events & ZMQ_POLLOUT) != 0 {
            item.revents |= ZMQ_POLLOUT;
        }
        if item.revents != 0 {
            ready += 1;
        }
    }
    ready
}

fn accumulate_buffered(items: &mut [ZmqPollItem]) -> i32 {
    let mut ready = 0i32;
    for item in items.iter_mut() {
        if item.socket.is_null() {
            continue;
        }
        // SAFETY: socket is non-null (checked above); caller guarantees a valid socket.
        let sock = unsafe { &*(item.socket.cast::<Arc<OmqSocket>>()) };

        if (item.events & ZMQ_POLLIN) != 0 {
            crate::socket::adopt_pending_bypass_recv(sock);
            let drain_nonempty = sock
                .drain_nonempty
                .load(std::sync::atomic::Ordering::Relaxed);

            // SAFETY: libzmq sockets are accessed by at most one application thread.
            let cons_ptr = &*unsafe { sock.recv_cons.get() };
            let recv_cons_has_data = cons_ptr
                .as_ref()
                .is_some_and(|c| !c.fast.is_empty() || !c.pump.is_empty());

            let bypass_recv_has_data = crate::notify::has_bypass_data(sock);
            let authenticated_recv_has_data = crate::send_recv::authenticated_recv_has_data(sock);

            let has_buffered = drain_nonempty
                || recv_cons_has_data
                || bypass_recv_has_data
                || authenticated_recv_has_data;

            if has_buffered {
                if item.revents == 0 {
                    ready += 1;
                }
                item.revents |= ZMQ_POLLIN;
            }
        }
    }
    ready
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

    let mut waiter = crate::notify::PollWaiter::new(items_slice);
    if waiter.has_no_handles() {
        if timeout_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
        }
        return 0;
    }

    waiter.prepare_for_wait();

    let ready = check_immediate(items_slice);
    if ready > 0 {
        return ready;
    }

    let _rc = waiter.wait(timeout_ms, items_slice);
    let _buffered = accumulate_buffered(items_slice);

    let mut ready = 0i32;
    for item in items_slice {
        if item.revents != 0 {
            ready += 1;
        }
    }
    ready
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
