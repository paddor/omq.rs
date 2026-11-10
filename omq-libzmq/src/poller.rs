//! `zmq_poller_*` draft polling API.

use std::ffi::{c_int, c_short, c_void};

use crate::error::{c_try, fail};
use crate::poll::{ZmqFd, ZmqPollItem, invalid_fd};

#[derive(Clone, Copy, Debug)]
enum PollerTarget {
    Socket(*mut c_void),
    Fd(ZmqFd),
}

#[derive(Clone, Copy, Debug)]
struct PollerEntry {
    target: PollerTarget,
    user_data: *mut c_void,
    events: c_short,
}

#[derive(Debug, Default)]
struct ZmqPoller {
    entries: Vec<PollerEntry>,
}

#[repr(C)]
#[derive(Debug)]
pub struct ZmqPollerEvent {
    pub socket: *mut c_void,
    pub fd: ZmqFd,
    pub user_data: *mut c_void,
    pub events: c_short,
}

fn fd_is_invalid(fd: ZmqFd) -> bool {
    #[cfg(unix)]
    {
        fd < 0
    }

    #[cfg(windows)]
    {
        fd == ZmqFd::MAX
    }
}

unsafe fn poller<'a>(ptr: *mut c_void) -> Result<&'a mut ZmqPoller, c_int> {
    if ptr.is_null() {
        return Err(libc::EFAULT);
    }

    // SAFETY: caller guarantees ptr is a valid poller from zmq_poller_new.
    Ok(unsafe { &mut *ptr.cast::<ZmqPoller>() })
}

fn socket_entry(entries: &[PollerEntry], socket: *mut c_void) -> Option<usize> {
    entries
        .iter()
        .position(|entry| matches!(entry.target, PollerTarget::Socket(s) if s == socket))
}

fn fd_entry(entries: &[PollerEntry], fd: ZmqFd) -> Option<usize> {
    entries
        .iter()
        .position(|entry| matches!(entry.target, PollerTarget::Fd(f) if f == fd))
}

/// Create an empty poller handle; release it with `zmq_poller_destroy`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_new() -> *mut c_void {
    Box::into_raw(Box::<ZmqPoller>::default()).cast()
}

/// Release the poller and clear the caller's handle pointer; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_destroy(poller_p: *mut *mut c_void) -> c_int {
    if poller_p.is_null() {
        return fail(libc::EFAULT);
    }

    // SAFETY: poller_p is non-null (checked above).
    let ptr = unsafe { *poller_p };
    if ptr.is_null() {
        return fail(libc::EFAULT);
    }

    // SAFETY: ptr came from Box::into_raw in zmq_poller_new.
    unsafe {
        drop(Box::from_raw(ptr.cast::<ZmqPoller>()));
        *poller_p = std::ptr::null_mut();
    }

    0
}

/// Return the number of registered poller items, or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_size(poller_ptr: *mut c_void) -> c_int {
    match unsafe { poller(poller_ptr) } {
        Ok(p) => c_int::try_from(p.entries.len()).unwrap_or(c_int::MAX),
        Err(e) => fail(e),
    }
}

/// Register a socket with event interests and opaque user data; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_add(
    poller_ptr: *mut c_void,
    socket: *mut c_void,
    user_data: *mut c_void,
    events: c_short,
) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }

    let p = c_try!(unsafe { poller(poller_ptr) });
    if socket_entry(&p.entries, socket).is_some() {
        return fail(libc::EINVAL);
    }

    p.entries.push(PollerEntry {
        target: PollerTarget::Socket(socket),
        user_data,
        events,
    });

    0
}

/// Replace a registered socket's event interests; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_modify(
    poller_ptr: *mut c_void,
    socket: *mut c_void,
    events: c_short,
) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }

    let p = c_try!(unsafe { poller(poller_ptr) });
    let Some(idx) = socket_entry(&p.entries, socket) else {
        return fail(libc::EINVAL);
    };

    p.entries[idx].events = events;
    0
}

/// Remove a registered socket; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_remove(poller_ptr: *mut c_void, socket: *mut c_void) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }

    let p = c_try!(unsafe { poller(poller_ptr) });
    let Some(idx) = socket_entry(&p.entries, socket) else {
        return fail(libc::EINVAL);
    };

    p.entries.swap_remove(idx);
    0
}

/// Register a native descriptor with event interests and user data; return zero or -1 with
/// errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_add_fd(
    poller_ptr: *mut c_void,
    fd: ZmqFd,
    user_data: *mut c_void,
    events: c_short,
) -> c_int {
    if fd_is_invalid(fd) {
        return fail(libc::EBADF);
    }

    let p = c_try!(unsafe { poller(poller_ptr) });
    if fd_entry(&p.entries, fd).is_some() {
        return fail(libc::EINVAL);
    }

    p.entries.push(PollerEntry {
        target: PollerTarget::Fd(fd),
        user_data,
        events,
    });

    0
}

/// Replace a registered descriptor's event interests; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_modify_fd(
    poller_ptr: *mut c_void,
    fd: ZmqFd,
    events: c_short,
) -> c_int {
    let p = c_try!(unsafe { poller(poller_ptr) });
    let Some(idx) = fd_entry(&p.entries, fd) else {
        return fail(libc::EINVAL);
    };

    p.entries[idx].events = events;
    0
}

/// Remove a registered native descriptor; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_remove_fd(poller_ptr: *mut c_void, fd: ZmqFd) -> c_int {
    let p = c_try!(unsafe { poller(poller_ptr) });
    let Some(idx) = fd_entry(&p.entries, fd) else {
        return fail(libc::EINVAL);
    };

    p.entries.swap_remove(idx);
    0
}

/// Wait for one event using a millisecond timeout; return one or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_wait(
    poller_ptr: *mut c_void,
    event: *mut ZmqPollerEvent,
    timeout: libc::c_long,
) -> c_int {
    zmq_poller_wait_all(poller_ptr, event, 1, timeout)
}

/// Wait for at most `n_events` events using a millisecond timeout; return their count or -1
/// with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_wait_all(
    poller_ptr: *mut c_void,
    events_out: *mut ZmqPollerEvent,
    n_events: c_int,
    timeout: libc::c_long,
) -> c_int {
    if events_out.is_null() || n_events <= 0 {
        return fail(libc::EFAULT);
    }

    let p = c_try!(unsafe { poller(poller_ptr) });

    // `map[i]` is the entry index behind `poll_items[i]`; entries with no
    // interests are skipped.
    let mut poll_items = Vec::new();
    let mut map = Vec::new();

    for (idx, entry) in p.entries.iter().enumerate() {
        if entry.events == 0 {
            continue;
        }

        poll_items.push(entry.poll_item());
        map.push(idx);
    }

    if poll_items.is_empty() {
        return fail(libc::EAGAIN);
    }

    let rc = crate::poll::zmq_poll(
        poll_items.as_mut_ptr(),
        c_int::try_from(poll_items.len()).unwrap_or(c_int::MAX),
        timeout,
    );

    if rc < 0 {
        return -1;
    }

    if rc == 0 {
        return fail(libc::EAGAIN);
    }

    let mut written = 0usize;
    for (poll_idx, item) in poll_items.iter().enumerate() {
        if item.revents == 0 {
            continue;
        }

        if written >= n_events as usize {
            break;
        }

        let out = p.entries[map[poll_idx]].event(item.revents);
        // SAFETY: events_out points to at least n_events entries (caller contract).
        unsafe { events_out.add(written).write(out) };
        written += 1;
    }

    c_int::try_from(written).unwrap_or(c_int::MAX)
}

impl PollerEntry {
    /// `zmq_poll` item for this entry; exactly one of socket and fd is set.
    fn poll_item(&self) -> ZmqPollItem {
        match self.target {
            PollerTarget::Socket(socket) => ZmqPollItem {
                socket,
                fd: invalid_fd(),
                events: self.events,
                revents: 0,
            },
            PollerTarget::Fd(fd) => ZmqPollItem {
                socket: std::ptr::null_mut(),
                fd,
                events: self.events,
                revents: 0,
            },
        }
    }

    /// Caller-visible event for this entry with the observed `revents`.
    fn event(&self, revents: c_short) -> ZmqPollerEvent {
        ZmqPollerEvent {
            socket: match self.target {
                PollerTarget::Socket(socket) => socket,
                PollerTarget::Fd(_) => std::ptr::null_mut(),
            },
            fd: match self.target {
                PollerTarget::Socket(_) => invalid_fd(),
                PollerTarget::Fd(fd) => fd,
            },
            user_data: self.user_data,
            events: revents,
        }
    }
}

/// Unsupported poller descriptor query; return -1 with EINVAL for valid arguments.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_fd(poller_ptr: *mut c_void, fd: *mut ZmqFd) -> c_int {
    if fd.is_null() {
        return fail(libc::EFAULT);
    }

    if unsafe { poller(poller_ptr) }.is_err() {
        return fail(libc::EFAULT);
    }

    fail(libc::EINVAL)
}

#[cfg(test)]
mod tests;
