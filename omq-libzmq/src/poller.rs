//! `zmq_poller_*` draft polling API.

use std::ffi::{c_int, c_short, c_void};

use crate::error::fail;
use crate::poll::{ZmqFd, ZmqPollItem};

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

fn invalid_fd() -> ZmqFd {
    #[cfg(unix)]
    {
        -1
    }
    #[cfg(windows)]
    {
        ZmqFd::MAX
    }
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

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_new() -> *mut c_void {
    Box::into_raw(Box::<ZmqPoller>::default()).cast()
}

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

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_size(poller_ptr: *mut c_void) -> c_int {
    match unsafe { poller(poller_ptr) } {
        Ok(p) => c_int::try_from(p.entries.len()).unwrap_or(c_int::MAX),
        Err(e) => fail(e),
    }
}

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
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
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

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_modify(
    poller_ptr: *mut c_void,
    socket: *mut c_void,
    events: c_short,
) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let Some(idx) = socket_entry(&p.entries, socket) else {
        return fail(libc::EINVAL);
    };
    p.entries[idx].events = events;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_remove(poller_ptr: *mut c_void, socket: *mut c_void) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let Some(idx) = socket_entry(&p.entries, socket) else {
        return fail(libc::EINVAL);
    };
    p.entries.swap_remove(idx);
    0
}

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
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
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

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_modify_fd(
    poller_ptr: *mut c_void,
    fd: ZmqFd,
    events: c_short,
) -> c_int {
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let Some(idx) = fd_entry(&p.entries, fd) else {
        return fail(libc::EINVAL);
    };
    p.entries[idx].events = events;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_remove_fd(poller_ptr: *mut c_void, fd: ZmqFd) -> c_int {
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let Some(idx) = fd_entry(&p.entries, fd) else {
        return fail(libc::EINVAL);
    };
    p.entries.swap_remove(idx);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_poller_wait(
    poller_ptr: *mut c_void,
    event: *mut ZmqPollerEvent,
    timeout: libc::c_long,
) -> c_int {
    zmq_poller_wait_all(poller_ptr, event, 1, timeout)
}

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
    let p = match unsafe { poller(poller_ptr) } {
        Ok(p) => p,
        Err(e) => return fail(e),
    };

    let mut poll_items = Vec::new();
    let mut map = Vec::new();
    for (idx, entry) in p.entries.iter().enumerate() {
        if entry.events == 0 {
            continue;
        }
        match entry.target {
            PollerTarget::Socket(socket) => {
                poll_items.push(ZmqPollItem {
                    socket,
                    fd: invalid_fd(),
                    events: entry.events,
                    revents: 0,
                });
            }
            PollerTarget::Fd(fd) => {
                poll_items.push(ZmqPollItem {
                    socket: std::ptr::null_mut(),
                    fd,
                    events: entry.events,
                    revents: 0,
                });
            }
        }
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
        let entry = p.entries[map[poll_idx]];
        let out = ZmqPollerEvent {
            socket: match entry.target {
                PollerTarget::Socket(socket) => socket,
                PollerTarget::Fd(_) => std::ptr::null_mut(),
            },
            fd: match entry.target {
                PollerTarget::Socket(_) => invalid_fd(),
                PollerTarget::Fd(fd) => fd,
            },
            user_data: entry.user_data,
            events: item.revents,
        };
        // SAFETY: events_out points to at least n_events entries (caller contract).
        unsafe { events_out.add(written).write(out) };
        written += 1;
    }

    c_int::try_from(written).unwrap_or(c_int::MAX)
}

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
mod tests {
    use std::ffi::CString;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::poll::ZMQ_POLLIN;

    const ZMQ_PUSH: c_int = 8;
    const ZMQ_PULL: c_int = 7;
    const ZMQ_SNDTIMEO: c_int = 28;

    static NEXT_ADDR: AtomicUsize = AtomicUsize::new(1);

    fn unique_inproc(name: &str) -> CString {
        CString::new(format!(
            "inproc://poller-{name}-{}",
            NEXT_ADDR.fetch_add(1, Ordering::Relaxed)
        ))
        .unwrap()
    }

    fn set_sndtimeo(sock: *mut c_void, timeout_ms: c_int) {
        assert_eq!(
            crate::zmq_setsockopt(
                sock,
                ZMQ_SNDTIMEO,
                std::ptr::from_ref(&timeout_ms).cast(),
                size_of::<c_int>(),
            ),
            0
        );
    }

    #[test]
    fn poller_socket_reports_user_data() {
        let ctx = crate::zmq_ctx_new();
        let push = crate::zmq_socket(ctx, ZMQ_PUSH);
        let pull = crate::zmq_socket(ctx, ZMQ_PULL);
        assert!(!push.is_null());
        assert!(!pull.is_null());

        let addr = unique_inproc("socket");
        assert_eq!(crate::zmq_bind(pull, addr.as_ptr()), 0);
        assert_eq!(crate::zmq_connect(push, addr.as_ptr()), 0);
        set_sndtimeo(push, 1000);
        std::thread::sleep(Duration::from_millis(20));

        let poller = zmq_poller_new();
        assert!(!poller.is_null());
        let tag = 0xA11CEusize;
        let user_data = std::ptr::from_ref(&tag).cast::<c_void>().cast_mut();
        assert_eq!(zmq_poller_add(poller, pull, user_data, ZMQ_POLLIN), 0);
        assert_eq!(zmq_poller_size(poller), 1);

        assert_eq!(crate::zmq_send(push, b"x".as_ptr().cast(), 1, 0), 1);
        let mut event = ZmqPollerEvent {
            socket: std::ptr::null_mut(),
            fd: invalid_fd(),
            user_data: std::ptr::null_mut(),
            events: 0,
        };
        assert_eq!(zmq_poller_wait(poller, &raw mut event, 1000), 1);
        assert_eq!(event.socket, pull);
        assert_eq!(event.user_data, user_data);
        assert_ne!(event.events & ZMQ_POLLIN, 0);

        assert_eq!(zmq_poller_remove(poller, pull), 0);
        assert_eq!(zmq_poller_size(poller), 0);
        let mut poller_slot = poller;
        assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);
        assert!(poller_slot.is_null());

        crate::zmq_close(push);
        crate::zmq_close(pull);
        crate::zmq_ctx_term(ctx);
    }

    #[test]
    fn poller_timeout_maps_to_eagain() {
        let ctx = crate::zmq_ctx_new();
        let pull = crate::zmq_socket(ctx, ZMQ_PULL);
        assert!(!pull.is_null());

        let addr = unique_inproc("timeout");
        assert_eq!(crate::zmq_bind(pull, addr.as_ptr()), 0);
        let poller = zmq_poller_new();
        assert_eq!(
            zmq_poller_add(poller, pull, std::ptr::null_mut(), ZMQ_POLLIN),
            0
        );

        let mut event = ZmqPollerEvent {
            socket: std::ptr::null_mut(),
            fd: invalid_fd(),
            user_data: std::ptr::null_mut(),
            events: 0,
        };
        assert_eq!(zmq_poller_wait(poller, &raw mut event, 1), -1);
        assert_eq!(crate::zmq_errno(), libc::EAGAIN);
        assert_eq!(zmq_poller_modify(poller, pull, 0), 0);
        assert_eq!(zmq_poller_wait(poller, &raw mut event, 0), -1);
        assert_eq!(crate::zmq_errno(), libc::EAGAIN);

        let mut poller_slot = poller;
        assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);
        crate::zmq_close(pull);
        crate::zmq_ctx_term(ctx);
    }

    #[cfg(unix)]
    #[test]
    fn poller_fd_reports_readable_pipe() {
        let mut fds = [-1; 2];
        // SAFETY: fds points to two valid ints for pipe output.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

        let poller = zmq_poller_new();
        let tag = 0xFDFDusize;
        let user_data = std::ptr::from_ref(&tag).cast::<c_void>().cast_mut();
        assert_eq!(zmq_poller_add_fd(poller, fds[0], user_data, ZMQ_POLLIN), 0);
        assert_eq!(zmq_poller_size(poller), 1);

        let mut poller_fd = -1;
        assert_eq!(zmq_poller_fd(poller, &raw mut poller_fd), -1);
        assert_eq!(crate::zmq_errno(), libc::EINVAL);

        // SAFETY: fds[1] is a valid pipe write fd and buffer has one byte.
        assert_eq!(unsafe { libc::write(fds[1], b"x".as_ptr().cast(), 1) }, 1);
        let mut event = ZmqPollerEvent {
            socket: std::ptr::null_mut(),
            fd: invalid_fd(),
            user_data: std::ptr::null_mut(),
            events: 0,
        };
        assert_eq!(zmq_poller_wait(poller, &raw mut event, 1000), 1);
        assert!(event.socket.is_null());
        assert_eq!(event.fd, fds[0]);
        assert_eq!(event.user_data, user_data);
        assert_ne!(event.events & ZMQ_POLLIN, 0);

        assert_eq!(zmq_poller_remove_fd(poller, fds[0]), 0);
        let mut poller_slot = poller;
        assert_eq!(zmq_poller_destroy(&raw mut poller_slot), 0);
        // SAFETY: fds were opened by pipe above and are closed once here.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }
}
