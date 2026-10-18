//! Tagged C handles. Context and socket pointers lead with a tag, so entry
//! points reject the wrong handle type, a closed handle, or a foreign pointer
//! with libzmq's errno instead of reading it as the wrong type.

use std::ffi::{c_int, c_void};
use std::sync::Arc;

use crate::context::OmqContext;
use crate::error::ENOTSOCK;
use crate::socket::OmqSocket;

// libzmq's ctx_t and socket_base_t tags.
const CONTEXT_TAG: u32 = 0xabad_cafe;
const SOCKET_TAG: u32 = 0xbadd_ecaf;
const DEAD_TAG: u32 = 0xdead_beef;

#[repr(C)]
struct Handle<T> {
    tag: u32,
    value: Arc<T>,
}

fn into_raw<T>(tag: u32, value: Arc<T>) -> *mut c_void {
    Box::into_raw(Box::new(Handle { tag, value })).cast()
}

/// # Safety
///
/// `ptr` must be null or readable for a handle tag. Like libzmq, a stale
/// pointer is caught only while its memory still holds the dead tag.
unsafe fn get<'a, T>(ptr: *mut c_void, tag: u32) -> Option<&'a Arc<T>> {
    if ptr.is_null() {
        return None;
    }
    let handle = ptr.cast::<Handle<T>>();
    // SAFETY: the caller guarantees the tag is readable; it sits at offset 0.
    if unsafe { (*handle).tag } != tag {
        return None;
    }
    // SAFETY: the tag matched, so this is a live handle of this type.
    Some(unsafe { &(*handle).value })
}

/// # Safety
///
/// As for [`get`]. On success the handle is freed and must not be used again.
unsafe fn take<T>(ptr: *mut c_void, tag: u32) -> Option<Arc<T>> {
    // SAFETY: forwarded caller contract.
    unsafe { get::<T>(ptr, tag) }?;
    let handle = ptr.cast::<Handle<T>>();
    // SAFETY: the tag matched; mark it dead before the memory is released.
    unsafe { std::ptr::write_volatile(&raw mut (*handle).tag, DEAD_TAG) };
    // SAFETY: the handle came from `into_raw`.
    let Handle { value, .. } = *unsafe { Box::from_raw(handle) };
    Some(value)
}

pub(crate) fn socket_into_raw(socket: Arc<OmqSocket>) -> *mut c_void {
    into_raw(SOCKET_TAG, socket)
}

/// Borrow a socket handle, or `ENOTSOCK`.
///
/// # Safety
///
/// `ptr` must be null or readable for a handle tag.
pub(crate) unsafe fn socket<'a>(ptr: *mut c_void) -> Result<&'a Arc<OmqSocket>, c_int> {
    // SAFETY: forwarded caller contract.
    unsafe { get(ptr, SOCKET_TAG) }.ok_or(ENOTSOCK)
}

/// Free a socket handle and return its socket, or `ENOTSOCK`.
///
/// # Safety
///
/// As for [`socket`]. On success `ptr` must not be used again.
pub(crate) unsafe fn take_socket(ptr: *mut c_void) -> Result<Arc<OmqSocket>, c_int> {
    // SAFETY: forwarded caller contract.
    unsafe { take(ptr, SOCKET_TAG) }.ok_or(ENOTSOCK)
}

pub(crate) fn context_into_raw(context: Arc<OmqContext>) -> *mut c_void {
    into_raw(CONTEXT_TAG, context)
}

/// Borrow a context handle, or `EFAULT`.
///
/// # Safety
///
/// `ptr` must be null or readable for a handle tag.
pub(crate) unsafe fn context<'a>(ptr: *mut c_void) -> Result<&'a Arc<OmqContext>, c_int> {
    // SAFETY: forwarded caller contract.
    unsafe { get(ptr, CONTEXT_TAG) }.ok_or(libc::EFAULT)
}

/// Free a context handle and return its context, or `EFAULT`.
///
/// # Safety
///
/// As for [`context`]. On success `ptr` must not be used again.
pub(crate) unsafe fn take_context(ptr: *mut c_void) -> Result<Arc<OmqContext>, c_int> {
    // SAFETY: forwarded caller contract.
    unsafe { take(ptr, CONTEXT_TAG) }.ok_or(libc::EFAULT)
}

#[cfg(test)]
mod tests {
    use crate::error::ENOTSOCK;
    use crate::poll::{ZMQ_POLLIN, ZmqPollItem};

    #[test]
    fn entry_points_reject_the_other_handle_type() {
        let context = crate::zmq_ctx_new();
        let socket = crate::zmq_socket(context, 8);
        assert!(!socket.is_null());

        assert_eq!(crate::zmq_close(context), -1);
        assert_eq!(crate::zmq_errno(), ENOTSOCK);
        assert_eq!(crate::zmq_send(context, b"x".as_ptr().cast(), 1, 0), -1);
        assert_eq!(crate::zmq_errno(), ENOTSOCK);
        let mut items = [ZmqPollItem {
            socket: context,
            fd: 0,
            events: ZMQ_POLLIN,
            revents: 0,
        }];
        assert_eq!(crate::zmq_poll(items.as_mut_ptr(), 1, 0), -1);
        assert_eq!(crate::zmq_errno(), ENOTSOCK);

        assert_eq!(crate::zmq_ctx_get(socket, 1), -1);
        assert_eq!(crate::zmq_errno(), libc::EFAULT);
        assert!(crate::zmq_socket(socket, 8).is_null());
        assert_eq!(crate::zmq_errno(), libc::EFAULT);
        assert_eq!(crate::zmq_ctx_term(socket), -1);
        assert_eq!(crate::zmq_errno(), libc::EFAULT);

        assert_eq!(crate::zmq_close(socket), 0);
        assert_eq!(crate::zmq_ctx_term(context), 0);
    }
}
