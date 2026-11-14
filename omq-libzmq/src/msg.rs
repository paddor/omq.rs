//! `zmq_msg_t` implementation.
//!
//! The repr must be exactly 64 bytes to match libzmq's ABI.
//! Public alignment follows `zmq_msg_t`: pointer-sized, not always 8 bytes.
//! kind values: 0=empty, 1=heap-alloc, 2=external-ptr, 3=bytes-arc (from recv),
//! 4=shared heap, 5=bytes plus authenticated peer properties.

use std::ffi::{CStr, c_int};

use bytes::{Bytes, BytesMut};

use crate::error::c_try;
use crate::send_recv::{ReceivedPart, checked_c_int_len};

mod repr;

pub use repr::OmqMsgRepr;
pub(crate) use repr::ZMQ_MSG_T_SIZE;
const LARGE_MSG_SHARED_THRESHOLD: usize = 64;

use repr::{
    GROUP_MAX, KIND_BYTES, KIND_BYTES_METADATA, KIND_EXTERNAL, KIND_HEAP, KIND_HEAP_SHARED,
    MsgFields, ReceivedFrame, repr, repr_ref,
};

/// Initialize caller-owned message storage as an empty message; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr(msg).clear() };
    0
}

/// Initialize message storage with an uninitialized payload of `size` bytes; return zero or
/// -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_size(msg: *mut OmqMsgRepr, size: usize) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    // SAFETY: libc::malloc is always safe to call; returns null on failure.
    let ptr = unsafe { libc::malloc(size).cast::<u8>() };
    if ptr.is_null() && size > 0 {
        return crate::error::fail(libc::ENOMEM);
    }

    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.init_fields(MsgFields::new(KIND_HEAP, 0, size, ptr));
    0
}

/// Initialize a message over external data; `ffn(data, hint)` releases it after its last
/// reference.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_data(
    msg: *mut OmqMsgRepr,
    data: *mut libc::c_void,
    size: usize,
    ffn: Option<unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void)>,
    hint: *mut libc::c_void,
) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if data.is_null() && size > 0 {
        return crate::error::fail(libc::EFAULT);
    }

    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    r.init_fields(MsgFields::new(KIND_EXTERNAL, 0, size, data.cast()).free_fn(ffn, hint));
    0
}

/// Initialize a message by copying `size` bytes from the supplied buffer.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_buffer(
    msg: *mut OmqMsgRepr,
    buf: *const libc::c_void,
    size: usize,
) -> c_int {
    if buf.is_null() && size > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    if zmq_msg_init_size(msg, size) != 0 {
        return -1;
    }
    if size > 0 && !buf.is_null() {
        // SAFETY: msg was just initialized by zmq_msg_init_size; buf is non-null.
        let r = unsafe { repr(msg) };
        unsafe {
            std::ptr::copy_nonoverlapping(buf.cast::<u8>(), r.ptr(), size);
        }
    }
    0
}

/// Return the payload pointer of an initialized message; valid until its storage changes or
/// closes.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_data(msg: *mut OmqMsgRepr) -> *mut libc::c_void {
    if msg.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    let writable = r.boxed_bytes_mut().map(|bytes| {
        // Received storage may be shared, static, or mapped read-only.
        // Preserve unique writable allocations; detach every other owner
        // before exposing the C API's mutable pointer.
        let mut writable = std::mem::take(bytes)
            .try_into_mut()
            .unwrap_or_else(|shared| BytesMut::from(shared.as_ref()));
        let pointer = writable.as_mut_ptr();
        *bytes = writable.freeze();
        pointer
    });
    if let Some(pointer) = writable {
        r.set_ptr(pointer);
    }
    r.ptr().cast()
}

/// Return the payload length of an initialized message in bytes.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_size(msg: *const OmqMsgRepr) -> usize {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr_ref(msg).size() }
}

/// Return one if another multipart frame follows, otherwise zero.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_more(msg: *const OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    c_int::from(unsafe { repr_ref(msg).more() })
}

/// Release an initialized message's payload and metadata; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_close(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    match r.kind() {
        KIND_HEAP | KIND_HEAP_SHARED => {
            let ptr = r.ptr();
            if !ptr.is_null() {
                // SAFETY: ptr was allocated by libc::malloc in zmq_msg_init_size.
                unsafe { libc::free(ptr.cast()) };
            }
        }
        KIND_EXTERNAL => {
            // Also for a null buffer, as libzmq does: the hint can own
            // resources of its own.
            if let Some(ffn) = r.free_fn() {
                // SAFETY: the caller of zmq_msg_init_data promised that
                // `ffn` accepts this buffer and hint.
                unsafe { ffn(r.ptr().cast(), r.hint()) };
            }
        }
        KIND_BYTES | KIND_BYTES_METADATA => r.drop_boxed(),
        _ => {}
    }
    r.drop_group();
    r.clear();
    0
}

/// Move a message into an initialized destination and leave the source empty.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_move(dst: *mut OmqMsgRepr, src: *mut OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if dst == src {
        return 0;
    }
    // Close dst first.
    zmq_msg_close(dst);
    // SAFETY: src and dst are non-null (checked above) and don't overlap
    // (ZMQ API contract). Move the repr then zero src to prevent double-free.
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, 1);
        repr(src).clear();
    }
    0
}

/// Copy a message into an initialized destination, sharing payload storage where supported.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_copy(dst: *mut OmqMsgRepr, src: *const OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    if std::ptr::addr_eq(dst.cast_const(), src) {
        return 0;
    }

    zmq_msg_close(dst);
    // SAFETY: src is non-null (checked above).
    let s = unsafe { repr_ref(src) };
    // SAFETY: dst is non-null (checked above), closed, and ready for reinitialization.
    let d = unsafe { repr(dst) };

    let copied = match s.kind() {
        KIND_BYTES | KIND_BYTES_METADATA => copy_boxed(s, d),
        KIND_HEAP | KIND_EXTERNAL | KIND_HEAP_SHARED => copy_heap_payload(s, d),
        _ => {
            // Empty: just zero dst.
            d.clear();
            Ok(())
        }
    };
    c_try!(copied);
    // The copy owns its group: a long one is a heap string per message.
    d.set_group(s.group());
    0
}

/// Share a received payload by cloning its box (refcount bump, no copy); a
/// metadata clone also shares the peer properties.
fn copy_boxed(s: &OmqMsgRepr, d: &mut OmqMsgRepr) -> Result<(), c_int> {
    let Some((data_ptr, boxed)) = s.clone_boxed() else {
        return Err(libc::EFAULT);
    };
    let fields = MsgFields::new(s.kind(), s.more(), s.size(), data_ptr)
        .boxed(boxed)
        .routing_id(s.routing_id());
    d.init_fields(fields);
    Ok(())
}

/// Copy `src` into a new malloc block; null when empty.
fn malloc_copy(src: &[u8]) -> Result<*mut u8, c_int> {
    if src.is_empty() {
        return Ok(std::ptr::null_mut());
    }
    // SAFETY: libc::malloc is always safe to call.
    let p = unsafe { libc::malloc(src.len()).cast::<u8>() };
    if p.is_null() {
        return Err(libc::ENOMEM);
    }
    // SAFETY: p was just allocated with src.len() bytes; src cannot overlap it.
    unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), p, src.len()) };
    Ok(p)
}

/// Deep copy a malloc'd or external payload into a new heap allocation.
fn copy_heap_payload(s: &OmqMsgRepr, d: &mut OmqMsgRepr) -> Result<(), c_int> {
    let size = s.size();
    if size > 0 && s.ptr().is_null() {
        return Err(libc::EFAULT);
    }
    let new_ptr = malloc_copy(s.payload())?;

    // Copies of external or large payloads report ZMQ_SHARED like libzmq,
    // which shares those payloads instead of copying them.
    let kind = if s.kind() == KIND_EXTERNAL || size >= LARGE_MSG_SHARED_THRESHOLD {
        KIND_HEAP_SHARED
    } else {
        KIND_HEAP
    };

    d.init_fields(MsgFields::new(kind, s.more(), size, new_ptr).routing_id(s.routing_id()));
    Ok(())
}

/// Read a supported integer message property; return -1 with errno for an invalid property.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_get(msg: *const OmqMsgRepr, property: c_int) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    match property {
        1 => zmq_msg_more(msg),
        3 => {
            // SAFETY: msg is non-null (checked above).
            let r = unsafe { repr_ref(msg) };
            i32::from(matches!(
                r.kind(),
                KIND_EXTERNAL | KIND_BYTES | KIND_HEAP_SHARED | KIND_BYTES_METADATA
            ))
        }
        5 => zmq_msg_routing_id(msg).cast_signed(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            -1
        }
    }
}

/// Set a supported integer message property; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set(msg: *mut OmqMsgRepr, property: c_int, val: c_int) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr(msg) };
    match property {
        1 => {
            r.set_more(u8::from(val != 0));
            0
        }
        5 => {
            r.set_routing_id(val as u32);
            0
        }
        _ => crate::error::fail(libc::EINVAL),
    }
}

/// Return borrowed, null-terminated message metadata; null with errno indicates an invalid
/// property.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_gets(
    msg: *const OmqMsgRepr,
    property: *const libc::c_char,
) -> *const libc::c_char {
    if msg.is_null() || property.is_null() {
        crate::error::set_errno(libc::EINVAL);
        return std::ptr::null();
    }
    // SAFETY: property is non-null (checked above); caller guarantees valid C string.
    let prop = unsafe { std::ffi::CStr::from_ptr(property) };
    let name = prop.to_bytes();
    // SAFETY: msg is non-null (checked above).
    let repr = unsafe { repr_ref(msg) };
    if let Some(value) = repr.received_frame().and_then(|frame| frame.property(name)) {
        return value;
    }
    match name {
        b"Socket-Type" | b"Identity" | b"Routing-Id" | b"Peer-Address" => c"".as_ptr(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            std::ptr::null()
        }
    }
}

/// Routing ID lives in reserved bytes 0..4 as little-endian u32.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_routing_id(msg: *mut OmqMsgRepr, routing_id: u32) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr(msg) }.set_routing_id(routing_id);
    0
}

/// Return the message's opaque routing ID, or zero when unset.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_routing_id(msg: *const OmqMsgRepr) -> u32 {
    if msg.is_null() {
        return 0;
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr_ref(msg) }.routing_id()
}

/// Set the RADIO/DISH group from a null-terminated string of at most
/// `ZMQ_GROUP_MAX_LENGTH` bytes; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_group(msg: *mut OmqMsgRepr, group: *const libc::c_char) -> c_int {
    if msg.is_null() || group.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: group is non-null (checked above); caller guarantees valid C string.
    let s = unsafe { CStr::from_ptr(group) }.to_bytes();
    if s.len() > GROUP_MAX {
        return crate::error::fail(libc::EINVAL);
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr(msg) }.set_group(s);
    0
}

/// Return the message's borrowed null-terminated RADIO/DISH group name.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_group(msg: *const OmqMsgRepr) -> *const libc::c_char {
    if msg.is_null() {
        return c"".as_ptr();
    }
    // SAFETY: msg is non-null (checked above).
    unsafe { repr_ref(msg) }.group_ptr()
}

/// Send the data held in `msg` on `sock`. The msg is zeroed on success.
///
/// For RADIO sockets: if the msg has a group set via `zmq_msg_set_group`,
/// the group is prepended as the first frame of a 2-part message
/// `[group, body]` which `send_radio` expects.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_send(
    msg: *mut OmqMsgRepr,
    sock: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    if msg.is_null() || sock.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    // SAFETY: C callers pass null or a pointer from this API.
    let sock_arc = c_try!(unsafe { crate::handle::socket(sock) });
    c_try!(crate::socket::materialize_for_io(sock_arc));

    let group = msg_group_bytes(msg);
    let routing_id = zmq_msg_routing_id(msg);

    // For KIND_BYTES, clone the Bytes (a zero-copy Arc bump) rather than
    // copying the payload. We must NOT mutate `msg` here: libzmq leaves the
    // message intact when a send fails, and the caller may then retry, copy,
    // or close it. Stealing the Box and nulling the fields used to leave the
    // message in a KIND_BYTES-with-null-boxed state that made a subsequent
    // zmq_msg_copy dereference a null pointer. On success the Box is dropped
    // by zmq_msg_close below.
    // SAFETY: msg is non-null (checked above).
    let r = unsafe { repr_ref(msg) };
    let bytes = if let Some(bytes) = r.boxed_bytes() {
        bytes.clone()
    } else if r.ptr().is_null() && r.size() > 0 {
        return crate::error::fail(libc::EFAULT);
    } else {
        extract_bytes(msg)
    };

    // Only RADIO sends a group. Build it into this message; never stage it
    // in the socket's frame buffer, where a failed or non-RADIO send would
    // leave it in front of the next message.
    let ret = if sock_arc.socket_type == omq_tokio::SocketType::Radio && !group.is_empty() {
        let ret_len = c_try!(checked_c_int_len(r.size()));
        let message = omq_tokio::Message::with_group(group, bytes);
        crate::send_recv::send_message(sock_arc, message, ret_len, flags)
    } else if sock_arc.socket_type == omq_tokio::SocketType::Server {
        let ret_len = c_try!(checked_c_int_len(r.size()));
        let message = omq_tokio::Message::single(bytes).with_routing_id(routing_id);
        crate::send_recv::send_message(sock_arc, message, ret_len, flags)
    } else {
        crate::send_recv::send_bytes(sock_arc, &bytes, flags)
    };
    if ret >= 0 {
        zmq_msg_close(msg);
    }
    crate::send_recv::fd_edge::after_send(sock_arc, ret)
}

/// Deprecated `zmq_sendmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_send` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_sendmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_send(msg, sock, flags)
}

/// Deprecated `zmq_recvmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_recv` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_recvmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_recv(msg, sock, flags)
}

fn msg_group_bytes(msg: *mut OmqMsgRepr) -> bytes::Bytes {
    // SAFETY: caller guarantees msg is non-null.
    bytes::Bytes::copy_from_slice(unsafe { repr_ref(msg) }.group())
}

/// Receive into `msg` as a zero-copy `KIND_BYTES` frame.
///
/// The `more` field is set to 1 when additional frames follow in the
/// current multipart message (i.e. `ZMQ_RCVMORE` would be true).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_recv(
    msg: *mut OmqMsgRepr,
    sock_ptr: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    if msg.is_null() || sock_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    // SAFETY: C callers pass null or a pointer from this API.
    let sock = c_try!(unsafe { crate::handle::socket(sock_ptr) });

    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return crate::error::fail(crate::error::ETERM);
    }
    c_try!(crate::socket::materialize_for_io(sock));

    let received = match sock.socket_type {
        omq_tokio::SocketType::Server | omq_tokio::SocketType::Dish => {
            crate::send_recv::pop_recv_message_with_metadata(sock, flags)
        }
        _ => crate::send_recv::pop_recv_frame_with_properties(sock, flags),
    };
    let part = c_try!(received);

    zmq_msg_close(msg);
    let sz = part.frame.len();
    // SAFETY: msg was just closed above and is ready for reinitialization.
    let r = unsafe { repr(msg) };
    c_try!(store_received_frame(r, part));
    c_try!(checked_c_int_len(sz))
}

/// Store a received frame in a closed message, picking the cheapest representation.
#[inline]
fn store_received_frame(r: &mut OmqMsgRepr, part: ReceivedPart) -> Result<(), c_int> {
    let ReceivedPart {
        frame,
        more,
        routing_id,
        group,
        properties,
    } = part;
    let sz = frame.len();
    let more = u8::from(more);

    let fields = if let Some(properties) = properties {
        // Authenticated peers: keep the properties for zmq_msg_gets.
        let boxed = Box::new(ReceivedFrame::new(frame, &properties));
        let data_ptr = boxed.bytes.as_ptr().cast_mut();
        MsgFields::new(KIND_BYTES_METADATA, more, sz, data_ptr)
            .boxed(Box::into_raw(boxed).cast::<libc::c_void>())
    } else if sz <= 128 {
        // Small frame: malloc + memcpy (1 alloc) instead of
        // Box<Bytes> (2 allocs: Bytes::copy_from_slice + Box::new).
        MsgFields::new(KIND_HEAP, more, sz, malloc_copy(&frame)?)
    } else {
        let boxed = Box::new(frame);
        let data_ptr = boxed.as_ptr().cast_mut();
        MsgFields::new(KIND_BYTES, more, sz, data_ptr)
            .boxed(Box::into_raw(boxed).cast::<libc::c_void>())
    };

    r.init_fields(fields.routing_id(routing_id));
    r.set_group(&group);
    Ok(())
}

/// Extract a Bytes copy out of a msg without consuming ownership.
fn extract_bytes(msg: *const OmqMsgRepr) -> Bytes {
    // SAFETY: caller guarantees msg is non-null and initialized.
    let r = unsafe { repr_ref(msg) };
    Bytes::copy_from_slice(r.payload())
}

#[cfg(test)]
mod tests;
