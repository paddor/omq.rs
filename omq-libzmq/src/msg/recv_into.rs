//! `omq_recv_into`: `zmq_recv` into a caller buffer without truncation.
//!
//! A frame that fits is copied into the buffer: one call and one copy, with
//! no `zmq_msg_t` allocation. A larger frame is stored in the caller's
//! overflow message exactly as `zmq_msg_recv` stores it.

use std::ffi::{c_char, c_int, c_void};

use super::{OmqMsgRepr, repr, store_message_frame, store_received_frame, zmq_msg_close};
use crate::error::{ETERM, c_try, fail};
use crate::send_recv::{ReceivedPart, checked_c_int_len};

/// `ZMQ_GROUP_MAX_LENGTH` plus the NUL terminator.
const GROUP_CAPACITY: usize = 256;

/// Frame metadata reported by [`omq_recv_into`] (`omq_frame_info_t`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OmqFrameInfo {
    /// 1 when another frame of the same message follows, else 0.
    pub more: c_int,
    /// Routing ID of a SERVER message, else 0.
    pub routing_id: u32,
    /// Group of a DISH message, NUL-terminated; empty for other types.
    pub group: [c_char; GROUP_CAPACITY],
}

impl Default for OmqFrameInfo {
    fn default() -> Self {
        Self {
            more: 0,
            routing_id: 0,
            group: [0; GROUP_CAPACITY],
        }
    }
}

/// Where a received frame goes: the caller buffer if it fits, else the
/// overflow message. `info` may be null.
struct Target {
    buf: *mut u8,
    size: usize,
    overflow: *mut OmqMsgRepr,
    info: *mut OmqFrameInfo,
}

impl Target {
    /// Deliver a frame borrowed from a ring message.
    fn borrowed(
        &self,
        message: &omq_tokio::Message,
        start: usize,
        more: bool,
    ) -> Result<c_int, c_int> {
        let frame = message.get(start).unwrap_or(&[]);
        self.report(more, 0, &[]);
        if frame.len() <= self.size {
            return self.copy(frame);
        }
        zmq_msg_close(self.overflow);
        // SAFETY: overflow is non-null (checked by the caller) and was just
        // closed.
        store_message_frame(unsafe { repr(self.overflow) }, message, start, more)
    }

    /// Deliver an owned frame with its metadata.
    fn part(&self, part: ReceivedPart) -> Result<c_int, c_int> {
        self.report(part.more, part.routing_id, &part.group);
        if part.frame.len() <= self.size {
            return self.copy(&part.frame);
        }
        let len = checked_c_int_len(part.frame.len())?;
        zmq_msg_close(self.overflow);
        // SAFETY: overflow is non-null (checked by the caller) and was just
        // closed.
        store_received_frame(unsafe { repr(self.overflow) }, part)?;
        Ok(len)
    }

    fn copy(&self, frame: &[u8]) -> Result<c_int, c_int> {
        let len = checked_c_int_len(frame.len())?;
        if !frame.is_empty() {
            // SAFETY: buf has `size` writable bytes (caller contract) and
            // frame.len() <= size; a non-empty frame implies size > 0, so buf
            // is non-null.
            unsafe { std::ptr::copy_nonoverlapping(frame.as_ptr(), self.buf, frame.len()) };
        }
        Ok(len)
    }

    fn report(&self, more: bool, routing_id: u32, group: &[u8]) {
        // SAFETY: a non-null info points to writable caller storage.
        let Some(info) = (unsafe { self.info.as_mut() }) else {
            return;
        };
        info.more = c_int::from(more);
        info.routing_id = routing_id;
        // Groups are at most ZMQ_GROUP_MAX_LENGTH bytes; send checks that.
        let len = group.len().min(GROUP_CAPACITY - 1);
        for (slot, &byte) in info.group.iter_mut().zip(&group[..len]) {
            *slot = c_char::from_ne_bytes([byte]);
        }
        info.group[len] = 0;
    }
}

/// Receive one frame like `zmq_recv` without truncating it. A frame of at
/// most `size` bytes is copied into `buf`; a larger one is stored in
/// `overflow`, an initialized `zmq_msg_t`, as `zmq_msg_recv` would store it.
/// Returns the frame size, or -1 with errno. `info` may be null; it gets
/// the more flag, a SERVER routing ID, and a DISH group.
#[unsafe(no_mangle)]
pub extern "C" fn omq_recv_into(
    sock_ptr: *mut c_void,
    buf: *mut c_void,
    size: usize,
    overflow: *mut OmqMsgRepr,
    flags: c_int,
    info: *mut OmqFrameInfo,
) -> c_int {
    if sock_ptr.is_null() || overflow.is_null() || (buf.is_null() && size > 0) {
        return fail(libc::EFAULT);
    }

    // SAFETY: C callers pass null or a pointer from this API.
    let sock = c_try!(unsafe { crate::handle::socket(sock_ptr) });
    if sock.ctx.is_effectively_terminated() {
        return fail(ETERM);
    }
    c_try!(crate::socket::materialize_for_io(sock));

    let target = Target {
        buf: buf.cast(),
        size,
        overflow,
        info,
    };
    let received = match sock.socket_type {
        omq_tokio::SocketType::Server | omq_tokio::SocketType::Dish => {
            crate::send_recv::pop_recv_message_with_metadata(sock, flags)
                .and_then(|part| target.part(part))
        }
        _ if crate::send_recv::direct_frame_recv(sock) => {
            crate::send_recv::pop_first_frame(sock, flags, |message, start, more| {
                target.borrowed(message, start, more)
            })
        }
        _ => crate::send_recv::pop_recv_frame_with_properties(sock, flags)
            .and_then(|part| target.part(part)),
    };

    c_try!(received)
}
