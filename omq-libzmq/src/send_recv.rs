//! `zmq_send` / `zmq_recv` entry points.
#![allow(clippy::cast_possible_wrap)]

use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flume::{RecvTimeoutError, TryRecvError};

use crate::error::{ETERM, fail};
use crate::socket::OmqSocket;

// ZMQ send/recv flags.
const ZMQ_DONTWAIT: c_int = 1;
const ZMQ_SNDMORE: c_int = 2;

/// Core send dispatch. Takes ownership of an already-constructed [`Bytes`].
///
/// Returns the number of bytes sent on success, or a negative errno on error.
/// Callers that construct `bytes` from a raw-pointer + length should use the
/// raw `len` as the success return value; here we use `bytes.len()` which is
/// identical for all well-formed callers.
pub(crate) fn send_bytes(sock: &Arc<OmqSocket>, bytes: Bytes, flags: c_int) -> c_int {
    let len = bytes.len();

    let max = sock
        .ctx
        .max_msg_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && len > max as usize {
        return fail(libc::EMSGSIZE);
    }

    // XSUB: intercept subscription frames (\x01topic / \x00topic) and
    // route to subscribe/unsubscribe instead of the send path.
    if sock.socket_type == omq_compio::SocketType::XSub && !bytes.is_empty() {
        crate::socket::ensure_materialized(sock);
        let (subscribe, prefix) = match bytes[0] {
            0x01 => (true, bytes.slice(1..)),
            0x00 => (false, bytes.slice(1..)),
            _ => (true, bytes.clone()),
        };
        let result =
            crate::socket::with_socket(&sock.ctx, sock.thread_idx, sock.id, move |s| async move {
                if subscribe {
                    s.subscribe(prefix).await
                } else {
                    s.unsubscribe(prefix).await
                }
            });
        return match result {
            Ok(Ok(())) => len as c_int,
            Ok(Err(ref e)) => fail(crate::error::map_omq_err(e)),
            Err(()) => fail(ETERM),
        };
    }

    // If SNDMORE: buffer and return immediately.
    if flags & ZMQ_SNDMORE != 0 {
        sock.send_accum.lock().unwrap().push(bytes);
        return len as c_int;
    }

    // Drain accumulated parts + current frame into one message.
    let msg = {
        let mut accum = sock.send_accum.lock().unwrap();
        if accum.is_empty() {
            omq_compio::Message::single(bytes)
        } else {
            let mut v: Vec<Bytes> = accum.drain(..).collect();
            v.push(bytes);
            omq_compio::Message::multipart(v)
        }
    };

    // Inproc bypass: push directly to lock-free SPSC ring.
    // Safety: zmq contract guarantees single-threaded access per socket.
    if let Some(bypass) = unsafe { &mut *sock.bypass_send.get() } {
        return match bypass.push(msg) {
            Ok(()) => len as c_int,
            Err(_msg) => fail(libc::EAGAIN),
        };
    }

    let send_tx = sock.send_tx.get().expect("socket not connected");
    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;

    let result = if dontwait {
        match send_tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(_)) => Err(libc::EAGAIN),
            Err(flume::TrySendError::Disconnected(_)) => Err(ETERM),
        }
    } else if sndtimeo > 0 {
        let timeout = Duration::from_millis(sndtimeo as u64);
        match send_tx.send_timeout(msg, timeout) {
            Ok(()) => Ok(()),
            Err(flume::SendTimeoutError::Timeout(_)) => Err(libc::EAGAIN),
            Err(flume::SendTimeoutError::Disconnected(_)) => Err(ETERM),
        }
    } else {
        send_tx.send(msg).map_err(|_| ETERM)
    };

    match result {
        Ok(()) => len as c_int,
        Err(e) => fail(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_send(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return fail(ETERM);
    }

    let bytes = if buf.is_null() || len == 0 {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) })
    };

    send_bytes(sock, bytes, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_send_const(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    // We always copy; the const hint is advisory only.
    zmq_send(sock_ptr, buf, len, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_recv(
    sock_ptr: *mut libc::c_void,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return fail(ETERM);
    }
    match pop_recv_frame(sock, flags) {
        Ok((frame, _more)) => {
            let frame_len = frame.len();
            copy_to_buf(buf, buf_len, &frame);
            frame_len as c_int
        }
        Err(e) => fail(e),
    }
}

/// Pop one frame from the socket, honouring flags/timeout.
///
/// Returns `(frame_bytes, more)` where `more` is true when the current
/// multipart message has additional frames waiting in `recv_drain`.
/// On error returns the errno value to pass to `fail()`.
pub(crate) fn pop_recv_frame(sock: &OmqSocket, flags: c_int) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    // Drain leftover frames from a partially-consumed multipart message.
    // drain_nonempty lets us skip the Mutex acquire on the common single-frame path.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let mut drain = sock.recv_drain.lock().unwrap();
        if let Some(frame) = drain.pop_front() {
            let more = !drain.is_empty();
            if !more {
                sock.drain_nonempty.store(false, Ordering::Relaxed);
            }
            return Ok((frame, more));
        }
        sock.drain_nonempty.store(false, Ordering::Relaxed);
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;

    // Inproc bypass: pop directly from lock-free SPSC ring.
    // Safety: zmq contract guarantees single-threaded access per socket.
    if let Some(bypass) = unsafe { &mut *sock.bypass_recv.get() } {
        let msg = if dontwait {
            match bypass.pop() {
                Some(m) => m,
                None => return Err(libc::EAGAIN),
            }
        } else {
            loop {
                if let Some(m) = bypass.pop() {
                    break m;
                }
                std::thread::yield_now();
            }
        };
        return Ok(decompose_message(sock, &msg));
    }

    let rx = sock.recv_rx.get().expect("socket not connected");

    let msg = if dontwait {
        match rx.try_recv() {
            Ok(m) => m,
            Err(TryRecvError::Empty) => return Err(libc::EAGAIN),
            Err(TryRecvError::Disconnected) => return Err(ETERM),
        }
    } else if rcvtimeo > 0 {
        let timeout = Duration::from_millis(rcvtimeo as u64);
        match rx.recv_timeout(timeout) {
            Ok(m) => m,
            Err(RecvTimeoutError::Timeout) => return Err(libc::EAGAIN),
            Err(RecvTimeoutError::Disconnected) => return Err(ETERM),
        }
    } else {
        match rx.recv() {
            Ok(m) => m,
            Err(_) => return Err(ETERM),
        }
    };

    if rx.is_empty() {
        crate::socket::NotifyFd::drain_recv(&sock.notify);
    }

    Ok(decompose_message(sock, &msg))
}

/// Extract the first frame from a `Message` and stash remaining parts
/// in `recv_drain` for subsequent `RCVMORE` calls.
fn decompose_message(sock: &OmqSocket, msg: &omq_compio::Message) -> (Bytes, bool) {
    use std::sync::atomic::Ordering;

    let dish = sock.socket_type == omq_compio::SocketType::Dish;
    let nparts = msg.len();

    if nparts <= 1 && !dish {
        let head = msg.part_bytes(0).unwrap_or_default();
        return (head, false);
    }

    let start = usize::from(dish && nparts >= 2);
    let head = msg.part_bytes(start).unwrap_or_default();

    let remaining = start + 1;
    if remaining < nparts {
        sock.drain_nonempty.store(true, Ordering::Relaxed);
        let mut drain = sock.recv_drain.lock().unwrap();
        for i in remaining..nparts {
            if let Some(b) = msg.part_bytes(i) {
                drain.push_back(b);
            }
        }
    }

    (head, remaining < nparts)
}

/// Copy `src` into the caller-supplied buffer (truncate if needed).
fn copy_to_buf(buf: *mut libc::c_void, buf_len: usize, src: &[u8]) {
    if buf.is_null() || buf_len == 0 {
        return;
    }
    let copy_len = src.len().min(buf_len);
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), buf.cast::<u8>(), copy_len);
    }
}
