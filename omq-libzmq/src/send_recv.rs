//! `zmq_send` / `zmq_recv` entry points.
//!
//! Send: direct `Handle::block_on(socket.send())`, no relay.
//! Recv: bypass ring -> yring consumers -> block on `RecvNotify`.
use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::consts::{ZMQ_DONTWAIT, ZMQ_SNDMORE};
use crate::error::{ETERM, fail, map_omq_err};
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;

fn checked_c_int_len(n: usize) -> Result<c_int, c_int> {
    c_int::try_from(n).map_err(|_| libc::EMSGSIZE)
}

/// Clear a bypass option if the peer has closed the pipe.
///
fn clear_stale_bypass<B: HasPipeClosed>(bypass_cell: &crate::local_cell::LocalCell<Option<B>>) {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let opt = unsafe { bypass_cell.get() };
    if opt
        .as_ref()
        .is_some_and(|b| b.pipe_closed().load(std::sync::atomic::Ordering::Acquire))
    {
        *opt = None;
    }
}

trait HasPipeClosed {
    fn pipe_closed(&self) -> &std::sync::atomic::AtomicBool;
}

impl HasPipeClosed for crate::inproc_bypass::BypassSend {
    fn pipe_closed(&self) -> &std::sync::atomic::AtomicBool {
        &self.pipe.closed
    }
}

impl HasPipeClosed for crate::inproc_bypass::BypassRecv {
    fn pipe_closed(&self) -> &std::sync::atomic::AtomicBool {
        &self.pipe.closed
    }
}

pub(crate) enum SendMessageAttempt {
    Sent,
    Full(omq_tokio::Message),
}

/// Non-blocking full-message receive used by `zmq_proxy`.
///
/// This reads the same libzmq-facing queues as `zmq_recv`: the direct yring
/// consumers, plus the inproc byte bypass where applicable. It must not call
/// `omq_tokio::Socket::try_recv`, because libzmq sockets install a custom
/// recv sink and the async socket recv pipe is not the owner of that hot path.
pub(crate) fn try_recv_message(sock: &OmqSocket) -> Result<Option<omq_tokio::Message>, c_int> {
    use std::sync::atomic::Ordering;

    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        if !drain.is_empty() {
            let parts: Vec<Bytes> = drain.drain(..).collect();
            sock.drain_nonempty.store(false, Ordering::Relaxed);
            return Ok(Some(omq_tokio::Message::multipart(parts)));
        }
        sock.drain_nonempty.store(false, Ordering::Relaxed);
    }

    clear_stale_bypass(&sock.bypass_recv);
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    if let Some(bypass) = unsafe { sock.bypass_recv.get() } {
        // SAFETY: same socket-thread invariant as above.
        if let Some(cons) = unsafe { sock.recv_cons.get() }
            && let Some(m) = try_pop_dual(cons, sock)
        {
            signal_recv_space(sock);
            return Ok(Some(m));
        }
        if let Some((ptr, len)) = bypass.peek() {
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let msg = omq_tokio::Message::single(Bytes::copy_from_slice(slice));
            bypass.advance(len);
            return Ok(Some(msg));
        }
        if bypass.pipe.closed.load(Ordering::Acquire) {
            return Err(ETERM);
        }
        return Ok(None);
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv_cons.get() }) else {
        return Err(ETERM);
    };
    if let Some(m) = try_pop_dual(cons, sock) {
        signal_recv_space(sock);
        return Ok(Some(m));
    }
    Ok(None)
}

/// Non-blocking full-message send used by `zmq_proxy`.
///
/// Single-frame PUSH/PULL inproc messages take the byte bypass. Other
/// messages go through the materialized omq-tokio socket so type-specific
/// send behavior, including XSUB raw subscribe commands, stays in one place.
pub(crate) fn try_send_message(
    sock: &Arc<OmqSocket>,
    msg: omq_tokio::Message,
) -> Result<SendMessageAttempt, c_int> {
    if msg.len() == 1 {
        clear_stale_bypass(&sock.bypass_send);
        // SAFETY: libzmq sockets are accessed by at most one application thread.
        if let Some(bypass) = unsafe { sock.bypass_send.get() } {
            let result = {
                let data = msg.get(0).unwrap_or(&[]);
                bypass.push(data)
            };
            return match result {
                Ok(()) => Ok(SendMessageAttempt::Sent),
                Err(libc::EAGAIN) => Ok(SendMessageAttempt::Full(msg)),
                Err(e) => Err(e),
            };
        }
    }

    let Some(inner) = sock.inner.get() else {
        return Err(ETERM);
    };
    if round_robin_send_mutes_without_ready_peer(sock)
        && !can_queue_without_ready_peer(sock)?
        && inner.ready_peer_count() == 0
    {
        return Ok(SendMessageAttempt::Full(msg));
    }
    match inner.try_send(msg) {
        Ok(()) => Ok(SendMessageAttempt::Sent),
        Err(omq_tokio::TrySendError::Full(msg)) => Ok(SendMessageAttempt::Full(msg)),
        Err(omq_tokio::TrySendError::Closed) => Err(ETERM),
        Err(omq_tokio::TrySendError::Error(e)) => Err(map_omq_err(&e)),
    }
}

fn round_robin_send_mutes_without_ready_peer(sock: &OmqSocket) -> bool {
    matches!(
        sock.socket_type,
        omq_tokio::SocketType::Push
            | omq_tokio::SocketType::Dealer
            | omq_tokio::SocketType::Req
            | omq_tokio::SocketType::Client
            | omq_tokio::SocketType::Scatter
    )
}

fn can_queue_without_ready_peer(sock: &OmqSocket) -> Result<bool, c_int> {
    let Ok(overlay) = sock.overlay.lock() else {
        return Err(ETERM);
    };
    Ok(!overlay.immediate
        && sock
            .connect_count
            .load(std::sync::atomic::Ordering::Acquire)
            > 0)
}

fn wait_for_ready_peer(sock: &OmqSocket, sndtimeo: i64) -> Result<(), c_int> {
    let Some(inner) = sock.inner.get() else {
        return Err(ETERM);
    };
    if inner.ready_peer_count() != 0 {
        return Ok(());
    }

    let result = if sndtimeo > 0 {
        let timeout = Duration::from_millis(sndtimeo as u64);
        crate::socket::with_socket(&sock.ctx, inner, move |s| async move {
            s.wait_connected(1, timeout).await.map(|_| ())
        })
    } else {
        crate::socket::with_socket(&sock.ctx, inner, move |s| async move {
            loop {
                match s.wait_connected(1, Duration::from_hours(24)).await {
                    Ok(_) => return Ok(()),
                    Err(omq_tokio::Error::Timeout) => {}
                    Err(e) => return Err(e),
                }
            }
        })
    };

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(omq_tokio::Error::Timeout)) => Err(libc::EAGAIN),
        Ok(Err(ref e)) => Err(map_omq_err(e)),
        Err(()) => Err(ETERM),
    }
}

fn ensure_libzmq_send_route(sock: &OmqSocket, flags: c_int, sndtimeo: i64) -> Result<(), c_int> {
    if !round_robin_send_mutes_without_ready_peer(sock) {
        return Ok(());
    }
    if can_queue_without_ready_peer(sock)? {
        return Ok(());
    }
    let Some(inner) = sock.inner.get() else {
        return Err(ETERM);
    };
    if inner.ready_peer_count() != 0 {
        return Ok(());
    }
    if (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0 {
        return Err(libc::EAGAIN);
    }
    wait_for_ready_peer(sock, sndtimeo)
}

fn block_recv<T>(
    sock: &OmqSocket,
    rcvtimeo: i64,
    mut try_pop: impl FnMut() -> Option<T>,
) -> Result<T, c_int> {
    if rcvtimeo > 0 {
        let deadline = std::time::Instant::now() + Duration::from_millis(rcvtimeo as u64);
        loop {
            if sock.ctx.is_effectively_terminated() {
                return Err(ETERM);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(libc::EAGAIN);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as c_int;
            let recv_notify = sock.notify.recv_notifier();
            let _ = recv_notify.wait_for_readable(ms);
            if sock.ctx.is_effectively_terminated() {
                return Err(ETERM);
            }
            if let Some(val) = try_pop() {
                return Ok(val);
            }
        }
    }
    loop {
        if sock.ctx.is_effectively_terminated() {
            return Err(ETERM);
        }
        let recv_notify = sock.notify.recv_notifier();
        let _ = recv_notify.wait_for_readable(100);
        if sock.ctx.is_effectively_terminated() {
            return Err(ETERM);
        }
        if let Some(val) = try_pop() {
            return Ok(val);
        }
    }
}

fn block_recv_result<T>(
    sock: &OmqSocket,
    rcvtimeo: i64,
    mut try_pop: impl FnMut() -> Result<Option<T>, c_int>,
) -> Result<T, c_int> {
    if rcvtimeo > 0 {
        let deadline = std::time::Instant::now() + Duration::from_millis(rcvtimeo as u64);
        loop {
            if sock.ctx.is_effectively_terminated() {
                return Err(ETERM);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(libc::EAGAIN);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as c_int;
            let recv_notify = sock.notify.recv_notifier();
            let _ = recv_notify.wait_for_readable(ms);
            if sock.ctx.is_effectively_terminated() {
                return Err(ETERM);
            }
            if let Some(val) = try_pop()? {
                return Ok(val);
            }
        }
    }
    loop {
        if sock.ctx.is_effectively_terminated() {
            return Err(ETERM);
        }
        let recv_notify = sock.notify.recv_notifier();
        let _ = recv_notify.wait_for_readable(100);
        if sock.ctx.is_effectively_terminated() {
            return Err(ETERM);
        }
        if let Some(val) = try_pop()? {
            return Ok(val);
        }
    }
}

/// Core send dispatch. Takes a raw slice to avoid heap-allocating a `Bytes`
/// on the hot path: single-part messages ≤55 bytes use `Message`'s inline
/// storage (zero alloc). Only SNDMORE accumulation and XSUB subscription
/// frames go through `Bytes::copy_from_slice`.
#[expect(clippy::too_many_lines)]
pub(crate) fn send_bytes(sock: &Arc<OmqSocket>, data: &[u8], flags: c_int) -> c_int {
    let len = data.len();
    let Ok(ret_len) = checked_c_int_len(len) else {
        return fail(libc::EMSGSIZE);
    };

    let max = sock
        .ctx
        .max_msg_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && len > max as usize {
        return fail(libc::EMSGSIZE);
    }

    // XSUB: intercept subscription frames.
    if sock.socket_type == omq_tokio::SocketType::XSub && !data.is_empty() {
        crate::socket::ensure_materialized(sock);
        let Some(inner) = sock.inner.get() else {
            return fail(ETERM);
        };
        let bytes = Bytes::copy_from_slice(data);
        let (subscribe, prefix) = match bytes[0] {
            0x01 => (true, bytes.slice(1..)),
            0x00 => (false, bytes.slice(1..)),
            _ => (true, bytes),
        };
        let result = crate::socket::with_socket(&sock.ctx, inner, move |s| async move {
            if subscribe {
                s.subscribe(prefix).await
            } else {
                s.unsubscribe(prefix).await
            }
        });
        return match result {
            Ok(Ok(())) => ret_len,
            Ok(Err(ref e)) => fail(crate::error::map_omq_err(e)),
            Err(()) => fail(ETERM),
        };
    }

    // Inproc bypass: write raw bytes into the byte ring.
    // Checked BEFORE Message construction to avoid heap allocation.
    if flags & ZMQ_SNDMORE == 0 {
        // SAFETY: libzmq sockets are accessed by at most one application thread.
        let accum = unsafe { sock.send_accum.get() };
        if accum.is_empty() {
            clear_stale_bypass(&sock.bypass_send);
        }
        if accum.is_empty()
            // SAFETY: same socket-thread invariant as `send_accum`.
            && let Some(bypass) = unsafe { sock.bypass_send.get() }
        {
            let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
            let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;
            if dontwait {
                return match bypass.push(data) {
                    Ok(()) => ret_len,
                    Err(e) => fail(e),
                };
            }
            return match bypass.push_blocking(data) {
                Ok(()) => ret_len,
                Err(e) => fail(e),
            };
        }
    }

    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    if let Err(e) = ensure_libzmq_send_route(sock, flags, sndtimeo) {
        return fail(e);
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let accum = unsafe { sock.send_accum.get() };

    // If SNDMORE: buffer and return immediately.
    if flags & ZMQ_SNDMORE != 0 {
        accum.push(Bytes::copy_from_slice(data));
        return ret_len;
    }

    // Drain accumulated parts + current frame into one message.
    let msg = if accum.is_empty() {
        omq_tokio::Message::from_slice(data)
    } else {
        let mut v: Vec<Bytes> = std::mem::take(accum);
        v.push(Bytes::copy_from_slice(data));
        omq_tokio::Message::multipart(v)
    };

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;

    match inner.try_send(msg) {
        Ok(()) => ret_len,
        Err(omq_tokio::TrySendError::Closed | omq_tokio::TrySendError::Error(_)) => fail(ETERM),
        Err(omq_tokio::TrySendError::Full(_)) if dontwait => fail(libc::EAGAIN),
        Err(omq_tokio::TrySendError::Full(mut msg)) => {
            for i in 0..8 {
                if i < 4 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                match inner.try_send(msg) {
                    Ok(()) => return ret_len,
                    Err(omq_tokio::TrySendError::Closed | omq_tokio::TrySendError::Error(_)) => {
                        return fail(ETERM);
                    }
                    Err(omq_tokio::TrySendError::Full(returned)) => msg = returned,
                }
            }
            let Some(handle) = sock.ctx.handle() else {
                return fail(ETERM);
            };
            let s = inner.clone();
            if sndtimeo > 0 {
                let timeout = Duration::from_millis(sndtimeo as u64);
                match handle.block_on(async { tokio::time::timeout(timeout, s.send(msg)).await }) {
                    Ok(Ok(())) => ret_len,
                    Ok(Err(_)) => fail(ETERM),
                    Err(_elapsed) => fail(libc::EAGAIN),
                }
            } else {
                match handle.block_on(s.send(msg)) {
                    Ok(()) => ret_len,
                    Err(_) => fail(ETERM),
                }
            }
        }
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
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.is_effectively_terminated() {
        return fail(ETERM);
    }
    if buf.is_null() && len > 0 {
        return fail(libc::EFAULT);
    }

    let data = if buf.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: buf is non-null with len readable bytes (caller contract).
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) }
    };

    send_bytes(sock, data, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_send_const(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
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
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock.ctx.is_effectively_terminated() {
        return fail(ETERM);
    }
    if buf.is_null() && buf_len > 0 {
        return fail(libc::EFAULT);
    }

    zmq_recv_impl(sock, buf, buf_len, flags)
}

fn zmq_recv_impl(sock: &OmqSocket, buf: *mut libc::c_void, buf_len: usize, flags: c_int) -> c_int {
    use std::sync::atomic::Ordering;

    // Inproc bypass fast path: copy from byte ring directly into user
    // buffer. Zero intermediate Bytes allocation.
    clear_stale_bypass(&sock.bypass_recv);
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    if let Some(bypass) = unsafe { sock.bypass_recv.get() } {
        match recv_bypass_direct(sock, bypass, buf, buf_len, flags) {
            Ok(n) => return n,
            Err(e) => return fail(e),
        }
    }

    // Multipart drain: leftover frames use the Bytes-returning path.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        return zmq_recv_via_frame(sock, buf, buf_len, flags);
    }

    // Fast path: pop Message from yring, borrow first frame directly.
    // Avoids the Bytes::copy_from_slice that pop_recv_frame/decompose_message
    // would do for inline messages.
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv_cons.get() }) else {
        return fail(ETERM);
    };

    if let Some(m) = try_pop_dual(cons, sock) {
        signal_recv_space(sock);
        return recv_msg_to_buf(sock, &m, buf, buf_len);
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;
    if dontwait {
        return fail(libc::EAGAIN);
    }

    match block_recv(sock, rcvtimeo, || {
        let m = try_pop_dual(cons, sock)?;
        signal_recv_space(sock);
        Some(m)
    }) {
        Ok(m) => recv_msg_to_buf(sock, &m, buf, buf_len),
        Err(e) => fail(e),
    }
}

/// Borrow the first frame of a Message and copy into the user buffer.
/// Zero heap allocation for inline messages.
#[inline]
fn recv_msg_to_buf(
    sock: &OmqSocket,
    m: &omq_tokio::Message,
    buf: *mut libc::c_void,
    buf_len: usize,
) -> c_int {
    let start = msg_start_index(sock, m);
    let data = m.get(start).unwrap_or(&[]);
    copy_to_buf(buf, buf_len, data);
    stash_remaining_parts(sock, m, start);
    match checked_c_int_len(data.len()) {
        Ok(n) => n,
        Err(e) => fail(e),
    }
}

/// Fallback for `zmq_recv` when multipart drain is non-empty.
fn zmq_recv_via_frame(
    sock: &OmqSocket,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> c_int {
    match pop_recv_frame(sock, flags) {
        Ok((frame, _more)) => {
            let frame_len = frame.len();
            copy_to_buf(buf, buf_len, &frame);
            match checked_c_int_len(frame_len) {
                Ok(n) => n,
                Err(e) => fail(e),
            }
        }
        Err(e) => fail(e),
    }
}

/// Signal the recv pump that space is available in the recv ring.
#[inline]
fn signal_recv_space(sock: &OmqSocket) {
    if let Some(cfg) = sock.recv_sink_config.get() {
        cfg.notify_space();
    }
}

/// Pop one frame from the socket, honoring flags/timeout.
pub(crate) fn pop_recv_frame(sock: &OmqSocket, flags: c_int) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    // Drain leftover frames from a partially-consumed multipart message.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
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

    // Inproc bypass path: peek from byte ring, wrap in Bytes.
    // Used by zmq_msg_recv (which needs an owned Bytes).
    // zmq_recv uses recv_bypass_direct instead (zero alloc).
    clear_stale_bypass(&sock.bypass_recv);
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    if let Some(bypass) = unsafe { sock.bypass_recv.get() } {
        // Drain yring first (messages from before bypass was installed,
        // or multipart messages that went through the regular tokio path
        // because the send-side bypass was skipped for SNDMORE batches).
        // SAFETY: same socket-thread invariant as above.
        if let Some(cons) = unsafe { sock.recv_cons.get() }
            && let Some(m) = try_pop_dual(cons, sock)
        {
            signal_recv_space(sock);
            return decompose_message(sock, &m);
        }
        if let Some(entry) = bypass.peek() {
            let (ptr, len) = entry;
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let bytes = Bytes::copy_from_slice(slice);
            bypass.advance(len);
            return Ok((bytes, false));
        }
        if dontwait {
            return Err(libc::EAGAIN);
        }
        // Fall through to the blocking recv path below: the message
        // might arrive via the regular tokio path (yring/dual consumer)
        // rather than the bypass ring.
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv_cons.get() }) else {
        return Err(ETERM);
    };

    if let Some(m) = try_pop_dual(cons, sock) {
        signal_recv_space(sock);
        return decompose_message(sock, &m);
    }

    if dontwait {
        return Err(libc::EAGAIN);
    }

    let m = block_recv(sock, rcvtimeo, || {
        let m = try_pop_dual(cons, sock)?;
        signal_recv_space(sock);
        Some(m)
    })?;
    decompose_message(sock, &m)
}

/// Zero-alloc recv for the inproc bypass: peek from byte ring,
/// copy directly into the user's buffer, advance.
fn recv_bypass_direct(
    sock: &OmqSocket,
    bypass: &mut crate::inproc_bypass::BypassRecv,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> Result<c_int, c_int> {
    use std::sync::atomic::Ordering;

    // Drain leftover frames from a partially-consumed multipart message.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        if let Some(frame) = drain.pop_front() {
            let more = !drain.is_empty();
            if !more {
                sock.drain_nonempty.store(false, Ordering::Relaxed);
            }
            let frame_len = frame.len();
            copy_to_buf(buf, buf_len, &frame);
            return checked_c_int_len(frame_len);
        }
        sock.drain_nonempty.store(false, Ordering::Relaxed);
    }

    // Drain yring first (multipart messages that went through omq-tokio).
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    if let Some(cons) = unsafe { sock.recv_cons.get() }
        && let Some(m) = try_pop_dual(cons, sock)
    {
        signal_recv_space(sock);
        let data = m.get(0).unwrap_or(&[]);
        copy_to_buf(buf, buf_len, data);
        stash_remaining_parts(sock, &m, 0);
        return checked_c_int_len(data.len());
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;

    if let Some(n) = try_recv_bypass_or_yring(sock, bypass, buf, buf_len)? {
        return Ok(n);
    }

    if dontwait {
        return Err(libc::EAGAIN);
    }

    let n = block_recv_result(sock, rcvtimeo, || {
        try_recv_bypass_or_yring(sock, bypass, buf, buf_len)
    })?;
    Ok(n)
}

/// Try byte ring first, then pump yring. Returns payload length on success.
#[inline]
fn try_recv_bypass_or_yring(
    sock: &OmqSocket,
    bypass: &mut crate::inproc_bypass::BypassRecv,
    buf: *mut libc::c_void,
    buf_len: usize,
) -> Result<Option<c_int>, c_int> {
    if let Some((ptr, len)) = bypass.peek() {
        let copy_len = len.min(buf_len);
        if !buf.is_null() && copy_len > 0 {
            // SAFETY: ptr/len valid for peeked entry; buf/buf_len from caller contract.
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, buf.cast::<u8>(), copy_len);
            }
        }
        bypass.advance(len);
        return checked_c_int_len(len).map(Some);
    }
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    if let Some(cons) = unsafe { sock.recv_cons.get() }
        && let Some(m) = try_pop_dual(cons, sock)
    {
        signal_recv_space(sock);
        let data = m.get(0).unwrap_or(&[]);
        let frame_len = data.len();
        copy_to_buf(buf, buf_len, data);
        stash_remaining_parts(sock, &m, 0);
        return checked_c_int_len(frame_len).map(Some);
    }
    if bypass
        .pipe
        .closed
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Err(ETERM);
    }
    Ok(None)
}

#[inline]
fn try_pop_dual(
    cons: &mut crate::socket::RecvConsumers,
    sock: &crate::socket::OmqSocket,
) -> Option<omq_tokio::Message> {
    if cons.fast.is_disconnected()
        && let Some(cfg) = sock.recv_sink_config.get()
        && let Some(new_cons) = cfg.try_take_pending_consumer()
    {
        cons.fast = new_cons;
    }
    cons.fast
        .prefetch_and_pop()
        .map(omq_tokio::engine::RecvItem::into_message)
        .or_else(|| {
            cons.pump
                .prefetch_and_pop()
                .map(omq_tokio::engine::RecvItem::into_message)
        })
}

#[inline]
fn msg_start_index(sock: &OmqSocket, msg: &omq_tokio::Message) -> usize {
    usize::from(sock.socket_type == omq_tokio::SocketType::Dish && msg.len() >= 2)
}

fn stash_remaining_parts(sock: &OmqSocket, msg: &omq_tokio::Message, start: usize) {
    let nparts = msg.len();
    let next = start + 1;
    if next < nparts {
        sock.drain_nonempty
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut drain = sock.recv_drain.lock().expect("recv_drain");
        for i in next..nparts {
            if let Some(b) = msg.part_bytes(i) {
                drain.push_back(b);
            }
        }
    }
}

fn decompose_message(sock: &OmqSocket, msg: &omq_tokio::Message) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    let dish = sock.socket_type == omq_tokio::SocketType::Dish;
    let nparts = msg.len();

    if nparts <= 1 && !dish {
        let head = msg.part_bytes(0).unwrap_or_default();
        return Ok((head, false));
    }

    let start = usize::from(dish && nparts >= 2);
    let head = msg.part_bytes(start).unwrap_or_default();

    let remaining = start + 1;
    if remaining < nparts {
        sock.drain_nonempty.store(true, Ordering::Relaxed);
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        for i in remaining..nparts {
            if let Some(b) = msg.part_bytes(i) {
                drain.push_back(b);
            }
        }
    }

    Ok((head, remaining < nparts))
}

fn copy_to_buf(buf: *mut libc::c_void, buf_len: usize, src: &[u8]) {
    if buf.is_null() || buf_len == 0 {
        return;
    }
    let copy_len = src.len().min(buf_len);
    // SAFETY: buf is non-null with buf_len writable bytes; copy_len <= buf_len.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), buf.cast::<u8>(), copy_len);
    }
}
