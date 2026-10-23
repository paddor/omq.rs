//! `zmq_recv`, frame popping, and authenticated receives.

use super::{ZmqIovec, block_recv_result, checked_c_int_len};
use crate::consts::ZMQ_DONTWAIT;
use crate::error::{ETERM, fail};
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;
use bytes::Bytes;
use omq_tokio::flow::DrainBudget;
use std::ffi::c_int;
use std::sync::Arc;

/// Receive one frame, truncating the copy to buffer capacity; return its full length or -1
/// with errno.
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
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = match unsafe { crate::handle::socket(sock_ptr) } {
        Ok(socket) => socket,
        Err(errno) => return fail(errno),
    };
    if sock.ctx.is_effectively_terminated() {
        return fail(ETERM);
    }
    if buf.is_null() && buf_len > 0 {
        return fail(libc::EFAULT);
    }

    zmq_recv_impl(sock, buf, buf_len, flags)
}

/// Unsupported vectored receive entry point; return -1 with ENOTSUP for valid arguments.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_recviov(
    sock_ptr: *mut libc::c_void,
    iov: *mut ZmqIovec,
    count: *mut usize,
    _flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    if iov.is_null() || count.is_null() {
        return fail(libc::EFAULT);
    }
    fail(crate::error::ENOTSUP)
}

fn zmq_recv_impl(sock: &OmqSocket, buf: *mut libc::c_void, buf_len: usize, flags: c_int) -> c_int {
    use std::sync::atomic::Ordering;

    if sock.zap_handler.load(Ordering::Acquire) {
        return zmq_recv_via_frame(sock, buf, buf_len, flags);
    }

    if authenticated_recv_configured(sock) {
        return match pop_recv_frame_with_properties(sock, flags) {
            Ok((frame, _, _)) => {
                let frame_len = frame.len();
                copy_to_buf(buf, buf_len, &frame);
                match checked_c_int_len(frame_len) {
                    Ok(size) => size,
                    Err(error) => fail(error),
                }
            }
            Err(error) => fail(error),
        };
    }

    // Multipart drain: leftover frames use the Bytes-returning path.
    if sock.recv.drain_nonempty.load(Ordering::Relaxed) {
        return zmq_recv_via_frame(sock, buf, buf_len, flags);
    }

    // Fast path: pop Message from yring, borrow first frame directly.
    // Avoids the Bytes::copy_from_slice that pop_recv_frame/decompose_message
    // would do for inline messages.
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv.consumers.get() }) else {
        return fail(ETERM);
    };

    if let Some(popped) = try_pop_dual(cons, sock) {
        signal_recv_space_if_full(sock, popped.released_full_slot);
        return recv_msg_to_buf(sock, &popped.message, buf, buf_len);
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;
    if dontwait {
        return fail(libc::EAGAIN);
    }

    match block_recv_result(sock, rcvtimeo, || {
        let Some(popped) = try_pop_dual(cons, sock) else {
            return Ok(None);
        };
        signal_recv_space_if_full(sock, popped.released_full_slot);
        Ok(Some(recv_msg_to_buf(sock, &popped.message, buf, buf_len)))
    }) {
        Ok(n) => n,
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

/// Signal the recv pump that space is available in the recv ring after a
/// full-ring pop. Avoid waking the producer on every normal recv.
#[inline]
pub(super) fn signal_recv_space_if_full(sock: &OmqSocket, released_full_slot: bool) {
    if released_full_slot && let Some(cfg) = sock.recv.sink_config.get() {
        cfg.notify_space();
    }
}

fn pop_zap_frame(sock: &OmqSocket, rcvtimeo: i64, dontwait: bool) -> Result<(Bytes, bool), c_int> {
    if let Some(frame) = sock.ctx.zap.try_recv_frame(sock.id)? {
        return Ok(frame);
    }
    if dontwait {
        return Err(libc::EAGAIN);
    }
    block_recv_result(sock, rcvtimeo, || sock.ctx.zap.try_recv_frame(sock.id))
}

pub(crate) fn authenticated_recv_configured(sock: &OmqSocket) -> bool {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    unsafe { sock.recv.authenticated.get() }.is_some()
}

pub(crate) fn authenticated_recv_has_data(sock: &OmqSocket) -> bool {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let queued = unsafe { sock.recv.authenticated.get() }
        .as_ref()
        .is_some_and(|receiver| !receiver.is_empty());
    // SAFETY: same socket-thread invariant as above.
    queued || !unsafe { sock.recv.authenticated_drain.get() }.is_empty()
}

pub(crate) fn authenticated_recv_has_more(sock: &OmqSocket) -> bool {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    !unsafe { sock.recv.authenticated_drain.get() }.is_empty()
}

type AuthenticatedMessage = (omq_tokio::Message, Arc<omq_tokio::proto::PeerProperties>);

pub(super) fn try_recv_authenticated_message(
    sock: &OmqSocket,
) -> Result<Option<AuthenticatedMessage>, c_int> {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(receiver) = (unsafe { sock.recv.authenticated.get() }).as_mut() else {
        return Err(ETERM);
    };
    let mut budget = DrainBudget::WORKER;
    while !budget.exhausted() {
        match receiver.try_recv() {
            Ok(item) => {
                let (message, properties) = item.into_parts();
                let _ = budget.account(message.byte_len());
                if let Some(message) = prepare_external_recv(sock, message) {
                    return Ok(Some((message, properties)));
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Err(ETERM),
        }
    }
    Ok(None)
}

fn pop_authenticated_message(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<AuthenticatedMessage, c_int> {
    let rcvtimeo = sock.rcvtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    if let Some(item) = try_recv_authenticated_message(sock)? {
        return Ok(item);
    }
    if flags & ZMQ_DONTWAIT != 0 || rcvtimeo == 0 {
        return Err(libc::EAGAIN);
    }
    block_recv_result(sock, rcvtimeo, || try_recv_authenticated_message(sock))
}

/// Pop one frame while retaining ZAP properties for `zmq_msg_gets`.
pub(crate) fn pop_recv_frame_with_properties(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<(Bytes, bool, Option<Arc<omq_tokio::proto::PeerProperties>>), c_int> {
    if !authenticated_recv_configured(sock) {
        return pop_recv_frame(sock, flags).map(|(frame, more)| (frame, more, None));
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let drain = unsafe { sock.recv.authenticated_drain.get() };
    if let Some((frame, properties)) = drain.pop_front() {
        return Ok((frame, !drain.is_empty(), Some(properties)));
    }

    let item = pop_authenticated_message(sock, flags)?;
    let (message, properties) = item;
    let start = msg_start_index(sock, &message);
    let frame = message.part_bytes(start).unwrap_or_default();
    for index in start + 1..message.len() {
        if let Some(part) = message.part_bytes(index) {
            drain.push_back((part, properties.clone()));
        }
    }
    Ok((frame, !drain.is_empty(), Some(properties)))
}

/// Pop one frame from the socket, honoring flags/timeout.
pub(crate) fn pop_recv_frame(sock: &OmqSocket, flags: c_int) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;
    if sock.zap_handler.load(Ordering::Acquire) {
        return pop_zap_frame(sock, rcvtimeo, dontwait);
    }

    // Drain leftover frames from a partially-consumed multipart message.
    if sock.recv.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv.drain.lock() else {
            return Err(ETERM);
        };
        if let Some(frame) = drain.pop_front() {
            let more = !drain.is_empty();
            if !more {
                sock.recv.drain_nonempty.store(false, Ordering::Relaxed);
            }
            return Ok((frame, more));
        }
        sock.recv.drain_nonempty.store(false, Ordering::Relaxed);
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv.consumers.get() }) else {
        return Err(ETERM);
    };

    if let Some(popped) = try_pop_dual(cons, sock) {
        signal_recv_space_if_full(sock, popped.released_full_slot);
        return decompose_message(sock, &popped.message);
    }

    if dontwait {
        return Err(libc::EAGAIN);
    }

    block_recv_result(sock, rcvtimeo, || {
        let Some(popped) = try_pop_dual(cons, sock) else {
            return Ok(None);
        };
        signal_recv_space_if_full(sock, popped.released_full_slot);
        decompose_message(sock, &popped.message).map(Some)
    })
}

/// Pop one complete SERVER message so `zmq_msg_recv` can preserve routing ID
/// metadata. SERVER messages are always single-part.
pub(crate) fn pop_recv_server_message(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<omq_tokio::Message, c_int> {
    use std::sync::atomic::Ordering;

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv.consumers.get() }) else {
        return Err(ETERM);
    };
    if let Some(popped) = try_pop_dual(cons, sock) {
        signal_recv_space_if_full(sock, popped.released_full_slot);
        return Ok(popped.message);
    }
    if dontwait {
        return Err(libc::EAGAIN);
    }
    block_recv_result(sock, rcvtimeo, || {
        let Some(popped) = try_pop_dual(cons, sock) else {
            return Ok(None);
        };
        signal_recv_space_if_full(sock, popped.released_full_slot);
        Ok(Some(popped.message))
    })
}

pub(crate) fn pop_recv_server_message_with_properties(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<
    (
        omq_tokio::Message,
        Option<Arc<omq_tokio::proto::PeerProperties>>,
    ),
    c_int,
> {
    if authenticated_recv_configured(sock) {
        let item = pop_authenticated_message(sock, flags)?;
        let (message, properties) = item;
        return Ok((message, Some(properties)));
    }
    pop_recv_server_message(sock, flags).map(|message| (message, None))
}

pub(super) struct PoppedMessage {
    pub(super) message: omq_tokio::Message,
    pub(super) released_full_slot: bool,
}

#[inline]
pub(super) fn try_pop_dual(
    cons: &mut crate::socket::RecvConsumers,
    sock: &crate::socket::OmqSocket,
) -> Option<PoppedMessage> {
    cons.refresh(sock.recv.sink_config.get());
    let mut released_full_slot = false;
    let mut budget = DrainBudget::WORKER;
    while !budget.exhausted() {
        let Some((message, released)) = cons.try_pop() else {
            break;
        };
        released_full_slot |= released;
        let _ = budget.account(message.byte_len());
        settle_recv_fd(sock, cons);
        if let Some(message) = prepare_external_recv(sock, message) {
            return Some(PoppedMessage {
                message,
                released_full_slot,
            });
        }
    }
    signal_recv_space_if_full(sock, released_full_slot);
    if !cons.fast.is_empty() || !cons.pump.is_empty() {
        sock.notify.recv_notifier().signal();
    }
    None
}

fn prepare_external_recv(
    sock: &OmqSocket,
    message: omq_tokio::Message,
) -> Option<omq_tokio::Message> {
    sock.inner.get()?.prepare_external_recv(message)
}

/// Keep `ZMQ_FD` level-triggered: once both rings are empty, reset the
/// readiness counter. A message that lands in between is signaled again.
fn settle_recv_fd(sock: &OmqSocket, cons: &crate::socket::RecvConsumers) {
    if !cons.fast.is_empty() || !cons.pump.is_empty() {
        return;
    }
    let notify = sock.notify.recv_notifier();
    notify.drain();
    if !cons.fast.is_empty() || !cons.pump.is_empty() {
        notify.signal();
    }
}

#[inline]
fn msg_start_index(sock: &OmqSocket, msg: &omq_tokio::Message) -> usize {
    if sock.socket_type == omq_tokio::SocketType::Dish && msg.len() >= 2 {
        return 1;
    }
    0
}

fn stash_remaining_parts(sock: &OmqSocket, msg: &omq_tokio::Message, start: usize) {
    let nparts = msg.len();
    let next = start + 1;
    if next < nparts {
        sock.recv
            .drain_nonempty
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut drain = sock.recv.drain.lock().expect("recv_drain");
        for i in next..nparts {
            if let Some(b) = msg.part_bytes(i) {
                drain.push_back(b);
            }
        }
    }
}

fn decompose_message(sock: &OmqSocket, msg: &omq_tokio::Message) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    let nparts = msg.len();

    let start = msg_start_index(sock, msg);
    if nparts <= 1 && start == 0 {
        let head = msg.part_bytes(0).unwrap_or_default();
        return Ok((head, false));
    }

    let head = msg.part_bytes(start).unwrap_or_default();

    let remaining = start + 1;
    if remaining < nparts {
        sock.recv.drain_nonempty.store(true, Ordering::Relaxed);
        let Ok(mut drain) = sock.recv.drain.lock() else {
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
