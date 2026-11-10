//! `zmq_recv`, frame popping, and authenticated receives.

use super::{RecvWait, ZmqIovec, block_recv_result, checked_c_int_len};
use crate::error::{ETERM, c_try, fail};
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
    let sock = c_try!(unsafe { crate::handle::socket(sock_ptr) });
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

    if sock.is_zap_handler() {
        return zmq_recv_via_frame(sock, buf, buf_len, flags);
    }

    if authenticated_recv_configured(sock) {
        let part = c_try!(pop_recv_frame_with_properties(sock, flags));
        return c_try!(copy_out(buf, buf_len, &part.frame));
    }

    // Multipart drain: leftover frames use the Bytes-returning path.
    if sock.recv.drain_nonempty.load(Ordering::Relaxed) {
        return zmq_recv_via_frame(sock, buf, buf_len, flags);
    }

    // Fast path: pop Message from yring, borrow first frame directly.
    // Avoids the Bytes::copy_from_slice that pop_recv_frame/decompose_message
    // would do for inline messages.
    c_try!(pop_message(sock, flags, |message| {
        recv_msg_to_buf(sock, &message, buf, buf_len)
    }))
}

/// Borrow the first frame of a Message and copy into the user buffer.
/// Zero heap allocation for inline messages.
#[inline]
fn recv_msg_to_buf(
    sock: &OmqSocket,
    m: &omq_tokio::Message,
    buf: *mut libc::c_void,
    buf_len: usize,
) -> Result<c_int, c_int> {
    let start = msg_start_index(sock, m);
    stash_remaining_parts(sock, m, start)?;
    copy_out(buf, buf_len, m.get(start).unwrap_or(&[]))
}

/// Fallback for `zmq_recv` when multipart drain is non-empty.
fn zmq_recv_via_frame(
    sock: &OmqSocket,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> c_int {
    let (frame, _more) = c_try!(pop_recv_frame(sock, flags));
    c_try!(copy_out(buf, buf_len, &frame))
}

/// Pop one message from the direct receive rings and `map` it, waiting per
/// `flags` and `ZMQ_RCVTIMEO` while both rings are empty.
#[inline]
fn pop_message<T>(
    sock: &OmqSocket,
    flags: c_int,
    mut map: impl FnMut(omq_tokio::Message) -> Result<T, c_int>,
) -> Result<T, c_int> {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv.consumers.get() }) else {
        return Err(ETERM);
    };

    if let Some(message) = try_pop_dual(cons, sock) {
        return map(message);
    }

    block_recv_result(sock, RecvWait::new(sock, flags), || {
        try_pop_dual(cons, sock).map(&mut map).transpose()
    })
}

/// Signal the recv pump that space is available in the recv ring after a
/// full-ring pop. Avoid waking the producer on every normal recv.
#[inline]
fn signal_recv_space_if_full(sock: &OmqSocket, released_full_slot: bool) {
    if released_full_slot && let Some(cfg) = sock.recv.sink_config.get() {
        cfg.notify_space();
    }
}

fn pop_zap_frame(sock: &OmqSocket, wait: RecvWait) -> Result<(Bytes, bool), c_int> {
    if let Some(frame) = sock.ctx.zap.try_recv_frame(sock.id)? {
        return Ok(frame);
    }

    block_recv_result(sock, wait, || sock.ctx.zap.try_recv_frame(sock.id))
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
    let wait = RecvWait::new(sock, flags);
    if let Some(item) = try_recv_authenticated_message(sock)? {
        return Ok(item);
    }

    block_recv_result(sock, wait, || try_recv_authenticated_message(sock))
}

/// Pop one frame while retaining ZAP properties for `zmq_msg_gets`.
pub(crate) fn pop_recv_frame_with_properties(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<ReceivedPart, c_int> {
    if !authenticated_recv_configured(sock) {
        return pop_recv_frame(sock, flags)
            .map(|(frame, more)| ReceivedPart::new(frame, more, None));
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let drain = unsafe { sock.recv.authenticated_drain.get() };
    if let Some((frame, properties)) = drain.pop_front() {
        return Ok(ReceivedPart::new(
            frame,
            !drain.is_empty(),
            Some(properties),
        ));
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

    Ok(ReceivedPart::new(
        frame,
        !drain.is_empty(),
        Some(properties),
    ))
}

/// Pop one frame from the socket, honoring flags/timeout.
pub(crate) fn pop_recv_frame(sock: &OmqSocket, flags: c_int) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    if sock.is_zap_handler() {
        return pop_zap_frame(sock, RecvWait::new(sock, flags));
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

    pop_message(sock, flags, |message| decompose_message(sock, &message))
}

/// Pop one complete SERVER message so `zmq_msg_recv` can preserve routing ID
/// metadata. SERVER messages are always single-part.
pub(crate) fn pop_recv_server_message(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<omq_tokio::Message, c_int> {
    pop_message(sock, flags, Ok)
}

/// Pop one SERVER message with its routing ID and any ZAP properties.
pub(crate) fn pop_recv_server_message_with_properties(
    sock: &OmqSocket,
    flags: c_int,
) -> Result<ReceivedPart, c_int> {
    let (message, properties) = if authenticated_recv_configured(sock) {
        let (message, properties) = pop_authenticated_message(sock, flags)?;
        (message, Some(properties))
    } else {
        (pop_recv_server_message(sock, flags)?, None)
    };

    let mut part = ReceivedPart::new(message.part_bytes(0).unwrap_or_default(), false, properties);
    part.routing_id = message.routing_id().unwrap_or(0);
    Ok(part)
}

/// One received frame with the metadata `zmq_msg_recv` stores.
pub(crate) struct ReceivedPart {
    pub(crate) frame: Bytes,
    pub(crate) more: bool,
    /// SERVER routing ID; zero for other socket types.
    pub(crate) routing_id: u32,
    /// ZAP peer properties of an authenticated connection.
    pub(crate) properties: Option<Arc<omq_tokio::proto::PeerProperties>>,
}

impl ReceivedPart {
    fn new(
        frame: Bytes,
        more: bool,
        properties: Option<Arc<omq_tokio::proto::PeerProperties>>,
    ) -> Self {
        Self {
            frame,
            more,
            routing_id: 0,
            properties,
        }
    }
}

#[inline]
pub(super) fn try_pop_dual(
    cons: &mut crate::socket::RecvConsumers,
    sock: &crate::socket::OmqSocket,
) -> Option<omq_tokio::Message> {
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
            signal_recv_space_if_full(sock, released_full_slot);
            return Some(message);
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

/// Queue the frames after `start` for the following receives; return
/// whether any follow. `ETERM` when the drain lock is poisoned.
fn stash_remaining_parts(
    sock: &OmqSocket,
    msg: &omq_tokio::Message,
    start: usize,
) -> Result<bool, c_int> {
    let nparts = msg.len();
    let next = start + 1;

    if next >= nparts {
        return Ok(false);
    }

    sock.recv
        .drain_nonempty
        .store(true, std::sync::atomic::Ordering::Relaxed);

    let Ok(mut drain) = sock.recv.drain.lock() else {
        return Err(ETERM);
    };

    for i in next..nparts {
        if let Some(b) = msg.part_bytes(i) {
            drain.push_back(b);
        }
    }

    Ok(true)
}

fn decompose_message(sock: &OmqSocket, msg: &omq_tokio::Message) -> Result<(Bytes, bool), c_int> {
    let start = msg_start_index(sock, msg);
    let head = msg.part_bytes(start).unwrap_or_default();
    let more = stash_remaining_parts(sock, msg, start)?;

    Ok((head, more))
}

/// Copy `data` into the caller buffer, truncating; return its full length.
fn copy_out(buf: *mut libc::c_void, buf_len: usize, data: &[u8]) -> Result<c_int, c_int> {
    copy_to_buf(buf, buf_len, data);
    checked_c_int_len(data.len())
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
