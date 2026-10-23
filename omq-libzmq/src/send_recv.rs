//! `zmq_send` / `zmq_recv` entry points.
//!
//! Send: direct `Handle::block_on(socket.send())`, no relay.
//! Recv: yring consumers -> block on `RecvNotify`.
use std::ffi::c_int;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::consts::{ZMQ_DONTWAIT, ZMQ_SNDMORE};
use crate::error::{ETERM, fail, map_omq_err};
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;

mod recv;

pub(crate) use recv::{
    authenticated_recv_configured, authenticated_recv_has_data, authenticated_recv_has_more,
    pop_recv_frame_with_properties, pop_recv_server_message_with_properties,
};
use recv::{signal_recv_space_if_full, try_pop_dual, try_recv_authenticated_message};
pub use recv::{zmq_recv, zmq_recviov};

#[repr(C)]
#[derive(Debug)]
pub struct ZmqIovec {
    pub iov_base: *mut libc::c_void,
    pub iov_len: usize,
}

fn checked_c_int_len(n: usize) -> Result<c_int, c_int> {
    c_int::try_from(n).map_err(|_| libc::EMSGSIZE)
}

pub(crate) enum SendMessageAttempt {
    Sent,
    Full(omq_tokio::Message),
}

/// Non-blocking full-message receive used by `zmq_proxy`.
///
/// This reads the same libzmq-facing queues as `zmq_recv`: the direct yring
/// consumers. It must not call
/// `omq_tokio::Socket::try_recv`, because libzmq sockets install a custom
/// recv sink and the async socket recv pipe is not the owner of that hot path.
pub(crate) fn try_recv_message(sock: &OmqSocket) -> Result<Option<omq_tokio::Message>, c_int> {
    use std::sync::atomic::Ordering;

    if sock.zap_handler.load(Ordering::Acquire) {
        return sock
            .ctx
            .zap
            .try_recv_message(sock.id)
            .map(|message| message.map(omq_tokio::Message::multipart));
    }

    if authenticated_recv_configured(sock) {
        let item = try_recv_authenticated_message(sock)?;
        return Ok(item.map(|item| item.0));
    }

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

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let Some(cons) = (unsafe { sock.recv_cons.get() }) else {
        return Err(ETERM);
    };
    if let Some(popped) = try_pop_dual(cons, sock) {
        signal_recv_space_if_full(sock, popped.released_full_slot);
        return Ok(Some(popped.message));
    }
    Ok(None)
}

/// Non-blocking full-message send used by `zmq_proxy`.
///
/// Messages go through the materialized omq-tokio socket so type-specific
/// send behavior, including XSUB raw subscribe commands, stays in one place.
pub(crate) fn try_send_message(
    sock: &Arc<OmqSocket>,
    msg: omq_tokio::Message,
) -> Result<SendMessageAttempt, c_int> {
    if sock.zap_handler.load(std::sync::atomic::Ordering::Acquire) {
        let response: Vec<Bytes> = (0..msg.len())
            .filter_map(|index| msg.part_bytes(index))
            .collect();
        sock.ctx.zap.respond(sock.id, &response)?;
        return Ok(SendMessageAttempt::Sent);
    }
    let Some(inner) = sock.inner.get() else {
        return Err(ETERM);
    };
    if round_robin_send_mutes_without_ready_peer(sock)
        && !can_queue_without_ready_peer(sock)
        && inner.ready_peer_count() == 0
    {
        return Ok(SendMessageAttempt::Full(msg));
    }
    match inner.try_send(msg) {
        Ok(()) => Ok(SendMessageAttempt::Sent),
        Err(omq_tokio::TrySendError::Full(msg)) => Ok(SendMessageAttempt::Full(msg)),
        Err(omq_tokio::TrySendError::Closed) => Err(ETERM),
        Err(omq_tokio::TrySendError::Error(e)) => Err(map_send_err(sock, &e)),
    }
}

/// libzmq `ZMQ_POLLOUT`: whether a `ZMQ_DONTWAIT` send would be accepted.
pub(crate) fn send_ready(sock: &OmqSocket) -> bool {
    if sock.zap_handler.load(std::sync::atomic::Ordering::Acquire) {
        return sock.ctx.zap.can_send(sock.id);
    }
    let Some(inner) = sock.inner.get() else {
        return false;
    };
    if waits_for_ready_peer(sock) && inner.ready_peer_count() == 0 {
        return false;
    }
    inner.send_ready()
}

/// A one-shot wait that signals `sock`'s send eventfd once [`send_ready`]
/// may have turned true. Dropping it cancels the wait.
#[derive(Debug)]
pub(crate) struct SendWake(tokio::task::AbortHandle);

impl Drop for SendWake {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Arm a [`SendWake`]. `None` when nothing could make `sock` writable.
pub(crate) fn arm_send_wake(sock: &OmqSocket) -> Option<SendWake> {
    let inner = sock.inner.get()?.clone();
    let gated = waits_for_ready_peer(sock);
    let notify = sock.notify.clone();
    let task = sock.ctx.handle()?.spawn(async move {
        loop {
            if gated && inner.wait_connected(1, Duration::MAX).await.is_err() {
                break;
            }
            inner.wait_send_ready().await;
            if !gated || inner.ready_peer_count() > 0 {
                break;
            }
        }
        notify.signal_send();
    });
    Some(SendWake(task.abort_handle()))
}

/// A send of one message running on its socket's runtime. Dropping it
/// abandons the send.
#[derive(Debug)]
pub(crate) struct SpawnedSend {
    result: Arc<std::sync::Mutex<Option<omq_tokio::Result<()>>>>,
    task: tokio::task::AbortHandle,
}

impl SpawnedSend {
    /// The send's result once accepted or failed. Stored before the
    /// socket's send eventfd is signaled.
    pub(crate) fn take_result(&self) -> Option<omq_tokio::Result<()>> {
        self.result.lock().expect("spawned send result").take()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.result.lock().expect("spawned send result").is_some()
    }
}

impl Drop for SpawnedSend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Send `msg` once accepted, on the socket's runtime. Mirrors the libzmq
/// gating of [`try_send_message`], then signals `sock`'s send eventfd.
pub(crate) fn spawn_send_message(
    sock: &OmqSocket,
    msg: omq_tokio::Message,
) -> Result<SpawnedSend, c_int> {
    let inner = sock.inner.get().ok_or(ETERM)?.clone();
    let handle = sock.ctx.handle().ok_or(ETERM)?;
    let gated = waits_for_ready_peer(sock);
    let notify = sock.notify.clone();
    let result = Arc::new(std::sync::Mutex::new(None));
    let stored = result.clone();
    let task = handle.spawn(async move {
        let sent = if gated {
            match inner.wait_connected(1, Duration::MAX).await {
                Ok(_) => inner.send(msg).await,
                Err(error) => Err(error),
            }
        } else {
            inner.send(msg).await
        };
        *stored.lock().expect("spawned send result") = Some(sent);
        notify.signal_send();
    });
    Ok(SpawnedSend {
        result,
        task: task.abort_handle(),
    })
}

/// libzmq types that mute without a ready peer unless they may queue.
fn waits_for_ready_peer(sock: &OmqSocket) -> bool {
    round_robin_send_mutes_without_ready_peer(sock) && !can_queue_without_ready_peer(sock)
}

fn send_zap_bytes(sock: &OmqSocket, data: &[u8], flags: c_int, ret_len: c_int) -> c_int {
    if !sock.ctx.zap.can_send(sock.id) {
        return fail(crate::error::EFSM);
    }
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let accum = unsafe { sock.send_accum.get() };
    if flags & ZMQ_SNDMORE != 0 {
        accum.push(Bytes::copy_from_slice(data));
        return ret_len;
    }
    let mut response = std::mem::take(accum);
    response.push(Bytes::copy_from_slice(data));
    match sock.ctx.zap.respond(sock.id, &response) {
        Ok(()) => ret_len,
        Err(error) => fail(error),
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

fn can_queue_without_ready_peer(sock: &OmqSocket) -> bool {
    if sock
        .connect_count
        .load(std::sync::atomic::Ordering::Acquire)
        == 0
    {
        return false;
    }
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    *unsafe { sock.queue_without_ready_peer.get() }
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
        Err(errno) => Err(errno),
    }
}

fn ensure_libzmq_send_route(sock: &OmqSocket, flags: c_int, sndtimeo: i64) -> Result<(), c_int> {
    if matches!(
        sock.socket_type,
        omq_tokio::SocketType::Req | omq_tokio::SocketType::Rep
    ) && (sock
        .drain_nonempty
        .load(std::sync::atomic::Ordering::Relaxed)
        || authenticated_recv_has_more(sock))
    {
        return Err(crate::error::EFSM);
    }
    if !round_robin_send_mutes_without_ready_peer(sock) {
        return Ok(());
    }
    if can_queue_without_ready_peer(sock) {
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

fn block_recv_result<T>(
    sock: &OmqSocket,
    rcvtimeo: i64,
    mut try_pop: impl FnMut() -> Result<Option<T>, c_int>,
) -> Result<T, c_int> {
    if rcvtimeo > 0 {
        let deadline = Instant::now().checked_add(Duration::from_millis(rcvtimeo as u64));
        loop {
            if sock.ctx.is_effectively_terminated() {
                return Err(ETERM);
            }
            let ms = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(libc::EAGAIN);
                    }
                    remaining.as_millis().min(i32::MAX as u128) as c_int
                }
                None => i32::MAX,
            };
            let recv_notify = sock.notify.recv_notifier();
            if recv_notify.wait_for_readable(ms) {
                recv_notify.drain();
            }
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
        if recv_notify.wait_for_readable(100) {
            recv_notify.drain();
        }
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

    if sock.zap_handler.load(std::sync::atomic::Ordering::Acquire) {
        return send_zap_bytes(sock, data, flags, ret_len);
    }

    // XSUB: intercept subscription frames.
    if sock.socket_type == omq_tokio::SocketType::XSub && !data.is_empty() {
        return send_xsub_subscription(sock, data, ret_len);
    }

    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    if let Err(e) = ensure_libzmq_send_route(sock, flags, sndtimeo) {
        return fail(e);
    }

    if sock.socket_type == omq_tokio::SocketType::Stream {
        return send_stream_frame(sock, data, ret_len, flags, sndtimeo);
    }

    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let accum = unsafe { sock.send_accum.get() };
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let retry = unsafe { sock.send_retry.get() };

    // If SNDMORE: buffer and return immediately.
    if flags & ZMQ_SNDMORE != 0 {
        if std::mem::take(retry) {
            // The caller restarted the message after a would-block, rather
            // than retrying its final frame.
            accum.clear();
        }
        if accum.is_empty()
            && let Err(errno) = admit_first_frame(sock, flags, sndtimeo)
        {
            return fail(errno);
        }
        accum.push(Bytes::copy_from_slice(data));
        return ret_len;
    }

    *retry = false;

    // Drain accumulated parts + current frame into one message.
    let msg = if accum.is_empty() {
        omq_tokio::Message::from_slice(data)
    } else {
        let mut v: Vec<Bytes> = std::mem::take(accum);
        v.push(Bytes::copy_from_slice(data));
        omq_tokio::Message::multipart(v)
    };

    match submit_message_or_return(sock, msg, ret_len, flags, sndtimeo) {
        Ok(rc) => rc,
        Err(unsent) => {
            // Messages are atomic: keep the staged frames so retrying this
            // final frame sends the whole message.
            *accum = (0..unsent.len().saturating_sub(1))
                .filter_map(|index| unsent.part_bytes(index))
                .collect();
            *retry = !accum.is_empty();
            fail(libc::EAGAIN)
        }
    }
}

/// XSUB data frames are subscription commands: a leading 0x01 subscribes,
/// 0x00 unsubscribes, and any other first byte subscribes to the whole frame.
fn send_xsub_subscription(sock: &Arc<OmqSocket>, data: &[u8], ret_len: c_int) -> c_int {
    if let Err(error) = crate::socket::ensure_materialized(sock) {
        return fail(error);
    }
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
    match result {
        Ok(Ok(())) => ret_len,
        Ok(Err(ref e)) => fail(crate::error::map_omq_err(e)),
        Err(errno) => fail(errno),
    }
}

/// STREAM frames come in [identity, data] pairs. The identity frame must
/// carry SNDMORE and is staged until its data frame arrives.
fn send_stream_frame(
    sock: &Arc<OmqSocket>,
    data: &[u8],
    ret_len: c_int,
    flags: c_int,
    sndtimeo: i64,
) -> c_int {
    // SAFETY: libzmq sockets are accessed by at most one application thread.
    let accum = unsafe { sock.send_accum.get() };

    if accum.is_empty() {
        if flags & ZMQ_SNDMORE == 0 {
            return fail(libc::EINVAL);
        }
        if let Err(error) = prepare_stream_route(sock, data, flags, sndtimeo) {
            return fail(error);
        }
        accum.push(Bytes::copy_from_slice(data));
        return ret_len;
    }

    let message = omq_tokio::Message::multipart([accum[0].clone(), Bytes::copy_from_slice(data)]);
    // STREAM data always ends the pair. Keep the identity on EAGAIN so
    // the caller can retry the same data frame.
    let result = submit_message(sock, message, ret_len, flags & !ZMQ_SNDMORE, sndtimeo);
    if result >= 0 || crate::error::zmq_errno() != libc::EAGAIN {
        accum.clear();
    }
    result
}

/// libzmq applies HWM at a message's first frame, so a would-block never
/// splits a message. Round-robin and exclusive sockets check readiness here,
/// before staging frames that could otherwise fail only at the last one.
fn admit_first_frame(sock: &Arc<OmqSocket>, flags: c_int, sndtimeo: i64) -> Result<(), c_int> {
    use omq_tokio::SocketType;
    if !matches!(
        sock.socket_type,
        SocketType::Push | SocketType::Dealer | SocketType::Req | SocketType::Pair
    ) {
        return Ok(());
    }
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;
    let Some(inner) = sock.inner.get() else {
        return Ok(());
    };
    if (!dontwait && sndtimeo < 0) || inner.send_ready() {
        return Ok(());
    }
    if dontwait {
        return Err(libc::EAGAIN);
    }
    let timeout = Duration::from_millis(sndtimeo as u64);
    let s = inner.clone();
    match crate::blocking::block_on(&sock.ctx, async move {
        tokio::time::timeout(timeout, s.wait_send_ready()).await
    })? {
        Ok(()) => Ok(()),
        Err(_elapsed) => Err(libc::EAGAIN),
    }
}

fn prepare_stream_route(
    sock: &Arc<OmqSocket>,
    identity: &[u8],
    flags: c_int,
    sndtimeo: i64,
) -> Result<(), c_int> {
    crate::socket::ensure_materialized(sock)?;
    let inner = sock.inner.get().ok_or(ETERM)?;
    match inner.check_stream_route(identity) {
        Ok(()) => return Ok(()),
        Err(omq_tokio::Error::WouldBlock) if flags & ZMQ_DONTWAIT == 0 && sndtimeo != 0 => {}
        Err(error) => return Err(map_omq_err(&error)),
    }
    let identity = Bytes::copy_from_slice(identity);
    let result = crate::socket::with_socket(&sock.ctx, inner, move |socket| async move {
        let ready = socket.wait_stream_route(identity);
        if sndtimeo > 0 {
            tokio::time::timeout(Duration::from_millis(sndtimeo as u64), ready)
                .await
                .unwrap_or(Err(omq_tokio::Error::WouldBlock))
        } else {
            ready.await
        }
    });
    result?.map_err(|error| map_omq_err(&error))
}

pub(crate) fn send_message(
    sock: &Arc<OmqSocket>,
    msg: omq_tokio::Message,
    ret_len: c_int,
    flags: c_int,
) -> c_int {
    let max = sock
        .ctx
        .max_msg_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && msg.byte_len() > max as usize {
        return fail(libc::EMSGSIZE);
    }
    if sock.zap_handler.load(std::sync::atomic::Ordering::Acquire) {
        let response: Vec<Bytes> = (0..msg.len())
            .filter_map(|index| msg.part_bytes(index))
            .collect();
        return match sock.ctx.zap.respond(sock.id, &response) {
            Ok(()) => ret_len,
            Err(error) => fail(error),
        };
    }
    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    if flags & ZMQ_SNDMORE != 0 {
        return fail(libc::EINVAL);
    }
    if let Err(e) = ensure_libzmq_send_route(sock, flags, sndtimeo) {
        return fail(e);
    }
    submit_message(sock, msg, ret_len, flags, sndtimeo)
}

pub(crate) fn map_send_err(sock: &OmqSocket, error: &omq_tokio::Error) -> c_int {
    if matches!(
        sock.socket_type,
        omq_tokio::SocketType::Req | omq_tokio::SocketType::Rep
    ) && matches!(error, omq_tokio::Error::Protocol(_))
    {
        crate::error::EFSM
    } else {
        map_omq_err(error)
    }
}

fn submit_message(
    sock: &Arc<OmqSocket>,
    msg: omq_tokio::Message,
    ret_len: c_int,
    flags: c_int,
    sndtimeo: i64,
) -> c_int {
    submit_message_or_return(sock, msg, ret_len, flags, sndtimeo)
        .unwrap_or_else(|_| fail(libc::EAGAIN))
}

/// Submit `msg`, or hand it back unsent when the send would block (EAGAIN).
fn submit_message_or_return(
    sock: &Arc<OmqSocket>,
    msg: omq_tokio::Message,
    ret_len: c_int,
    flags: c_int,
    sndtimeo: i64,
) -> Result<c_int, omq_tokio::Message> {
    let Some(inner) = sock.inner.get() else {
        return Ok(fail(ETERM));
    };
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;

    match inner.try_send(msg) {
        Ok(()) => Ok(ret_len),
        Err(omq_tokio::TrySendError::Closed) => Ok(fail(ETERM)),
        Err(omq_tokio::TrySendError::Error(ref error)) => Ok(fail(map_send_err(sock, error))),
        Err(omq_tokio::TrySendError::Full(msg)) if dontwait => Err(msg),
        Err(omq_tokio::TrySendError::Full(mut msg)) => {
            for i in 0..8 {
                if i < 4 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                match inner.try_send(msg) {
                    Ok(()) => return Ok(ret_len),
                    Err(omq_tokio::TrySendError::Closed) => return Ok(fail(ETERM)),
                    Err(omq_tokio::TrySendError::Error(ref error)) => {
                        return Ok(fail(map_send_err(sock, error)));
                    }
                    Err(omq_tokio::TrySendError::Full(returned)) => msg = returned,
                }
            }
            let s = inner.clone();
            let timeout = (sndtimeo > 0)
                .then(|| Duration::from_millis(sndtimeo as u64))
                .filter(|timeout| Instant::now().checked_add(*timeout).is_some());
            // Context shutdown ends either wait with ETERM.
            let Some(timeout) = timeout else {
                return Ok(match crate::blocking::block_on(&sock.ctx, s.send(msg)) {
                    Ok(Ok(())) => ret_len,
                    Ok(Err(_)) => fail(ETERM),
                    Err(errno) => fail(errno),
                });
            };
            // A timed-out send drops its copy; keep the original for EAGAIN.
            let attempt = msg.clone();
            match crate::blocking::block_on(&sock.ctx, async move {
                tokio::time::timeout(timeout, s.send(attempt)).await
            }) {
                Ok(Ok(Ok(()))) => Ok(ret_len),
                Ok(Ok(Err(_))) => Ok(fail(ETERM)),
                Ok(Err(_elapsed)) => Err(msg),
                Err(errno) => Ok(fail(errno)),
            }
        }
    }
}

/// Copy and send one frame with the requested flags; return its byte length or -1 with errno.
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
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = match unsafe { crate::handle::socket(sock_ptr) } {
        Ok(socket) => socket,
        Err(errno) => return fail(errno),
    };
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

/// Send one frame by copying caller storage; equivalent to `zmq_send`.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_send_const(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    zmq_send(sock_ptr, buf, len, flags)
}

/// Unsupported vectored send entry point; return -1 with ENOTSUP for valid arguments.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_sendiov(
    sock_ptr: *mut libc::c_void,
    iov: *mut ZmqIovec,
    count: usize,
    _flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    if iov.is_null() && count > 0 {
        return fail(libc::EFAULT);
    }
    fail(crate::error::ENOTSUP)
}

#[cfg(test)]
mod tests;
