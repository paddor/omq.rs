//! Receive operations and buffer conversion.

use crate::cancel::OmqGoCancel;
use crate::convert::{bytes_from_c_mut, duration_from_timeout_millis};
use crate::error::{AGAIN, CANCELED, CLOSED, CONFIG, TIMEOUT, status_from_result};
use crate::ffi::{OmqGoMessage, OmqGoStatus};
use crate::message::message_to_c;
use crate::socket::OmqGoSocket;
use omq_tokio::blocking::{BlockingRecvCancel, Socket as BlockingSocket};
use omq_tokio::{Error, Message};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{ptr, slice};

pub(crate) const RECV_BATCH: usize = 64;

pub(crate) fn recv_many_into(
    native: &BlockingSocket,
    max: usize,
    timeout_millis: i64,
    out: &mut Vec<Message>,
) -> Result<usize, Error> {
    match timeout_millis {
        i64::MIN..=-1 => native.recv_many_into(max, out),
        0 => native.try_recv_many_into(max, out),
        _ => native.recv_many_timeout_into(max, Duration::from_millis(timeout_millis as u64), out),
    }
}

fn refill_recv_cache(socket: &OmqGoSocket, timeout_millis: i64) -> Result<(), Error> {
    let native = socket.materialize()?;
    let mut scratch = socket
        .recv_scratch
        .lock()
        .map_err(|_| Error::Config("receive scratch lock poisoned".to_string()))?;
    scratch.clear();

    let received = recv_many_into(&native, RECV_BATCH, timeout_millis, &mut scratch)?;

    if received == 0 {
        return Err(Error::WouldBlock);
    }

    let mut cache = socket
        .recv_cache
        .lock()
        .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?;
    cache.extend(scratch.drain(..));
    Ok(())
}

fn recv_one(socket: &OmqGoSocket, timeout_millis: i64) -> Result<Message, Error> {
    if socket.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }

    if let Some(message) = socket
        .recv_cache
        .lock()
        .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?
        .pop_front()
    {
        return Ok(message);
    }

    refill_recv_cache(socket, timeout_millis)?;
    socket
        .recv_cache
        .lock()
        .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?
        .pop_front()
        .ok_or(Error::WouldBlock)
}

fn recv_one_cancelable(
    socket: &OmqGoSocket,
    cancel: &BlockingRecvCancel,
) -> Result<Option<Message>, Error> {
    if socket.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }

    if let Some(message) = socket
        .recv_cache
        .lock()
        .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?
        .pop_front()
    {
        return Ok(Some(message));
    }

    let native = socket.materialize()?;
    cancel.register_current_thread_once();
    let mut scratch = socket
        .recv_scratch
        .lock()
        .map_err(|_| Error::Config("receive scratch lock poisoned".to_string()))?;
    scratch.clear();
    let Some(_) = native.recv_many_registered_cancelable_into(RECV_BATCH, cancel, &mut scratch)?
    else {
        return Ok(None);
    };

    let mut cache = socket
        .recv_cache
        .lock()
        .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?;
    cache.extend(scratch.drain(..));
    Ok(cache.pop_front())
}

struct ReceiveAnyEntry<'a> {
    index: usize,
    socket: &'a OmqGoSocket,
    native: BlockingSocket,
}

fn try_receive_any_entry(
    entries: &[ReceiveAnyEntry<'_>],
) -> Result<Option<(usize, Message)>, Error> {
    for entry in entries {
        if entry.socket.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(message) = entry
            .socket
            .recv_cache
            .lock()
            .map_err(|_| Error::Config("receive cache lock poisoned".to_string()))?
            .pop_front()
        {
            return Ok(Some((entry.index, message)));
        }
        match entry.native.try_recv() {
            Ok(message) => return Ok(Some((entry.index, message))),
            Err(Error::WouldBlock | Error::Timeout) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

async fn receive_any_loop(
    entries: Vec<ReceiveAnyEntry<'_>>,
    timeout: Option<Duration>,
) -> Result<Option<(usize, Message)>, Error> {
    if let Some(event) = try_receive_any_entry(&entries)? {
        return Ok(Some(event));
    }
    if timeout.is_some_and(|timeout| timeout.is_zero()) {
        return Ok(None);
    }
    // Async receives are cancel-safe: the losers consume nothing.
    let receives = entries.iter().map(|entry| {
        let index = entry.index;
        let socket = entry.native.clone_shared().into_async();
        Box::pin(async move { socket.recv().await.map(|message| (index, message)) })
    });
    let any = futures::future::select_all(receives);
    match timeout {
        None => any.await.0.map(Some),
        Some(timeout) => match tokio::time::timeout(timeout, any).await {
            Ok((result, _, _)) => result.map(Some),
            Err(_) => Ok(None),
        },
    }
}

fn copy_message_into(message: &Message, destination: &mut [u8]) -> Result<usize, Error> {
    if message.len() != 1 {
        return Err(Error::Config(
            "RecvInto requires a single-part message".to_string(),
        ));
    }
    let part = message
        .part_slice(0)
        .ok_or_else(|| Error::Config("missing message part".to_string()))?;
    if part.len() > destination.len() {
        return Err(Error::MessageTooLarge {
            size: part.len(),
            max: destination.len(),
        });
    }
    destination[..part.len()].copy_from_slice(part);
    Ok(part.len())
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_receive_any(
    sockets: *const *mut OmqGoSocket,
    socket_count: usize,
    timeout_millis: i64,
    index: *mut usize,
    out: *mut OmqGoMessage,
) -> OmqGoStatus {
    if socket_count == 0 {
        return OmqGoStatus::err(CONFIG, "receive-any requires at least one socket");
    }
    if sockets.is_null() || index.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive-any pointer");
    }
    unsafe {
        *index = 0;
        *out = OmqGoMessage {
            parts: ptr::null_mut(),
            part_count: 0,
            routing_id: 0,
        };
    }

    let raw_sockets = unsafe { slice::from_raw_parts(sockets, socket_count) };
    let mut entries = Vec::with_capacity(socket_count);
    for (entry_index, socket) in raw_sockets.iter().copied().enumerate() {
        if socket.is_null() {
            return OmqGoStatus::err(CONFIG, "receive-any socket is null");
        }
        let socket = unsafe { &*socket };
        if socket.closed.load(Ordering::Acquire) {
            return OmqGoStatus::err(CLOSED, "socket closed");
        }
        match socket.materialize() {
            Ok(native) => entries.push(ReceiveAnyEntry {
                index: entry_index,
                socket,
                native,
            }),
            Err(error) => return OmqGoStatus::from_error(error),
        }
    }

    let timeout = duration_from_timeout_millis(timeout_millis);
    let ctx = entries[0].socket.ctx.clone();
    match ctx.block_on(receive_any_loop(entries, timeout)) {
        Ok(Some((event_index, message))) => unsafe {
            *index = event_index;
            *out = message_to_c(message);
            OmqGoStatus::ok()
        },
        Ok(None) => {
            if timeout_millis == 0 {
                OmqGoStatus::err(AGAIN, "operation would block")
            } else {
                OmqGoStatus::err(TIMEOUT, "operation timed out")
            }
        }
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_recv(
    socket: *mut OmqGoSocket,
    timeout_millis: i64,
    out: *mut OmqGoMessage,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive output pointer");
    }
    let socket = unsafe { &*socket };
    match recv_one(socket, timeout_millis) {
        Ok(message) => {
            unsafe {
                *out = message_to_c(message);
            }
            OmqGoStatus::ok()
        }
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_recv_cancelable(
    socket: *mut OmqGoSocket,
    cancel: *const OmqGoCancel,
    out: *mut OmqGoMessage,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive output pointer");
    }
    if cancel.is_null() {
        return OmqGoStatus::err(CONFIG, "receive cancel handle is null");
    }
    let socket = unsafe { &*socket };
    let cancel = unsafe { &*cancel };
    match recv_one_cancelable(socket, cancel.token()) {
        Ok(Some(message)) => {
            unsafe {
                *out = message_to_c(message);
            }
            OmqGoStatus::ok()
        }
        Ok(None) => OmqGoStatus::err(CANCELED, "operation canceled"),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_recv_one_into(
    socket: *mut OmqGoSocket,
    timeout_millis: i64,
    data: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if written.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive length pointer");
    }
    unsafe {
        *written = 0;
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let destination = bytes_from_c_mut(data, capacity)?;
        let message = recv_one(socket, timeout_millis)?;
        let copied = copy_message_into(&message, destination)?;
        unsafe {
            *written = copied;
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_recv_one_into_cancelable(
    socket: *mut OmqGoSocket,
    cancel: *const OmqGoCancel,
    data: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if written.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive length pointer");
    }
    if cancel.is_null() {
        return OmqGoStatus::err(CONFIG, "receive cancel handle is null");
    }
    unsafe {
        *written = 0;
    }
    let socket = unsafe { &*socket };
    let cancel = unsafe { &*cancel };
    let result = (|| -> Result<Option<usize>, Error> {
        let destination = bytes_from_c_mut(data, capacity)?;
        let Some(message) = recv_one_cancelable(socket, cancel.token())? else {
            return Ok(None);
        };
        let copied = copy_message_into(&message, destination)?;
        Ok(Some(copied))
    })();
    match result {
        Ok(Some(copied)) => {
            unsafe {
                *written = copied;
            }
            OmqGoStatus::ok()
        }
        Ok(None) => OmqGoStatus::err(CANCELED, "operation canceled"),
        Err(error) => OmqGoStatus::from_error(error),
    }
}
