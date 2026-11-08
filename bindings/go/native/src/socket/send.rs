//! Send operations.

use crate::cancel::OmqGoCancel;
use crate::convert::bytes_from_c;
use crate::error::{CANCELED, CLOSED, CONFIG, TIMEOUT, status_from_result};
use crate::ffi::{OmqGoPart, OmqGoStatus, OmqGoWireMessage};
use crate::message::{message_from_c, messages_from_c};
use crate::socket::OmqGoSocket;
use omq_proto::TrySendError;
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message};
use std::sync::atomic::Ordering;
use std::time::Duration;

fn try_send_with_timeout(
    native: &BlockingSocket,
    message: Message,
    timeout_millis: i64,
) -> OmqGoStatus {
    if timeout_millis == 0 {
        return match native.try_send(message) {
            Ok(()) => OmqGoStatus::ok(),
            Err(error) => OmqGoStatus::from_try_send(error),
        };
    }

    if timeout_millis < 0 {
        return status_from_result(native.send(message));
    }

    match native.send_timeout(message, Duration::from_millis(timeout_millis as u64)) {
        Ok(()) => OmqGoStatus::ok(),
        Err(Error::Timeout) => OmqGoStatus::err(TIMEOUT, "operation timed out"),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_send(
    socket: *mut OmqGoSocket,
    parts: *const OmqGoPart,
    part_count: usize,
    routing_id: u32,
    timeout_millis: i64,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        if socket.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let native = socket.materialize()?;
        let message = message_from_c(parts, part_count, routing_id)?;
        Ok((native, message))
    })();
    match result {
        Ok((native, message)) => try_send_with_timeout(&native, message, timeout_millis),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

/// Send, parking on a muted socket until accepted or `cancel` fires.
#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_send_cancelable(
    socket: *mut OmqGoSocket,
    cancel: *const OmqGoCancel,
    parts: *const OmqGoPart,
    part_count: usize,
    routing_id: u32,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if cancel.is_null() {
        return OmqGoStatus::err(CONFIG, "send cancel handle is null");
    }
    let socket = unsafe { &*socket };
    let cancel = unsafe { &*cancel };
    let result = (|| {
        if socket.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let native = socket.materialize()?;
        let message = message_from_c(parts, part_count, routing_id)?;
        native.send_cancelable(message, cancel.token())
    })();
    match result {
        Ok(true) => OmqGoStatus::ok(),
        Ok(false) => OmqGoStatus::err(CANCELED, "operation canceled"),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_send_one(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
    routing_id: u32,
    timeout_millis: i64,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        if socket.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let native = socket.materialize()?;
        let mut message = Message::from_slice(bytes_from_c(data, len)?);
        if routing_id != 0 {
            message = message.with_routing_id(routing_id);
        }
        Ok((native, message))
    })();
    match result {
        Ok((native, message)) => try_send_with_timeout(&native, message, timeout_millis),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_try_send_batch(
    socket: *mut OmqGoSocket,
    messages: *const OmqGoWireMessage,
    message_count: usize,
    sent: *mut usize,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if sent.is_null() {
        return OmqGoStatus::err(CONFIG, "null sent output pointer");
    }
    unsafe {
        *sent = 0;
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        if socket.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let native = socket.materialize()?;
        let mut queue = messages_from_c(messages, message_count)?;
        match native.try_send_many(&mut queue, message_count) {
            Ok(count) => {
                unsafe {
                    *sent = count;
                }
                Ok(())
            }
            Err(error) => Err(match error {
                TrySendError::Full(_) => Error::WouldBlock,
                TrySendError::Closed => Error::Closed,
                TrySendError::Error(error) => error,
            }),
        }
    })();
    status_from_result(result)
}
