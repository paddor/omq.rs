//! Message conversion and foreign buffer ownership.

use crate::convert::bytes_from_c;
use crate::ffi::{OmqGoMessage, OmqGoPart, OmqGoWireMessage};
use bytes::Bytes;
use omq_tokio::{Error, Message};
use std::collections::VecDeque;
use std::{ptr, slice};

pub(crate) fn message_from_c(
    parts: *const OmqGoPart,
    part_count: usize,
    routing_id: u32,
) -> Result<Message, Error> {
    let message = if part_count == 0 {
        Message::new()
    } else {
        if parts.is_null() {
            return Err(Error::Config("null message parts pointer".to_string()));
        }
        let parts = unsafe { slice::from_raw_parts(parts, part_count) };
        if part_count == 1 {
            let part = &parts[0];
            Message::from_slice(bytes_from_c(part.data, part.len)?)
        } else {
            let mut copied = Vec::with_capacity(part_count);
            for part in parts {
                copied.push(Bytes::copy_from_slice(bytes_from_c(part.data, part.len)?));
            }
            Message::multipart(copied)
        }
    };
    Ok(if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id)
    })
}

pub(crate) fn messages_from_c(
    messages: *const OmqGoWireMessage,
    message_count: usize,
) -> Result<VecDeque<Message>, Error> {
    if message_count == 0 {
        return Ok(VecDeque::new());
    }
    if messages.is_null() {
        return Err(Error::Config("null message batch pointer".to_string()));
    }
    let messages = unsafe { slice::from_raw_parts(messages, message_count) };
    let mut out = VecDeque::with_capacity(message_count);
    for message in messages {
        out.push_back(message_from_c(
            message.parts,
            message.part_count,
            message.routing_id,
        )?);
    }
    Ok(out)
}

pub(crate) fn raw_bytes(bytes: &[u8]) -> (*mut u8, usize) {
    if bytes.is_empty() {
        return (ptr::null_mut(), 0);
    }
    let mut boxed = bytes.to_vec().into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    (data, len)
}

pub(crate) fn message_to_c(message: Message) -> OmqGoMessage {
    let routing_id = message.routing_id().unwrap_or(0);
    let part_count = message.len();
    if part_count == 0 {
        return OmqGoMessage {
            parts: ptr::null_mut(),
            part_count: 0,
            routing_id,
        };
    }

    let mut parts = Vec::with_capacity(part_count);
    for index in 0..part_count {
        let bytes = message.part_bytes(index).unwrap_or_default();
        let (data, len) = raw_bytes(&bytes);
        parts.push(OmqGoPart { data, len });
    }
    let mut boxed = parts.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);
    OmqGoMessage {
        parts: ptr,
        part_count: len,
        routing_id,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_message_free(message: OmqGoMessage) {
    if message.parts.is_null() || message.part_count == 0 {
        return;
    }
    let parts = unsafe { slice::from_raw_parts_mut(message.parts, message.part_count) };
    for part in parts {
        if !part.data.is_null() && part.len > 0 {
            unsafe {
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                    part.data, part.len,
                )));
            }
        }
    }
    unsafe {
        drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
            message.parts,
            message.part_count,
        )));
    }
}
