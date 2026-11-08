//! Native recv ring storage and entry points.

use crate::error::{
    RECV_RING_STATUS_CLOSED, RECV_RING_STATUS_ERROR, RECV_RING_STATUS_OK, clear_ffm_last_error,
    cstring_lossy, empty_cstring, guard, recv_ring_status, set_ffm_last_error, throw_omq,
};
use crate::identity::identity_object;
use crate::ring::codec::{encode_message, encoded_message_len, write_message_encoded};
use crate::socket::recv::recv_many_into;
use crate::socket::socket_from_handle;
use jni::JNIEnv;
use jni::objects::{JClass, JObject};
use jni::sys::{jlong, jobject};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Most messages one fill receives.
const RECV_BATCH_MESSAGES: usize = 256;

#[repr(C, align(128))]
struct RecvRingControl {
    head: AtomicUsize,
    _pad0: [u8; 120],
    tail: AtomicUsize,
    _pad1: [u8; 120],
}

impl RecvRingControl {
    fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
            _pad0: [0; 120],
            tail: AtomicUsize::new(0),
            _pad1: [0; 120],
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct RecvRingDesc {
    payload: u64,
    payload_len: u64,
    total_len: u64,
    part_count: u64,
    flags: u64,
    payload_end: u64,
    routing_id: u64,
    _reserved1: u64,
}

struct ExternalBlock {
    cursor: usize,
    _bytes: Box<[u8]>,
}

struct JavaRecvRing {
    socket: BlockingSocket,
    control: Box<RecvRingControl>,
    desc: Box<[RecvRingDesc]>,
    identities: Box<[Option<omq_proto::Identity>]>,
    payload: Box<[u8]>,
    desc_mask: usize,
    payload_mask: usize,
    cursor: usize,
    cached_head: usize,
    reclaimed_head: usize,
    payload_cursor: usize,
    payload_head: usize,
    scratch: Vec<Message>,
    external: VecDeque<ExternalBlock>,
    last_error_code: Cell<i32>,
    last_error_message: RefCell<CString>,
}

const RECV_RING_FLAG_MULTIPART: u64 = 1;

const RECV_RING_FLAG_EXTERNAL: u64 = 2;

impl JavaRecvRing {
    fn new(socket: BlockingSocket, desc_capacity: usize, payload_capacity: usize) -> Self {
        let desc_capacity = desc_capacity.max(1).next_power_of_two();
        let payload_capacity = payload_capacity.max(1).next_power_of_two();
        Self {
            socket,
            control: Box::new(RecvRingControl::new()),
            desc: vec![RecvRingDesc::default(); desc_capacity].into_boxed_slice(),
            identities: vec![None; desc_capacity].into_boxed_slice(),
            payload: vec![0; payload_capacity].into_boxed_slice(),
            desc_mask: desc_capacity - 1,
            payload_mask: payload_capacity - 1,
            cursor: 0,
            cached_head: 0,
            reclaimed_head: 0,
            payload_cursor: 0,
            payload_head: 0,
            scratch: Vec::with_capacity(desc_capacity.min(RECV_BATCH_MESSAGES)),
            external: VecDeque::new(),
            last_error_code: Cell::new(RECV_RING_STATUS_OK),
            last_error_message: RefCell::new(empty_cstring()),
        }
    }

    fn control_addr(&self) -> i64 {
        self.control.as_ref() as *const RecvRingControl as i64
    }

    fn desc_addr(&self) -> i64 {
        self.desc.as_ptr() as i64
    }

    fn payload_addr(&self) -> i64 {
        self.payload.as_ptr() as i64
    }

    fn desc_capacity(&self) -> i32 {
        self.desc.len() as i32
    }

    fn payload_capacity(&self) -> i64 {
        self.payload.len() as i64
    }

    fn error_message_addr(&self) -> i64 {
        self.last_error_message.borrow().as_ptr() as i64
    }

    fn set_error(&self, code: i32, message: impl AsRef<str>) {
        self.last_error_code.set(code);
        *self.last_error_message.borrow_mut() = cstring_lossy(message.as_ref());
    }

    fn clear_error(&self) {
        self.last_error_code.set(RECV_RING_STATUS_OK);
        *self.last_error_message.borrow_mut() = empty_cstring();
    }

    fn reclaim_consumed(&mut self) {
        let head = self.control.head.load(Ordering::Acquire);
        while self.reclaimed_head != head {
            let index = self.reclaimed_head & self.desc_mask;
            let desc = self.desc[index];
            self.identities[index] = None;
            if desc.flags & RECV_RING_FLAG_EXTERNAL != 0 {
                if self
                    .external
                    .front()
                    .is_some_and(|block| block.cursor == self.reclaimed_head)
                {
                    self.external.pop_front();
                }
            } else {
                self.payload_head = desc.payload_end as usize;
            }
            self.reclaimed_head = self.reclaimed_head.wrapping_add(1);
        }
        self.cached_head = head;
    }

    fn desc_is_full(&mut self) -> bool {
        if self.cursor.wrapping_sub(self.cached_head) >= self.desc.len() {
            self.cached_head = self.control.head.load(Ordering::Acquire);
            self.cursor.wrapping_sub(self.cached_head) >= self.desc.len()
        } else {
            false
        }
    }

    fn reserve_payload(&mut self, len: usize) -> Option<(usize, usize)> {
        if len == 0 {
            return Some((0, self.payload_cursor));
        }
        if len > self.payload.len() {
            return None;
        }

        let mut cursor = self.payload_cursor;
        let mut offset = cursor & self.payload_mask;
        let mut needed = len;
        if offset + len > self.payload.len() {
            let pad = self.payload.len() - offset;
            cursor = cursor.wrapping_add(pad);
            needed = needed.wrapping_add(pad);
            offset = 0;
        }

        if cursor.wrapping_add(len).wrapping_sub(self.payload_head) > self.payload.len() {
            return None;
        }

        self.payload_cursor = self.payload_cursor.wrapping_add(needed);
        Some((offset, cursor.wrapping_add(len)))
    }

    fn publish(&mut self, message: &Message) -> bool {
        if self.desc_is_full() {
            return false;
        }

        let part_count = message.len();
        let total_len = message.byte_len();
        let encoded_len = encoded_message_len(message);
        let cursor = self.cursor;
        let index = cursor & self.desc_mask;
        let flags = if part_count > 1 {
            RECV_RING_FLAG_MULTIPART
        } else {
            0
        };

        if let Some((offset, payload_end)) = self.reserve_payload(encoded_len) {
            write_message_encoded(message, &mut self.payload[offset..offset + encoded_len]);
            self.desc[index] = RecvRingDesc {
                payload: offset as u64,
                payload_len: encoded_len as u64,
                total_len: total_len as u64,
                part_count: part_count as u64,
                flags,
                payload_end: payload_end as u64,
                routing_id: message.routing_id().unwrap_or(0) as u64,
                _reserved1: 0,
            };
        } else {
            let bytes = encode_message(message).into_boxed_slice();
            let addr = bytes.as_ptr() as u64;
            self.external.push_back(ExternalBlock {
                cursor,
                _bytes: bytes,
            });
            self.desc[index] = RecvRingDesc {
                payload: addr,
                payload_len: encoded_len as u64,
                total_len: total_len as u64,
                part_count: part_count as u64,
                flags: flags | RECV_RING_FLAG_EXTERNAL,
                payload_end: self.payload_cursor as u64,
                routing_id: message.routing_id().unwrap_or(0) as u64,
                _reserved1: 0,
            };
        }

        self.identities[index] = message.sender_identity().cloned();
        self.cursor = self.cursor.wrapping_add(1);
        true
    }

    fn flush(&self, old_cursor: usize) -> usize {
        if self.cursor == old_cursor {
            return 0;
        }
        self.control.tail.store(self.cursor, Ordering::Release);
        self.cursor.wrapping_sub(old_cursor)
    }

    fn fill(&mut self, timeout_millis: i64, max_messages: i32) -> Result<usize, Error> {
        if max_messages <= 0 {
            return Err(Error::Config(
                "maxMessages must be greater than zero".to_string(),
            ));
        }

        self.fill_with(max_messages as usize, |socket, max, out| {
            recv_many_into(socket, max as i32, timeout_millis, out)
        })
    }

    fn fill_with<F>(&mut self, max_messages: usize, receive: F) -> Result<usize, Error>
    where
        F: FnOnce(&BlockingSocket, usize, &mut Vec<Message>) -> Result<usize, Error>,
    {
        self.reclaim_consumed();
        if self.desc_is_full() {
            return Ok(0);
        }

        let desc_space = self.desc.len() - self.cursor.wrapping_sub(self.cached_head);
        let batch_max = max_messages.min(desc_space).min(RECV_BATCH_MESSAGES);
        self.scratch.clear();
        receive(&self.socket, batch_max, &mut self.scratch)?;

        let start_cursor = self.cursor;
        self.publish_scratch();
        Ok(self.flush(start_cursor))
    }

    /// Publishes the received batch. Receives never return more than the
    /// free descriptor count, so every publish succeeds.
    fn publish_scratch(&mut self) {
        let mut batch = std::mem::take(&mut self.scratch);
        for message in &batch {
            let published = self.publish(message);
            debug_assert!(published, "receive batch exceeded descriptor space");
        }
        batch.clear();
        self.scratch = batch;
    }
}

fn recv_ring_from_handle<'a>(handle: i64) -> Result<&'a mut JavaRecvRing, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &mut *(handle as *mut JavaRecvRing) })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_create(
    socket_handle: i64,
    desc_capacity: i32,
    payload_capacity: i64,
) -> i64 {
    clear_ffm_last_error();
    let result = catch_unwind(AssertUnwindSafe(|| {
        if desc_capacity <= 0 {
            return Err(Error::Config(
                "descCapacity must be greater than zero".to_string(),
            ));
        }
        if payload_capacity <= 0 {
            return Err(Error::Config(
                "payloadCapacity must be greater than zero".to_string(),
            ));
        }
        let socket = socket_from_handle(socket_handle)?.materialize()?;
        Ok(Box::into_raw(Box::new(JavaRecvRing::new(
            socket,
            desc_capacity as usize,
            payload_capacity as usize,
        ))) as i64)
    }));

    match result {
        Ok(Ok(handle)) => handle,
        Ok(Err(error)) => {
            set_ffm_last_error(recv_ring_status(&error), error.to_string());
            0
        }
        Err(_) => {
            set_ffm_last_error(RECV_RING_STATUS_ERROR, "native OMQ panic");
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_close(handle: i64) {
    if handle == 0 {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(handle as *mut JavaRecvRing));
    }));
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_control_addr(handle: i64) -> i64 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.control_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_desc_addr(handle: i64) -> i64 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.desc_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_payload_addr(handle: i64) -> i64 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.payload_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_desc_capacity(handle: i64) -> i32 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.desc_capacity())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_payload_capacity(handle: i64) -> i64 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.payload_capacity())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_error_code(handle: i64) -> i32 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.last_error_code.get())
        .unwrap_or(RECV_RING_STATUS_CLOSED)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_error_message(handle: i64) -> i64 {
    recv_ring_from_handle(handle)
        .map(|ring| ring.error_message_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_recv_ring_fill(
    handle: i64,
    timeout_millis: i64,
    max_messages: i32,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let ring = recv_ring_from_handle(handle)?;
        match ring.fill(timeout_millis, max_messages) {
            Ok(_) => {
                ring.clear_error();
                Ok(RECV_RING_STATUS_OK)
            }
            Err(error) => {
                let status = recv_ring_status(&error);
                ring.set_error(status, error.to_string());
                Ok(status)
            }
        }
    }));

    match result {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => recv_ring_status(&error),
        Err(_) => RECV_RING_STATUS_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_recvRingIdentity(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    cursor: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            // The Java socket serializes fill/read/release under its existing lock.
            let ring = recv_ring_from_handle(handle)?;
            let cursor = cursor as usize;
            if cursor.wrapping_sub(ring.reclaimed_head)
                >= ring.cursor.wrapping_sub(ring.reclaimed_head)
            {
                return Err(Error::Config("receive ring cursor is not readable".into()));
            }
            let identity = ring.identities[cursor & ring.desc_mask]
                .as_ref()
                .ok_or_else(|| Error::Config("message has no sender snapshot".into()))?;
            identity_object(env, identity).map(JObject::into_raw)
        })();
        match result {
            Ok(identity) => identity,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[cfg(test)]
mod tests;
