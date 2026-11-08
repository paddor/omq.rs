//! Native recv ring storage and entry points.

use super::RING_BATCH;
use crate::cancel::OmqGoCancel;
use crate::error::{CANCELED, CLOSED, CONFIG};
use crate::ffi::{OmqGoRecvRingMemory, OmqGoStatus};
use crate::ring::codec::{encode_message, encoded_message_len, write_message_encoded};
use crate::socket::OmqGoSocket;
use crate::socket::recv::recv_many_into;
use crate::stats::{RECV_RINGS_CREATED, RECV_RINGS_FREED, bump};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    _reserved0: u64,
    _reserved1: u64,
}

struct RecvExternalBlock {
    cursor: usize,
    _bytes: Box<[u8]>,
}

pub struct OmqGoRecvRing {
    socket: BlockingSocket,
    control: Box<RecvRingControl>,
    desc: Box<[RecvRingDesc]>,
    payload: Box<[u8]>,
    desc_mask: usize,
    payload_mask: usize,
    cursor: usize,
    cached_head: usize,
    reclaimed_head: usize,
    payload_cursor: usize,
    payload_head: usize,
    scratch: Vec<Message>,
    external: VecDeque<RecvExternalBlock>,
}

const RECV_RING_FLAG_MULTIPART: u64 = 1;

const RECV_RING_FLAG_EXTERNAL: u64 = 2;

impl OmqGoRecvRing {
    fn new(socket: BlockingSocket, desc_capacity: usize, payload_capacity: usize) -> Self {
        let desc_capacity = desc_capacity.max(1).next_power_of_two();
        let payload_capacity = payload_capacity.max(1).next_power_of_two();
        Self {
            socket,
            control: Box::new(RecvRingControl::new()),
            desc: vec![RecvRingDesc::default(); desc_capacity].into_boxed_slice(),
            payload: vec![0; payload_capacity].into_boxed_slice(),
            desc_mask: desc_capacity - 1,
            payload_mask: payload_capacity - 1,
            cursor: 0,
            cached_head: 0,
            reclaimed_head: 0,
            payload_cursor: 0,
            payload_head: 0,
            scratch: Vec::with_capacity(desc_capacity.min(RING_BATCH)),
            external: VecDeque::new(),
        }
    }

    fn memory(&self) -> OmqGoRecvRingMemory {
        OmqGoRecvRingMemory {
            control: self.control.as_ref() as *const RecvRingControl as *mut c_void,
            descriptors: self.desc.as_ptr() as *mut c_void,
            payload: self.payload.as_ptr() as *mut c_void,
            desc_capacity: self.desc.len(),
            payload_capacity: self.payload.len(),
        }
    }

    fn reclaim_consumed(&mut self) {
        let head = self.control.head.load(Ordering::Acquire);
        while self.reclaimed_head != head {
            let index = self.reclaimed_head & self.desc_mask;
            let desc = self.desc[index];
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
        if self.cursor.wrapping_sub(self.cached_head) < self.desc.len() {
            return false;
        }
        self.cached_head = self.control.head.load(Ordering::Acquire);
        self.cursor.wrapping_sub(self.cached_head) >= self.desc.len()
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
                _reserved0: 0,
                _reserved1: 0,
            };
        } else {
            let bytes = encode_message(message).into_boxed_slice();
            let addr = bytes.as_ptr() as u64;
            self.external.push_back(RecvExternalBlock {
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
                _reserved0: 0,
                _reserved1: 0,
            };
        }

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

    fn fill_with<F>(&mut self, max_messages: usize, receive: F) -> Result<Option<usize>, Error>
    where
        F: FnOnce(&BlockingSocket, usize, &mut Vec<Message>) -> Result<Option<usize>, Error>,
    {
        if max_messages == 0 {
            return Err(Error::Config(
                "max messages must be greater than zero".to_string(),
            ));
        }

        self.reclaim_consumed();
        if self.desc_is_full() {
            return Err(Error::WouldBlock);
        }

        let desc_space = self.desc.len() - self.cursor.wrapping_sub(self.cached_head);
        let batch_max = max_messages.min(desc_space).min(RING_BATCH);
        self.scratch.clear();
        if receive(&self.socket, batch_max, &mut self.scratch)?.is_none() {
            return Ok(None);
        }

        let start_cursor = self.cursor;
        self.publish_scratch();
        let filled = self.flush(start_cursor);
        if filled == 0 {
            return Err(Error::WouldBlock);
        }
        Ok(Some(filled))
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

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_recv_ring_create(
    socket: *mut OmqGoSocket,
    desc_capacity: usize,
    payload_capacity: usize,
    out: *mut *mut OmqGoRecvRing,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive ring output pointer");
    }
    unsafe {
        *out = ptr::null_mut();
    }
    if desc_capacity == 0 {
        return OmqGoStatus::err(CONFIG, "receive ring descriptor capacity must be positive");
    }
    if payload_capacity == 0 {
        return OmqGoStatus::err(CONFIG, "receive ring payload capacity must be positive");
    }

    let socket = unsafe { &*socket };
    match socket.materialize() {
        Ok(native) => {
            let ring = OmqGoRecvRing::new(native, desc_capacity, payload_capacity);
            unsafe {
                *out = Box::into_raw(Box::new(ring));
            }
            bump(&RECV_RINGS_CREATED);
            OmqGoStatus::ok()
        }
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_recv_ring_memory(
    ring: *mut OmqGoRecvRing,
    out: *mut OmqGoRecvRingMemory,
) -> OmqGoStatus {
    if ring.is_null() {
        return OmqGoStatus::err(CLOSED, "receive ring closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null receive ring memory pointer");
    }
    unsafe {
        *out = (&*ring).memory();
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_recv_ring_fill(
    ring: *mut OmqGoRecvRing,
    timeout_millis: i64,
    max_messages: usize,
) -> OmqGoStatus {
    if ring.is_null() {
        return OmqGoStatus::err(CLOSED, "receive ring closed");
    }
    let ring = unsafe { &mut *ring };
    match ring.fill_with(max_messages, |socket, max, out| {
        recv_many_into(socket, max, timeout_millis, out).map(Some)
    }) {
        Ok(_) => OmqGoStatus::ok(),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_recv_ring_fill_cancelable(
    ring: *mut OmqGoRecvRing,
    cancel: *const OmqGoCancel,
    max_messages: usize,
) -> OmqGoStatus {
    if ring.is_null() {
        return OmqGoStatus::err(CLOSED, "receive ring closed");
    }
    if cancel.is_null() {
        return OmqGoStatus::err(CONFIG, "receive cancel handle is null");
    }
    let ring = unsafe { &mut *ring };
    let cancel = unsafe { &*cancel };
    match ring.fill_with(max_messages, |socket, max, out| {
        socket.recv_many_registered_cancelable_into(max, cancel.token(), out)
    }) {
        Ok(Some(_)) => OmqGoStatus::ok(),
        Ok(None) => OmqGoStatus::err(CANCELED, "operation canceled"),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_recv_ring_close(ring: *mut OmqGoRecvRing) {
    if ring.is_null() {
        return;
    }
    bump(&RECV_RINGS_FREED);
    unsafe {
        drop(Box::from_raw(ring));
    }
}

#[cfg(test)]
mod tests;
