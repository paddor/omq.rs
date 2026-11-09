//! Native send ring storage and entry points.
//!
//! NOTE: `bindings/java/native/src/ring/send.rs` duplicates this
//! ring logic. Apply every change or fix to both.

use super::RING_BATCH;
use crate::error::{CLOSED, CONFIG, OK, cstring_lossy, empty_cstring, status_code_from_error};
use crate::ffi::{OmqGoSendRingMemory, OmqGoStatus};
use crate::socket::OmqGoSocket;
use crate::stats::{SEND_RINGS_CREATED, SEND_RINGS_FREED, bump};
use bytes::Bytes;
use omq_proto::TrySendError;
use omq_tokio::blocking::{BlockingRecvCancel, Socket as BlockingSocket};
use omq_tokio::{Error, Message};
use std::collections::VecDeque;
use std::ffi::{CString, c_void};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{ptr, thread};

#[repr(C, align(128))]
struct SendRingControl {
    head: AtomicUsize,
    _pad0: [u8; 120],
    tail: AtomicUsize,
    _pad1: [u8; 120],
    closed: AtomicUsize,
    _pad2: [u8; 120],
    /// Set by the worker before it parks on an empty ring.
    worker_parked: AtomicUsize,
    _pad3: [u8; 120],
    /// Bumped by every wake so producer waits never miss one.
    wake_epoch: AtomicUsize,
    _pad4: [u8; 120],
    /// Set by the producer while it waits for ring space.
    producer_waiting: AtomicUsize,
    _pad5: [u8; 120],
}

impl SendRingControl {
    fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
            _pad0: [0; 120],
            tail: AtomicUsize::new(0),
            _pad1: [0; 120],
            closed: AtomicUsize::new(0),
            _pad2: [0; 120],
            worker_parked: AtomicUsize::new(0),
            _pad3: [0; 120],
            wake_epoch: AtomicUsize::new(0),
            _pad4: [0; 120],
            producer_waiting: AtomicUsize::new(0),
            _pad5: [0; 120],
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct SendRingDesc {
    payload: u64,
    payload_len: u64,
    payload_end: u64,
    _reserved0: u64,
    _reserved1: u64,
    _reserved2: u64,
    _reserved3: u64,
    _reserved4: u64,
}

pub struct OmqGoSendRing {
    shared: Arc<OmqGoSendRingShared>,
    worker: Option<JoinHandle<()>>,
}

struct OmqGoSendRingShared {
    socket: BlockingSocket,
    control: Box<SendRingControl>,
    desc: Box<[SendRingDesc]>,
    payload: Box<[u8]>,
    done: Box<[AtomicBool]>,
    desc_mask: usize,
    last_error_code: AtomicI32,
    last_error_message: Mutex<CString>,
    reclaim: Mutex<SendRingReclaim>,
    worker: OnceLock<thread::Thread>,
    /// Interrupts a worker send blocked on a muted socket at close.
    send_cancel: BlockingRecvCancel,
    space: Mutex<()>,
    space_changed: Condvar,
}

struct SendRingReclaim {
    cursor: usize,
}

struct SendSlotOwner {
    shared: Arc<OmqGoSendRingShared>,
    cursor: usize,
    offset: usize,
    len: usize,
}

impl OmqGoSendRing {
    fn new(socket: BlockingSocket, desc_capacity: usize, payload_capacity: usize) -> Self {
        let desc_capacity = desc_capacity.max(1).next_power_of_two();
        let payload_capacity = payload_capacity.max(1).next_power_of_two();
        let shared = Arc::new(OmqGoSendRingShared {
            socket,
            control: Box::new(SendRingControl::new()),
            desc: vec![SendRingDesc::default(); desc_capacity].into_boxed_slice(),
            payload: vec![0; payload_capacity].into_boxed_slice(),
            done: (0..desc_capacity).map(|_| AtomicBool::new(false)).collect(),
            desc_mask: desc_capacity - 1,
            last_error_code: AtomicI32::new(OK),
            last_error_message: Mutex::new(empty_cstring()),
            reclaim: Mutex::new(SendRingReclaim { cursor: 0 }),
            worker: OnceLock::new(),
            send_cancel: BlockingRecvCancel::new(),
            space: Mutex::new(()),
            space_changed: Condvar::new(),
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::spawn(move || send_ring_worker(worker_shared));
        let _ = shared.worker.set(worker.thread().clone());
        Self {
            shared,
            worker: Some(worker),
        }
    }

    fn memory(&self) -> OmqGoSendRingMemory {
        OmqGoSendRingMemory {
            control: self.shared.control.as_ref() as *const SendRingControl as *mut c_void,
            descriptors: self.shared.desc.as_ptr() as *mut c_void,
            payload: self.shared.payload.as_ptr() as *mut c_void,
            desc_capacity: self.shared.desc.len(),
            payload_capacity: self.shared.payload.len(),
        }
    }

    fn status(&self) -> OmqGoStatus {
        let code = self.shared.last_error_code.load(Ordering::Acquire);
        if code == OK {
            if self.shared.closed() {
                return OmqGoStatus::err(CLOSED, "send ring closed");
            }
            return OmqGoStatus::ok();
        }
        let message = self
            .shared
            .last_error_message
            .lock()
            .map(|message| message.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "send ring error".to_string());
        OmqGoStatus::err(code, message)
    }

    fn close(&mut self) {
        self.shared.control.closed.store(1, Ordering::Release);
        self.shared.send_cancel.cancel();
        self.shared.wake();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for OmqGoSendRing {
    fn drop(&mut self) {
        self.close();
    }
}

impl AsRef<[u8]> for SendSlotOwner {
    fn as_ref(&self) -> &[u8] {
        &self.shared.payload[self.offset..self.offset + self.len]
    }
}

impl Drop for SendSlotOwner {
    fn drop(&mut self) {
        self.shared.release_slot(self.cursor);
    }
}

impl OmqGoSendRingShared {
    fn set_error(&self, code: i32, message: impl AsRef<str>) {
        self.last_error_code.store(code, Ordering::Release);
        if let Ok(mut slot) = self.last_error_message.lock() {
            *slot = cstring_lossy(message.as_ref());
        }
        self.control.closed.store(1, Ordering::Release);
    }

    fn closed(&self) -> bool {
        self.control.closed.load(Ordering::Acquire) != 0
    }

    /// Wake a parked worker and any producer waiting for space.
    fn wake(&self) {
        self.control.wake_epoch.fetch_add(1, Ordering::SeqCst);
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
        let _space = self.space.lock();
        self.space_changed.notify_all();
    }

    /// Park the worker until the producer publishes past `cursor` or closes.
    fn park_worker(&self, cursor: usize) {
        self.control.worker_parked.store(1, Ordering::SeqCst);
        if self.control.tail.load(Ordering::SeqCst) == cursor && !self.closed() {
            thread::park();
        }
        self.control.worker_parked.store(0, Ordering::Relaxed);
    }

    /// Park the producer until the head moves past `seen_head`, a wake bumps
    /// the epoch past `seen_epoch`, the ring closes, or `timeout` elapses.
    fn wait_space(&self, seen_head: usize, seen_epoch: usize, timeout: Option<Duration>) {
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let Ok(mut space) = self.space.lock() else {
            return;
        };
        self.control.producer_waiting.store(1, Ordering::SeqCst);
        loop {
            if self.control.head.load(Ordering::SeqCst) != seen_head
                || self.control.wake_epoch.load(Ordering::SeqCst) != seen_epoch
                || self.closed()
            {
                break;
            }
            let next = match deadline {
                None => self.space_changed.wait(space).ok(),
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    self.space_changed
                        .wait_timeout(space, remaining)
                        .ok()
                        .map(|(space, _)| space)
                }
            };
            let Some(next) = next else {
                break;
            };
            space = next;
        }
        self.control.producer_waiting.store(0, Ordering::Relaxed);
    }

    fn message_at(self: &Arc<Self>, cursor: usize) -> Message {
        let desc = self.desc[cursor & self.desc_mask];
        Message::single(Bytes::from_owner(SendSlotOwner {
            shared: Arc::clone(self),
            cursor,
            offset: desc.payload as usize,
            len: desc.payload_len as usize,
        }))
    }

    fn release_slot(&self, cursor: usize) {
        self.done[cursor & self.desc_mask].store(true, Ordering::Release);
        let Ok(mut reclaim) = self.reclaim.lock() else {
            return;
        };
        loop {
            let index = reclaim.cursor & self.desc_mask;
            if !self.done[index].swap(false, Ordering::AcqRel) {
                break;
            }
            reclaim.cursor = reclaim.cursor.wrapping_add(1);
            self.control.head.store(reclaim.cursor, Ordering::SeqCst);
        }
        drop(reclaim);
        if self.control.producer_waiting.load(Ordering::SeqCst) != 0 {
            let _space = self.space.lock();
            self.space_changed.notify_all();
        }
    }
}

fn send_ring_worker(shared: Arc<OmqGoSendRingShared>) {
    let mut head = 0usize;
    let mut cached_tail = 0usize;
    let mut batch = VecDeque::with_capacity(RING_BATCH);
    let mut spins = 0u32;

    loop {
        while batch.len() < RING_BATCH {
            let cursor = head.wrapping_add(batch.len());
            if cursor == cached_tail {
                cached_tail = shared.control.tail.load(Ordering::Acquire);
                if cursor == cached_tail {
                    break;
                }
            }
            batch.push_back(shared.message_at(cursor));
        }

        if batch.is_empty() {
            if shared.closed() {
                break;
            }
            if spins < 512 {
                send_ring_spin(&mut spins);
            } else {
                shared.park_worker(head);
            }
            continue;
        }
        spins = 0;

        match shared.socket.try_send_many(&mut batch, RING_BATCH) {
            Ok(sent) => head = head.wrapping_add(sent),
            Err(TrySendError::Full(returned)) => {
                // Park on queue space; ring close cancels the wait.
                match shared
                    .socket
                    .send_cancelable(returned.clone(), &shared.send_cancel)
                {
                    Ok(true) => head = head.wrapping_add(1),
                    Ok(false) => {
                        batch.push_front(returned);
                        break;
                    }
                    Err(Error::Closed) => {
                        shared.set_error(CLOSED, "socket closed");
                        break;
                    }
                    Err(error) => {
                        shared.set_error(status_code_from_error(&error), error.to_string());
                        break;
                    }
                }
            }
            Err(TrySendError::Closed) => {
                shared.set_error(CLOSED, "socket closed");
                break;
            }
            Err(TrySendError::Error(error)) => {
                shared.set_error(status_code_from_error(&error), error.to_string());
                break;
            }
        }
    }

    shared.control.closed.store(1, Ordering::Release);
    shared.wake();
}

fn send_ring_spin(spins: &mut u32) {
    if *spins < 256 {
        std::hint::spin_loop();
    } else {
        thread::yield_now();
    }
    *spins += 1;
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_create(
    socket: *mut OmqGoSocket,
    desc_capacity: usize,
    payload_capacity: usize,
    out: *mut *mut OmqGoSendRing,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null send ring output pointer");
    }
    unsafe {
        *out = ptr::null_mut();
    }
    if desc_capacity == 0 {
        return OmqGoStatus::err(CONFIG, "send ring descriptor capacity must be positive");
    }
    if payload_capacity == 0 {
        return OmqGoStatus::err(CONFIG, "send ring payload capacity must be positive");
    }

    let socket = unsafe { &*socket };
    match socket.materialize() {
        Ok(native) => {
            let ring = OmqGoSendRing::new(native, desc_capacity, payload_capacity);
            unsafe {
                *out = Box::into_raw(Box::new(ring));
            }
            bump(&SEND_RINGS_CREATED);
            OmqGoStatus::ok()
        }
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_memory(
    ring: *mut OmqGoSendRing,
    out: *mut OmqGoSendRingMemory,
) -> OmqGoStatus {
    if ring.is_null() {
        return OmqGoStatus::err(CLOSED, "send ring closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null send ring memory pointer");
    }
    unsafe {
        *out = (&*ring).memory();
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_error(ring: *mut OmqGoSendRing) -> OmqGoStatus {
    if ring.is_null() {
        return OmqGoStatus::err(CLOSED, "send ring closed");
    }
    unsafe { (&*ring).status() }
}

/// Wake the ring worker and any producer waiting for space.
#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_wake(ring: *mut OmqGoSendRing) {
    if ring.is_null() {
        return;
    }
    unsafe { (&*ring).shared.wake() }
}

/// Park until the ring head moves past `seen_head`, a wake bumps the epoch
/// past `seen_epoch`, the ring closes, or `timeout_millis` elapses (negative
/// waits forever). Callers recheck their condition after it returns.
#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_wait(
    ring: *mut OmqGoSendRing,
    seen_head: u64,
    seen_epoch: u64,
    timeout_millis: i64,
) {
    if ring.is_null() {
        return;
    }
    let timeout = u64::try_from(timeout_millis)
        .ok()
        .map(Duration::from_millis);
    unsafe {
        (&*ring)
            .shared
            .wait_space(seen_head as usize, seen_epoch as usize, timeout);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_close(ring: *mut OmqGoSendRing) {
    if ring.is_null() {
        return;
    }
    bump(&SEND_RINGS_FREED);
    unsafe {
        drop(Box::from_raw(ring));
    }
}

#[cfg(test)]
mod tests;
