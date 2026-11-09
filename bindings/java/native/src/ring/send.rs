//! Native send ring storage and entry points.
//!
//! NOTE: `bindings/go/native/src/ring/send.rs` duplicates this
//! ring logic. Apply every change or fix to both.

use crate::error::{
    RECV_RING_STATUS_CLOSED, RECV_RING_STATUS_ERROR, RECV_RING_STATUS_OK, clear_ffm_last_error,
    cstring_lossy, empty_cstring, recv_ring_status, set_ffm_last_error,
};
use crate::socket::socket_from_handle;
use bytes::Bytes;
use omq_proto::{PayloadPool, TrySendError};
use omq_tokio::blocking::{BlockingRecvCancel, Socket as BlockingSocket};
use omq_tokio::{Error, Message};
use std::collections::VecDeque;
use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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
    /// Set by the producer while it waits for ring space.
    producer_waiting: AtomicUsize,
    _pad4: [u8; 120],
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
            producer_waiting: AtomicUsize::new(0),
            _pad4: [0; 120],
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

struct JavaSendRing {
    shared: Arc<JavaSendRingShared>,
    worker: Option<JoinHandle<()>>,
}

struct JavaSendRingShared {
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
    shared: Arc<JavaSendRingShared>,
    cursor: usize,
    offset: usize,
    len: usize,
}

const SEND_BATCH_MESSAGES: usize = 256;

const SEND_POOL_SLOT_BYTES: usize = 256;

const SEND_POOL_SLOTS: usize = 2_000;

impl JavaSendRing {
    fn new(socket: BlockingSocket, desc_capacity: usize, payload_capacity: usize) -> Self {
        let desc_capacity = desc_capacity.max(1).next_power_of_two();
        let payload_capacity = payload_capacity.max(1).next_power_of_two();
        let shared = Arc::new(JavaSendRingShared {
            socket,
            control: Box::new(SendRingControl::new()),
            desc: vec![SendRingDesc::default(); desc_capacity].into_boxed_slice(),
            payload: vec![0; payload_capacity].into_boxed_slice(),
            done: (0..desc_capacity).map(|_| AtomicBool::new(false)).collect(),
            desc_mask: desc_capacity - 1,
            last_error_code: AtomicI32::new(RECV_RING_STATUS_OK),
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

    fn control_addr(&self) -> i64 {
        self.shared.control.as_ref() as *const SendRingControl as i64
    }

    fn desc_addr(&self) -> i64 {
        self.shared.desc.as_ptr() as i64
    }

    fn payload_addr(&self) -> i64 {
        self.shared.payload.as_ptr() as i64
    }

    fn desc_capacity(&self) -> i32 {
        self.shared.desc.len() as i32
    }

    fn payload_capacity(&self) -> i64 {
        self.shared.payload.len() as i64
    }

    fn error_code(&self) -> i32 {
        self.shared.last_error_code.load(Ordering::Acquire)
    }

    fn error_message_addr(&self) -> i64 {
        self.shared
            .last_error_message
            .lock()
            .map(|message| message.as_ptr() as i64)
            .unwrap_or(0)
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

impl Drop for JavaSendRing {
    fn drop(&mut self) {
        self.close();
    }
}

impl JavaSendRingShared {
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

    /// Wake a parked worker and a producer waiting for space.
    fn wake(&self) {
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

    /// Park the producer until the head moves past `seen`, the ring closes,
    /// or `timeout` elapses. Returns false on timeout.
    fn wait_space(&self, seen: usize, timeout: Option<Duration>) -> bool {
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let Ok(mut space) = self.space.lock() else {
            return true;
        };
        self.control.producer_waiting.store(1, Ordering::SeqCst);
        let moved = loop {
            if self.control.head.load(Ordering::SeqCst) != seen || self.closed() {
                break true;
            }
            let wait = match deadline {
                None => self.space_changed.wait(space).ok(),
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break false;
                    }
                    self.space_changed
                        .wait_timeout(space, remaining)
                        .ok()
                        .map(|(space, _)| space)
                }
            };
            let Some(next) = wait else {
                break true;
            };
            space = next;
        };
        self.control.producer_waiting.store(0, Ordering::Relaxed);
        moved
    }

    fn message_at(
        self: &Arc<Self>,
        cursor: usize,
        pool: &OnceLock<Option<PayloadPool>>,
    ) -> (Message, bool) {
        let desc = self.desc[cursor & self.desc_mask];
        let len = desc.payload_len as usize;
        let offset = desc.payload as usize;
        let body = &self.payload[offset..offset + len];
        if len <= omq_proto::message::MAX_INLINE_MESSAGE {
            return (Message::from_slice(body), true);
        }
        if len <= SEND_POOL_SLOT_BYTES
            && let Some(pool) = pool
                .get_or_init(|| PayloadPool::new([(SEND_POOL_SLOT_BYTES, SEND_POOL_SLOTS)]).ok())
            && let Ok(Some(message)) = pool.try_message(len, |target| target.copy_from_slice(body))
        {
            return (message, true);
        }
        (
            Message::single(Bytes::from_owner(SendSlotOwner {
                shared: Arc::clone(self),
                cursor,
                offset,
                len,
            })),
            false,
        )
    }

    fn release_slot(&self, cursor: usize) {
        self.release_slots(std::iter::once(cursor));
    }

    fn release_slots(&self, cursors: impl IntoIterator<Item = usize>) {
        let mut released = false;
        for cursor in cursors {
            self.done[cursor & self.desc_mask].store(true, Ordering::Release);
            released = true;
        }
        if !released {
            return;
        }
        let Ok(mut reclaim) = self.reclaim.lock() else {
            return;
        };
        loop {
            let index = reclaim.cursor & self.desc_mask;
            if !self.done[index].swap(false, Ordering::AcqRel) {
                break;
            }
            reclaim.cursor = reclaim.cursor.wrapping_add(1);
        }
        self.control.head.store(reclaim.cursor, Ordering::SeqCst);
        drop(reclaim);
        if self.control.producer_waiting.load(Ordering::SeqCst) != 0 {
            let _space = self.space.lock();
            self.space_changed.notify_all();
        }
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

fn send_ring_worker(shared: Arc<JavaSendRingShared>) {
    let mut head = 0usize;
    let mut cached_tail = 0usize;
    let mut batch = VecDeque::with_capacity(SEND_BATCH_MESSAGES);
    let mut copied = VecDeque::with_capacity(SEND_BATCH_MESSAGES);
    let pool = OnceLock::new();
    let mut spins = 0u32;

    loop {
        while batch.len() < SEND_BATCH_MESSAGES {
            let cursor = head.wrapping_add(batch.len());
            if cursor == cached_tail {
                cached_tail = shared.control.tail.load(Ordering::Acquire);
                if cursor == cached_tail {
                    break;
                }
            }
            let (message, was_copied) = shared.message_at(cursor, &pool);
            batch.push_back(message);
            copied.push_back(was_copied);
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

        match shared.socket.try_send_many(&mut batch, SEND_BATCH_MESSAGES) {
            Ok(sent) => finish_send_batch(&shared, &mut copied, &mut head, sent),
            Err(TrySendError::Full(returned)) => {
                // Park on queue space; ring close cancels the wait.
                match shared
                    .socket
                    .send_cancelable(returned.clone(), &shared.send_cancel)
                {
                    Ok(true) => finish_send_batch(&shared, &mut copied, &mut head, 1),
                    Ok(false) => {
                        batch.push_front(returned);
                        break;
                    }
                    Err(Error::Closed) => {
                        shared.set_error(RECV_RING_STATUS_CLOSED, "socket closed");
                        break;
                    }
                    Err(error) => {
                        shared.set_error(recv_ring_status(&error), error.to_string());
                        break;
                    }
                }
            }
            Err(TrySendError::Closed) => {
                shared.set_error(RECV_RING_STATUS_CLOSED, "socket closed");
                break;
            }
            Err(TrySendError::Error(error)) => {
                shared.set_error(recv_ring_status(&error), error.to_string());
                break;
            }
        }
    }

    shared.control.closed.store(1, Ordering::Release);
    shared.wake();
}

fn finish_send_batch(
    shared: &JavaSendRingShared,
    copied: &mut VecDeque<bool>,
    head: &mut usize,
    sent: usize,
) {
    // Publish copied slots only after submission, so draining the Java ring
    // orders these messages before subsequent JNI sends.
    shared.release_slots(
        copied
            .drain(..sent)
            .enumerate()
            .filter(|(_, copied)| *copied)
            .map(|(index, _)| head.wrapping_add(index)),
    );
    *head = head.wrapping_add(sent);
}

fn send_ring_spin(spins: &mut u32) {
    if *spins < 256 {
        std::hint::spin_loop();
    } else {
        thread::yield_now();
    }
    *spins += 1;
}

fn send_ring_from_handle<'a>(handle: i64) -> Result<&'a JavaSendRing, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &*(handle as *const JavaSendRing) })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_create(
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
        Ok(Box::into_raw(Box::new(JavaSendRing::new(
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
pub extern "C" fn omq_java_send_ring_close(handle: i64) {
    if handle == 0 {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(handle as *mut JavaSendRing));
    }));
}

/// Wake the ring worker and any producer waiting for space.
#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_wake(handle: i64) {
    if let Ok(ring) = send_ring_from_handle(handle) {
        ring.shared.wake();
    }
}

/// Park until the ring head moves past `seen_head`, the ring closes, or
/// `timeout_millis` elapses (negative waits forever). Returns 1 on timeout.
#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_wait(handle: i64, seen_head: i64, timeout_millis: i64) -> i32 {
    let Ok(ring) = send_ring_from_handle(handle) else {
        return 0;
    };
    let timeout = u64::try_from(timeout_millis)
        .ok()
        .map(Duration::from_millis);
    i32::from(!ring.shared.wait_space(seen_head as usize, timeout))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_control_addr(handle: i64) -> i64 {
    send_ring_from_handle(handle)
        .map(|ring| ring.control_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_desc_addr(handle: i64) -> i64 {
    send_ring_from_handle(handle)
        .map(|ring| ring.desc_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_payload_addr(handle: i64) -> i64 {
    send_ring_from_handle(handle)
        .map(|ring| ring.payload_addr())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_desc_capacity(handle: i64) -> i32 {
    send_ring_from_handle(handle)
        .map(|ring| ring.desc_capacity())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_payload_capacity(handle: i64) -> i64 {
    send_ring_from_handle(handle)
        .map(|ring| ring.payload_capacity())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_error_code(handle: i64) -> i32 {
    send_ring_from_handle(handle)
        .map(JavaSendRing::error_code)
        .unwrap_or(RECV_RING_STATUS_CLOSED)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_send_ring_error_message(handle: i64) -> i64 {
    send_ring_from_handle(handle)
        .map(JavaSendRing::error_message_addr)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
