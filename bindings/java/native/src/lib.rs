use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use jni::objects::{
    GlobalRef, JByteArray, JByteBuffer, JClass, JLongArray, JObject, JObjectArray, JString,
    JThrowable, JValue,
};
use jni::sys::{jboolean, jint, jlong, jlongArray, jobject, jobjectArray, jsize, jstring};
use jni::{JNIEnv, JavaVM};
use omq_proto::TrySendError;
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy, WorkloadProfile};
use omq_tokio::{
    Authenticator, Context, ContextConfig, CurveKeypair, CurvePublicKey, CurveSecretKey,
    CurveServerOptions, DisconnectReason, Endpoint, Error, MechanismPeerInfo, MechanismSetup,
    Message, MonitorEvent as NativeMonitorEvent, MonitorRecvError, MonitorStream,
    MonitorTryRecvError, Options, PeerCommandKind, PeerInfo as NativePeerInfo, SocketType,
};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("OMQ.java native fast path requires a 64-bit target");

struct JavaContext {
    ctx: Context,
    closed: AtomicBool,
}

struct JavaSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    socket: OnceLock<BlockingSocket>,
    materialize_lock: Mutex<()>,
    closed: AtomicBool,
}

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

#[repr(C, align(128))]
struct SendRingControl {
    head: AtomicUsize,
    _pad0: [u8; 120],
    tail: AtomicUsize,
    _pad1: [u8; 120],
    closed: AtomicUsize,
    _pad2: [u8; 120],
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

struct ExternalBlock {
    cursor: usize,
    _bytes: Box<[u8]>,
}

struct JavaRecvRing {
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
    pending: VecDeque<Message>,
    scratch: Vec<Message>,
    external: VecDeque<ExternalBlock>,
    last_error_code: Cell<i32>,
    last_error_message: RefCell<CString>,
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

const RECV_RING_STATUS_OK: i32 = 0;
const RECV_RING_STATUS_TIMEOUT: i32 = 1;
const RECV_RING_STATUS_CLOSED: i32 = 2;
const RECV_RING_STATUS_INVALID_ENDPOINT: i32 = 3;
const RECV_RING_STATUS_PROTOCOL: i32 = 4;
const RECV_RING_STATUS_ERROR: i32 = 5;
const MAX_ERROR_MESSAGE_BYTES: usize = 4095;

const RECV_RING_FLAG_MULTIPART: u64 = 1;
const RECV_RING_FLAG_EXTERNAL: u64 = 2;

struct JavaMonitor {
    ctx: Context,
    stream: Mutex<Option<MonitorStream>>,
    closed: AtomicBool,
}

impl JavaSocket {
    fn materialize(&self) -> Result<BlockingSocket, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone());
        }

        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| Error::Config("materialize lock poisoned".to_string()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone());
        }

        let options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .clone();
        options.validate()?;

        let created = self.ctx.blocking_socket(self.socket_type, options);
        self.socket
            .set(created.clone())
            .map_err(|_| Error::Config("socket materialized concurrently".to_string()))?;
        Ok(created)
    }

    fn shutdown(&self) {
        let _guard = self
            .materialize_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(socket) = self.socket.get() {
            let _ = socket.clone().close();
        }
    }

    fn set_option<F>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Options),
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| Error::Config("materialize lock poisoned".to_string()))?;
        if self.socket.get().is_some() {
            return Err(Error::Config(
                "socket options must be set before bind/connect/send/receive".to_string(),
            ));
        }

        let mut options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?;
        let mut next = options.clone();
        f(&mut next);
        next.validate()?;
        *options = next;
        Ok(())
    }
}

impl JavaRecvRing {
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
            pending: VecDeque::new(),
            scratch: Vec::with_capacity(desc_capacity.min(256)),
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
                _reserved0: message.routing_id().unwrap_or(0) as u64,
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
                _reserved0: message.routing_id().unwrap_or(0) as u64,
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

    fn fill(&mut self, timeout_millis: i64, max_messages: i32) -> Result<usize, Error> {
        if max_messages <= 0 {
            return Err(Error::Config(
                "maxMessages must be greater than zero".to_string(),
            ));
        }

        self.reclaim_consumed();
        let start_cursor = self.cursor;
        let max_messages = max_messages as usize;

        while self.cursor.wrapping_sub(start_cursor) < max_messages {
            if let Some(message) = self.pending.pop_front() {
                if self.publish(&message) {
                    continue;
                }
                self.pending.push_front(message);
                break;
            }

            if self.desc_is_full() {
                break;
            }

            let remaining = max_messages - self.cursor.wrapping_sub(start_cursor);
            let desc_space = self.desc.len() - self.cursor.wrapping_sub(self.cached_head);
            let batch_max = remaining.min(desc_space).min(256);
            self.scratch.clear();
            recv_many_into(
                &self.socket,
                batch_max as i32,
                timeout_millis,
                &mut self.scratch,
            )?;

            let mut blocked = false;
            let mut batch = Vec::new();
            std::mem::swap(&mut batch, &mut self.scratch);
            let mut drained = batch.drain(..);
            while let Some(message) = drained.next() {
                if self.publish(&message) {
                    continue;
                }
                self.pending.push_back(message);
                self.pending.extend(&mut drained);
                blocked = true;
                break;
            }
            drop(drained);
            batch.clear();
            std::mem::swap(&mut batch, &mut self.scratch);
            if blocked {
                break;
            }
            break;
        }

        Ok(self.flush(start_cursor))
    }
}

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
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::spawn(move || send_ring_worker(worker_shared));
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

    fn message_at(self: &Arc<Self>, cursor: usize) -> Message {
        let desc = self.desc[cursor & self.desc_mask];
        let len = desc.payload_len as usize;
        let offset = desc.payload as usize;
        Message::single(Bytes::from_owner(SendSlotOwner {
            shared: Arc::clone(self),
            cursor,
            offset,
            len,
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
            self.control.head.store(reclaim.cursor, Ordering::Release);
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
    let mut batch = VecDeque::with_capacity(256);
    let mut spins = 0u32;

    loop {
        while batch.len() < 256 {
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
            send_ring_backoff(&mut spins);
            continue;
        }

        match shared.socket.try_send_many(&mut batch, 256) {
            Ok(sent) => {
                if sent == 0 {
                    if shared.closed() {
                        break;
                    }
                    send_ring_backoff(&mut spins);
                    continue;
                }
                head = head.wrapping_add(sent);
                spins = 0;
            }
            Err(TrySendError::Full(returned)) => {
                batch.push_front(returned);
                if shared.closed() {
                    break;
                }
                send_ring_backoff(&mut spins);
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
}

fn send_ring_backoff(spins: &mut u32) {
    if *spins < 256 {
        std::hint::spin_loop();
        *spins += 1;
    } else if *spins < 512 {
        thread::yield_now();
        *spins += 1;
    } else {
        thread::sleep(Duration::from_micros(50));
    }
}

thread_local! {
    static FFM_LAST_ERROR_CODE: Cell<i32> = const { Cell::new(RECV_RING_STATUS_OK) };
    static FFM_LAST_ERROR_MESSAGE: RefCell<CString> = RefCell::new(empty_cstring());
}

fn empty_cstring() -> CString {
    CString::new("").expect("empty string contains no NUL")
}

fn cstring_lossy(message: &str) -> CString {
    let mut bytes = Vec::with_capacity(message.len().min(MAX_ERROR_MESSAGE_BYTES));
    for ch in message.chars() {
        if ch == '\0' {
            continue;
        }
        let mut encoded = [0; 4];
        let chunk = ch.encode_utf8(&mut encoded).as_bytes();
        if bytes.len() + chunk.len() > MAX_ERROR_MESSAGE_BYTES {
            break;
        }
        bytes.extend_from_slice(chunk);
    }
    CString::new(bytes).unwrap_or_else(|_| empty_cstring())
}

fn set_ffm_last_error(code: i32, message: impl AsRef<str>) {
    FFM_LAST_ERROR_CODE.with(|slot| slot.set(code));
    FFM_LAST_ERROR_MESSAGE.with(|slot| *slot.borrow_mut() = cstring_lossy(message.as_ref()));
}

fn clear_ffm_last_error() {
    set_ffm_last_error(RECV_RING_STATUS_OK, "");
}

fn recv_ring_status(error: &Error) -> i32 {
    match error {
        Error::Timeout | Error::WouldBlock => RECV_RING_STATUS_TIMEOUT,
        Error::Closed => RECV_RING_STATUS_CLOSED,
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            RECV_RING_STATUS_INVALID_ENDPOINT
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            RECV_RING_STATUS_PROTOCOL
        }
        _ => RECV_RING_STATUS_ERROR,
    }
}

fn encoded_message_len(message: &Message) -> usize {
    if message.len() == 1 {
        return message.part_slice(0).unwrap_or_default().len();
    }
    4 + 4 * message.len() + message.byte_len()
}

fn encode_message(message: &Message) -> Vec<u8> {
    let mut out = vec![0; encoded_message_len(message)];
    write_message_encoded(message, &mut out);
    out
}

fn write_message_encoded(message: &Message, out: &mut [u8]) {
    if message.len() == 1 {
        let body = message.part_slice(0).unwrap_or_default();
        out.copy_from_slice(body);
        return;
    }

    let part_count = message.len() as u32;
    out[..4].copy_from_slice(&part_count.to_ne_bytes());
    let mut offset = 4 + 4 * message.len();
    for index in 0..message.len() {
        let part = message.part_slice(index).unwrap_or_default();
        let len_offset = 4 + 4 * index;
        out[len_offset..len_offset + 4].copy_from_slice(&(part.len() as u32).to_ne_bytes());
        out[offset..offset + part.len()].copy_from_slice(part);
        offset += part.len();
    }
}

fn recv_ring_from_handle<'a>(handle: i64) -> Result<&'a mut JavaRecvRing, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &mut *(handle as *mut JavaRecvRing) })
}

fn send_ring_from_handle<'a>(handle: i64) -> Result<&'a JavaSendRing, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &*(handle as *const JavaSendRing) })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_last_error_code() -> i32 {
    FFM_LAST_ERROR_CODE.with(Cell::get)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_last_error_message() -> i64 {
    FFM_LAST_ERROR_MESSAGE.with(|slot| slot.borrow().as_ptr() as i64)
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

struct JavaAsyncTask {
    abort: tokio::task::AbortHandle,
}

fn async_task_handle(join: tokio::task::JoinHandle<()>) -> jlong {
    let abort = join.abort_handle();
    Box::into_raw(Box::new(JavaAsyncTask { abort })) as jlong
}

fn guard<R>(env: &mut JNIEnv<'_>, default: R, body: impl FnOnce(&mut JNIEnv<'_>) -> R) -> R {
    match catch_unwind(AssertUnwindSafe(|| body(env))) {
        Ok(value) => value,
        Err(_) => {
            throw_java(env, "io/omq/OMQException", "native OMQ panic");
            default
        }
    }
}

fn throw_java(env: &mut JNIEnv<'_>, class: &str, message: impl AsRef<str>) {
    let _ = env.throw_new(class, message.as_ref());
}

fn throw_java_default(env: &mut JNIEnv<'_>, class: &str) {
    let result = env
        .new_object(class, "()V", &[])
        .and_then(|throwable| env.throw(JThrowable::from(throwable)));
    if result.is_err() {
        throw_java(env, "io/omq/OMQException", class);
    }
}

fn throw_transport_java(
    env: &mut JNIEnv<'_>,
    class: &str,
    operation: &str,
    endpoint: &str,
    detail: &str,
) {
    let result = (|| {
        let operation = env.new_string(operation)?;
        let endpoint = env.new_string(endpoint)?;
        let detail = env.new_string(detail)?;
        let throwable = env.new_object(
            class,
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
            &[
                JValue::Object(&operation),
                JValue::Object(&endpoint),
                JValue::Object(&detail),
            ],
        )?;
        env.throw(JThrowable::from(throwable))
    })();

    if result.is_err() {
        let _ = env.throw_new(
            "io/omq/OMQException",
            format!("{operation} failed for {endpoint}: {detail}"),
        );
    }
}

fn is_name_resolution_error(error: &std::io::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    matches!(error.kind(), std::io::ErrorKind::NotFound)
        || text.contains("lookup")
        || text.contains("no address")
        || text.contains("no addresses")
        || text.contains("no such host")
        || text.contains("name or service")
        || text.contains("nodename")
        || text.contains("host not found")
        || text.contains("temporary failure in name resolution")
        || error.raw_os_error() == Some(11001)
}

fn throw_omq_for_endpoint(env: &mut JNIEnv<'_>, error: Error, operation: &str, endpoint: &str) {
    if let Error::Io(io_error) = error {
        let class = if is_name_resolution_error(&io_error) {
            "io/omq/NameResolutionException"
        } else {
            match operation {
                "bind" => "io/omq/BindException",
                "connect" => "io/omq/ConnectException",
                _ => "io/omq/TransportException",
            }
        };
        throw_transport_java(env, class, operation, endpoint, &io_error.to_string());
    } else {
        throw_omq(env, error);
    }
}

fn throw_omq(env: &mut JNIEnv<'_>, error: Error) {
    let class = match error {
        Error::Timeout | Error::WouldBlock => "io/omq/TimeoutException",
        Error::Closed => "io/omq/ClosedException",
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            "io/omq/InvalidEndpointException"
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            "io/omq/ProtocolException"
        }
        _ => "io/omq/OMQException",
    };
    throw_java(env, class, error.to_string());
}

fn exception_class(error: &Error) -> &'static str {
    match error {
        Error::Timeout | Error::WouldBlock => "io/omq/TimeoutException",
        Error::Closed => "io/omq/ClosedException",
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            "io/omq/InvalidEndpointException"
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            "io/omq/ProtocolException"
        }
        _ => "io/omq/OMQException",
    }
}

fn exception_object<'local>(
    env: &mut JNIEnv<'local>,
    error: Error,
) -> jni::errors::Result<JObject<'local>> {
    let message = env.new_string(error.to_string())?;
    env.new_object(
        exception_class(&error),
        "(Ljava/lang/String;)V",
        &[JValue::Object(&message)],
    )
}

fn runtime_exception_object<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> jni::errors::Result<JObject<'local>> {
    let message = env.new_string(message)?;
    env.new_object(
        "io/omq/OMQException",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&message)],
    )
}

fn mechanism_peer_info_object<'local>(
    env: &mut JNIEnv<'local>,
    peer: &MechanismPeerInfo,
) -> Result<JObject<'local>, Error> {
    let mechanism = env
        .new_string(peer.mechanism.as_str()?.to_string())
        .map_err(jni_error)?;
    let public_key = if peer.mechanism == omq_proto::proto::MechanismName::CURVE {
        JObject::from(
            env.new_string(CurvePublicKey::from_bytes(peer.public_key).to_z85())
                .map_err(jni_error)?,
        )
    } else {
        JObject::null()
    };
    let identity = match &peer.identity {
        Some(identity) => JObject::from(env.byte_array_from_slice(identity).map_err(jni_error)?),
        None => JObject::null(),
    };
    let peer_address = match &peer.peer_address {
        Some(address) => JObject::from(env.new_string(address).map_err(jni_error)?),
        None => JObject::null(),
    };
    let username = match &peer.username {
        Some(username) => JObject::from(env.new_string(username).map_err(jni_error)?),
        None => JObject::null(),
    };
    let password = match &peer.password {
        Some(password) => JObject::from(env.new_string(password).map_err(jni_error)?),
        None => JObject::null(),
    };

    env.new_object(
        "io/omq/PeerInfo",
        "(Ljava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&mechanism),
            JValue::Object(&public_key),
            JValue::Object(&identity),
            JValue::Object(&username),
            JValue::Object(&password),
            JValue::Object(&peer_address),
        ],
    )
    .map_err(jni_error)
}

fn monitor_peer_info_object<'local>(
    env: &mut JNIEnv<'local>,
    peer: &NativePeerInfo,
) -> Result<JObject<'local>, Error> {
    let identity = match &peer.peer_identity {
        Some(identity) => JObject::from(env.byte_array_from_slice(identity).map_err(jni_error)?),
        None => JObject::null(),
    };
    let peer_address = match peer.peer_address {
        Some(address) => JObject::from(env.new_string(address.to_string()).map_err(jni_error)?),
        None => JObject::null(),
    };
    let socket_type = match peer.peer_properties.socket_type {
        Some(socket_type) => JObject::from(
            env.new_string(format!("{socket_type:?}"))
                .map_err(jni_error)?,
        ),
        None => JObject::null(),
    };

    let null = JObject::null();
    env.new_object(
        "io/omq/PeerInfo",
        "(Ljava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;II)V",
        &[
            JValue::Object(&null),
            JValue::Object(&null),
            JValue::Object(&identity),
            JValue::Object(&null),
            JValue::Object(&null),
            JValue::Long(peer.connection_id as jlong),
            JValue::Object(&peer_address),
            JValue::Object(&socket_type),
            JValue::Int(peer.zmtp_version.0 as jint),
            JValue::Int(peer.zmtp_version.1 as jint),
        ],
    )
    .map_err(jni_error)
}

struct EventParts<'a> {
    kind: &'a str,
    endpoint: Option<String>,
    peer: Option<NativePeerInfo>,
    peer_ident: Option<String>,
    connection_id: Option<u64>,
    reason: Option<String>,
    retry_millis: Option<u128>,
    attempt: Option<u32>,
    data: Option<Bytes>,
    command_name: Option<String>,
    command_body: Option<Bytes>,
}

fn nullable_string<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&str>,
) -> Result<JObject<'local>, Error> {
    match value {
        Some(value) => env.new_string(value).map(JObject::from).map_err(jni_error),
        None => Ok(JObject::null()),
    }
}

fn nullable_bytes<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&[u8]>,
) -> Result<JObject<'local>, Error> {
    match value {
        Some(value) => env
            .byte_array_from_slice(value)
            .map(JObject::from)
            .map_err(jni_error),
        None => Ok(JObject::null()),
    }
}

fn disconnect_reason(reason: DisconnectReason) -> String {
    match reason {
        DisconnectReason::PeerClosed => "peer closed".to_string(),
        DisconnectReason::LocalClose => "local close".to_string(),
        DisconnectReason::Error(error) => error,
        DisconnectReason::Handover => "handover".to_string(),
        _ => "unknown".to_string(),
    }
}

fn monitor_event_parts(event: NativeMonitorEvent) -> EventParts<'static> {
    match event {
        NativeMonitorEvent::Listening { endpoint } => EventParts {
            kind: "LISTENING",
            endpoint: Some(endpoint.to_string()),
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::Accepted {
            endpoint,
            peer_ident,
            connection_id,
        } => EventParts {
            kind: "ACCEPTED",
            endpoint: Some(endpoint.to_string()),
            peer: None,
            peer_ident: Some(peer_ident.to_string()),
            connection_id: Some(connection_id),
            reason: None,
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::Connected {
            endpoint,
            peer_ident,
            connection_id,
        } => EventParts {
            kind: "CONNECTED",
            endpoint: Some(endpoint.to_string()),
            peer: None,
            peer_ident: Some(peer_ident.to_string()),
            connection_id: Some(connection_id),
            reason: None,
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::HandshakeSucceeded { endpoint, peer } => EventParts {
            kind: "HANDSHAKE_SUCCEEDED",
            endpoint: Some(endpoint.to_string()),
            peer: Some(peer),
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::HandshakeFailed {
            endpoint,
            peer_ident,
            reason,
        } => EventParts {
            kind: "HANDSHAKE_FAILED",
            endpoint: Some(endpoint.to_string()),
            peer: None,
            peer_ident: Some(peer_ident.to_string()),
            connection_id: None,
            reason: Some(reason),
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::ConnectDelayed {
            endpoint,
            retry_in,
            attempt,
        } => EventParts {
            kind: "CONNECT_DELAYED",
            endpoint: Some(endpoint.to_string()),
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: Some(retry_in.as_millis()),
            attempt: Some(attempt),
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::Disconnected {
            endpoint,
            peer,
            reason,
        } => EventParts {
            kind: "DISCONNECTED",
            endpoint: Some(endpoint.to_string()),
            peer: Some(peer),
            peer_ident: None,
            connection_id: None,
            reason: Some(disconnect_reason(reason)),
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::SubscribeReceived { prefix } => EventParts {
            kind: "SUBSCRIBE_RECEIVED",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: Some(prefix),
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::UnsubscribeReceived { prefix } => EventParts {
            kind: "UNSUBSCRIBE_RECEIVED",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: Some(prefix),
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::JoinReceived { group } => EventParts {
            kind: "JOIN_RECEIVED",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: Some(group),
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::LeaveReceived { group } => EventParts {
            kind: "LEAVE_RECEIVED",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: Some(group),
            command_name: None,
            command_body: None,
        },
        NativeMonitorEvent::PeerCommand {
            endpoint,
            peer,
            command,
        } => {
            let (reason, command_name, command_body) = match command {
                PeerCommandKind::Error { reason } => {
                    (Some(reason), Some("ERROR".to_string()), None)
                }
                PeerCommandKind::Unknown { name, body } => (
                    None,
                    Some(String::from_utf8_lossy(&name).into_owned()),
                    Some(body),
                ),
                _ => (Some("unknown peer command".to_string()), None, None),
            };
            EventParts {
                kind: "PEER_COMMAND",
                endpoint: Some(endpoint.to_string()),
                peer: Some(peer),
                peer_ident: None,
                connection_id: None,
                reason,
                retry_millis: None,
                attempt: None,
                data: None,
                command_name,
                command_body,
            }
        }
        NativeMonitorEvent::Closed => EventParts {
            kind: "CLOSED",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: None,
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
        _ => EventParts {
            kind: "PEER_COMMAND",
            endpoint: None,
            peer: None,
            peer_ident: None,
            connection_id: None,
            reason: Some("unknown monitor event".to_string()),
            retry_millis: None,
            attempt: None,
            data: None,
            command_name: None,
            command_body: None,
        },
    }
}

fn monitor_event_object<'local>(
    env: &mut JNIEnv<'local>,
    event: NativeMonitorEvent,
) -> Result<JObject<'local>, Error> {
    let parts = monitor_event_parts(event);
    let kind = env.new_string(parts.kind).map_err(jni_error)?;
    let endpoint = nullable_string(env, parts.endpoint.as_deref())?;
    let peer = match parts.peer {
        Some(peer) => monitor_peer_info_object(env, &peer)?,
        None => JObject::null(),
    };
    let peer_ident = nullable_string(env, parts.peer_ident.as_deref())?;
    let reason = nullable_string(env, parts.reason.as_deref())?;
    let data = nullable_bytes(env, parts.data.as_deref())?;
    let command_name = nullable_string(env, parts.command_name.as_deref())?;
    let command_body = nullable_bytes(env, parts.command_body.as_deref())?;

    env.new_object(
        "io/omq/MonitorEvent",
        "(Ljava/lang/String;Ljava/lang/String;Lio/omq/PeerInfo;Ljava/lang/String;JLjava/lang/String;JI[BLjava/lang/String;[B)V",
        &[
            JValue::Object(&kind),
            JValue::Object(&endpoint),
            JValue::Object(&peer),
            JValue::Object(&peer_ident),
            JValue::Long(parts.connection_id.map_or(-1, |id| id as jlong)),
            JValue::Object(&reason),
            JValue::Long(parts.retry_millis.map_or(-1, |millis| {
                millis.min(jlong::MAX as u128) as jlong
            })),
            JValue::Int(parts.attempt.map_or(-1, |attempt| attempt as jint)),
            JValue::Object(&data),
            JValue::Object(&command_name),
            JValue::Object(&command_body),
        ],
    )
    .map_err(jni_error)
}

fn java_authenticator(
    env: &mut JNIEnv<'_>,
    authenticator: JObject<'_>,
) -> Result<Authenticator, Error> {
    if authenticator.is_null() {
        return Err(Error::Config("authenticator must not be null".to_string()));
    }
    let jvm = env.get_java_vm().map_err(jni_error)?;
    let authenticator = env.new_global_ref(authenticator).map_err(jni_error)?;
    Ok(Authenticator::new(move |peer| {
        let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
            return false;
        };
        let Ok(info) = mechanism_peer_info_object(&mut env, peer) else {
            return false;
        };
        match env.call_method(
            &authenticator,
            "test",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&info)],
        ) {
            Ok(value) => value.z().unwrap_or(false),
            Err(_) => {
                let _ = env.exception_clear();
                false
            }
        }
    }))
}

fn complete_future_exceptionally(env: &mut JNIEnv<'_>, future: &GlobalRef, error: Error) {
    let throwable = exception_object(env, error)
        .or_else(|_| runtime_exception_object(env, "failed to create native OMQ exception"));
    if let Ok(throwable) = throwable {
        let _ = env.call_method(
            future,
            "completeExceptionally",
            "(Ljava/lang/Throwable;)Z",
            &[JValue::Object(&throwable)],
        );
    }
}

fn complete_future_message(jvm: JavaVM, future: GlobalRef, result: Result<Message, Error>) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(message) => match message_to_java_object(&mut env, message) {
            Ok(message) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&message)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn receive_event_object<'local>(
    env: &mut JNIEnv<'local>,
    socket: &GlobalRef,
    message: Message,
) -> Result<JObject<'local>, Error> {
    let message = message_to_java_object(env, message)?;
    let socket = env.new_local_ref(socket.as_obj()).map_err(jni_error)?;
    env.new_object(
        "io/omq/ReceiveEvent",
        "(Lio/omq/Socket;Lio/omq/Message;)V",
        &[JValue::Object(&socket), JValue::Object(&message)],
    )
    .map_err(jni_error)
}

fn complete_future_receive_event(
    jvm: JavaVM,
    future: GlobalRef,
    result: Result<(GlobalRef, Message), Error>,
) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok((socket, message)) => match receive_event_object(&mut env, &socket, message) {
            Ok(event) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&event)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn optional_receive_event_object<'local>(
    env: &mut JNIEnv<'local>,
    event: Option<(GlobalRef, Message)>,
) -> Result<JObject<'local>, Error> {
    match event {
        Some((socket, message)) => {
            let event = receive_event_object(env, &socket, message)?;
            env.call_static_method(
                "java/util/Optional",
                "of",
                "(Ljava/lang/Object;)Ljava/util/Optional;",
                &[JValue::Object(&event)],
            )
            .and_then(|value| value.l())
            .map_err(jni_error)
        }
        None => env
            .call_static_method("java/util/Optional", "empty", "()Ljava/util/Optional;", &[])
            .and_then(|value| value.l())
            .map_err(jni_error),
    }
}

fn complete_future_optional_receive_event(
    jvm: JavaVM,
    future: GlobalRef,
    result: Result<Option<(GlobalRef, Message)>, Error>,
) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(event) => match optional_receive_event_object(&mut env, event) {
            Ok(event) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&event)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn complete_future_void(jvm: JavaVM, future: GlobalRef, result: Result<(), Error>) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(()) => {
            let value = JObject::null();
            let _ = env.call_method(
                &future,
                "complete",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&value)],
            );
        }
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn jni_error(error: jni::errors::Error) -> Error {
    Error::Config(format!("JNI error: {error}"))
}

fn context_from_handle(handle: jlong) -> Result<&'static JavaContext, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    let ctx = unsafe { &*(handle as *mut JavaContext) };
    if ctx.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }
    Ok(ctx)
}

fn socket_from_handle(handle: jlong) -> Result<&'static JavaSocket, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &*(handle as *mut JavaSocket) })
}

fn java_string(env: &mut JNIEnv<'_>, value: JString<'_>) -> Result<String, Error> {
    env.get_string(&value).map(|s| s.into()).map_err(jni_error)
}

fn byte_array(env: &mut JNIEnv<'_>, value: JByteArray<'_>) -> Result<Vec<u8>, Error> {
    env.convert_byte_array(value).map_err(jni_error)
}

fn bytes_from_parts(env: &mut JNIEnv<'_>, parts: JObjectArray<'_>) -> Result<Vec<Bytes>, Error> {
    let len = env.get_array_length(&parts).map_err(jni_error)?;
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        let part = env.get_object_array_element(&parts, i).map_err(jni_error)?;
        if part.is_null() {
            return Err(Error::Config("message part must not be null".to_string()));
        }
        let part = JByteArray::from(part);
        out.push(Bytes::from(byte_array(env, part)?));
    }
    Ok(out)
}

fn java_message(parts: Vec<Bytes>, routing_id: jint) -> Result<Message, Error> {
    if routing_id < 0 {
        return Err(Error::Config("routing ID must be non-negative".to_string()));
    }
    let message = Message::multipart(parts);
    Ok(if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id as u32)
    })
}

fn java_try_send(
    socket: &BlockingSocket,
    message: Message,
) -> core::result::Result<(), TrySendError> {
    match socket.socket_type() {
        SocketType::Scatter if message.len() != 1 => {
            Err(TrySendError::Error(Error::Protocol(format!(
                "Scatter socket requires single-part messages (got {})",
                message.len()
            ))))
        }
        SocketType::Push | SocketType::Scatter => {
            let mut messages = VecDeque::with_capacity(1);
            messages.push_back(message);
            match socket.try_send_many(&mut messages, 1) {
                Ok(sent) if sent > 0 => Ok(()),
                Ok(_) => {
                    let message = messages
                        .pop_front()
                        .expect("try_send_many returned zero with message pending");
                    Err(TrySendError::Full(message))
                }
                Err(error) => Err(error),
            }
        }
        _ => socket.try_send(message),
    }
}

fn java_send(socket: &BlockingSocket, message: Message) -> Result<(), Error> {
    match socket.socket_type() {
        SocketType::Push | SocketType::Scatter => {
            let mut message = message;
            loop {
                match java_try_send(socket, message) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(returned)) => message = returned,
                    Err(TrySendError::Closed) => return Err(Error::Closed),
                    Err(TrySendError::Error(error)) => return Err(error),
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        _ => socket.send(message),
    }
}

fn send_with_timeout(
    socket: &BlockingSocket,
    message: Message,
    timeout_millis: jlong,
) -> Result<bool, Error> {
    if timeout_millis < 0 {
        socket.send(message)?;
        return Ok(true);
    }

    let timeout = duration_from_millis(timeout_millis)?;
    let deadline = Instant::now().checked_add(timeout);
    let mut message = message;
    loop {
        match java_try_send(socket, message) {
            Ok(()) => return Ok(true),
            Err(TrySendError::Full(returned)) => message = returned,
            Err(TrySendError::Closed) => return Err(Error::Closed),
            Err(TrySendError::Error(error)) => return Err(error),
        }

        let Some(deadline) = deadline else {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        std::thread::sleep(remaining.min(Duration::from_millis(1)));
    }
}

fn message_to_java_parts<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObjectArray<'local>, Error> {
    let byte_array_class = env.find_class("[B").map_err(jni_error)?;
    let parts = env
        .new_object_array(message.len() as jint, byte_array_class, JObject::null())
        .map_err(jni_error)?;

    for i in 0..message.len() {
        let array = message_part_to_java(env, message, i)?;
        env.set_object_array_element(&parts, i as jint, array)
            .map_err(jni_error)?;
    }

    Ok(parts)
}

fn message_part_to_java<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
    index: usize,
) -> Result<JByteArray<'local>, Error> {
    env.byte_array_from_slice(message.part_slice(index).unwrap_or_default())
        .map_err(jni_error)
}

fn message_to_java_object<'local>(
    env: &mut JNIEnv<'local>,
    message: Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_object_ref(env, &message)
}

fn message_to_java_object_ref<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObject<'local>, Error> {
    let routing_id = message.routing_id().unwrap_or(0);
    if message.len() == 1 {
        let part = message_part_to_java(env, message, 0)?;
        let part = JObject::from(part);
        return env
            .call_static_method(
                "io/omq/Message",
                "fromNative",
                "([BI)Lio/omq/Message;",
                &[JValue::Object(&part), JValue::Int(routing_id as jint)],
            )
            .and_then(|value| value.l())
            .map_err(jni_error);
    }

    let parts = message_to_java_parts(env, message)?;
    let parts = JObject::from(parts);
    env.call_static_method(
        "io/omq/Message",
        "fromNative",
        "([[BI)Lio/omq/Message;",
        &[JValue::Object(&parts), JValue::Int(routing_id as jint)],
    )
    .and_then(|value| value.l())
    .map_err(jni_error)
}

fn message_to_java_native<'local>(
    env: &mut JNIEnv<'local>,
    message: Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_native_ref(env, &message)
}

fn message_to_java_native_ref<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_object_ref(env, message)
}

fn recv_with_timeout(socket: &BlockingSocket, timeout_millis: jlong) -> Result<Message, Error> {
    if timeout_millis < 0 {
        socket.recv()
    } else if timeout_millis == 0 {
        socket.try_recv()
    } else {
        socket.recv_timeout(Duration::from_millis(timeout_millis as u64))
    }
}

fn byte_buffer_int(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    method: &str,
) -> Result<jint, Error> {
    env.call_method(buffer, method, "()I", &[])
        .and_then(|value| value.i())
        .map_err(jni_error)
}

fn byte_buffer_bool(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    method: &str,
) -> Result<bool, Error> {
    env.call_method(buffer, method, "()Z", &[])
        .and_then(|value| value.z())
        .map_err(jni_error)
}

fn set_byte_buffer_position(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    position: jint,
) -> Result<(), Error> {
    env.call_method(
        buffer,
        "position",
        "(I)Ljava/nio/Buffer;",
        &[JValue::Int(position)],
    )
    .map(|_| ())
    .map_err(jni_error)
}

fn jbyte_slice(bytes: &[u8]) -> &[i8] {
    // Java byte is signed; JNI copies raw byte values without conversion.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) }
}

fn write_message_to_byte_buffer(
    env: &mut JNIEnv<'_>,
    buffer: JObject<'_>,
    message: &Message,
) -> Result<usize, Error> {
    if message.len() != 1 {
        throw_java(
            env,
            "java/lang/IllegalStateException",
            format!("message has {} parts", message.len()),
        );
        return Err(Error::Config("message is multipart".to_string()));
    }
    if byte_buffer_bool(env, &buffer, "isReadOnly")? {
        throw_java_default(env, "java/nio/ReadOnlyBufferException");
        return Err(Error::Config("destination is read-only".to_string()));
    }

    let body = message.part_slice(0).unwrap_or_default();
    let remaining = byte_buffer_int(env, &buffer, "remaining")?;
    if body.len() > remaining as usize {
        throw_java_default(env, "java/nio/BufferOverflowException");
        return Err(Error::Config(
            "destination has insufficient remaining space".to_string(),
        ));
    }

    let position = byte_buffer_int(env, &buffer, "position")?;
    if byte_buffer_bool(env, &buffer, "isDirect")? {
        let direct = <&JByteBuffer>::from(&buffer);
        let capacity = env.get_direct_buffer_capacity(direct).map_err(jni_error)?;
        let end = position as usize + body.len();
        if end > capacity {
            throw_java_default(env, "java/nio/BufferOverflowException");
            return Err(Error::Config(
                "destination has insufficient direct capacity".to_string(),
            ));
        }
        let base = env.get_direct_buffer_address(direct).map_err(jni_error)?;
        unsafe {
            std::ptr::copy_nonoverlapping(body.as_ptr(), base.add(position as usize), body.len());
        }
    } else if byte_buffer_bool(env, &buffer, "hasArray")? {
        let array_offset = byte_buffer_int(env, &buffer, "arrayOffset")?;
        let array = env
            .call_method(&buffer, "array", "()[B", &[])
            .and_then(|value| value.l())
            .map(JByteArray::from)
            .map_err(jni_error)?;
        let start = array_offset
            .checked_add(position)
            .ok_or_else(|| Error::Config("byte buffer offset overflow".to_string()))?;
        env.set_byte_array_region(&array, start as jsize, jbyte_slice(body))
            .map_err(jni_error)?;
    } else {
        throw_java(
            env,
            "java/lang/UnsupportedOperationException",
            "ByteBuffer must be direct or array-backed",
        );
        return Err(Error::Config(
            "destination is neither direct nor array-backed".to_string(),
        ));
    }

    set_byte_buffer_position(env, &buffer, position + body.len() as jint)?;
    Ok(body.len())
}

fn recv_many_into(
    socket: &BlockingSocket,
    max_messages: jint,
    timeout_millis: jlong,
    out: &mut Vec<Message>,
) -> Result<usize, Error> {
    if max_messages <= 0 {
        return Err(Error::Config(
            "maxMessages must be greater than zero".to_string(),
        ));
    }

    let max = max_messages as usize;
    if timeout_millis < 0 {
        socket.recv_many_into(max, out)
    } else if timeout_millis == 0 {
        socket.try_recv_many_into(max, out)
    } else {
        socket.recv_many_timeout_into(max, Duration::from_millis(timeout_millis as u64), out)
    }
}

fn monitor_recv_error(error: MonitorRecvError) -> Error {
    match error {
        MonitorRecvError::Closed => Error::Closed,
        MonitorRecvError::Lagged(count) => {
            Error::Config(format!("monitor lagged behind; missed {count} events"))
        }
        _ => Error::Config("unknown monitor receive error".to_string()),
    }
}

fn monitor_try_recv_result(
    result: Result<NativeMonitorEvent, MonitorTryRecvError>,
) -> Result<Option<NativeMonitorEvent>, Error> {
    match result {
        Ok(event) => Ok(Some(event)),
        Err(error) => monitor_try_recv_error(error),
    }
}

fn monitor_try_recv_error(error: MonitorTryRecvError) -> Result<Option<NativeMonitorEvent>, Error> {
    match error {
        MonitorTryRecvError::Empty => Ok(None),
        MonitorTryRecvError::Closed => Err(Error::Closed),
        MonitorTryRecvError::Lagged(count) => Err(Error::Config(format!(
            "monitor lagged behind; missed {count} events"
        ))),
        _ => Err(Error::Config("unknown monitor receive error".to_string())),
    }
}

fn monitor_recv_with_timeout(
    monitor: &JavaMonitor,
    timeout_millis: jlong,
) -> Result<Option<NativeMonitorEvent>, Error> {
    if monitor.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }

    let mut stream = {
        let mut guard = monitor
            .stream
            .lock()
            .map_err(|_| Error::Config("monitor lock poisoned".to_string()))?;
        guard
            .take()
            .ok_or_else(|| Error::Config("monitor receive is already in progress".to_string()))?
    };

    let result = if timeout_millis == 0 {
        monitor_try_recv_result(stream.try_recv())
    } else {
        let timeout = optional_duration_from_millis(timeout_millis)?;
        let (returned, result) = monitor.ctx.block_on(async move {
            let result = match timeout {
                Some(timeout) => match tokio::time::timeout(timeout, stream.recv()).await {
                    Ok(Ok(event)) => Ok(Some(event)),
                    Ok(Err(error)) => Err(monitor_recv_error(error)),
                    Err(_) => Ok(None),
                },
                None => stream.recv().await.map(Some).map_err(monitor_recv_error),
            };
            (stream, result)
        });
        stream = returned;
        result
    };

    let mut guard = monitor
        .stream
        .lock()
        .map_err(|_| Error::Config("monitor lock poisoned".to_string()))?;
    *guard = Some(stream);
    result
}

fn try_receive_any(
    entries: &[(BlockingSocket, GlobalRef)],
) -> Result<Option<(GlobalRef, Message)>, Error> {
    for (socket, java_socket) in entries {
        match socket.try_recv() {
            Ok(message) => return Ok(Some((java_socket.clone(), message))),
            Err(Error::WouldBlock | Error::Timeout) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

async fn receive_any_loop(
    entries: Vec<(BlockingSocket, GlobalRef)>,
    timeout: Option<Duration>,
) -> Result<Option<(GlobalRef, Message)>, Error> {
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    let mut spins = 0u32;

    loop {
        if let Some(event) = try_receive_any(&entries)? {
            return Ok(Some(event));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            tokio::time::sleep(remaining.min(Duration::from_micros(50))).await;
        } else if spins < 256 {
            spins += 1;
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    }
}

fn duration_from_millis(millis: jlong) -> Result<Duration, Error> {
    if millis < 0 {
        return Err(Error::Config("duration must be non-negative".to_string()));
    }
    Ok(Duration::from_millis(millis as u64))
}

fn optional_duration_from_millis(millis: jlong) -> Result<Option<Duration>, Error> {
    if millis == -1 {
        return Ok(None);
    }
    duration_from_millis(millis).map(Some)
}

fn optional_usize_from_long(name: &str, value: jlong) -> Result<Option<usize>, Error> {
    if value == -1 {
        return Ok(None);
    }
    if value < 0 {
        return Err(Error::Config(format!("{name} must be non-negative")));
    }
    Ok(Some(value as usize))
}

fn socket_type_from_code(code: jint) -> Result<SocketType, Error> {
    Ok(match code {
        0 => SocketType::Req,
        1 => SocketType::Rep,
        2 => SocketType::Pub,
        3 => SocketType::Sub,
        4 => SocketType::XPub,
        5 => SocketType::XSub,
        6 => SocketType::Push,
        7 => SocketType::Pull,
        8 => SocketType::Dealer,
        9 => SocketType::Router,
        10 => SocketType::Pair,
        11 => SocketType::Client,
        12 => SocketType::Server,
        13 => SocketType::Radio,
        14 => SocketType::Dish,
        15 => SocketType::Scatter,
        16 => SocketType::Gather,
        17 => SocketType::Channel,
        18 => SocketType::Peer,
        19 => SocketType::Stream,
        _ => return Err(Error::Config(format!("unknown socket type code {code}"))),
    })
}

fn curve_keypair_from_z85(public_key: String, secret_key: String) -> Result<CurveKeypair, Error> {
    let public = CurvePublicKey::from_z85(&public_key)?;
    let secret = CurveSecretKey::from_z85(&secret_key)?;
    Ok(CurveKeypair { public, secret })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_asyncTaskCancel(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }
        let task = unsafe { Box::from_raw(handle as *mut JavaAsyncTask) };
        task.abort.abort();
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextCreate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    io_threads: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        if io_threads <= 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "ioThreads must be greater than zero",
            );
            return 0;
        }

        let ctx = Context::with_config(ContextConfig {
            io_threads: io_threads as usize,
        });
        Box::into_raw(Box::new(JavaContext {
            ctx,
            closed: AtomicBool::new(false),
        })) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextClose(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    owner: jboolean,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }

        let ctx = unsafe { Box::from_raw(handle as *mut JavaContext) };
        let was_open = !ctx.closed.swap(true, Ordering::AcqRel);
        if owner != 0 && was_open {
            ctx.ctx.term();
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextShareKey(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) -> jlongArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let ctx = context_from_handle(handle)?;
            let key = ctx.ctx.share_key();
            let high = (key >> 64) as u64 as jlong;
            let low = key as u64 as jlong;
            let out = env.new_long_array(2).map_err(jni_error)?;
            env.set_long_array_region(&out, 0, &[high, low])
                .map_err(jni_error)?;
            Ok(out.into_raw())
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextFromShareKey(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    high: jlong,
    low: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let key = ((high as u64 as u128) << 64) | (low as u64 as u128);
        let Some(ctx) = Context::from_share_key(key) else {
            return 0;
        };
        Box::into_raw(Box::new(JavaContext {
            ctx,
            closed: AtomicBool::new(false),
        })) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketMonitor(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let materialized = socket.materialize()?;
            Ok(Box::into_raw(Box::new(JavaMonitor {
                ctx: socket.ctx.clone(),
                stream: Mutex::new(Some(materialized.monitor())),
                closed: AtomicBool::new(false),
            })) as jlong)
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_monitorRecv(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            if handle == 0 {
                return Err(Error::Closed);
            }
            let monitor = unsafe { &*(handle as *mut JavaMonitor) };
            let Some(event) = monitor_recv_with_timeout(monitor, timeout_millis)? else {
                return Ok(std::ptr::null_mut());
            };
            monitor_event_object(env, event).map(JObject::into_raw)
        })();

        match result {
            Ok(event) => event,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_monitorClose(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }
        let monitor = unsafe { Box::from_raw(handle as *mut JavaMonitor) };
        monitor.closed.store(true, Ordering::Release);
        if let Ok(mut stream) = monitor.stream.lock() {
            stream.take();
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_curveKeypair(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let keypair = CurveKeypair::generate();
            let string_class = env.find_class("java/lang/String").map_err(jni_error)?;
            let out = env
                .new_object_array(2, string_class, JObject::null())
                .map_err(jni_error)?;
            let public = env.new_string(keypair.public.to_z85()).map_err(jni_error)?;
            let secret = env.new_string(keypair.secret.to_z85()).map_err(jni_error)?;
            env.set_object_array_element(&out, 0, public)
                .map_err(jni_error)?;
            env.set_object_array_element(&out, 1, secret)
                .map_err(jni_error)?;
            Ok(out.into_raw())
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_curvePublic(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    secret_key: JString<'_>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let secret = CurveSecretKey::from_z85(&java_string(env, secret_key)?)?;
            env.new_string(secret.derive_public().to_z85())
                .map(|s| s.into_raw())
                .map_err(jni_error)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_receiveAnyAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    sockets: JObjectArray<'_>,
    handles: JLongArray<'_>,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let len = env.get_array_length(&sockets).map_err(jni_error)?;
            if len <= 0 {
                return Err(Error::Config("at least one socket is required".to_string()));
            }
            if env.get_array_length(&handles).map_err(jni_error)? != len {
                return Err(Error::Config("socket and handle arrays differ".to_string()));
            }
            let mut raw_handles = vec![0; len as usize];
            env.get_long_array_region(&handles, 0, &mut raw_handles)
                .map_err(jni_error)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let mut entries = Vec::with_capacity(len as usize);
            for i in 0..len {
                let socket_obj = env
                    .get_object_array_element(&sockets, i)
                    .map_err(jni_error)?;
                if socket_obj.is_null() {
                    return Err(Error::Config(format!("socket {i} must not be null")));
                }
                let handle = raw_handles[i as usize];
                let socket = socket_from_handle(handle)?;
                let java_socket = env.new_global_ref(&socket_obj).map_err(jni_error)?;
                let runtime = socket.ctx.handle().clone();
                let socket = socket.materialize()?;
                entries.push((runtime, socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let entries: Vec<(BlockingSocket, GlobalRef)> = entries
                .into_iter()
                .map(|(_runtime, socket, java_socket)| (socket, java_socket))
                .collect();
            let join = parent_runtime.spawn(async move {
                let result = match receive_any_loop(entries, None).await {
                    Ok(Some(event)) => Ok(event),
                    Ok(None) => Err(Error::Closed),
                    Err(error) => Err(error),
                };
                complete_future_receive_event(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_receiveAnyAsyncOptional(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    sockets: JObjectArray<'_>,
    handles: JLongArray<'_>,
    timeout_millis: jlong,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let timeout = optional_duration_from_millis(timeout_millis)?;
            let len = env.get_array_length(&sockets).map_err(jni_error)?;
            if len <= 0 {
                return Err(Error::Config("at least one socket is required".to_string()));
            }
            if env.get_array_length(&handles).map_err(jni_error)? != len {
                return Err(Error::Config("socket and handle arrays differ".to_string()));
            }
            let mut raw_handles = vec![0; len as usize];
            env.get_long_array_region(&handles, 0, &mut raw_handles)
                .map_err(jni_error)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let mut entries = Vec::with_capacity(len as usize);
            for i in 0..len {
                let socket_obj = env
                    .get_object_array_element(&sockets, i)
                    .map_err(jni_error)?;
                if socket_obj.is_null() {
                    return Err(Error::Config(format!("socket {i} must not be null")));
                }
                let handle = raw_handles[i as usize];
                let socket = socket_from_handle(handle)?;
                let java_socket = env.new_global_ref(&socket_obj).map_err(jni_error)?;
                let runtime = socket.ctx.handle().clone();
                let socket = socket.materialize()?;
                entries.push((runtime, socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let entries: Vec<(BlockingSocket, GlobalRef)> = entries
                .into_iter()
                .map(|(_runtime, socket, java_socket)| (socket, java_socket))
                .collect();
            let join = parent_runtime.spawn(async move {
                let result = receive_any_loop(entries, timeout).await;
                complete_future_optional_receive_event(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketCreate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    context_handle: jlong,
    socket_type: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let ctx = match context_from_handle(context_handle) {
            Ok(ctx) => ctx,
            Err(error) => {
                throw_omq(env, error);
                return 0;
            }
        };
        if ctx.closed.load(Ordering::Acquire) {
            throw_omq(env, Error::Closed);
            return 0;
        }

        let socket_type = match socket_type_from_code(socket_type) {
            Ok(socket_type) => socket_type,
            Err(error) => {
                throw_omq(env, error);
                return 0;
            }
        };

        Box::into_raw(Box::new(JavaSocket {
            ctx: ctx.ctx.clone(),
            socket_type,
            options: Mutex::new(Options::default()),
            socket: OnceLock::new(),
            materialize_lock: Mutex::new(()),
            closed: AtomicBool::new(false),
        })) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketShutdown(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |env| match socket_from_handle(handle) {
        Ok(socket) => socket.shutdown(),
        Err(error) => throw_omq(env, error),
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketClose(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }

        let socket = unsafe { Box::from_raw(handle as *mut JavaSocket) };
        socket.shutdown();
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketBind(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            let bound = socket.materialize()?.bind(endpoint)?;
            env.new_string(bound.to_string())
                .map(|s| s.into_raw())
                .map_err(jni_error)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq_for_endpoint(env, error, "bind", &endpoint_text);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketConnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.connect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "connect", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketUnbind(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.unbind(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "unbind", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketDisconnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.disconnect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "disconnect", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSend(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    data: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let data = byte_array(env, data)?;
            java_send(&socket.materialize()?, Message::single(Bytes::from(data)))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendMultipart(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            java_send(&socket.materialize()?, java_message(parts, routing_id)?)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendMultipartTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            send_with_timeout(
                &socket.materialize()?,
                java_message(parts, routing_id)?,
                timeout_millis,
            )
            .map(i32::from)
        })();

        match result {
            Ok(sent) => sent,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketTrySendMultipart(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            match java_try_send(&socket.materialize()?, java_message(parts, routing_id)?) {
                Ok(()) => Ok(1),
                Err(TrySendError::Full(_)) => Ok(0),
                Err(TrySendError::Closed) => Err(Error::Closed),
                Err(TrySendError::Error(error)) => Err(error),
            }
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let parts = bytes_from_parts(env, parts)?;
            let message = java_message(parts, routing_id)?;
            let handle = socket.ctx.handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = socket.send(message).await;
                complete_future_void(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecv(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = recv_with_timeout(&socket, timeout_millis)?;
            message_to_java_native(env, message).map(JObject::into_raw)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecvInto(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    destination: JObject<'_>,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = recv_with_timeout(&socket, timeout_millis)?;
            write_message_to_byte_buffer(env, destination, &message).map(|len| len as jint)
        })();

        match result {
            Ok(len) => len,
            Err(error) => {
                if env.exception_check().unwrap_or(false) {
                    return 0;
                }
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecvAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(timeout_millis)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let handle = socket.ctx.handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = match timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, socket.recv()).await {
                        Ok(result) => result,
                        Err(_) => Err(Error::Timeout),
                    },
                    None => socket.recv().await,
                };
                complete_future_message(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSubscribe(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    prefix: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let prefix = byte_array(env, prefix)?;
            socket.materialize()?.subscribe(Bytes::from(prefix))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketUnsubscribe(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    prefix: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let prefix = byte_array(env, prefix)?;
            socket.materialize()?.unsubscribe(Bytes::from(prefix))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketJoin(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    group: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let group = byte_array(env, group)?;
            socket.materialize()?.join(Bytes::from(group))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketLeave(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    group: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let group = byte_array(env, group)?;
            socket.materialize()?.leave(Bytes::from(group))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketWaitConnected(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    min_peers: jint,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, -1, |env| {
        if min_peers < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "minPeers must be non-negative",
            );
            return -1;
        }

        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = duration_from_millis(timeout_millis)?;
            let count = socket
                .materialize()?
                .wait_connected(min_peers as usize, timeout)?;
            Ok(count as jint)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                -1
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketWaitSubscribed(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    min_subscriptions: jlong,
    timeout_millis: jlong,
) -> jlong {
    guard(&mut env, -1, |env| {
        if min_subscriptions < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "minSubscriptions must be non-negative",
            );
            return -1;
        }

        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = duration_from_millis(timeout_millis)?;
            let count = socket
                .materialize()?
                .wait_subscribed(min_subscriptions as u64, timeout)?;
            Ok(count as jlong)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                -1
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetLinger(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let linger = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.linger = linger)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetIdentity(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    identity: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let identity = Bytes::from(byte_array(env, identity)?);
            socket.set_option(|options| options.identity = identity)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetSendHighWaterMark(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    hwm: jint,
) {
    guard(&mut env, (), |env| {
        if hwm < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "HWM must be non-negative",
            );
            return;
        }
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.send_hwm = hwm as u32))
        {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReceiveHighWaterMark(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    hwm: jint,
) {
    guard(&mut env, (), |env| {
        if hwm < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "HWM must be non-negative",
            );
            return;
        }
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.recv_hwm = hwm as u32))
        {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatInterval(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let interval = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.heartbeat_interval = interval)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHandshakeTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(millis)?;
            socket.set_option(|options| options.handshake_timeout = timeout)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxMessageSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    size: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let max = if size < 0 { None } else { Some(size as usize) };
            socket.set_option(|options| options.max_message_size = max)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionAutoTrain(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let enabled = enabled != 0;
        if let Err(error) = socket_from_handle(handle).and_then(|socket| {
            socket.set_option(|options| options.compression_auto_train = enabled)
        }) {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    threshold: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let threshold = if threshold < 0 {
                None
            } else {
                Some(threshold as usize)
            };
            socket.set_option(|options| options.compression_threshold = threshold)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionLevel(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    level: jint,
) {
    guard(&mut env, (), |env| {
        let level = if level == i32::MIN { None } else { Some(level) };
        if let Err(error) = socket_from_handle(handle)
            .and_then(|socket| socket.set_option(|options| options.compression_level = level))
        {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    username: JString<'_>,
    password: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let expected_username = java_string(env, username)?;
            let expected_password = java_string(env, password)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainServer {
                    authenticator: Authenticator::plain_credentials([(
                        expected_username,
                        expected_password,
                    )]),
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let authenticator = java_authenticator(env, authenticator)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainServer { authenticator };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetPlainClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    username: JString<'_>,
    password: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let username = java_string(env, username)?;
            let password = java_string(env, password)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainClient { username, password };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::CurveServer {
                    our_keypair: keypair,
                    options: CurveServerOptions::default(),
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
    authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            let authenticator = java_authenticator(env, authenticator)?;
            socket.set_option(move |options| {
                let mut curve_options = CurveServerOptions::default();
                curve_options.authenticator = Some(authenticator);
                options.mechanism = MechanismSetup::CurveServer {
                    our_keypair: keypair,
                    options: curve_options,
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCurveClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
    server_public_key: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            let server_public = CurvePublicKey::from_z85(&java_string(env, server_public_key)?)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::CurveClient {
                    our_keypair: keypair,
                    server_public,
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetWorkloadProfile(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    profile: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let profile = match profile {
                -1 => None,
                0 => Some(WorkloadProfile::Throughput),
                1 => Some(WorkloadProfile::Latency),
                other => return Err(Error::Config(format!("unknown workload profile {other}"))),
            };
            socket.set_option(move |options| options.workload_profile = profile)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReconnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
    min_millis: jlong,
    max_millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let reconnect = match mode {
                0 => ReconnectPolicy::Disabled,
                1 => ReconnectPolicy::Fixed(duration_from_millis(min_millis)?),
                2 => {
                    let min = duration_from_millis(min_millis)?;
                    let max = duration_from_millis(max_millis)?;
                    if max < min {
                        return Err(Error::Config(
                            "reconnect max must be greater than or equal to min".to_string(),
                        ));
                    }
                    ReconnectPolicy::Exponential { min, max }
                }
                other => return Err(Error::Config(format!("unknown reconnect mode {other}"))),
            };
            socket.set_option(move |options| options.reconnect = reconnect)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReconnectStopConnRefused(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.reconnect_stop_conn_refused = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatTtl(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let ttl = optional_duration_from_millis(millis)?;
            socket.set_option(move |options| options.heartbeat_ttl = ttl)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetHeartbeatTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    millis: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(millis)?;
            socket.set_option(move |options| options.heartbeat_timeout = timeout)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxPendingHandshakes(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    max: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            if max <= 0 {
                return Err(Error::Config(
                    "max pending handshakes must be greater than zero".to_string(),
                ));
            }
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.max_pending_handshakes = max as usize)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetConflate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.conflate = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetRouterMandatory(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.router_mandatory = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetOnMute(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let on_mute = match mode {
                0 => OnMute::Block,
                1 => OnMute::DropNewest,
                2 => OnMute::DropOldest,
                other => return Err(Error::Config(format!("unknown on-mute mode {other}"))),
            };
            socket.set_option(move |options| options.on_mute = on_mute)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetTcpKeepalive(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    mode: jint,
    idle_millis: jlong,
    interval_millis: jlong,
    count: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keepalive = match mode {
                0 => KeepAlive::Default,
                1 => KeepAlive::Disabled,
                2 => {
                    if count <= 0 {
                        return Err(Error::Config(
                            "TCP keepalive count must be greater than zero".to_string(),
                        ));
                    }
                    KeepAlive::Enabled {
                        idle: duration_from_millis(idle_millis)?,
                        intvl: duration_from_millis(interval_millis)?,
                        cnt: count as u32,
                    }
                }
                other => return Err(Error::Config(format!("unknown TCP keepalive mode {other}"))),
            };
            socket.set_option(move |options| options.tcp_keepalive = keepalive)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetSendBufferSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("send buffer size", bytes)?;
            socket.set_option(move |options| options.send_buffer_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetReceiveBufferSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("receive buffer size", bytes)?;
            socket.set_option(move |options| options.recv_buffer_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionDict(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    dict: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let dict = byte_array(env, dict)?;
            let dict = if dict.is_empty() {
                None
            } else {
                Some(Bytes::from(dict))
            };
            socket.set_option(move |options| options.compression_dict = dict)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionDictCapacity(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("compression dictionary capacity", bytes)?;
            socket.set_option(move |options| options.compression_dict_capacity = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetMaxReceiveDictSize(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("max receive dictionary size", bytes)?;
            socket.set_option(move |options| options.max_recv_dict_size = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetCompressionOffloadThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("compression offload threshold", bytes)?;
            socket.set_option(move |options| options.compression_offload_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetLargeMessageThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("large message threshold", bytes)?
                .filter(|bytes| *bytes != 0);
            socket.set_option(move |options| options.large_message_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetArenaThreshold(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("arena threshold", bytes)?;
            socket.set_option(move |options| options.arena_threshold = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetTransmitSlotCap(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    bytes: jlong,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let bytes = optional_usize_from_long("transmit slot capacity", bytes)?;
            socket.set_option(move |options| options.transmit_slot_cap = bytes)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSetXpubNoDrop(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    enabled: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            socket.set_option(move |options| options.xpub_nodrop = enabled != 0)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[cfg(test)]
mod recv_ring_tests {
    use std::io;

    use bytes::Bytes;

    use super::*;

    #[test]
    fn windows_dns_error_is_name_resolution_error() {
        let error = io::Error::from_raw_os_error(11001);
        assert!(is_name_resolution_error(&error));
    }

    #[test]
    fn windows_dns_error_message_is_name_resolution_error() {
        let error = io::Error::other("No such host is known. (os error 11001)");
        assert!(is_name_resolution_error(&error));
    }

    #[test]
    fn cstring_lossy_removes_nul_and_fits_java_read_window() {
        let message = format!("a\0{}", "b".repeat(MAX_ERROR_MESSAGE_BYTES + 10));
        let cstring = cstring_lossy(&message);
        let bytes = cstring.to_bytes();

        assert_eq!(MAX_ERROR_MESSAGE_BYTES, bytes.len());
        assert!(!bytes.contains(&0));
    }

    #[test]
    fn cstring_lossy_truncates_at_utf8_boundary() {
        let message = "é".repeat(MAX_ERROR_MESSAGE_BYTES);
        let cstring = cstring_lossy(&message);
        let bytes = cstring.to_bytes();

        assert_eq!(MAX_ERROR_MESSAGE_BYTES - 1, bytes.len());
        assert!(std::str::from_utf8(bytes).is_ok());
    }

    #[test]
    fn encode_single_part_as_raw_payload() {
        let message = Message::single(Bytes::from_static(b"hello"));

        assert_eq!(encoded_message_len(&message), 5);
        assert_eq!(encode_message(&message), b"hello");
    }

    #[test]
    fn encode_multipart_with_native_lengths() {
        let message = Message::multipart([
            Bytes::from_static(b"one"),
            Bytes::from_static(b""),
            Bytes::from_static(b"three"),
        ]);
        let encoded = encode_message(&message);

        assert_eq!(encoded_message_len(&message), encoded.len());
        assert_eq!(u32::from_ne_bytes(encoded[0..4].try_into().unwrap()), 3);
        assert_eq!(u32::from_ne_bytes(encoded[4..8].try_into().unwrap()), 3);
        assert_eq!(u32::from_ne_bytes(encoded[8..12].try_into().unwrap()), 0);
        assert_eq!(u32::from_ne_bytes(encoded[12..16].try_into().unwrap()), 5);
        assert_eq!(&encoded[16..], b"onethree");
    }
}
