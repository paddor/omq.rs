#![expect(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "C ABI entrypoints validate raw pointers and report status codes"
)]

use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::slice;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::TrySendError;
#[cfg(any(feature = "plain", feature = "curve"))]
use omq_tokio::Authenticator;
#[cfg(any(feature = "plain", feature = "curve"))]
use omq_tokio::MechanismPeerInfo;
use omq_tokio::blocking::{BlockingRecvCancel, Socket as BlockingSocket};
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy, WorkloadProfile};
use omq_tokio::{
    Context, ContextConfig, DisconnectReason, Endpoint, Error, MechanismSetup, Message,
    MonitorEvent as NativeMonitorEvent, MonitorRecvError, MonitorStream, MonitorTryRecvError,
    Options, PeerCommandKind, PeerInfo as NativePeerInfo, SocketType,
};
#[cfg(feature = "curve")]
use omq_tokio::{CurveKeypair, CurvePublicKey, CurveSecretKey, CurveServerOptions};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("OMQ Go native binding requires a 64-bit target");

const OK: i32 = 0;
const AGAIN: i32 = 1;
const CLOSED: i32 = 2;
const TIMEOUT: i32 = 3;
const CANCELED: i32 = 4;
const INVALID_ENDPOINT: i32 = 5;
const UNSUPPORTED_SCHEME: i32 = 6;
const PROTOCOL: i32 = 7;
const CONFIG: i32 = 8;
const IO: i32 = 9;
const UNROUTABLE: i32 = 10;
const MESSAGE_TOO_LARGE: i32 = 11;
const ERROR: i32 = 99;

const RECV_BATCH: usize = 64;
const RING_BATCH: usize = 512;

#[repr(C)]
pub struct OmqGoStatus {
    code: i32,
    message: *mut c_char,
}

#[repr(C)]
pub struct OmqGoPart {
    data: *mut u8,
    len: usize,
}

#[repr(C)]
pub struct OmqGoMessage {
    parts: *mut OmqGoPart,
    part_count: usize,
}

#[repr(C)]
pub struct OmqGoWireMessage {
    parts: *const OmqGoPart,
    part_count: usize,
}

#[repr(C)]
pub struct OmqGoEvent {
    kind: *mut c_char,
    endpoint: *mut c_char,
    peer_ident: *mut c_char,
    peer_address: *mut c_char,
    peer_socket_type: *mut c_char,
    reason: *mut c_char,
    command_name: *mut c_char,
    data: *mut u8,
    data_len: usize,
    peer_identity: *mut u8,
    peer_identity_len: usize,
    connection_id: u64,
    retry_millis: u64,
    attempt: u32,
    zmtp_major: u32,
    zmtp_minor: u32,
    has_peer: c_int,
}

#[repr(C)]
pub struct OmqGoAuthPeer {
    mechanism_data: *const u8,
    mechanism_len: usize,
    public_key_data: *const u8,
    public_key_len: usize,
    identity_data: *const u8,
    identity_len: usize,
    username_data: *const u8,
    username_len: usize,
    password_data: *const u8,
    password_len: usize,
}

#[cfg(any(feature = "plain", feature = "curve"))]
unsafe extern "C" {
    fn omqGoAuthCallback(callback_id: u64, peer: *const OmqGoAuthPeer) -> c_int;
}

#[repr(C)]
pub struct OmqGoSendRingMemory {
    control: *mut c_void,
    descriptors: *mut c_void,
    payload: *mut c_void,
    desc_capacity: usize,
    payload_capacity: usize,
}

#[repr(C)]
pub struct OmqGoRecvRingMemory {
    control: *mut c_void,
    descriptors: *mut c_void,
    payload: *mut c_void,
    desc_capacity: usize,
    payload_capacity: usize,
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
    pending: VecDeque<Message>,
    scratch: Vec<Message>,
    external: VecDeque<RecvExternalBlock>,
}

pub struct OmqGoCancel {
    inner: BlockingRecvCancel,
}

const RECV_RING_FLAG_MULTIPART: u64 = 1;
const RECV_RING_FLAG_EXTERNAL: u64 = 2;

pub struct OmqGoContext {
    ctx: Context,
    owner: bool,
    closed: AtomicBool,
}

pub struct OmqGoSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    socket: OnceLock<BlockingSocket>,
    materialize_lock: Mutex<()>,
    recv_cache: Mutex<VecDeque<Message>>,
    recv_scratch: Mutex<Vec<Message>>,
    closed: AtomicBool,
}

pub struct OmqGoMonitor {
    ctx: Context,
    stream: Mutex<Option<MonitorStream>>,
    closed: AtomicBool,
}

impl OmqGoStatus {
    fn ok() -> Self {
        Self {
            code: OK,
            message: ptr::null_mut(),
        }
    }

    fn err(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: string_to_raw(message.into()),
        }
    }

    fn from_error(error: Error) -> Self {
        match error {
            Error::InvalidEndpoint(message) => Self::err(INVALID_ENDPOINT, message),
            Error::UnsupportedScheme(message) => Self::err(UNSUPPORTED_SCHEME, message),
            Error::UnsupportedZmtpVersion { major, minor } => Self::err(
                PROTOCOL,
                format!("unsupported ZMTP version: {major}.{minor}"),
            ),
            Error::Protocol(message) | Error::HandshakeFailed(message) => {
                Self::err(PROTOCOL, message)
            }
            Error::Closed => Self::err(CLOSED, "socket closed"),
            Error::Timeout => Self::err(TIMEOUT, "operation timed out"),
            Error::MessageTooLarge { size, max } => Self::err(
                MESSAGE_TOO_LARGE,
                format!("message too large: {size} bytes exceeds max {max}"),
            ),
            Error::Unroutable => Self::err(UNROUTABLE, "no route to peer"),
            Error::WouldBlock => Self::err(AGAIN, "operation would block"),
            Error::Config(message) => Self::err(CONFIG, message),
            Error::Io(error) => Self::err(IO, error.to_string()),
            _ => Self::err(ERROR, error.to_string()),
        }
    }

    fn from_try_send(error: TrySendError) -> Self {
        match error {
            TrySendError::Full(_) => Self::err(AGAIN, "send queue full"),
            TrySendError::Closed => Self::err(CLOSED, "socket closed"),
            TrySendError::Error(error) => Self::from_error(error),
        }
    }
}

fn status_code_from_error(error: &Error) -> i32 {
    match error {
        Error::InvalidEndpoint(_) => INVALID_ENDPOINT,
        Error::UnsupportedScheme(_) => UNSUPPORTED_SCHEME,
        Error::UnsupportedZmtpVersion { .. } | Error::Protocol(_) | Error::HandshakeFailed(_) => {
            PROTOCOL
        }
        Error::Closed => CLOSED,
        Error::Timeout => TIMEOUT,
        Error::MessageTooLarge { .. } => MESSAGE_TOO_LARGE,
        Error::Unroutable => UNROUTABLE,
        Error::WouldBlock => AGAIN,
        Error::Config(_) => CONFIG,
        Error::Io(_) => IO,
        _ => ERROR,
    }
}

impl OmqGoSocket {
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
        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone());
        }

        let options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .clone();
        options.validate()?;
        let socket = self.ctx.blocking_socket(self.socket_type, options);
        self.socket
            .set(socket.clone())
            .map_err(|_| Error::Config("socket materialized concurrently".to_string()))?;
        Ok(socket)
    }

    fn set_option<F>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Options),
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.socket.get().is_some() {
            return Err(Error::Config(
                "socket options must be set before bind, connect, send, or recv".to_string(),
            ));
        }
        let mut options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?;
        f(&mut options);
        Ok(())
    }

    fn configured_linger(&self) -> Result<Option<Duration>, Error> {
        Ok(self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .linger)
    }
}

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
            pending: VecDeque::new(),
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

    fn fill(&mut self, timeout_millis: i64, max_messages: usize) -> Result<usize, Error> {
        if max_messages == 0 {
            return Err(Error::Config(
                "max messages must be greater than zero".to_string(),
            ));
        }

        self.reclaim_consumed();
        let start_cursor = self.cursor;

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
            let batch_max = remaining.min(desc_space).min(RING_BATCH);
            self.scratch.clear();
            recv_many_into(&self.socket, batch_max, timeout_millis, &mut self.scratch)?;

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

        let filled = self.flush(start_cursor);
        if filled == 0 {
            return Err(Error::WouldBlock);
        }
        Ok(filled)
    }

    fn fill_cancelable(
        &mut self,
        cancel: &BlockingRecvCancel,
        max_messages: usize,
    ) -> Result<Option<usize>, Error> {
        if max_messages == 0 {
            return Err(Error::Config(
                "max messages must be greater than zero".to_string(),
            ));
        }
        self.reclaim_consumed();
        let start_cursor = self.cursor;

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
            let batch_max = remaining.min(desc_space).min(RING_BATCH);
            self.scratch.clear();
            let Some(_) = self.socket.recv_many_registered_cancelable_into(
                batch_max,
                cancel,
                &mut self.scratch,
            )?
            else {
                return Ok(None);
            };

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

        let filled = self.flush(start_cursor);
        if filled == 0 {
            return Err(Error::WouldBlock);
        }
        Ok(Some(filled))
    }
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
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::spawn(move || send_ring_worker(worker_shared));
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
            self.control.head.store(reclaim.cursor, Ordering::Release);
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
            send_ring_backoff(&mut spins);
            continue;
        }

        match shared.socket.try_send_many(&mut batch, RING_BATCH) {
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

fn status_from_result(result: Result<(), Error>) -> OmqGoStatus {
    match result {
        Ok(()) => OmqGoStatus::ok(),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

fn empty_cstring() -> CString {
    CString::new("").expect("empty string contains no NUL")
}

fn cstring_lossy(message: &str) -> CString {
    let bytes: Vec<u8> = message
        .as_bytes()
        .iter()
        .copied()
        .filter(|byte| *byte != 0)
        .collect();
    CString::new(bytes).unwrap_or_else(|_| empty_cstring())
}

fn string_to_raw(value: String) -> *mut c_char {
    let value = value.replace('\0', "\\0");
    CString::new(value).expect("nul removed").into_raw()
}

fn str_from_c(ptr: *const c_char) -> Result<&'static str, Error> {
    if ptr.is_null() {
        return Err(Error::Config("null string pointer".to_string()));
    }
    let value = unsafe { CStr::from_ptr(ptr) };
    value
        .to_str()
        .map_err(|error| Error::Config(format!("invalid UTF-8 string: {error}")))
}

fn bytes_from_c<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Error::Config("null byte pointer".to_string()));
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

fn bytes_from_c_mut<'a>(ptr: *mut u8, len: usize) -> Result<&'a mut [u8], Error> {
    if len == 0 {
        return Ok(&mut []);
    }
    if ptr.is_null() {
        return Err(Error::Config("null byte pointer".to_string()));
    }
    Ok(unsafe { slice::from_raw_parts_mut(ptr, len) })
}

fn endpoint_from_c(ptr: *const c_char) -> Result<Endpoint, Error> {
    Endpoint::from_str(str_from_c(ptr)?)
}

fn socket_type_from_i32(value: i32) -> Result<SocketType, Error> {
    match value {
        1 => Ok(SocketType::Pair),
        2 => Ok(SocketType::Pub),
        3 => Ok(SocketType::Sub),
        4 => Ok(SocketType::Req),
        5 => Ok(SocketType::Rep),
        6 => Ok(SocketType::Dealer),
        7 => Ok(SocketType::Router),
        8 => Ok(SocketType::Pull),
        9 => Ok(SocketType::Push),
        10 => Ok(SocketType::XPub),
        11 => Ok(SocketType::XSub),
        12 => Ok(SocketType::Stream),
        13 => Ok(SocketType::Server),
        14 => Ok(SocketType::Client),
        15 => Ok(SocketType::Radio),
        16 => Ok(SocketType::Dish),
        17 => Ok(SocketType::Gather),
        18 => Ok(SocketType::Scatter),
        19 => Ok(SocketType::Peer),
        20 => Ok(SocketType::Channel),
        _ => Err(Error::Config(format!("unknown socket type {value}"))),
    }
}

fn duration_from_timeout_millis(timeout_millis: i64) -> Option<Duration> {
    if timeout_millis < 0 {
        None
    } else {
        Some(Duration::from_millis(timeout_millis as u64))
    }
}

fn duration_from_millis(millis: i64) -> Result<Duration, Error> {
    if millis < 0 {
        return Err(Error::Config("duration must be non-negative".to_string()));
    }
    Ok(Duration::from_millis(millis as u64))
}

fn optional_duration_from_millis(millis: i64) -> Result<Option<Duration>, Error> {
    if millis < 0 {
        Ok(None)
    } else {
        duration_from_millis(millis).map(Some)
    }
}

fn optional_usize_from_i64(name: &str, value: i64) -> Result<Option<usize>, Error> {
    if value < 0 {
        Ok(None)
    } else {
        usize::try_from(value)
            .map(Some)
            .map_err(|_| Error::Config(format!("{name} exceeds usize")))
    }
}

fn linger_from_millis(millis: i64) -> Option<Duration> {
    if millis < 0 {
        None
    } else {
        Some(Duration::from_millis(millis as u64))
    }
}

#[cfg(feature = "curve")]
fn curve_keypair_from_z85(public_key: &str, secret_key: &str) -> Result<CurveKeypair, Error> {
    let public = CurvePublicKey::from_z85(public_key)?;
    let secret = CurveSecretKey::from_z85(secret_key)?;
    Ok(CurveKeypair { public, secret })
}

fn message_from_c(parts: *const OmqGoPart, part_count: usize) -> Result<Message, Error> {
    if part_count == 0 {
        return Ok(Message::new());
    }
    if parts.is_null() {
        return Err(Error::Config("null message parts pointer".to_string()));
    }
    let parts = unsafe { slice::from_raw_parts(parts, part_count) };
    if part_count == 1 {
        let part = &parts[0];
        return Ok(Message::from_slice(bytes_from_c(part.data, part.len)?));
    }

    let mut copied = Vec::with_capacity(part_count);
    for part in parts {
        copied.push(Bytes::copy_from_slice(bytes_from_c(part.data, part.len)?));
    }
    Ok(Message::multipart(copied))
}

fn messages_from_c(
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
        out.push_back(message_from_c(message.parts, message.part_count)?);
    }
    Ok(out)
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

fn raw_bytes(bytes: &[u8]) -> (*mut u8, usize) {
    if bytes.is_empty() {
        return (ptr::null_mut(), 0);
    }
    let mut boxed = bytes.to_vec().into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    (data, len)
}

fn message_to_c(message: Message) -> OmqGoMessage {
    let part_count = message.len();
    if part_count == 0 {
        return OmqGoMessage {
            parts: ptr::null_mut(),
            part_count: 0,
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
    }
}

fn recv_many_into(
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
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    let mut spins = 0u32;

    loop {
        if let Some(event) = try_receive_any_entry(&entries)? {
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

fn try_send_with_timeout(
    native: &BlockingSocket,
    mut message: Message,
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

    let deadline = Instant::now() + Duration::from_millis(timeout_millis as u64);
    loop {
        match native.try_send(message) {
            Ok(()) => return OmqGoStatus::ok(),
            Err(TrySendError::Full(returned)) => {
                if Instant::now() >= deadline {
                    return OmqGoStatus::err(TIMEOUT, "operation timed out");
                }
                message = returned;
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return OmqGoStatus::from_try_send(error),
        }
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
    monitor: &OmqGoMonitor,
    timeout_millis: i64,
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
            .ok_or_else(|| Error::Config("monitor receive already in progress".to_string()))?
    };

    let result = if timeout_millis == 0 {
        monitor_try_recv_error(match stream.try_recv() {
            Ok(event) => {
                let mut guard = monitor
                    .stream
                    .lock()
                    .map_err(|_| Error::Config("monitor lock poisoned".to_string()))?;
                *guard = Some(stream);
                return Ok(Some(event));
            }
            Err(error) => error,
        })
    } else {
        let timeout = duration_from_timeout_millis(timeout_millis);
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

fn disconnect_reason(reason: DisconnectReason) -> String {
    match reason {
        DisconnectReason::PeerClosed => "peer closed".to_string(),
        DisconnectReason::LocalClose => "local close".to_string(),
        DisconnectReason::Error(error) => error,
        DisconnectReason::Handover => "handover".to_string(),
        _ => "unknown".to_string(),
    }
}

fn bytes_to_event_data(bytes: Bytes) -> (*mut u8, usize) {
    raw_bytes(&bytes)
}

fn fill_monitor_peer(out: &mut OmqGoEvent, peer: NativePeerInfo) {
    out.has_peer = 1;
    out.connection_id = peer.connection_id;
    out.zmtp_major = u32::from(peer.zmtp_version.0);
    out.zmtp_minor = u32::from(peer.zmtp_version.1);
    if let Some(identity) = peer.peer_identity {
        let (data, len) = bytes_to_event_data(identity);
        out.peer_identity = data;
        out.peer_identity_len = len;
    }
    if let Some(address) = peer.peer_address {
        out.peer_address = string_to_raw(address.to_string());
    }
    if let Some(socket_type) = peer.peer_properties.socket_type {
        out.peer_socket_type = string_to_raw(format!("{socket_type:?}").to_ascii_uppercase());
    }
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn go_authenticator(callback_id: u64) -> Authenticator {
    Authenticator::new(move |peer| go_authenticate(callback_id, peer))
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn go_authenticate(callback_id: u64, peer: &MechanismPeerInfo) -> bool {
    let mechanism = peer.mechanism.as_str().unwrap_or("");
    let public_key = peer_public_key_z85(peer);

    let (mechanism_data, mechanism_len) = str_view(mechanism);
    let (public_key_data, public_key_len) = str_view(&public_key);
    let (identity_data, identity_len) = optional_bytes_view(peer.identity.as_deref());
    let (username_data, username_len) = optional_str_view(peer.username.as_deref());
    let (password_data, password_len) = optional_str_view(peer.password.as_deref());

    let c_peer = OmqGoAuthPeer {
        mechanism_data,
        mechanism_len,
        public_key_data,
        public_key_len,
        identity_data,
        identity_len,
        username_data,
        username_len,
        password_data,
        password_len,
    };
    unsafe { omqGoAuthCallback(callback_id, &c_peer) != 0 }
}

#[cfg(feature = "curve")]
fn peer_public_key_z85(peer: &MechanismPeerInfo) -> String {
    if peer.mechanism == omq_proto::proto::MechanismName::CURVE {
        return CurvePublicKey::from_bytes(peer.public_key).to_z85();
    }
    String::new()
}

#[cfg(not(feature = "curve"))]
fn peer_public_key_z85(_peer: &MechanismPeerInfo) -> String {
    String::new()
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn str_view(value: &str) -> (*const u8, usize) {
    if value.is_empty() {
        return (ptr::null(), 0);
    }
    (value.as_ptr(), value.len())
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn optional_str_view(value: Option<&str>) -> (*const u8, usize) {
    value.map_or((ptr::null(), 0), str_view)
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn optional_bytes_view(value: Option<&[u8]>) -> (*const u8, usize) {
    match value {
        Some(bytes) if !bytes.is_empty() => (bytes.as_ptr(), bytes.len()),
        _ => (ptr::null(), 0),
    }
}

fn event_to_c(event: NativeMonitorEvent) -> OmqGoEvent {
    let mut out = OmqGoEvent {
        kind: ptr::null_mut(),
        endpoint: ptr::null_mut(),
        peer_ident: ptr::null_mut(),
        peer_address: ptr::null_mut(),
        peer_socket_type: ptr::null_mut(),
        reason: ptr::null_mut(),
        command_name: ptr::null_mut(),
        data: ptr::null_mut(),
        data_len: 0,
        peer_identity: ptr::null_mut(),
        peer_identity_len: 0,
        connection_id: 0,
        retry_millis: 0,
        attempt: 0,
        zmtp_major: 0,
        zmtp_minor: 0,
        has_peer: 0,
    };

    match event {
        NativeMonitorEvent::Listening { endpoint } => {
            out.kind = string_to_raw("LISTENING".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
        }
        NativeMonitorEvent::Accepted {
            endpoint,
            peer_ident,
            connection_id,
        } => {
            out.kind = string_to_raw("ACCEPTED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            out.peer_ident = string_to_raw(peer_ident.to_string());
            out.connection_id = connection_id;
        }
        NativeMonitorEvent::Connected {
            endpoint,
            peer_ident,
            connection_id,
        } => {
            out.kind = string_to_raw("CONNECTED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            out.peer_ident = string_to_raw(peer_ident.to_string());
            out.connection_id = connection_id;
        }
        NativeMonitorEvent::HandshakeSucceeded { endpoint, peer } => {
            out.kind = string_to_raw("HANDSHAKE_SUCCEEDED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            fill_monitor_peer(&mut out, peer);
        }
        NativeMonitorEvent::HandshakeFailed {
            endpoint,
            peer_ident,
            reason,
        } => {
            out.kind = string_to_raw("HANDSHAKE_FAILED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            out.peer_ident = string_to_raw(peer_ident.to_string());
            out.reason = string_to_raw(reason);
        }
        NativeMonitorEvent::ConnectDelayed {
            endpoint,
            retry_in,
            attempt,
        } => {
            out.kind = string_to_raw("CONNECT_DELAYED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            out.retry_millis = retry_in.as_millis() as u64;
            out.attempt = attempt;
        }
        NativeMonitorEvent::Disconnected {
            endpoint,
            peer,
            reason,
        } => {
            out.kind = string_to_raw("DISCONNECTED".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            fill_monitor_peer(&mut out, peer);
            out.reason = string_to_raw(disconnect_reason(reason));
        }
        NativeMonitorEvent::SubscribeReceived { prefix } => {
            out.kind = string_to_raw("SUBSCRIBE_RECEIVED".to_string());
            let (data, len) = bytes_to_event_data(prefix);
            out.data = data;
            out.data_len = len;
        }
        NativeMonitorEvent::UnsubscribeReceived { prefix } => {
            out.kind = string_to_raw("UNSUBSCRIBE_RECEIVED".to_string());
            let (data, len) = bytes_to_event_data(prefix);
            out.data = data;
            out.data_len = len;
        }
        NativeMonitorEvent::JoinReceived { group } => {
            out.kind = string_to_raw("JOIN_RECEIVED".to_string());
            let (data, len) = bytes_to_event_data(group);
            out.data = data;
            out.data_len = len;
        }
        NativeMonitorEvent::LeaveReceived { group } => {
            out.kind = string_to_raw("LEAVE_RECEIVED".to_string());
            let (data, len) = bytes_to_event_data(group);
            out.data = data;
            out.data_len = len;
        }
        NativeMonitorEvent::PeerCommand {
            endpoint, command, ..
        } => {
            out.kind = string_to_raw("PEER_COMMAND".to_string());
            out.endpoint = string_to_raw(endpoint.to_string());
            match command {
                PeerCommandKind::Error { reason } => {
                    out.command_name = string_to_raw("ERROR".to_string());
                    out.reason = string_to_raw(reason);
                }
                PeerCommandKind::Unknown { name, body } => {
                    out.command_name = string_to_raw(String::from_utf8_lossy(&name).into_owned());
                    let (data, len) = bytes_to_event_data(body);
                    out.data = data;
                    out.data_len = len;
                }
                _ => {
                    out.reason = string_to_raw("unknown peer command".to_string());
                }
            }
        }
        NativeMonitorEvent::Closed => {
            out.kind = string_to_raw("CLOSED".to_string());
        }
        _ => {
            out.kind = string_to_raw("UNKNOWN".to_string());
        }
    }
    out
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_open(
    io_threads: usize,
    out: *mut *mut OmqGoContext,
) -> OmqGoStatus {
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null context output pointer");
    }
    if io_threads == 0 {
        return OmqGoStatus::err(CONFIG, "io_threads must be greater than zero");
    }

    let ctx = Context::with_config(ContextConfig { io_threads });
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoContext {
            ctx,
            owner: true,
            closed: AtomicBool::new(false),
        }));
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_from_share_key(
    high: u64,
    low: u64,
    out: *mut *mut OmqGoContext,
) -> OmqGoStatus {
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null context output pointer");
    }
    let key = ((high as u128) << 64) | low as u128;
    let Some(ctx) = Context::from_share_key(key) else {
        return OmqGoStatus::err(CLOSED, "shared context not found");
    };
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoContext {
            ctx,
            owner: false,
            closed: AtomicBool::new(false),
        }));
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_share_key(
    ctx: *mut OmqGoContext,
    high: *mut u64,
    low: *mut u64,
) -> OmqGoStatus {
    if ctx.is_null() || high.is_null() || low.is_null() {
        return OmqGoStatus::err(CONFIG, "null context share key pointer");
    }
    let ctx = unsafe { &*ctx };
    if ctx.closed.load(Ordering::Acquire) {
        return OmqGoStatus::err(CLOSED, "context closed");
    }
    let key = ctx.ctx.share_key();
    unsafe {
        *high = (key >> 64) as u64;
        *low = key as u64;
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_close(ctx: *mut OmqGoContext) {
    if ctx.is_null() {
        return;
    }
    let ctx = unsafe { &*ctx };
    let was_open = !ctx.closed.swap(true, Ordering::AcqRel);
    if was_open && ctx.owner {
        ctx.ctx.term();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_free(ctx: *mut OmqGoContext) {
    if ctx.is_null() {
        return;
    }
    omq_go_context_close(ctx);
    unsafe {
        drop(Box::from_raw(ctx));
    }
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_curve_keypair(
    public_key: *mut *mut c_char,
    secret_key: *mut *mut c_char,
) -> OmqGoStatus {
    if public_key.is_null() || secret_key.is_null() {
        return OmqGoStatus::err(CONFIG, "null CURVE key output pointer");
    }
    let keypair = CurveKeypair::generate();
    unsafe {
        *public_key = string_to_raw(keypair.public.to_z85());
        *secret_key = string_to_raw(keypair.secret.to_z85());
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_curve_keypair(
    _public_key: *mut *mut c_char,
    _secret_key: *mut *mut c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_curve_public(
    secret_key: *const c_char,
    public_key: *mut *mut c_char,
) -> OmqGoStatus {
    if public_key.is_null() {
        return OmqGoStatus::err(CONFIG, "null CURVE public key output pointer");
    }
    let result = (|| {
        let secret = CurveSecretKey::from_z85(str_from_c(secret_key)?)?;
        unsafe {
            *public_key = string_to_raw(secret.derive_public().to_z85());
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_curve_public(
    _secret_key: *const c_char,
    _public_key: *mut *mut c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_new(
    ctx: *mut OmqGoContext,
    socket_type: i32,
    out: *mut *mut OmqGoSocket,
) -> OmqGoStatus {
    if ctx.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null socket creation pointer");
    }
    let ctx = unsafe { &*ctx };
    if ctx.closed.load(Ordering::Acquire) {
        return OmqGoStatus::err(CLOSED, "context closed");
    }
    let socket_type = match socket_type_from_i32(socket_type) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoSocket {
            ctx: ctx.ctx.clone(),
            socket_type,
            options: Mutex::new(Options::default()),
            socket: OnceLock::new(),
            materialize_lock: Mutex::new(()),
            recv_cache: Mutex::new(VecDeque::new()),
            recv_scratch: Mutex::new(Vec::with_capacity(RECV_BATCH)),
            closed: AtomicBool::new(false),
        }));
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_bind(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
    bound_endpoint: *mut *mut c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let endpoint = endpoint_from_c(endpoint)?;
        let bound = socket.materialize()?.bind(endpoint)?;
        if !bound_endpoint.is_null() {
            unsafe {
                *bound_endpoint = string_to_raw(bound.to_string());
            }
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_connect(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.connect(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_unbind(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.unbind(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_disconnect(
    socket: *mut OmqGoSocket,
    endpoint: *const c_char,
) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| socket.materialize()?.disconnect(endpoint_from_c(endpoint)?))();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_send(
    socket: *mut OmqGoSocket,
    parts: *const OmqGoPart,
    part_count: usize,
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
        let message = message_from_c(parts, part_count)?;
        Ok((native, message))
    })();
    match result {
        Ok((native, message)) => try_send_with_timeout(&native, message, timeout_millis),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_send_one(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
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
        let message = Message::from_slice(bytes_from_c(data, len)?);
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
pub extern "C" fn omq_go_socket_subscribe(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.subscribe(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_unsubscribe(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.unsubscribe(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_join(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.join(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_leave(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    socket_bytes_op(socket, data, len, |socket, bytes| {
        socket.leave(Bytes::copy_from_slice(bytes))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_wait_connected(
    socket: *mut OmqGoSocket,
    min_peers: usize,
    timeout_millis: i64,
    out: *mut usize,
) -> OmqGoStatus {
    if socket.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null wait_connected pointer");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let timeout = duration_from_millis(timeout_millis)?;
        let count = socket.materialize()?.wait_connected(min_peers, timeout)?;
        unsafe {
            *out = count;
        }
        Ok(())
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_wait_subscribed(
    socket: *mut OmqGoSocket,
    min_subscriptions: u64,
    timeout_millis: i64,
    out: *mut u64,
) -> OmqGoStatus {
    if socket.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null wait_subscribed pointer");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let timeout = duration_from_millis(timeout_millis)?;
        let count = socket
            .materialize()?
            .wait_subscribed(min_subscriptions, timeout)?;
        unsafe {
            *out = count;
        }
        Ok(())
    })();
    status_from_result(result)
}

fn socket_bytes_op<F>(socket: *mut OmqGoSocket, data: *const u8, len: usize, op: F) -> OmqGoStatus
where
    F: FnOnce(BlockingSocket, &[u8]) -> Result<(), Error>,
{
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    let socket = unsafe { &*socket };
    let result = (|| {
        let data = bytes_from_c(data, len)?;
        op(socket.materialize()?, data)
    })();
    status_from_result(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_close(socket: *mut OmqGoSocket, linger_millis: i64) -> OmqGoStatus {
    if socket.is_null() {
        return OmqGoStatus::ok();
    }
    let socket = unsafe { &*socket };
    let was_open = !socket.closed.swap(true, Ordering::AcqRel);
    if !was_open {
        return OmqGoStatus::ok();
    }

    let Some(native) = socket.socket.get() else {
        return OmqGoStatus::ok();
    };
    let linger = if linger_millis == -2 {
        match socket.configured_linger() {
            Ok(value) => value,
            Err(error) => return OmqGoStatus::from_error(error),
        }
    } else {
        linger_from_millis(linger_millis)
    };
    status_from_result(native.clone().close_with_linger(linger))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_free(socket: *mut OmqGoSocket) {
    if socket.is_null() {
        return;
    }
    let _ = omq_go_socket_close(socket, -2);
    unsafe {
        drop(Box::from_raw(socket));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_send_hwm(socket: *mut OmqGoSocket, value: u32) -> OmqGoStatus {
    set_socket_option(socket, |options| options.send_hwm = value)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_recv_hwm(socket: *mut OmqGoSocket, value: u32) -> OmqGoStatus {
    set_socket_option(socket, |options| options.recv_hwm = value)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_linger(socket: *mut OmqGoSocket, millis: i64) -> OmqGoStatus {
    set_socket_option(socket, |options| {
        options.linger = linger_from_millis(millis)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_identity(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    let bytes = match bytes_from_c(data, len) {
        Ok(bytes) => Bytes::copy_from_slice(bytes),
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, |options| options.identity = bytes)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_heartbeat_interval(
    socket: *mut OmqGoSocket,
    millis: i64,
) -> OmqGoStatus {
    let interval = match optional_duration_from_millis(millis) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.heartbeat_interval = interval)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_handshake_timeout(
    socket: *mut OmqGoSocket,
    millis: i64,
) -> OmqGoStatus {
    let timeout = match optional_duration_from_millis(millis) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.handshake_timeout = timeout)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_max_message_size(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let max = match optional_usize_from_i64("max message size", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.max_message_size = max)
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_server(
    socket: *mut OmqGoSocket,
    username: *const c_char,
    password: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let expected_username = str_from_c(username)?.to_string();
        let expected_password = str_from_c(password)?.to_string();
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::PlainServer {
                authenticator: Authenticator::new(move |peer| {
                    peer.username.as_deref() == Some(expected_username.as_str())
                        && peer.password.as_deref() == Some(expected_password.as_str())
                }),
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_server(
    _socket: *mut OmqGoSocket,
    _username: *const c_char,
    _password: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_server_callback(
    socket: *mut OmqGoSocket,
    callback_id: u64,
) -> OmqGoStatus {
    if callback_id == 0 {
        return OmqGoStatus::err(CONFIG, "invalid auth callback id");
    }
    set_socket_option(socket, move |options| {
        options.mechanism = MechanismSetup::PlainServer {
            authenticator: go_authenticator(callback_id),
        };
    })
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_server_callback(
    _socket: *mut OmqGoSocket,
    _callback_id: u64,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "C" fn omq_go_socket_set_plain_client(
    socket: *mut OmqGoSocket,
    username: *const c_char,
    password: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let username = str_from_c(username)?.to_string();
        let password = str_from_c(password)?.to_string();
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::PlainClient { username, password };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "C" fn omq_go_socket_set_plain_client(
    _socket: *mut OmqGoSocket,
    _username: *const c_char,
    _password: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "PLAIN support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_server(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::CurveServer {
                our_keypair: keypair,
                options: CurveServerOptions::default(),
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_server(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_server_callback(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
    callback_id: u64,
) -> OmqGoStatus {
    if callback_id == 0 {
        return OmqGoStatus::err(CONFIG, "invalid auth callback id");
    }
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            let mut curve_options = CurveServerOptions::default();
            curve_options.authenticator = Some(go_authenticator(callback_id));
            options.mechanism = MechanismSetup::CurveServer {
                our_keypair: keypair,
                options: curve_options,
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_server_callback(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
    _callback_id: u64,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "C" fn omq_go_socket_set_curve_client(
    socket: *mut OmqGoSocket,
    public_key: *const c_char,
    secret_key: *const c_char,
    server_public_key: *const c_char,
) -> OmqGoStatus {
    let result = (|| {
        let keypair = curve_keypair_from_z85(str_from_c(public_key)?, str_from_c(secret_key)?)?;
        let server_public = CurvePublicKey::from_z85(str_from_c(server_public_key)?)?;
        Ok(set_socket_option(socket, move |options| {
            options.mechanism = MechanismSetup::CurveClient {
                our_keypair: keypair,
                server_public,
            };
        }))
    })();
    match result {
        Ok(status) => status,
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "C" fn omq_go_socket_set_curve_client(
    _socket: *mut OmqGoSocket,
    _public_key: *const c_char,
    _secret_key: *const c_char,
    _server_public_key: *const c_char,
) -> OmqGoStatus {
    OmqGoStatus::err(CONFIG, "CURVE support not enabled")
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_workload_profile(
    socket: *mut OmqGoSocket,
    profile: i32,
) -> OmqGoStatus {
    let profile = match profile {
        -1 => None,
        0 => Some(WorkloadProfile::Throughput),
        1 => Some(WorkloadProfile::Latency),
        other => return OmqGoStatus::err(CONFIG, format!("unknown workload profile {other}")),
    };
    set_socket_option(socket, move |options| options.workload_profile = profile)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_reconnect(
    socket: *mut OmqGoSocket,
    mode: i32,
    min_millis: i64,
    max_millis: i64,
) -> OmqGoStatus {
    let reconnect = match mode {
        0 => ReconnectPolicy::Disabled,
        1 => match duration_from_millis(min_millis) {
            Ok(min) => ReconnectPolicy::Fixed(min),
            Err(error) => return OmqGoStatus::from_error(error),
        },
        2 => {
            let min = match duration_from_millis(min_millis) {
                Ok(min) => min,
                Err(error) => return OmqGoStatus::from_error(error),
            };
            let max = match duration_from_millis(max_millis) {
                Ok(max) => max,
                Err(error) => return OmqGoStatus::from_error(error),
            };
            if max < min {
                return OmqGoStatus::err(
                    CONFIG,
                    "reconnect max must be greater than or equal to min",
                );
            }
            ReconnectPolicy::Exponential { min, max }
        }
        other => return OmqGoStatus::err(CONFIG, format!("unknown reconnect mode {other}")),
    };
    set_socket_option(socket, move |options| options.reconnect = reconnect)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_reconnect_stop_conn_refused(
    socket: *mut OmqGoSocket,
    enabled: i32,
) -> OmqGoStatus {
    set_socket_option(socket, move |options| {
        options.reconnect_stop_conn_refused = enabled != 0
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_heartbeat_ttl(
    socket: *mut OmqGoSocket,
    millis: i64,
) -> OmqGoStatus {
    let ttl = match optional_duration_from_millis(millis) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.heartbeat_ttl = ttl)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_heartbeat_timeout(
    socket: *mut OmqGoSocket,
    millis: i64,
) -> OmqGoStatus {
    let timeout = match optional_duration_from_millis(millis) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.heartbeat_timeout = timeout)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_max_pending_handshakes(
    socket: *mut OmqGoSocket,
    max: usize,
) -> OmqGoStatus {
    if max == 0 {
        return OmqGoStatus::err(CONFIG, "max pending handshakes must be greater than zero");
    }
    set_socket_option(socket, move |options| options.max_pending_handshakes = max)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_conflate(
    socket: *mut OmqGoSocket,
    enabled: i32,
) -> OmqGoStatus {
    set_socket_option(socket, |options| options.conflate = enabled != 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_router_mandatory(
    socket: *mut OmqGoSocket,
    enabled: i32,
) -> OmqGoStatus {
    set_socket_option(socket, |options| options.router_mandatory = enabled != 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_on_mute(socket: *mut OmqGoSocket, mode: i32) -> OmqGoStatus {
    let on_mute = match mode {
        0 => OnMute::Block,
        1 => OnMute::DropNewest,
        2 => OnMute::DropOldest,
        other => return OmqGoStatus::err(CONFIG, format!("unknown on-mute mode {other}")),
    };
    set_socket_option(socket, move |options| options.on_mute = on_mute)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_tcp_keepalive(
    socket: *mut OmqGoSocket,
    mode: i32,
    idle_millis: i64,
    interval_millis: i64,
    count: u32,
) -> OmqGoStatus {
    let keepalive = match mode {
        0 => KeepAlive::Default,
        1 => KeepAlive::Disabled,
        2 => {
            if count == 0 {
                return OmqGoStatus::err(CONFIG, "TCP keepalive count must be greater than zero");
            }
            let idle = match duration_from_millis(idle_millis) {
                Ok(idle) => idle,
                Err(error) => return OmqGoStatus::from_error(error),
            };
            let intvl = match duration_from_millis(interval_millis) {
                Ok(intvl) => intvl,
                Err(error) => return OmqGoStatus::from_error(error),
            };
            KeepAlive::Enabled {
                idle,
                intvl,
                cnt: count,
            }
        }
        other => return OmqGoStatus::err(CONFIG, format!("unknown TCP keepalive mode {other}")),
    };
    set_socket_option(socket, move |options| options.tcp_keepalive = keepalive)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_send_buffer_size(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("send buffer size", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.send_buffer_size = bytes)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_recv_buffer_size(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("receive buffer size", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.recv_buffer_size = bytes)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_xpub_nodrop(
    socket: *mut OmqGoSocket,
    enabled: i32,
) -> OmqGoStatus {
    set_socket_option(socket, |options| options.xpub_nodrop = enabled != 0)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_auto_train(
    socket: *mut OmqGoSocket,
    enabled: i32,
) -> OmqGoStatus {
    set_socket_option(socket, |options| {
        options.compression_auto_train = enabled != 0
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_threshold(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let value = if bytes < 0 {
        None
    } else {
        Some(bytes as usize)
    };
    set_socket_option(socket, |options| options.compression_threshold = value)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_level(
    socket: *mut OmqGoSocket,
    level: i64,
) -> OmqGoStatus {
    let value = if level == i64::MIN {
        None
    } else {
        Some(level as i32)
    };
    set_socket_option(socket, |options| options.compression_level = value)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_dict(
    socket: *mut OmqGoSocket,
    data: *const u8,
    len: usize,
) -> OmqGoStatus {
    let value = if len == 0 {
        None
    } else {
        match bytes_from_c(data, len) {
            Ok(bytes) => Some(Bytes::copy_from_slice(bytes)),
            Err(error) => return OmqGoStatus::from_error(error),
        }
    };
    set_socket_option(socket, |options| options.compression_dict = value)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_dict_capacity(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("compression dictionary capacity", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| {
        options.compression_dict_capacity = bytes
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_max_recv_dict_size(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("max receive dictionary size", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.max_recv_dict_size = bytes)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_compression_offload_threshold(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("compression offload threshold", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| {
        options.compression_offload_threshold = bytes
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_large_message_threshold(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("large message threshold", bytes) {
        Ok(value) => value.filter(|bytes| *bytes != 0),
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| {
        options.large_message_threshold = bytes
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_arena_threshold(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("arena threshold", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.arena_threshold = bytes)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_transmit_slot_cap(
    socket: *mut OmqGoSocket,
    bytes: i64,
) -> OmqGoStatus {
    let bytes = match optional_usize_from_i64("transmit slot capacity", bytes) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.transmit_slot_cap = bytes)
}

fn set_socket_option<F>(socket: *mut OmqGoSocket, f: F) -> OmqGoStatus
where
    F: FnOnce(&mut Options),
{
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    status_from_result(unsafe { &*socket }.set_option(f))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_monitor(
    socket: *mut OmqGoSocket,
    out: *mut *mut OmqGoMonitor,
) -> OmqGoStatus {
    if socket.is_null() || out.is_null() {
        return OmqGoStatus::err(CONFIG, "null monitor pointer");
    }
    let socket = unsafe { &*socket };
    match socket.materialize() {
        Ok(native) => {
            unsafe {
                *out = Box::into_raw(Box::new(OmqGoMonitor {
                    ctx: socket.ctx.clone(),
                    stream: Mutex::new(Some(native.monitor())),
                    closed: AtomicBool::new(false),
                }));
            }
            OmqGoStatus::ok()
        }
        Err(error) => OmqGoStatus::from_error(error),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_monitor_recv(
    monitor: *mut OmqGoMonitor,
    timeout_millis: i64,
    out: *mut OmqGoEvent,
) -> OmqGoStatus {
    if monitor.is_null() {
        return OmqGoStatus::err(CLOSED, "monitor closed");
    }
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null monitor event output pointer");
    }
    let monitor = unsafe { &*monitor };
    match monitor_recv_with_timeout(monitor, timeout_millis) {
        Ok(Some(event)) => {
            unsafe {
                *out = event_to_c(event);
            }
            OmqGoStatus::ok()
        }
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
pub extern "C" fn omq_go_monitor_close(monitor: *mut OmqGoMonitor) {
    if monitor.is_null() {
        return;
    }
    let monitor = unsafe { &*monitor };
    monitor.closed.store(true, Ordering::Release);
    if let Ok(mut guard) = monitor.stream.lock() {
        guard.take();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_monitor_free(monitor: *mut OmqGoMonitor) {
    if monitor.is_null() {
        return;
    }
    omq_go_monitor_close(monitor);
    unsafe {
        drop(Box::from_raw(monitor));
    }
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

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_send_ring_close(ring: *mut OmqGoSendRing) {
    if ring.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ring));
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
    match ring.fill(timeout_millis, max_messages) {
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
    match ring.fill_cancelable(&cancel.inner, max_messages) {
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
    unsafe {
        drop(Box::from_raw(ring));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_new() -> *mut OmqGoCancel {
    Box::into_raw(Box::new(OmqGoCancel {
        inner: BlockingRecvCancel::new(),
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel(cancel: *const OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    unsafe {
        (*cancel).inner.cancel();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_register_current(cancel: *const OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    unsafe {
        (*cancel).inner.register_current_thread_once();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_free(cancel: *mut OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(cancel));
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

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_event_free(event: OmqGoEvent) {
    omq_go_string_free(event.kind);
    omq_go_string_free(event.endpoint);
    omq_go_string_free(event.peer_ident);
    omq_go_string_free(event.peer_address);
    omq_go_string_free(event.peer_socket_type);
    omq_go_string_free(event.reason);
    omq_go_string_free(event.command_name);
    if !event.data.is_null() && event.data_len > 0 {
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                event.data,
                event.data_len,
            )));
        }
    }
    if !event.peer_identity.is_null() && event.peer_identity_len > 0 {
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                event.peer_identity,
                event.peer_identity_len,
            )));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_string_free(value: *mut c_char) {
    if value.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(value));
    }
}
