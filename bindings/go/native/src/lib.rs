#![expect(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "C ABI entrypoints validate raw pointers and report status codes"
)]

use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char};
use std::ptr;
use std::slice;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::TrySendError;
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{
    Context, ContextConfig, DisconnectReason, Endpoint, Error, Message,
    MonitorEvent as NativeMonitorEvent, MonitorRecvError, MonitorStream, MonitorTryRecvError,
    Options, PeerCommandKind, SocketType,
};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("OMQ Go native binding requires a 64-bit target");

const OK: i32 = 0;
const AGAIN: i32 = 1;
const CLOSED: i32 = 2;
const TIMEOUT: i32 = 3;
const INVALID_ENDPOINT: i32 = 5;
const UNSUPPORTED_SCHEME: i32 = 6;
const PROTOCOL: i32 = 7;
const CONFIG: i32 = 8;
const IO: i32 = 9;
const UNROUTABLE: i32 = 10;
const MESSAGE_TOO_LARGE: i32 = 11;
const ERROR: i32 = 99;

const RECV_BATCH: usize = 64;

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
    reason: *mut c_char,
    command_name: *mut c_char,
    data: *mut u8,
    data_len: usize,
    connection_id: u64,
    retry_millis: u64,
    attempt: u32,
}

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

fn status_from_result(result: Result<(), Error>) -> OmqGoStatus {
    match result {
        Ok(()) => OmqGoStatus::ok(),
        Err(error) => OmqGoStatus::from_error(error),
    }
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

fn linger_from_millis(millis: i64) -> Option<Duration> {
    if millis < 0 {
        None
    } else {
        Some(Duration::from_millis(millis as u64))
    }
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

fn refill_recv_cache(socket: &OmqGoSocket, timeout_millis: i64) -> Result<(), Error> {
    let native = socket.materialize()?;
    let mut scratch = socket
        .recv_scratch
        .lock()
        .map_err(|_| Error::Config("receive scratch lock poisoned".to_string()))?;
    scratch.clear();

    let received = match timeout_millis {
        i64::MIN..=-1 => native.recv_many_into(RECV_BATCH, &mut scratch),
        0 => native.try_recv_many_into(RECV_BATCH, &mut scratch),
        _ => native.recv_many_timeout_into(
            RECV_BATCH,
            Duration::from_millis(timeout_millis as u64),
            &mut scratch,
        ),
    }?;

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

fn event_to_c(event: NativeMonitorEvent) -> OmqGoEvent {
    let mut out = OmqGoEvent {
        kind: ptr::null_mut(),
        endpoint: ptr::null_mut(),
        peer_ident: ptr::null_mut(),
        reason: ptr::null_mut(),
        command_name: ptr::null_mut(),
        data: ptr::null_mut(),
        data_len: 0,
        connection_id: 0,
        retry_millis: 0,
        attempt: 0,
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
            out.connection_id = peer.connection_id;
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
            out.connection_id = peer.connection_id;
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
