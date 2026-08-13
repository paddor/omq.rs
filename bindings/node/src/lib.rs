use std::ffi::c_void;
use std::mem;
use std::ptr;
use std::slice;
use std::str::FromStr;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use napi::bindgen_prelude::{
    AbortSignal, AsyncTask, BufferSlice, Env, FromNapiValue, Task, TypedArrayType, Uint8Array,
    Uint32Array,
};
use napi::{Either, Error as NapiError, Result, Status, sys};
use napi_derive::napi;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{
    Context as OmqContext, ContextConfig, CurveKeypair, CurvePublicKey, CurveSecretKey, Endpoint,
    Message, OnMute, Options, ReconnectPolicy, SocketType,
};

#[derive(Debug)]
struct ContextState {
    ctx: RwLock<Option<OmqContext>>,
    sockets: Mutex<Vec<Weak<SocketState>>>,
    closed: AtomicBool,
    owns_runtime: bool,
    share_key: u128,
}

impl ContextState {
    fn register_socket(&self, socket: &Arc<SocketState>) -> Result<()> {
        self.sockets
            .lock()
            .map_err(|_| napi_error("context socket list poisoned"))?
            .push(Arc::downgrade(socket));
        Ok(())
    }

    fn close_live_sockets(&self) {
        let sockets = match self.sockets.lock() {
            Ok(mut guard) => mem::take(&mut *guard),
            Err(_) => return,
        };
        for socket in sockets.into_iter().filter_map(|socket| socket.upgrade()) {
            let _ = socket.close_socket();
        }
    }
}

impl SocketState {
    fn socket_clone(&self) -> Result<omq_tokio::blocking::Socket> {
        self.check_open()?;
        let guard = self
            .socket
            .read()
            .map_err(|_| napi_error("socket lock poisoned"))?;
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| napi_error("socket closed"))
    }

    fn take_socket(&self) -> Result<Option<omq_tokio::blocking::Socket>> {
        self.socket
            .write()
            .map_err(|_| napi_error("socket lock poisoned"))
            .map(|mut guard| guard.take())
    }

    fn close_socket(&self) -> Result<()> {
        if !self.closed.swap(true, Ordering::AcqRel)
            && let Some(socket) = self.take_socket()?
        {
            socket.close().map_err(map_omq_error)?;
        }
        Ok(())
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(napi_error("socket closed"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SocketState {
    socket: RwLock<Option<omq_tokio::blocking::Socket>>,
    closed: AtomicBool,
}

#[derive(Debug)]
#[napi]
pub struct NativeContext {
    inner: Arc<ContextState>,
}

#[derive(Debug)]
#[napi]
pub struct NativeSocket {
    inner: Arc<SocketState>,
}

#[expect(
    missing_debug_implementations,
    reason = "napi typed arrays do not implement Debug"
)]
#[napi(object)]
pub struct NativeContextOptions {
    pub io_threads: Option<u32>,
}

#[expect(
    missing_debug_implementations,
    reason = "napi typed arrays do not implement Debug"
)]
#[napi(object)]
pub struct NativeSocketOptions {
    pub identity: Option<Uint8Array>,
    pub send_high_water_mark: Option<u32>,
    pub receive_high_water_mark: Option<u32>,
    pub reconnect_initial_delay_ms: Option<u32>,
    pub reconnect_max_delay_ms: Option<u32>,
    pub linger_ms: Option<i64>,
    pub router_mandatory: Option<bool>,
    pub conflate: Option<bool>,
    pub xpub_nodrop: Option<bool>,
    pub on_mute: Option<String>,
    pub workload_profile: Option<String>,
    pub compression_dictionary: Option<Uint8Array>,
    pub plain: Option<NativePlainOptions>,
    pub curve: Option<NativeCurveOptions>,
}

#[expect(missing_debug_implementations, reason = "napi object")]
#[napi(object)]
pub struct NativePlainOptions {
    pub username: String,
    pub password: String,
    pub server: Option<bool>,
}

#[expect(missing_debug_implementations, reason = "napi object")]
#[napi(object)]
pub struct NativeCurveOptions {
    pub server_key: Option<String>,
    pub public_key: String,
    pub secret_key: String,
    pub server: Option<bool>,
}

#[derive(Debug)]
#[napi(object)]
pub struct NativeCurveKeypair {
    pub public_key: String,
    pub secret_key: String,
}

#[expect(
    missing_debug_implementations,
    reason = "napi typed arrays do not implement Debug"
)]
#[napi(object)]
pub struct NativePackedMessages {
    pub data: Uint8Array,
    pub part_offsets: Uint32Array,
    pub part_lengths: Uint32Array,
    pub message_parts: Uint32Array,
}

pub struct RecvRawTask {
    socket: omq_tokio::blocking::Socket,
    cancel: Arc<omq_tokio::blocking::BlockingRecvCancel>,
}

impl Task for RecvRawTask {
    type Output = Message;
    type JsValue = Either<Uint8Array, Vec<Uint8Array>>;

    fn compute(&mut self) -> Result<Self::Output> {
        self.cancel.register_current_thread_once();
        let mut out = Vec::with_capacity(1);
        match self
            .socket
            .recv_many_registered_cancelable_into(1, &self.cancel, &mut out)
        {
            Ok(Some(_)) => out
                .pop()
                .ok_or_else(|| napi_error("recv returned no message")),
            Ok(None) => Err(NapiError::new(Status::Cancelled, "recv aborted")),
            Err(error) => Err(map_omq_error(error)),
        }
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        message_to_raw(&env, output)
    }
}

#[napi]
impl NativeContext {
    #[napi(constructor)]
    pub fn new(options: Option<NativeContextOptions>) -> Self {
        let io_threads = options
            .and_then(|options| options.io_threads)
            .unwrap_or(1)
            .max(1) as usize;
        let ctx = OmqContext::with_config(ContextConfig { io_threads });
        let share_key = ctx.share_key();
        Self {
            inner: Arc::new(ContextState {
                ctx: RwLock::new(Some(ctx)),
                sockets: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
                owns_runtime: true,
                share_key,
            }),
        }
    }

    #[napi]
    pub fn socket(
        &self,
        socket_type: String,
        options: Option<NativeSocketOptions>,
    ) -> Result<NativeSocket> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(napi_error("context closed"));
        }
        let ctx_guard = self
            .inner
            .ctx
            .read()
            .map_err(|_| napi_error("context lock poisoned"))?;
        let ctx = ctx_guard
            .as_ref()
            .ok_or_else(|| napi_error("context closed"))?;
        if ctx.is_terminated() {
            return Err(napi_error("context closed"));
        }
        let socket_type = parse_socket_type(&socket_type)?;
        let options = build_options(options)?;
        let socket = ctx.blocking_socket(socket_type, options);
        let inner = Arc::new(SocketState {
            socket: RwLock::new(Some(socket)),
            closed: AtomicBool::new(false),
        });
        self.inner.register_socket(&inner)?;
        Ok(NativeSocket { inner })
    }

    #[napi]
    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            let ctx = self.inner.ctx.write().ok().and_then(|mut guard| {
                self.inner.close_live_sockets();
                guard.take()
            });
            if let Some(ctx) = ctx
                && self.inner.owns_runtime
            {
                ctx.term();
            }
        }
    }

    #[napi]
    pub fn share_key(&self) -> String {
        format!("{:032x}", self.inner.share_key)
    }
}

#[napi]
impl NativeSocket {
    #[napi]
    pub fn bind(&self, endpoint: String) -> Result<String> {
        let endpoint = parse_endpoint(&endpoint)?;
        self.with_socket_ref(|socket| {
            socket
                .bind(endpoint)
                .map(|bound| bound.to_string())
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn connect(&self, endpoint: String) -> Result<()> {
        let endpoint = parse_endpoint(&endpoint)?;
        self.with_socket_ref(|socket| socket.connect(endpoint).map_err(map_omq_error))
    }

    #[napi]
    pub fn unbind(&self, endpoint: String) -> Result<()> {
        let endpoint = parse_endpoint(&endpoint)?;
        self.with_socket_ref(|socket| socket.unbind(endpoint).map_err(map_omq_error))
    }

    #[napi]
    pub fn disconnect(&self, endpoint: String) -> Result<()> {
        let endpoint = parse_endpoint(&endpoint)?;
        self.with_socket_ref(|socket| socket.disconnect(endpoint).map_err(map_omq_error))
    }

    #[napi]
    pub fn send(&self, parts: Vec<Uint8Array>) -> Result<()> {
        self.with_socket_ref(|socket| {
            socket
                .send(message_from_parts(parts))
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn send_sync(&self, parts: Vec<Uint8Array>) -> Result<()> {
        self.with_socket_ref(|socket| {
            socket
                .send(message_from_parts(parts))
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn send_one_sync(&self, payload: Uint8Array) -> Result<()> {
        self.with_socket_ref(|socket| {
            socket
                .send(Message::from_slice(payload.as_ref()))
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn send_buffer_sync(&self, payload: BufferSlice) -> Result<()> {
        self.with_socket_ref(|socket| {
            socket
                .send(Message::from_slice(payload.as_ref()))
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn recv(&self, env: Env) -> Result<Vec<Uint8Array>> {
        self.with_socket_ref(|socket| {
            socket
                .recv()
                .map_err(map_omq_error)
                .and_then(|message| message_to_arrays(&env, message))
        })
    }

    #[napi]
    pub fn recv_sync(&self, env: Env) -> Result<Vec<Uint8Array>> {
        self.with_socket_ref(|socket| {
            socket
                .recv()
                .map_err(map_omq_error)
                .and_then(|message| message_to_arrays(&env, message))
        })
    }

    #[napi]
    pub fn recv_raw_sync(&self, env: Env) -> Result<Either<Uint8Array, Vec<Uint8Array>>> {
        self.with_socket_ref(|socket| {
            socket
                .recv()
                .map_err(map_omq_error)
                .and_then(|message| message_to_raw(&env, message))
        })
    }

    #[napi(ts_return_type = "Promise<Uint8Array | Array<Uint8Array>>")]
    pub fn recv_raw(&self, signal: Option<AbortSignal>) -> Result<AsyncTask<RecvRawTask>> {
        let socket = self.socket_clone()?;
        let cancel = Arc::new(omq_tokio::blocking::BlockingRecvCancel::new());
        if let Some(signal) = signal.as_ref() {
            let cancel = cancel.clone();
            signal.on_abort(move || cancel.cancel());
        }
        Ok(AsyncTask::with_optional_signal(
            RecvRawTask { socket, cancel },
            signal,
        ))
    }

    #[napi]
    pub fn recv_timeout(&self, env: Env, timeout_ms: u32) -> Result<Option<Vec<Uint8Array>>> {
        self.with_socket_ref(|socket| {
            match socket.recv_timeout(Duration::from_millis(timeout_ms.into())) {
                Ok(message) => Ok(Some(message_to_arrays(&env, message)?)),
                Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(None),
                Err(error) => Err(map_omq_error(error)),
            }
        })
    }

    #[napi]
    pub fn try_recv(&self, env: Env) -> Result<Option<Vec<Uint8Array>>> {
        self.with_socket_ref(|socket| match socket.try_recv() {
            Ok(message) => Ok(Some(message_to_arrays(&env, message)?)),
            Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(None),
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn try_recv_raw(&self, env: Env) -> Result<Option<Either<Uint8Array, Vec<Uint8Array>>>> {
        self.with_socket_ref(|socket| match socket.try_recv() {
            Ok(message) => Ok(Some(message_to_raw(&env, message)?)),
            Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(None),
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn recv_raw_many_sync(
        &self,
        env: Env,
        max: u32,
    ) -> Result<Vec<Either<Uint8Array, Vec<Uint8Array>>>> {
        let max = max as usize;
        let mut out = Vec::with_capacity(max.min(512));
        self.with_socket_ref(|socket| match socket.recv_many_into(max, &mut out) {
            Ok(_) => out
                .into_iter()
                .map(|message| message_to_raw(&env, message))
                .collect(),
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn recv_packed_many_sync(&self, env: Env, max: u32) -> Result<NativePackedMessages> {
        let max = max as usize;
        let mut out = Vec::with_capacity(max.min(512));
        self.with_socket_ref(|socket| match socket.recv_many_into(max, &mut out) {
            Ok(_) => messages_to_packed(&env, out),
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn try_recv_raw_many_sync(
        &self,
        env: Env,
        max: u32,
    ) -> Result<Vec<Either<Uint8Array, Vec<Uint8Array>>>> {
        let max = max as usize;
        let mut out = Vec::with_capacity(max.min(512));
        self.with_socket_ref(|socket| match socket.try_recv_many_into(max, &mut out) {
            Ok(_) => out
                .into_iter()
                .map(|message| message_to_raw(&env, message))
                .collect(),
            Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(Vec::new()),
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn try_recv_packed_many_sync(&self, env: Env, max: u32) -> Result<NativePackedMessages> {
        let max = max as usize;
        let mut out = Vec::with_capacity(max.min(512));
        self.with_socket_ref(|socket| match socket.try_recv_many_into(max, &mut out) {
            Ok(_) => messages_to_packed(&env, out),
            Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => {
                messages_to_packed(&env, Vec::new())
            }
            Err(error) => Err(map_omq_error(error)),
        })
    }

    #[napi]
    pub fn wait_connected_sync(&self, min_peers: u32, timeout_ms: u32) -> Result<u32> {
        self.with_socket_ref(|socket| {
            socket
                .wait_connected(min_peers as usize, Duration::from_millis(timeout_ms.into()))
                .map(|count| count as u32)
                .map_err(map_omq_error)
        })
    }

    #[napi]
    pub fn recv_many_sync(
        &self,
        env: Env,
        max: u32,
        timeout_ms: Option<u32>,
    ) -> Result<Vec<Vec<Uint8Array>>> {
        let max = max as usize;
        let mut out = Vec::with_capacity(max.min(512));
        self.with_socket_ref(|socket| {
            let received = match timeout_ms {
                Some(timeout_ms) => socket.recv_many_timeout_into(
                    max,
                    Duration::from_millis(timeout_ms.into()),
                    &mut out,
                ),
                None => socket.recv_many_into(max, &mut out),
            };
            match received {
                Ok(_) => out
                    .into_iter()
                    .map(|message| message_to_arrays(&env, message))
                    .collect(),
                Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(Vec::new()),
                Err(error) => Err(map_omq_error(error)),
            }
        })
    }

    #[napi]
    pub fn subscribe(&self, prefix: Uint8Array) -> Result<()> {
        let prefix = Bytes::copy_from_slice(prefix.as_ref());
        self.with_socket_ref(|socket| socket.subscribe(prefix).map_err(map_omq_error))
    }

    #[napi]
    pub fn unsubscribe(&self, prefix: Uint8Array) -> Result<()> {
        let prefix = Bytes::copy_from_slice(prefix.as_ref());
        self.with_socket_ref(|socket| socket.unsubscribe(prefix).map_err(map_omq_error))
    }

    #[napi]
    pub fn join(&self, group: Uint8Array) -> Result<()> {
        let group = Bytes::copy_from_slice(group.as_ref());
        self.with_socket_ref(|socket| socket.join(group).map_err(map_omq_error))
    }

    #[napi]
    pub fn leave(&self, group: Uint8Array) -> Result<()> {
        let group = Bytes::copy_from_slice(group.as_ref());
        self.with_socket_ref(|socket| socket.leave(group).map_err(map_omq_error))
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner.close_socket()
    }

    fn with_socket_ref<T>(
        &self,
        f: impl FnOnce(&omq_tokio::blocking::Socket) -> Result<T>,
    ) -> Result<T> {
        let socket = self.socket_clone()?;
        f(&socket)
    }

    fn socket_clone(&self) -> Result<omq_tokio::blocking::Socket> {
        self.inner.socket_clone()
    }
}

#[napi(js_name = "curveKeypair")]
pub fn curve_keypair() -> NativeCurveKeypair {
    let keypair = CurveKeypair::generate();
    NativeCurveKeypair {
        public_key: keypair.public.to_z85(),
        secret_key: keypair.secret.to_z85(),
    }
}

#[napi(js_name = "curvePublic")]
pub fn curve_public(secret_key: String) -> Result<String> {
    let secret = CurveSecretKey::from_z85(&secret_key).map_err(map_omq_error)?;
    Ok(secret.derive_public().to_z85())
}

#[napi(js_name = "nativeContextFromShareKey")]
pub fn native_context_from_share_key(share_key: String) -> Result<NativeContext> {
    let share_key = u128::from_str_radix(&share_key, 16)
        .map_err(|_| napi_error("invalid native context share key"))?;
    let ctx = OmqContext::from_share_key(share_key)
        .ok_or_else(|| napi_error("native context share key not found"))?;
    Ok(NativeContext {
        inner: Arc::new(ContextState {
            share_key: ctx.share_key(),
            ctx: RwLock::new(Some(ctx)),
            sockets: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            owns_runtime: false,
        }),
    })
}

impl Drop for NativeSocket {
    fn drop(&mut self) {
        let _ = self.inner.close_socket();
    }
}

fn parse_socket_type(name: &str) -> Result<SocketType> {
    let upper = name.to_ascii_uppercase();
    Ok(match upper.as_str() {
        "REQ" => SocketType::Req,
        "REP" => SocketType::Rep,
        "PUB" => SocketType::Pub,
        "SUB" => SocketType::Sub,
        "XPUB" => SocketType::XPub,
        "XSUB" => SocketType::XSub,
        "PUSH" => SocketType::Push,
        "PULL" => SocketType::Pull,
        "DEALER" => SocketType::Dealer,
        "ROUTER" => SocketType::Router,
        "PAIR" => SocketType::Pair,
        "CLIENT" => SocketType::Client,
        "SERVER" => SocketType::Server,
        "RADIO" => SocketType::Radio,
        "DISH" => SocketType::Dish,
        "SCATTER" => SocketType::Scatter,
        "GATHER" => SocketType::Gather,
        "CHANNEL" => SocketType::Channel,
        "PEER" => SocketType::Peer,
        "STREAM" => SocketType::Stream,
        _ => return Err(napi_error(format!("unknown socket type {name}"))),
    })
}

fn build_options(input: Option<NativeSocketOptions>) -> Result<Options> {
    let Some(input) = input else {
        return Ok(Options::default());
    };
    let mut options = Options::default();
    if let Some(identity) = input.identity {
        options = options.identity(Bytes::copy_from_slice(identity.as_ref()));
    }
    if let Some(hwm) = input.send_high_water_mark {
        options = options.send_hwm(hwm);
    }
    if let Some(hwm) = input.receive_high_water_mark {
        options = options.recv_hwm(hwm);
    }
    if let (Some(min), Some(max)) = (
        input.reconnect_initial_delay_ms,
        input.reconnect_max_delay_ms,
    ) {
        options = options.reconnect(ReconnectPolicy::Exponential {
            min: Duration::from_millis(min.into()),
            max: Duration::from_millis(max.into()),
        });
    } else if let Some(delay) = input.reconnect_initial_delay_ms {
        options = options.reconnect(ReconnectPolicy::Fixed(Duration::from_millis(delay.into())));
    }
    if let Some(linger_ms) = input.linger_ms {
        options = if linger_ms < 0 {
            options.linger_forever()
        } else {
            options.linger(Duration::from_millis(linger_ms as u64))
        };
    }
    if let Some(router_mandatory) = input.router_mandatory {
        options = options.router_mandatory(router_mandatory);
    }
    if let Some(conflate) = input.conflate {
        options = options.conflate(conflate);
    }
    if let Some(xpub_nodrop) = input.xpub_nodrop {
        options.xpub_nodrop = xpub_nodrop;
    }
    if let Some(on_mute) = input.on_mute {
        options = options.on_mute(parse_on_mute(&on_mute)?);
    }
    if let Some(profile) = input.workload_profile {
        options = options.workload_profile(parse_workload_profile(&profile)?);
    }
    if let Some(dict) = input.compression_dictionary {
        options = options.compression_dict(Bytes::copy_from_slice(dict.as_ref()));
    }
    if let Some(plain) = input.plain {
        let username = plain.username;
        let password = plain.password;
        if plain.server.unwrap_or(false) {
            options = options.plain_server(move |peer| {
                peer.username.as_deref() == Some(username.as_str())
                    && peer.password.as_deref() == Some(password.as_str())
            });
        } else {
            options = options.plain_client(username, password);
        }
    }
    if let Some(curve) = input.curve {
        let keypair = curve_keypair_from_z85(curve.public_key, curve.secret_key)?;
        if curve.server.unwrap_or(false) {
            options = options.curve_server(keypair);
        } else {
            let server_key = curve
                .server_key
                .ok_or_else(|| napi_error("curve.serverKey is required for CURVE clients"))?;
            let server_public = CurvePublicKey::from_z85(&server_key).map_err(map_omq_error)?;
            options = options.curve_client(keypair, server_public);
        }
    }
    options.validate().map_err(map_omq_error)?;
    Ok(options)
}

fn curve_keypair_from_z85(public_key: String, secret_key: String) -> Result<CurveKeypair> {
    let public = CurvePublicKey::from_z85(&public_key).map_err(map_omq_error)?;
    let secret = CurveSecretKey::from_z85(&secret_key).map_err(map_omq_error)?;
    if secret.derive_public() != public {
        return Err(napi_error("CURVE public key does not match secret key"));
    }
    Ok(CurveKeypair { public, secret })
}

fn parse_on_mute(value: &str) -> Result<OnMute> {
    Ok(match value {
        "block" | "Block" | "BLOCK" => OnMute::Block,
        "dropNewest" | "drop-newest" | "DropNewest" | "DROP_NEWEST" => OnMute::DropNewest,
        "dropOldest" | "drop-oldest" | "DropOldest" | "DROP_OLDEST" => OnMute::DropOldest,
        _ => return Err(napi_error(format!("unknown onMute value {value}"))),
    })
}

fn parse_workload_profile(value: &str) -> Result<WorkloadProfile> {
    Ok(match value {
        "throughput" | "Throughput" | "THROUGHPUT" => WorkloadProfile::Throughput,
        "latency" | "Latency" | "LATENCY" => WorkloadProfile::Latency,
        _ => return Err(napi_error(format!("unknown workloadProfile value {value}"))),
    })
}

fn parse_endpoint(endpoint: &str) -> Result<Endpoint> {
    Endpoint::from_str(endpoint).map_err(map_omq_error)
}

fn message_from_parts(parts: Vec<Uint8Array>) -> Message {
    if parts.len() == 1 {
        return Message::from_slice(parts[0].as_ref());
    }
    Message::multipart(
        parts
            .into_iter()
            .map(|part| Bytes::copy_from_slice(part.as_ref())),
    )
}

fn message_to_arrays(env: &Env, message: Message) -> Result<Vec<Uint8Array>> {
    (0..message.len())
        .map(|index| {
            let part = message
                .part_slice(index)
                .expect("message part index checked");
            uint8_array_from_slice(env, part)
        })
        .collect()
}

fn message_to_raw(env: &Env, message: Message) -> Result<Either<Uint8Array, Vec<Uint8Array>>> {
    if message.len() == 1 {
        let part = message.part_slice(0).expect("message part index checked");
        return uint8_array_from_slice(env, part).map(Either::A);
    }
    message_to_arrays(env, message).map(Either::B)
}

fn messages_to_packed(env: &Env, messages: Vec<Message>) -> Result<NativePackedMessages> {
    let mut total_bytes = 0usize;
    let mut total_parts = 0usize;
    for message in &messages {
        total_parts = total_parts
            .checked_add(message.len())
            .ok_or_else(|| napi_error("message batch has too many parts"))?;
        for index in 0..message.len() {
            let part = message
                .part_slice(index)
                .expect("message part index checked");
            total_bytes = total_bytes
                .checked_add(part.len())
                .ok_or_else(|| napi_error("message batch too large"))?;
        }
    }
    if total_bytes > u32::MAX as usize || total_parts > u32::MAX as usize {
        return Err(napi_error("message batch too large"));
    }

    let (data, data_ptr) = create_uint8_array(env, total_bytes)?;
    let (part_offsets, part_offsets_ptr) = create_uint32_array(env, total_parts)?;
    let (part_lengths, part_lengths_ptr) = create_uint32_array(env, total_parts)?;
    let (message_parts, message_parts_ptr) = create_uint32_array(env, messages.len())?;

    let data_slice = mut_slice_or_empty(data_ptr, total_bytes);
    let part_offsets_slice = mut_slice_or_empty(part_offsets_ptr, total_parts);
    let part_lengths_slice = mut_slice_or_empty(part_lengths_ptr, total_parts);
    let message_parts_slice = mut_slice_or_empty(message_parts_ptr, messages.len());

    let mut data_offset = 0usize;
    let mut part_index = 0usize;
    for (message_index, message) in messages.iter().enumerate() {
        message_parts_slice[message_index] = message.len() as u32;
        for index in 0..message.len() {
            let part = message
                .part_slice(index)
                .expect("message part index checked");
            part_offsets_slice[part_index] = data_offset as u32;
            part_lengths_slice[part_index] = part.len() as u32;
            data_slice[data_offset..data_offset + part.len()].copy_from_slice(part);
            data_offset += part.len();
            part_index += 1;
        }
    }

    Ok(NativePackedMessages {
        data,
        part_offsets,
        part_lengths,
        message_parts,
    })
}

fn create_uint8_array(env: &Env, len: usize) -> Result<(Uint8Array, *mut u8)> {
    let (arraybuffer, data) = create_arraybuffer(env, len)?;
    let value = create_typed_array(env, TypedArrayType::Uint8, len, arraybuffer)?;
    let array = unsafe { Uint8Array::from_napi_value(env.raw(), value)? };
    Ok((array, data.cast()))
}

fn uint8_array_from_slice(env: &Env, data: &[u8]) -> Result<Uint8Array> {
    let (array, dst) = create_uint8_array(env, data.len())?;
    if !data.is_empty() {
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len()) };
    }
    Ok(array)
}

fn create_uint32_array(env: &Env, len: usize) -> Result<(Uint32Array, *mut u32)> {
    let byte_len = len
        .checked_mul(mem::size_of::<u32>())
        .ok_or_else(|| napi_error("message batch too large"))?;
    let (arraybuffer, data) = create_arraybuffer(env, byte_len)?;
    let value = create_typed_array(env, TypedArrayType::Uint32, len, arraybuffer)?;
    let array = unsafe { Uint32Array::from_napi_value(env.raw(), value)? };
    Ok((array, data.cast()))
}

fn create_arraybuffer(env: &Env, byte_len: usize) -> Result<(sys::napi_value, *mut c_void)> {
    let mut arraybuffer = ptr::null_mut();
    let mut data = ptr::null_mut();
    let status =
        unsafe { sys::napi_create_arraybuffer(env.raw(), byte_len, &mut data, &mut arraybuffer) };
    check_napi_status(status, "create arraybuffer")?;
    Ok((arraybuffer, data))
}

fn create_typed_array(
    env: &Env,
    kind: TypedArrayType,
    len: usize,
    arraybuffer: sys::napi_value,
) -> Result<sys::napi_value> {
    let mut value = ptr::null_mut();
    let status = unsafe {
        sys::napi_create_typedarray(env.raw(), kind as i32, len, arraybuffer, 0, &mut value)
    };
    check_napi_status(status, "create typed array")?;
    Ok(value)
}

fn mut_slice_or_empty<T>(ptr: *mut T, len: usize) -> &'static mut [T] {
    if len == 0 {
        return &mut [];
    }
    unsafe { slice::from_raw_parts_mut(ptr, len) }
}

fn check_napi_status(status: i32, message: &str) -> Result<()> {
    if status == sys::Status::napi_ok {
        return Ok(());
    }
    Err(NapiError::new(Status::from(status), message.to_string()))
}

fn map_omq_error(error: omq_tokio::Error) -> NapiError {
    napi_error(error.to_string())
}

fn napi_error(message: impl Into<String>) -> NapiError {
    NapiError::new(Status::GenericFailure, message.into())
}
