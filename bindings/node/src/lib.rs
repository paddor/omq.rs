use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;
use napi::bindgen_prelude::{Buffer, Uint8Array};
use napi::{Error as NapiError, Result, Status};
use napi_derive::napi;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{
    Context as OmqContext, ContextConfig, CurveKeypair, CurvePublicKey, CurveSecretKey, Endpoint,
    Message, OnMute, Options, ReconnectPolicy, SocketType,
};

#[derive(Debug)]
struct ContextState {
    ctx: OmqContext,
    closed: AtomicBool,
    owns_runtime: bool,
}

struct SocketState {
    worker: Mutex<Option<SocketWorker>>,
    interrupt: omq_tokio::blocking::Socket,
    closed: AtomicBool,
}

struct SocketWorker {
    tx: mpsc::Sender<SocketCommand>,
    join: Option<JoinHandle<()>>,
}

#[derive(Debug)]
#[napi]
pub struct NativeContext {
    inner: Arc<ContextState>,
}

#[napi]
pub struct NativeSocket {
    inner: Arc<SocketState>,
}

type WorkerResult<T> = std::result::Result<T, String>;
type Reply<T> = mpsc::Sender<WorkerResult<T>>;

enum SocketCommand {
    Bind {
        endpoint: String,
        reply: Reply<String>,
    },
    Connect {
        endpoint: String,
        reply: Reply<()>,
    },
    Unbind {
        endpoint: String,
        reply: Reply<()>,
    },
    Disconnect {
        endpoint: String,
        reply: Reply<()>,
    },
    Send {
        parts: Vec<Bytes>,
        reply: Reply<()>,
    },
    Recv {
        reply: Reply<Vec<Bytes>>,
    },
    RecvTimeout {
        timeout: Duration,
        reply: Reply<Option<Vec<Bytes>>>,
    },
    TryRecv {
        reply: Reply<Option<Vec<Bytes>>>,
    },
    WaitConnected {
        min_peers: u32,
        timeout: Duration,
        reply: Reply<u32>,
    },
    RecvMany {
        max: usize,
        timeout: Option<Duration>,
        reply: Reply<Vec<Vec<Bytes>>>,
    },
    Subscribe {
        prefix: Bytes,
        reply: Reply<()>,
    },
    Unsubscribe {
        prefix: Bytes,
        reply: Reply<()>,
    },
    Join {
        group: Bytes,
        reply: Reply<()>,
    },
    Leave {
        group: Bytes,
        reply: Reply<()>,
    },
    Close {
        reply: Option<Reply<()>>,
    },
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

#[napi]
impl NativeContext {
    #[napi(constructor)]
    pub fn new(options: Option<NativeContextOptions>) -> Self {
        let io_threads = options
            .and_then(|options| options.io_threads)
            .unwrap_or(1)
            .max(1) as usize;
        Self {
            inner: Arc::new(ContextState {
                ctx: OmqContext::with_config(ContextConfig { io_threads }),
                closed: AtomicBool::new(false),
                owns_runtime: true,
            }),
        }
    }

    #[napi]
    pub fn socket(
        &self,
        socket_type: String,
        options: Option<NativeSocketOptions>,
    ) -> Result<NativeSocket> {
        if self.inner.closed.load(Ordering::Acquire) || self.inner.ctx.is_terminated() {
            return Err(napi_error("context closed"));
        }
        let socket_type = parse_socket_type(&socket_type)?;
        let options = build_options(options)?;
        let socket = self.inner.ctx.blocking_socket(socket_type, options);
        let interrupt = socket.clone();
        let worker = SocketWorker::spawn(socket);
        Ok(NativeSocket {
            inner: Arc::new(SocketState {
                worker: Mutex::new(Some(worker)),
                interrupt,
                closed: AtomicBool::new(false),
            }),
        })
    }

    #[napi]
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        if self.inner.owns_runtime {
            self.inner.ctx.term();
        }
    }

    #[napi]
    pub fn share_key(&self) -> String {
        format!("{:032x}", self.inner.ctx.share_key())
    }
}

#[napi]
impl NativeSocket {
    #[napi]
    pub async fn bind(&self, endpoint: String) -> Result<String> {
        self.request_async(|reply| SocketCommand::Bind { endpoint, reply })
            .await
    }

    #[napi]
    pub async fn connect(&self, endpoint: String) -> Result<()> {
        self.request_async(|reply| SocketCommand::Connect { endpoint, reply })
            .await
    }

    #[napi]
    pub async fn unbind(&self, endpoint: String) -> Result<()> {
        self.request_async(|reply| SocketCommand::Unbind { endpoint, reply })
            .await
    }

    #[napi]
    pub async fn disconnect(&self, endpoint: String) -> Result<()> {
        self.request_async(|reply| SocketCommand::Disconnect { endpoint, reply })
            .await
    }

    #[napi]
    pub async fn send(&self, parts: Vec<Uint8Array>) -> Result<()> {
        let parts = copy_parts(parts);
        self.request_async(|reply| SocketCommand::Send { parts, reply })
            .await
    }

    #[napi]
    pub fn send_sync(&self, parts: Vec<Uint8Array>) -> Result<()> {
        let parts = copy_parts(parts);
        self.request_sync(|reply| SocketCommand::Send { parts, reply })
    }

    #[napi]
    pub async fn recv(&self) -> Result<Vec<Buffer>> {
        self.request_async(|reply| SocketCommand::Recv { reply })
            .await
            .map(parts_to_buffers)
    }

    #[napi]
    pub fn recv_sync(&self) -> Result<Vec<Buffer>> {
        self.request_sync(|reply| SocketCommand::Recv { reply })
            .map(parts_to_buffers)
    }

    #[napi]
    pub async fn recv_timeout(&self, timeout_ms: u32) -> Result<Option<Vec<Buffer>>> {
        self.request_async(move |reply| SocketCommand::RecvTimeout {
            timeout: Duration::from_millis(timeout_ms.into()),
            reply,
        })
        .await
        .map(|value| value.map(parts_to_buffers))
    }

    #[napi]
    pub fn try_recv(&self) -> Result<Option<Vec<Buffer>>> {
        self.request_sync(|reply| SocketCommand::TryRecv { reply })
            .map(|value| value.map(parts_to_buffers))
    }

    #[napi]
    pub fn wait_connected_sync(&self, min_peers: u32, timeout_ms: u32) -> Result<u32> {
        self.request_sync(|reply| SocketCommand::WaitConnected {
            min_peers,
            timeout: Duration::from_millis(timeout_ms.into()),
            reply,
        })
    }

    #[napi]
    pub fn recv_many_sync(&self, max: u32, timeout_ms: Option<u32>) -> Result<Vec<Vec<Buffer>>> {
        let timeout = timeout_ms.map(|timeout_ms| Duration::from_millis(timeout_ms.into()));
        self.request_sync(|reply| SocketCommand::RecvMany {
            max: max as usize,
            timeout,
            reply,
        })
        .map(|messages| messages.into_iter().map(parts_to_buffers).collect())
    }

    #[napi]
    pub async fn subscribe(&self, prefix: Uint8Array) -> Result<()> {
        let prefix = Bytes::copy_from_slice(prefix.as_ref());
        self.request_async(|reply| SocketCommand::Subscribe { prefix, reply })
            .await
    }

    #[napi]
    pub async fn unsubscribe(&self, prefix: Uint8Array) -> Result<()> {
        let prefix = Bytes::copy_from_slice(prefix.as_ref());
        self.request_async(|reply| SocketCommand::Unsubscribe { prefix, reply })
            .await
    }

    #[napi]
    pub async fn join(&self, group: Uint8Array) -> Result<()> {
        let group = Bytes::copy_from_slice(group.as_ref());
        self.request_async(|reply| SocketCommand::Join { group, reply })
            .await
    }

    #[napi]
    pub async fn leave(&self, group: Uint8Array) -> Result<()> {
        let group = Bytes::copy_from_slice(group.as_ref());
        self.request_async(|reply| SocketCommand::Leave { group, reply })
            .await
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        if let Some(mut worker) = self.take_worker()? {
            let _ = self.inner.interrupt.clone().close();
            let (reply, rx) = mpsc::channel();
            let _ = worker.tx.send(SocketCommand::Close { reply: Some(reply) });
            let result = rx
                .recv()
                .unwrap_or_else(|_| Err("socket worker closed".to_string()))
                .map_err(napi_error);
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
            result?;
        }
        Ok(())
    }

    fn request_sync<T, F>(&self, build: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Reply<T>) -> SocketCommand,
    {
        let tx = self.worker_tx()?;
        let (reply, rx) = mpsc::channel();
        tx.send(build(reply))
            .map_err(|_| napi_error("socket closed"))?;
        rx.recv()
            .unwrap_or_else(|_| Err("socket worker closed".to_string()))
            .map_err(napi_error)
    }

    async fn request_async<T, F>(&self, build: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Reply<T>) -> SocketCommand + Send + 'static,
    {
        let tx = self.worker_tx()?;
        let (reply, rx) = mpsc::channel();
        tx.send(build(reply))
            .map_err(|_| napi_error("socket closed"))?;
        run_blocking_recv(rx).await
    }

    fn worker_tx(&self) -> Result<mpsc::Sender<SocketCommand>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(napi_error("socket closed"));
        }
        self.inner
            .worker
            .lock()
            .map_err(|_| napi_error("socket lock poisoned"))?
            .as_ref()
            .map(|worker| worker.tx.clone())
            .ok_or_else(|| napi_error("socket closed"))
    }

    fn take_worker(&self) -> Result<Option<SocketWorker>> {
        self.inner
            .worker
            .lock()
            .map_err(|_| napi_error("socket lock poisoned"))
            .map(|mut guard| guard.take())
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
            ctx,
            closed: AtomicBool::new(false),
            owns_runtime: false,
        }),
    })
}

impl Drop for NativeSocket {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        if let Ok(mut guard) = self.inner.worker.lock()
            && let Some(mut worker) = guard.take()
        {
            let _ = self.inner.interrupt.clone().close();
            let _ = worker.tx.send(SocketCommand::Close { reply: None });
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

async fn run_blocking_recv<T>(rx: mpsc::Receiver<WorkerResult<T>>) -> Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        rx.recv()
            .unwrap_or_else(|_| Err("socket worker closed".to_string()))
    })
    .await
    .map_err(|error| napi_error(format!("native worker failed: {error}")))?
    .map_err(napi_error)
}

impl SocketWorker {
    fn spawn(socket: omq_tokio::blocking::Socket) -> Self {
        let (tx, rx) = mpsc::channel();
        let join = thread::Builder::new()
            .name("omq-node-socket".to_string())
            .spawn(move || run_socket_worker(socket, rx))
            .expect("failed to spawn omq-node socket worker");
        Self {
            tx,
            join: Some(join),
        }
    }
}

fn run_socket_worker(socket: omq_tokio::blocking::Socket, rx: mpsc::Receiver<SocketCommand>) {
    for command in rx {
        match command {
            SocketCommand::Bind { endpoint, reply } => {
                let result = parse_endpoint_worker(&endpoint)
                    .and_then(|endpoint| socket.bind(endpoint).map_err(worker_error))
                    .map(|endpoint| endpoint.to_string());
                let _ = reply.send(result);
            }
            SocketCommand::Connect { endpoint, reply } => {
                let result = parse_endpoint_worker(&endpoint)
                    .and_then(|endpoint| socket.connect(endpoint).map_err(worker_error));
                let _ = reply.send(result);
            }
            SocketCommand::Unbind { endpoint, reply } => {
                let result = parse_endpoint_worker(&endpoint)
                    .and_then(|endpoint| socket.unbind(endpoint).map_err(worker_error));
                let _ = reply.send(result);
            }
            SocketCommand::Disconnect { endpoint, reply } => {
                let result = parse_endpoint_worker(&endpoint)
                    .and_then(|endpoint| socket.disconnect(endpoint).map_err(worker_error));
                let _ = reply.send(result);
            }
            SocketCommand::Send { parts, reply } => {
                let result = socket
                    .send(message_from_bytes(parts))
                    .map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::Recv { reply } => {
                let result = socket.recv().map(message_to_bytes).map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::RecvTimeout { timeout, reply } => {
                let result = match socket.recv_timeout(timeout) {
                    Ok(message) => Ok(Some(message_to_bytes(message))),
                    Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(None),
                    Err(error) => Err(worker_error(error)),
                };
                let _ = reply.send(result);
            }
            SocketCommand::TryRecv { reply } => {
                let result = match socket.try_recv() {
                    Ok(message) => Ok(Some(message_to_bytes(message))),
                    Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(None),
                    Err(error) => Err(worker_error(error)),
                };
                let _ = reply.send(result);
            }
            SocketCommand::WaitConnected {
                min_peers,
                timeout,
                reply,
            } => {
                let result = socket
                    .wait_connected(min_peers as usize, timeout)
                    .map(|count| count as u32)
                    .map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::RecvMany {
                max,
                timeout,
                reply,
            } => {
                let mut out = Vec::with_capacity(max.min(512));
                let received = match timeout {
                    Some(timeout) => socket.recv_many_timeout_into(max, timeout, &mut out),
                    None => socket.recv_many_into(max, &mut out),
                };
                let result = match received {
                    Ok(_) => Ok(out.into_iter().map(message_to_bytes).collect()),
                    Err(omq_tokio::Error::Timeout | omq_tokio::Error::WouldBlock) => Ok(Vec::new()),
                    Err(error) => Err(worker_error(error)),
                };
                let _ = reply.send(result);
            }
            SocketCommand::Subscribe { prefix, reply } => {
                let result = socket.subscribe(prefix).map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::Unsubscribe { prefix, reply } => {
                let result = socket.unsubscribe(prefix).map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::Join { group, reply } => {
                let result = socket.join(group).map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::Leave { group, reply } => {
                let result = socket.leave(group).map_err(worker_error);
                let _ = reply.send(result);
            }
            SocketCommand::Close { reply } => {
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(()));
                }
                break;
            }
        }
    }
}

fn parse_endpoint_worker(endpoint: &str) -> WorkerResult<Endpoint> {
    Endpoint::from_str(endpoint).map_err(worker_error)
}

fn worker_error(error: omq_tokio::Error) -> String {
    error.to_string()
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

fn copy_parts(parts: Vec<Uint8Array>) -> Vec<Bytes> {
    parts
        .into_iter()
        .map(|part| Bytes::copy_from_slice(part.as_ref()))
        .collect()
}

fn message_from_bytes(mut parts: Vec<Bytes>) -> Message {
    if parts.len() == 1 {
        return Message::single(parts.remove(0));
    }
    Message::multipart(parts)
}

fn message_to_bytes(message: Message) -> Vec<Bytes> {
    (0..message.len())
        .map(|index| {
            Bytes::copy_from_slice(
                message
                    .part_slice(index)
                    .expect("message part index checked"),
            )
        })
        .collect()
}

fn parts_to_buffers(parts: Vec<Bytes>) -> Vec<Buffer> {
    parts
        .into_iter()
        .map(|part| Buffer::from(part.to_vec()))
        .collect()
}

fn map_omq_error(error: omq_tokio::Error) -> NapiError {
    napi_error(error.to_string())
}

fn napi_error(message: impl Into<String>) -> NapiError {
    NapiError::new(Status::GenericFailure, message.into())
}
