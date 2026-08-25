use std::collections::VecDeque;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{
    Context, ContextConfig, Endpoint, Error as OmqError, KeepAlive, Message, OnMute, Options,
    ReconnectPolicy, SocketType, TrySendError,
};
#[cfg(feature = "curve")]
use omq_tokio::{CurveKeypair, CurvePublicKey, CurveSecretKey};
use rustler::{Binary, Encoder, Env, OwnedBinary, ResourceArc, Term};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        badarg,
        closed,
        config,
        handshake_failed,
        invalid_endpoint,
        io,
        message_too_large,
        no_route,
        protocol,
        rate_limit,
        timeout,
        unsupported_scheme,
        would_block,
        undefined,
    }
}

struct NativeContext {
    ctx: Context,
}

#[rustler::resource_impl]
impl rustler::Resource for NativeContext {}

struct NativeSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    rcvtimeo: Mutex<Option<Duration>>,
    sndtimeo: Mutex<Option<Duration>>,
    plain_server: AtomicBool,
    plain_username: Mutex<Option<String>>,
    plain_password: Mutex<Option<String>>,
    curve_server: AtomicBool,
    curve_publickey: Mutex<Option<Vec<u8>>>,
    curve_secretkey: Mutex<Option<Vec<u8>>>,
    curve_serverkey: Mutex<Option<Vec<u8>>>,
    last_endpoint: Mutex<Vec<u8>>,
    recv_buffer: Mutex<VecDeque<Message>>,
    socket: RwLock<Option<omq_tokio::blocking::Socket>>,
    closed: AtomicBool,
}

#[rustler::resource_impl]
impl rustler::Resource for NativeSocket {
    fn destructor(self, _env: Env<'_>) {
        let _ = self.close_with_linger(None);
    }
}

impl NativeSocket {
    fn materialize(&self) -> Result<omq_tokio::blocking::Socket, OmqError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(OmqError::Closed);
        }
        if let Some(socket) = self.socket.read().unwrap().as_ref() {
            return Ok(socket.clone());
        }
        let mut guard = self.socket.write().unwrap();
        if let Some(socket) = guard.as_ref() {
            return Ok(socket.clone());
        }
        let options = self.build_options()?;
        options.validate()?;
        let socket = self.ctx.blocking_socket(self.socket_type, options);
        *guard = Some(socket.clone());
        Ok(socket)
    }

    fn build_options(&self) -> Result<Options, OmqError> {
        #[allow(unused_mut)]
        let mut options = self.options.lock().unwrap().clone();
        #[cfg(feature = "plain")]
        {
            if self.plain_server.load(Ordering::Acquire) {
                options = options.plain_server(|_| true);
            } else {
                let username = self.plain_username.lock().unwrap().clone();
                let password = self.plain_password.lock().unwrap().clone();
                if let (Some(username), Some(password)) = (username, password) {
                    options = options.plain_client(username, password);
                }
            }
        }
        #[cfg(feature = "curve")]
        {
            let public = self.curve_publickey.lock().unwrap().clone();
            let secret = self.curve_secretkey.lock().unwrap().clone();
            let server_key = self.curve_serverkey.lock().unwrap().clone();
            if self.curve_server.load(Ordering::Acquire) {
                if let (Some(public), Some(secret)) = (public, secret) {
                    let keypair = curve_keypair(&public, &secret)?;
                    options = options.curve_server(keypair);
                }
            } else if let (Some(public), Some(secret), Some(server_key)) =
                (public, secret, server_key)
            {
                let keypair = curve_keypair(&public, &secret)?;
                let server_public = curve_public_key(&server_key)?;
                options = options.curve_client(keypair, server_public);
            }
        }
        Ok(options)
    }

    fn close_with_linger(&self, linger: Option<Duration>) -> Result<(), OmqError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(socket) = self.socket.write().unwrap().take() {
            socket.close_with_linger(linger)?;
        }
        Ok(())
    }
}

#[cfg(feature = "curve")]
fn z85_text(bytes: &[u8], name: &str) -> Result<&str, OmqError> {
    str::from_utf8(bytes).map_err(|_| OmqError::Config(format!("invalid {name}")))
}

#[cfg(feature = "curve")]
fn curve_public_key(bytes: &[u8]) -> Result<CurvePublicKey, OmqError> {
    CurvePublicKey::from_z85(z85_text(bytes, "CURVE_PUBLICKEY")?)
        .map_err(|err| OmqError::Config(format!("invalid CURVE_PUBLICKEY: {err}")))
}

#[cfg(feature = "curve")]
fn curve_secret_key(bytes: &[u8]) -> Result<CurveSecretKey, OmqError> {
    CurveSecretKey::from_z85(z85_text(bytes, "CURVE_SECRETKEY")?)
        .map_err(|err| OmqError::Config(format!("invalid CURVE_SECRETKEY: {err}")))
}

#[cfg(feature = "curve")]
fn curve_keypair(public: &[u8], secret: &[u8]) -> Result<CurveKeypair, OmqError> {
    Ok(CurveKeypair {
        public: curve_public_key(public)?,
        secret: curve_secret_key(secret)?,
    })
}

fn ok<'a, T: Encoder>(env: Env<'a>, value: T) -> Term<'a> {
    (atoms::ok(), value).encode(env)
}

fn ok_unit<'a>(env: Env<'a>) -> Term<'a> {
    atoms::ok().encode(env)
}

fn err_term<'a>(env: Env<'a>, class: rustler::Atom, reason: impl ToString) -> Term<'a> {
    (atoms::error(), class, reason.to_string()).encode(env)
}

fn map_error<'a>(env: Env<'a>, err: OmqError) -> Term<'a> {
    match err {
        OmqError::InvalidEndpoint(reason) => err_term(env, atoms::invalid_endpoint(), reason),
        OmqError::UnsupportedScheme(reason) => err_term(env, atoms::unsupported_scheme(), reason),
        OmqError::Protocol(reason) => err_term(env, atoms::protocol(), reason),
        OmqError::HandshakeFailed(reason) => err_term(env, atoms::handshake_failed(), reason),
        OmqError::Closed => err_term(env, atoms::closed(), "socket closed"),
        OmqError::Timeout => err_term(env, atoms::timeout(), "operation timed out"),
        OmqError::MessageTooLarge { size, max } => {
            err_term(env, atoms::message_too_large(), format!("{size} > {max}"))
        }
        OmqError::ReceiveRateLimitExceeded => {
            err_term(env, atoms::rate_limit(), "receive rate limit exceeded")
        }
        OmqError::Unroutable => err_term(env, atoms::no_route(), "no route to peer"),
        OmqError::WouldBlock => err_term(env, atoms::would_block(), "operation would block"),
        OmqError::Config(reason) => err_term(env, atoms::config(), reason),
        OmqError::Io(reason) => err_term(env, atoms::io(), reason),
        OmqError::UnsupportedZmtpVersion { major, minor } => err_term(
            env,
            atoms::protocol(),
            format!("unsupported ZMTP {major}.{minor}"),
        ),
        _ => err_term(env, atoms::undefined(), "unknown error"),
    }
}

fn map_try_send_error<'a>(env: Env<'a>, err: TrySendError) -> Term<'a> {
    match err {
        TrySendError::Full(_) => err_term(env, atoms::would_block(), "send queue full"),
        TrySendError::Closed => err_term(env, atoms::closed(), "socket closed"),
        TrySendError::Error(err) => map_error(env, err),
    }
}

fn parse_endpoint(endpoint: &Binary<'_>) -> Result<Endpoint, OmqError> {
    let text = str::from_utf8(endpoint.as_slice())
        .map_err(|_| OmqError::InvalidEndpoint("endpoint must be UTF-8".into()))?;
    text.parse()
}

fn owned_binary(bytes: &[u8]) -> OwnedBinary {
    let mut out = OwnedBinary::new(bytes.len()).expect("allocate BEAM binary");
    out.as_mut_slice().copy_from_slice(bytes);
    out
}

fn ok_binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let binary = owned_binary(bytes).release(env);
    ok(env, binary)
}

fn message_from_parts(parts: Vec<Binary<'_>>, routing_id: u32) -> Message {
    let bytes = parts
        .into_iter()
        .map(|part| Bytes::copy_from_slice(part.as_slice()));
    let message = Message::multipart(bytes);
    if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id)
    }
}

fn message_to_term<'a>(env: Env<'a>, message: Message) -> Term<'a> {
    let parts: Vec<_> = message
        .iter()
        .map(|part| owned_binary(&part).release(env))
        .collect();
    let routing_id = message.routing_id().unwrap_or(0);
    (atoms::ok(), parts, routing_id).encode(env)
}

fn socket_type_from_i64(value: i64) -> Option<SocketType> {
    match value {
        0 => Some(SocketType::Pair),
        1 => Some(SocketType::Pub),
        2 => Some(SocketType::Sub),
        3 => Some(SocketType::Req),
        4 => Some(SocketType::Rep),
        5 => Some(SocketType::Dealer),
        6 => Some(SocketType::Router),
        7 => Some(SocketType::Pull),
        8 => Some(SocketType::Push),
        9 => Some(SocketType::XPub),
        10 => Some(SocketType::XSub),
        11 => Some(SocketType::Stream),
        12 => Some(SocketType::Server),
        13 => Some(SocketType::Client),
        14 => Some(SocketType::Radio),
        15 => Some(SocketType::Dish),
        16 => Some(SocketType::Gather),
        17 => Some(SocketType::Scatter),
        19 => Some(SocketType::Peer),
        20 => Some(SocketType::Channel),
        _ => None,
    }
}

fn socket_type_to_i64(socket_type: SocketType) -> i64 {
    match socket_type {
        SocketType::Pair => 0,
        SocketType::Pub => 1,
        SocketType::Sub => 2,
        SocketType::Req => 3,
        SocketType::Rep => 4,
        SocketType::Dealer => 5,
        SocketType::Router => 6,
        SocketType::Pull => 7,
        SocketType::Push => 8,
        SocketType::XPub => 9,
        SocketType::XSub => 10,
        SocketType::Stream => 11,
        SocketType::Server => 12,
        SocketType::Client => 13,
        SocketType::Radio => 14,
        SocketType::Dish => 15,
        SocketType::Gather => 16,
        SocketType::Scatter => 17,
        SocketType::Peer => 19,
        SocketType::Channel => 20,
        _ => -1,
    }
}

fn duration_from_millis(value: i64) -> Option<Duration> {
    if value < 0 {
        None
    } else {
        Some(Duration::from_millis(value as u64))
    }
}

fn duration_to_millis(value: Option<Duration>) -> i64 {
    value.map_or(-1, |duration| {
        i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
    })
}

fn deadline_after(timeout: Duration) -> Option<Instant> {
    if timeout.is_zero() {
        Some(Instant::now())
    } else {
        Instant::now().checked_add(timeout)
    }
}

fn send_with_timeout(
    socket: &omq_tokio::blocking::Socket,
    message: Message,
    timeout: Option<Duration>,
) -> Result<(), OmqError> {
    match timeout {
        None => socket.send(message),
        Some(timeout) if timeout.is_zero() => socket.try_send(message).map_err(|err| match err {
            TrySendError::Full(_) => OmqError::WouldBlock,
            TrySendError::Closed => OmqError::Closed,
            TrySendError::Error(err) => err,
        }),
        Some(timeout) => {
            let deadline = deadline_after(timeout);
            let mut message = message;
            loop {
                match socket.try_send(message) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(returned)) => message = returned,
                    Err(TrySendError::Closed) => return Err(OmqError::Closed),
                    Err(TrySendError::Error(err)) => return Err(err),
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Err(OmqError::Timeout);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn recv_with_timeout(
    recv_buffer: &Mutex<VecDeque<Message>>,
    socket: &omq_tokio::blocking::Socket,
    timeout: Option<Duration>,
) -> Result<Message, OmqError> {
    if let Some(message) = recv_buffer.lock().unwrap().pop_front() {
        return Ok(message);
    }
    match timeout {
        None => socket.recv(),
        Some(timeout) if timeout.is_zero() => socket.try_recv(),
        Some(timeout) => socket.recv_timeout(timeout),
    }
}

fn socket_has_message(socket: &NativeSocket) -> bool {
    if !socket.recv_buffer.lock().unwrap().is_empty() {
        return true;
    }
    let Ok(materialized) = socket.materialize() else {
        return false;
    };
    match materialized.try_recv() {
        Ok(message) => {
            socket.recv_buffer.lock().unwrap().push_back(message);
            true
        }
        Err(_) => false,
    }
}

fn keepalive_parts(keepalive: KeepAlive) -> (Duration, Duration, u32) {
    match keepalive {
        KeepAlive::Enabled { idle, intvl, cnt } => (idle, intvl, cnt),
        _ => (Duration::from_secs(60), Duration::from_secs(10), 3),
    }
}

#[rustler::nif]
fn context_new<'a>(env: Env<'a>, io_threads: i64) -> Term<'a> {
    if io_threads < 1 {
        return err_term(env, atoms::badarg(), "io_threads must be >= 1");
    }
    let ctx = Context::with_config(ContextConfig {
        io_threads: io_threads as usize,
    });
    ok(env, ResourceArc::new(NativeContext { ctx }))
}

#[rustler::nif]
fn context_term<'a>(env: Env<'a>, context: ResourceArc<NativeContext>) -> Term<'a> {
    context.ctx.term();
    ok_unit(env)
}

#[rustler::nif]
fn socket_new<'a>(env: Env<'a>, context: ResourceArc<NativeContext>, socket_type: i64) -> Term<'a> {
    let Some(socket_type) = socket_type_from_i64(socket_type) else {
        return err_term(env, atoms::badarg(), "unknown socket type");
    };
    ok(
        env,
        ResourceArc::new(NativeSocket {
            ctx: context.ctx.clone(),
            socket_type,
            options: Mutex::new(Options::default()),
            rcvtimeo: Mutex::new(None),
            sndtimeo: Mutex::new(None),
            plain_server: AtomicBool::new(false),
            plain_username: Mutex::new(None),
            plain_password: Mutex::new(None),
            curve_server: AtomicBool::new(false),
            curve_publickey: Mutex::new(None),
            curve_secretkey: Mutex::new(None),
            curve_serverkey: Mutex::new(None),
            last_endpoint: Mutex::new(Vec::new()),
            recv_buffer: Mutex::new(VecDeque::new()),
            socket: RwLock::new(None),
            closed: AtomicBool::new(false),
        }),
    )
}

#[rustler::nif]
fn socket_type<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>) -> Term<'a> {
    ok(env, socket_type_to_i64(socket.socket_type))
}

#[rustler::nif(schedule = "DirtyIo")]
fn bind<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, endpoint: Binary<'a>) -> Term<'a> {
    let endpoint = match parse_endpoint(&endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return map_error(env, error),
    };
    match socket
        .materialize()
        .and_then(|socket| socket.bind(endpoint))
    {
        Ok(bound) => {
            let bound = bound.to_string();
            *socket.last_endpoint.lock().unwrap() = bound.as_bytes().to_vec();
            ok_binary(env, bound.as_bytes())
        }
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, endpoint: Binary<'a>) -> Term<'a> {
    let endpoint = match parse_endpoint(&endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return map_error(env, error),
    };
    let endpoint_text = endpoint.to_string();
    match socket
        .materialize()
        .and_then(|socket| socket.connect(endpoint))
    {
        Ok(()) => {
            *socket.last_endpoint.lock().unwrap() = endpoint_text.into_bytes();
            ok_unit(env)
        }
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn unbind<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, endpoint: Binary<'a>) -> Term<'a> {
    let endpoint = match parse_endpoint(&endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return map_error(env, error),
    };
    let endpoint_text = endpoint.to_string();
    match socket
        .materialize()
        .and_then(|socket| socket.unbind(endpoint))
    {
        Ok(()) => {
            let mut last = socket.last_endpoint.lock().unwrap();
            if last.as_slice() == endpoint_text.as_bytes() {
                last.clear();
            }
            ok_unit(env)
        }
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn disconnect<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    endpoint: Binary<'a>,
) -> Term<'a> {
    let endpoint = match parse_endpoint(&endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => return map_error(env, error),
    };
    let endpoint_text = endpoint.to_string();
    match socket
        .materialize()
        .and_then(|socket| socket.disconnect(endpoint))
    {
        Ok(()) => {
            let mut last = socket.last_endpoint.lock().unwrap();
            if last.as_slice() == endpoint_text.as_bytes() {
                last.clear();
            }
            ok_unit(env)
        }
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn send<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    parts: Vec<Binary<'a>>,
    routing_id: u32,
) -> Term<'a> {
    let message = message_from_parts(parts, routing_id);
    let timeout = *socket.sndtimeo.lock().unwrap();
    match socket
        .materialize()
        .and_then(|socket| send_with_timeout(&socket, message, timeout))
    {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif]
fn try_send<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    parts: Vec<Binary<'a>>,
    routing_id: u32,
) -> Term<'a> {
    let message = message_from_parts(parts, routing_id);
    match socket.materialize().map(|socket| socket.try_send(message)) {
        Ok(Ok(())) => ok_unit(env),
        Ok(Err(error)) => map_try_send_error(env, error),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn recv<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, timeout_ms: i64) -> Term<'a> {
    let timeout = if timeout_ms == -2 {
        *socket.rcvtimeo.lock().unwrap()
    } else {
        duration_from_millis(timeout_ms)
    };
    let result = socket
        .materialize()
        .and_then(|materialized| recv_with_timeout(&socket.recv_buffer, &materialized, timeout));
    match result {
        Ok(message) => message_to_term(env, message),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif]
fn try_recv<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>) -> Term<'a> {
    let result = socket.materialize().and_then(|materialized| {
        recv_with_timeout(&socket.recv_buffer, &materialized, Some(Duration::ZERO))
    });
    match result {
        Ok(message) => message_to_term(env, message),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_any<'a>(
    env: Env<'a>,
    sockets: Vec<ResourceArc<NativeSocket>>,
    timeout_ms: i64,
) -> Term<'a> {
    if sockets.is_empty() {
        return ok(env, Vec::<usize>::new());
    }
    let deadline = duration_from_millis(timeout_ms).and_then(deadline_after);
    loop {
        let ready: Vec<_> = sockets
            .iter()
            .enumerate()
            .filter_map(|(index, socket)| socket_has_message(socket).then_some(index))
            .collect();
        if !ready.is_empty() {
            return ok(env, ready);
        }
        if timeout_ms == 0 || deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return ok(env, Vec::<usize>::new());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn subscribe<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, prefix: Binary<'a>) -> Term<'a> {
    let prefix = Bytes::copy_from_slice(prefix.as_slice());
    match socket
        .materialize()
        .and_then(|socket| socket.subscribe(prefix))
    {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn unsubscribe<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    prefix: Binary<'a>,
) -> Term<'a> {
    let prefix = Bytes::copy_from_slice(prefix.as_slice());
    match socket
        .materialize()
        .and_then(|socket| socket.unsubscribe(prefix))
    {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn join<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, group: Binary<'a>) -> Term<'a> {
    let group = Bytes::copy_from_slice(group.as_slice());
    match socket.materialize().and_then(|socket| socket.join(group)) {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn leave<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, group: Binary<'a>) -> Term<'a> {
    let group = Bytes::copy_from_slice(group.as_slice());
    match socket.materialize().and_then(|socket| socket.leave(group)) {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn send_group<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    group: Binary<'a>,
    body: Binary<'a>,
) -> Term<'a> {
    let group = Bytes::copy_from_slice(group.as_slice());
    let body = Bytes::copy_from_slice(body.as_slice());
    match socket
        .materialize()
        .and_then(|socket| socket.send_group(group, body))
    {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn close<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, linger_ms: i64) -> Term<'a> {
    match socket.close_with_linger(duration_from_millis(linger_ms)) {
        Ok(()) => ok_unit(env),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_connected<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    min_peers: usize,
    timeout_ms: i64,
) -> Term<'a> {
    let timeout = duration_from_millis(timeout_ms).unwrap_or(Duration::from_secs(u64::MAX));
    match socket
        .materialize()
        .and_then(|socket| socket.wait_connected(min_peers, timeout))
    {
        Ok(count) => ok(env, count),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn wait_subscribed<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    min_subscriptions: u64,
    timeout_ms: i64,
) -> Term<'a> {
    let timeout = duration_from_millis(timeout_ms).unwrap_or(Duration::from_secs(u64::MAX));
    match socket
        .materialize()
        .and_then(|socket| socket.wait_subscribed(min_subscriptions, timeout))
    {
        Ok(generation) => ok(env, generation),
        Err(error) => map_error(env, error),
    }
}

#[rustler::nif]
fn setsockopt<'a>(
    env: Env<'a>,
    socket: ResourceArc<NativeSocket>,
    option: i64,
    int_value: i64,
    bin_value: Binary<'a>,
) -> Term<'a> {
    if socket.socket.read().unwrap().is_some() && !matches!(option, 6 | 7 | 27 | 28) {
        return err_term(
            env,
            atoms::badarg(),
            "option must be set before bind/connect/send/recv",
        );
    }
    let mut options = socket.options.lock().unwrap();
    match option {
        5 => options.identity = Bytes::copy_from_slice(bin_value.as_slice()),
        17 => options.linger = duration_from_millis(int_value),
        18 => options.reconnect = ReconnectPolicy::Fixed(duration_from_millis(int_value).unwrap()),
        21 => {
            let min = match options.reconnect {
                ReconnectPolicy::Fixed(min) | ReconnectPolicy::Exponential { min, .. } => min,
                ReconnectPolicy::Disabled => Duration::from_millis(100),
                _ => Duration::from_millis(100),
            };
            options.reconnect = ReconnectPolicy::Exponential {
                min,
                max: duration_from_millis(int_value).unwrap(),
            };
        }
        22 => {
            options.max_message_size = if int_value < 0 {
                None
            } else {
                Some(int_value as usize)
            };
        }
        23 => {
            if int_value < 0 {
                return err_term(env, atoms::badarg(), "SNDHWM must be >= 0");
            }
            options.send_hwm = int_value as u32;
        }
        24 => {
            if int_value < 0 {
                return err_term(env, atoms::badarg(), "RCVHWM must be >= 0");
            }
            options.recv_hwm = int_value as u32;
        }
        27 => *socket.rcvtimeo.lock().unwrap() = duration_from_millis(int_value),
        28 => *socket.sndtimeo.lock().unwrap() = duration_from_millis(int_value),
        33 => options.router_mandatory = int_value != 0,
        34 => {
            options.tcp_keepalive = match int_value {
                -1 => KeepAlive::Default,
                0 => KeepAlive::Disabled,
                _ => KeepAlive::Enabled {
                    idle: Duration::from_secs(60),
                    intvl: Duration::from_secs(10),
                    cnt: 3,
                },
            };
        }
        35 => {
            let (idle, intvl, _) = keepalive_parts(options.tcp_keepalive);
            options.tcp_keepalive = KeepAlive::Enabled {
                idle,
                intvl,
                cnt: int_value.max(0) as u32,
            };
        }
        36 => {
            let (_, intvl, cnt) = keepalive_parts(options.tcp_keepalive);
            options.tcp_keepalive = KeepAlive::Enabled {
                idle: Duration::from_secs(int_value.max(0) as u64),
                intvl,
                cnt,
            };
        }
        37 => {
            let (idle, _, cnt) = keepalive_parts(options.tcp_keepalive);
            options.tcp_keepalive = KeepAlive::Enabled {
                idle,
                intvl: Duration::from_secs(int_value.max(0) as u64),
                cnt,
            };
        }
        54 => options.conflate = int_value != 0,
        66 => options.handshake_timeout = duration_from_millis(int_value),
        75 => options.heartbeat_interval = duration_from_millis(int_value),
        76 => options.heartbeat_ttl = duration_from_millis(int_value),
        77 => options.heartbeat_timeout = duration_from_millis(int_value),
        11 => {
            options.send_buffer_size = if int_value <= 0 {
                None
            } else {
                Some(int_value as usize)
            };
        }
        12 => {
            options.recv_buffer_size = if int_value <= 0 {
                None
            } else {
                Some(int_value as usize)
            };
        }
        44 => socket.plain_server.store(int_value != 0, Ordering::Release),
        45 => {
            *socket.plain_username.lock().unwrap() =
                Some(String::from_utf8_lossy(bin_value.as_slice()).into_owned());
        }
        46 => {
            *socket.plain_password.lock().unwrap() =
                Some(String::from_utf8_lossy(bin_value.as_slice()).into_owned());
        }
        47 => socket.curve_server.store(int_value != 0, Ordering::Release),
        48 => *socket.curve_publickey.lock().unwrap() = Some(bin_value.as_slice().to_vec()),
        49 => *socket.curve_secretkey.lock().unwrap() = Some(bin_value.as_slice().to_vec()),
        50 => *socket.curve_serverkey.lock().unwrap() = Some(bin_value.as_slice().to_vec()),
        1004 => {
            options.on_mute = match int_value {
                0 => OnMute::Block,
                1 => OnMute::DropNewest,
                2 => OnMute::DropOldest,
                _ => return err_term(env, atoms::badarg(), "invalid on_mute"),
            };
        }
        1005 => {
            if !(-8..=4).contains(&int_value) {
                return err_term(env, atoms::badarg(), "invalid compression level");
            }
            options.compression_level = Some(int_value as i32);
        }
        1006 => {
            const DICT_MAX: usize = 8 * 1024;
            if bin_value.as_slice().len() > DICT_MAX {
                return err_term(env, atoms::badarg(), "compression dict too large");
            }
            options.compression_dict = if bin_value.as_slice().is_empty() {
                None
            } else {
                Some(Bytes::copy_from_slice(bin_value.as_slice()))
            };
        }
        1007 => options.compression_auto_train = int_value != 0,
        1100 => {
            options.workload_profile = match int_value {
                0 => Some(WorkloadProfile::Throughput),
                1 => Some(WorkloadProfile::Latency),
                _ => return err_term(env, atoms::badarg(), "invalid workload profile"),
            };
        }
        109 => options.reconnect_stop_conn_refused = int_value != 0,
        39 | 42 | 31 | 8 | 79 | 40 | 51 | 52 | 53 | 56 | 38 | 80 | 25 | 9 | 55 => {}
        6 | 7 => return err_term(env, atoms::badarg(), "use subscribe/unsubscribe"),
        16 | 13 => return err_term(env, atoms::badarg(), "read-only option"),
        _ => return err_term(env, atoms::badarg(), format!("unsupported option {option}")),
    }
    ok_unit(env)
}

#[rustler::nif]
fn getsockopt<'a>(env: Env<'a>, socket: ResourceArc<NativeSocket>, option: i64) -> Term<'a> {
    let options = socket.options.lock().unwrap();
    match option {
        5 => ok_binary(env, &options.identity),
        13 => ok(env, 0i64),
        16 => ok(env, socket_type_to_i64(socket.socket_type)),
        17 => ok(env, duration_to_millis(options.linger)),
        18 => match options.reconnect {
            ReconnectPolicy::Fixed(duration)
            | ReconnectPolicy::Exponential { min: duration, .. } => {
                ok(env, duration_to_millis(Some(duration)))
            }
            ReconnectPolicy::Disabled => ok(env, -1i64),
            _ => ok(env, -1i64),
        },
        21 => match options.reconnect {
            ReconnectPolicy::Exponential { max, .. } => ok(env, duration_to_millis(Some(max))),
            _ => ok(env, 0i64),
        },
        22 => ok(env, options.max_message_size.map_or(-1, |v| v as i64)),
        23 => ok(env, i64::from(options.send_hwm)),
        24 => ok(env, i64::from(options.recv_hwm)),
        27 => ok(env, duration_to_millis(*socket.rcvtimeo.lock().unwrap())),
        28 => ok(env, duration_to_millis(*socket.sndtimeo.lock().unwrap())),
        33 => ok(env, i64::from(options.router_mandatory)),
        34 => {
            let value = match options.tcp_keepalive {
                KeepAlive::Default => -1,
                KeepAlive::Disabled => 0,
                KeepAlive::Enabled { .. } => 1,
                _ => -1,
            };
            ok(env, value)
        }
        35 => {
            let value = match options.tcp_keepalive {
                KeepAlive::Enabled { cnt, .. } => cnt as i64,
                _ => -1,
            };
            ok(env, value)
        }
        36 => {
            let value = match options.tcp_keepalive {
                KeepAlive::Enabled { idle, .. } => idle.as_secs() as i64,
                _ => -1,
            };
            ok(env, value)
        }
        37 => {
            let value = match options.tcp_keepalive {
                KeepAlive::Enabled { intvl, .. } => intvl.as_secs() as i64,
                _ => -1,
            };
            ok(env, value)
        }
        54 => ok(env, i64::from(options.conflate)),
        66 => ok(env, duration_to_millis(options.handshake_timeout)),
        75 => ok(env, duration_to_millis(options.heartbeat_interval)),
        76 => ok(env, duration_to_millis(options.heartbeat_ttl)),
        77 => ok(env, duration_to_millis(options.heartbeat_timeout)),
        11 => ok(
            env,
            options.send_buffer_size.map_or(0, |value| value as i64),
        ),
        12 => ok(
            env,
            options.recv_buffer_size.map_or(0, |value| value as i64),
        ),
        44 => ok(env, i64::from(socket.plain_server.load(Ordering::Acquire))),
        45 => {
            let value = socket
                .plain_username
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            ok_binary(env, value.as_bytes())
        }
        46 => {
            let value = socket
                .plain_password
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            ok_binary(env, value.as_bytes())
        }
        47 => ok(env, i64::from(socket.curve_server.load(Ordering::Acquire))),
        48 => {
            let value = socket
                .curve_publickey
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            ok_binary(env, &value)
        }
        49 => {
            let value = socket
                .curve_secretkey
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            ok_binary(env, &value)
        }
        50 => {
            let value = socket
                .curve_serverkey
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            ok_binary(env, &value)
        }
        1004 => {
            let value = match options.on_mute {
                OnMute::Block => 0,
                OnMute::DropNewest => 1,
                OnMute::DropOldest => 2,
                _ => 0,
            };
            ok(env, value)
        }
        1005 => ok(env, options.compression_level.map_or(0, i64::from)),
        1006 => match &options.compression_dict {
            Some(dict) => ok_binary(env, dict),
            None => ok_binary(env, &[]),
        },
        1007 => ok(env, i64::from(options.compression_auto_train)),
        1100 => {
            let value = match options.workload_profile {
                Some(WorkloadProfile::Throughput) => 0,
                Some(WorkloadProfile::Latency) => 1,
                None => -1,
            };
            ok(env, value)
        }
        109 => ok(env, i64::from(options.reconnect_stop_conn_refused)),
        43 | 39 | 42 | 31 | 8 | 79 | 40 | 51 | 52 | 53 | 56 | 80 | 25 | 9 => ok(env, 0i64),
        32 => ok_binary(env, &socket.last_endpoint.lock().unwrap()),
        38 | 55 => ok_binary(env, &[]),
        _ => err_term(env, atoms::badarg(), format!("unsupported option {option}")),
    }
}

rustler::init!("omq_nif");
