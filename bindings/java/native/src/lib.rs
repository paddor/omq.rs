use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong, jobjectArray, jstring};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{
    Authenticator, Context, ContextConfig, CurveKeypair, CurvePublicKey, CurveSecretKey,
    CurveServerOptions, Endpoint, Error, MechanismSetup, Message, Options, SocketType,
};

struct JavaContext {
    ctx: Context,
    closed: AtomicBool,
}

struct JavaSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    socket: Mutex<Option<BlockingSocket>>,
    closed: AtomicBool,
}

impl JavaSocket {
    fn materialize(&self) -> Result<BlockingSocket, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        let mut socket = self
            .socket
            .lock()
            .map_err(|_| Error::Config("socket lock poisoned".to_string()))?;
        if let Some(socket) = socket.as_ref() {
            return Ok(socket.clone());
        }

        let options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .clone();
        options.validate()?;

        let created = self.ctx.blocking_socket(self.socket_type, options);
        *socket = Some(created.clone());
        Ok(created)
    }

    fn set_option<F>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Options),
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        if self
            .socket
            .lock()
            .map_err(|_| Error::Config("socket lock poisoned".to_string()))?
            .is_some()
        {
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

fn jni_error(error: jni::errors::Error) -> Error {
    Error::Config(format!("JNI error: {error}"))
}

fn context_from_handle(handle: jlong) -> Result<&'static JavaContext, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &*(handle as *mut JavaContext) })
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

fn message_to_java(env: &mut JNIEnv<'_>, message: Message) -> Result<jobjectArray, Error> {
    let byte_array_class = env.find_class("[B").map_err(jni_error)?;
    let parts = env
        .new_object_array(message.len() as jint, byte_array_class, JObject::null())
        .map_err(jni_error)?;

    for i in 0..message.len() {
        let part = message.part_bytes(i).unwrap_or_default();
        let array = env.byte_array_from_slice(&part).map_err(jni_error)?;
        env.set_object_array_element(&parts, i as jint, array)
            .map_err(jni_error)?;
    }

    Ok(parts.into_raw())
}

fn endpoint_from_java(env: &mut JNIEnv<'_>, endpoint: JString<'_>) -> Result<Endpoint, Error> {
    Endpoint::from_str(&java_string(env, endpoint)?)
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
    if secret.derive_public() != public {
        return Err(Error::Config(
            "CURVE public key does not match secret key".to_string(),
        ));
    }
    Ok(CurveKeypair { public, secret })
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
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }

        let ctx = unsafe { Box::from_raw(handle as *mut JavaContext) };
        if !ctx.closed.swap(true, Ordering::AcqRel) {
            ctx.ctx.term();
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
            socket: Mutex::new(None),
            closed: AtomicBool::new(false),
        })) as jlong
    })
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
        if socket.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        if let Ok(mut guard) = socket.socket.lock()
            && let Some(socket) = guard.take()
        {
            let _ = socket.close();
        }
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
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let endpoint = endpoint_from_java(env, endpoint)?;
            let bound = socket.materialize()?.bind(endpoint)?;
            env.new_string(bound.to_string())
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
pub extern "system" fn Java_io_omq_Native_socketConnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let endpoint = endpoint_from_java(env, endpoint)?;
            socket.materialize()?.connect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
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
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let endpoint = endpoint_from_java(env, endpoint)?;
            socket.materialize()?.unbind(endpoint)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
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
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let endpoint = endpoint_from_java(env, endpoint)?;
            socket.materialize()?.disconnect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
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
            socket.materialize()?.send(Message::from_slice(&data))
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
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            socket.materialize()?.send(Message::multipart(parts))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecv(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = if timeout_millis < 0 {
                socket.recv()?
            } else if timeout_millis == 0 {
                socket.try_recv()?
            } else {
                socket.recv_timeout(Duration::from_millis(timeout_millis as u64))?
            };
            message_to_java(env, message)
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
                    authenticator: Authenticator::new(move |peer| {
                        peer.username.as_deref() == Some(expected_username.as_str())
                            && peer.password.as_deref() == Some(expected_password.as_str())
                    }),
                };
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
