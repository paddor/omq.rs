use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
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
    recv_scratch: Mutex<Vec<Message>>,
    closed: AtomicBool,
}

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

struct JavaAsyncTask {
    abort: tokio::task::AbortHandle,
}

struct AbortOnDrop {
    joins: Vec<tokio::task::JoinHandle<()>>,
}

impl AbortOnDrop {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            joins: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, join: tokio::task::JoinHandle<()>) {
        self.joins.push(join);
    }

    fn abort_all(&self) {
        for join in &self.joins {
            join.abort();
        }
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.abort_all();
    }
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
        || text.contains("name or service")
        || text.contains("nodename")
        || text.contains("temporary failure in name resolution")
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
        "(Ljava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&mechanism),
            JValue::Object(&public_key),
            JValue::Object(&identity),
            JValue::Object(&username),
            JValue::Object(&password),
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

fn send_bodies(
    env: &mut JNIEnv<'_>,
    bodies: &JObjectArray<'_>,
    socket: &BlockingSocket,
) -> Result<(), Error> {
    let len = env.get_array_length(bodies).map_err(jni_error)?;
    for i in 0..len {
        let body = env.get_object_array_element(bodies, i).map_err(jni_error)?;
        if body.is_null() {
            return Err(Error::Config(format!("message body {i} must not be null")));
        }
        let body = JByteArray::from(body);
        socket.send(Message::single(Bytes::from(byte_array(env, body)?)))?;
    }
    Ok(())
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
        match socket.try_send(message) {
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
    if message.len() == 1 {
        let part = message_part_to_java(env, message, 0)?;
        let part = JObject::from(part);
        return env
            .call_static_method(
                "io/omq/Message",
                "fromNative",
                "([B)Lio/omq/Message;",
                &[JValue::Object(&part)],
            )
            .and_then(|value| value.l())
            .map_err(jni_error);
    }

    let parts = message_to_java_parts(env, message)?;
    let parts = JObject::from(parts);
    env.call_static_method(
        "io/omq/Message",
        "fromNative",
        "([[B)Lio/omq/Message;",
        &[JValue::Object(&parts)],
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
    if message.len() == 1 {
        return message_part_to_java(env, message, 0).map(JObject::from);
    }
    message_to_java_parts(env, message).map(JObject::from)
}

fn messages_to_java_native_array(
    env: &mut JNIEnv<'_>,
    messages: &[Message],
) -> Result<jobjectArray, Error> {
    let object_class = env.find_class("java/lang/Object").map_err(jni_error)?;
    let out = env
        .new_object_array(messages.len() as jint, object_class, JObject::null())
        .map_err(jni_error)?;
    for (index, message) in messages.iter().enumerate() {
        let item = message_to_java_native_ref(env, message)?;
        env.set_object_array_element(&out, index as jint, item)
            .map_err(jni_error)?;
    }
    Ok(out.into_raw())
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

fn fill_java_byte_arrays(
    env: &mut JNIEnv<'_>,
    out: &JObjectArray<'_>,
    offset: jint,
    messages: &[Message],
) -> Result<(), Error> {
    for (index, message) in messages.iter().enumerate() {
        if message.len() != 1 {
            throw_java(
                env,
                "java/lang/IllegalStateException",
                format!("message has {} parts", message.len()),
            );
            return Err(Error::Config("message is multipart".to_string()));
        }
        let body = message_part_to_java(env, message, 0)?;
        env.set_object_array_element(out, offset + index as jint, body)
            .map_err(jni_error)?;
    }
    Ok(())
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
    if secret.derive_public() != public {
        return Err(Error::Config(
            "CURVE public key does not match secret key".to_string(),
        ));
    }
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
                let async_socket = socket.materialize()?.into_async();
                entries.push((runtime, async_socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let join = parent_runtime.spawn(async move {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let mut joins = AbortOnDrop::with_capacity(entries.len());
                for (runtime, socket, java_socket) in entries {
                    let tx = tx.clone();
                    let join = runtime.spawn(async move {
                        let result = socket.recv().await.map(|message| (java_socket, message));
                        let _ = tx.send(result);
                    });
                    joins.push(join);
                }
                drop(tx);

                let result = rx.recv().await.unwrap_or(Err(Error::Closed));
                joins.abort_all();
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
                let async_socket = socket.materialize()?.into_async();
                entries.push((runtime, async_socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let join = parent_runtime.spawn(async move {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let mut joins = AbortOnDrop::with_capacity(entries.len());
                for (runtime, socket, java_socket) in entries {
                    let tx = tx.clone();
                    let join = runtime.spawn(async move {
                        let result = socket.recv().await.map(|message| (java_socket, message));
                        let _ = tx.send(result);
                    });
                    joins.push(join);
                }
                drop(tx);

                let result = match timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, rx.recv()).await {
                        Ok(Some(result)) => result.map(Some),
                        Ok(None) => Err(Error::Closed),
                        Err(_) => Ok(None),
                    },
                    None => rx.recv().await.unwrap_or(Err(Error::Closed)).map(Some),
                };
                joins.abort_all();
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
            recv_scratch: Mutex::new(Vec::new()),
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

        if let Some(socket) = socket.socket.get() {
            let _ = socket.clone().close();
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
            socket
                .materialize()?
                .send(Message::single(Bytes::from(data)))
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
pub extern "system" fn Java_io_omq_Native_socketSendMultipartTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            send_with_timeout(
                &socket.materialize()?,
                Message::multipart(parts),
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
pub extern "system" fn Java_io_omq_Native_socketSendMany(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    messages: JObjectArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            send_bodies(env, &messages, &socket)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketTrySendMultipart(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            match socket.materialize()?.try_send(Message::multipart(parts)) {
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
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let parts = bytes_from_parts(env, parts)?;
            let handle = socket.ctx.handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = socket.send(Message::multipart(parts)).await;
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
pub extern "system" fn Java_io_omq_Native_socketRecvMany(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    max_messages: jint,
    timeout_millis: jlong,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let java_socket = socket_from_handle(handle)?;
            let socket = java_socket.materialize()?;
            let mut scratch = java_socket
                .recv_scratch
                .lock()
                .map_err(|_| Error::Config("recv scratch lock poisoned".to_string()))?;
            scratch.clear();
            recv_many_into(&socket, max_messages, timeout_millis, &mut scratch)?;
            let out = messages_to_java_native_array(env, &scratch);
            scratch.clear();
            out
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
pub extern "system" fn Java_io_omq_Native_socketRecvManyBytesInto(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    out: JObjectArray<'_>,
    offset: jint,
    max_messages: jint,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let java_socket = socket_from_handle(handle)?;
            let socket = java_socket.materialize()?;
            let mut scratch = java_socket
                .recv_scratch
                .lock()
                .map_err(|_| Error::Config("recv scratch lock poisoned".to_string()))?;
            scratch.clear();
            let count = recv_many_into(&socket, max_messages, timeout_millis, &mut scratch)?;
            let fill_result = fill_java_byte_arrays(env, &out, offset, &scratch);
            scratch.clear();
            fill_result?;
            Ok(count as jint)
        })();

        match result {
            Ok(value) => value,
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
