//! Monitor handles, events, and receive operations.

use crate::convert::{nullable_bytes, nullable_string, optional_duration_from_millis};
use crate::error::{guard, jni_error, throw_omq};
use crate::identity::peer_info_object;
use crate::socket::socket_from_handle;
use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jint, jlong, jobject};
use omq_tokio::{
    Context, DisconnectReason, Error, MonitorEvent as NativeMonitorEvent, MonitorRecvError,
    MonitorStream, MonitorTryRecvError, PeerCommandKind, PeerInfo as NativePeerInfo,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

struct JavaMonitor {
    ctx: Context,
    stream: Mutex<Option<MonitorStream>>,
    closed: AtomicBool,
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
        NativeMonitorEvent::SubscribeReceived { prefix, .. } => EventParts {
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
        NativeMonitorEvent::UnsubscribeReceived { prefix, .. } => EventParts {
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
        Some(peer) => peer_info_object(env, &peer)?,
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
                ctx: socket.context().clone(),
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
