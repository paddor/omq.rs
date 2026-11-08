//! Monitor handles, events, and receive operations.

use crate::convert::{duration_from_timeout_millis, omq_go_string_free, string_to_raw};
use crate::error::{AGAIN, CLOSED, CONFIG, TIMEOUT};
use crate::ffi::{OmqGoEvent, OmqGoStatus};
use crate::message::raw_bytes;
use crate::socket::OmqGoSocket;
use crate::stats::{MONITORS_CREATED, MONITORS_FREED, bump};
use bytes::Bytes;
use omq_tokio::{
    Context, DisconnectReason, Error, MonitorEvent as NativeMonitorEvent, MonitorRecvError,
    MonitorStream, MonitorTryRecvError, PeerCommandKind, PeerInfo as NativePeerInfo,
};
use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct OmqGoMonitor {
    ctx: Context,
    stream: Mutex<Option<MonitorStream>>,
    closed: AtomicBool,
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
        NativeMonitorEvent::SubscribeReceived { prefix, .. } => {
            out.kind = string_to_raw("SUBSCRIBE_RECEIVED".to_string());
            let (data, len) = bytes_to_event_data(prefix);
            out.data = data;
            out.data_len = len;
        }
        NativeMonitorEvent::UnsubscribeReceived { prefix, .. } => {
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
                    ctx: socket.context().clone(),
                    stream: Mutex::new(Some(native.monitor())),
                    closed: AtomicBool::new(false),
                }));
            }
            bump(&MONITORS_CREATED);
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
    bump(&MONITORS_FREED);
    unsafe {
        drop(Box::from_raw(monitor));
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
