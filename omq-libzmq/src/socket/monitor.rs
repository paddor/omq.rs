//! `zmq_socket_monitor` and related peer-state calls.

use super::{ensure_materialized, with_socket};
use crate::error::{ETERM, fail, map_omq_err};
use std::ffi::{CStr, c_int, c_void};
use std::str::FromStr;

/// Start monitoring events on `sock`, publishing them as two-frame messages
/// on an inproc PAIR socket bound to `addr`.
///
/// Frame layout (libzmq v1 monitor protocol):
///   frame 1: `event_id` (`u16` LE) + value (`i32` LE) = 6 bytes
///   frame 2: endpoint string (UTF-8)
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor(
    sock_ptr: *mut c_void,
    addr: *const libc::c_char,
    events: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    // addr == NULL means stop monitoring.
    if addr.is_null() {
        return 0;
    }
    // SAFETY: C callers pass null or a pointer from this API.
    let sock = match unsafe { crate::handle::socket(sock_ptr) } {
        Ok(socket) => socket,
        Err(errno) => return fail(errno),
    };
    // SAFETY: addr is non-null (checked above); caller guarantees a valid C string.
    let Ok(addr_str) = unsafe { CStr::from_ptr(addr) }.to_str() else {
        return fail(libc::EINVAL);
    };
    let addr_str = addr_str.to_owned();
    let events_mask = events as u16;

    if let Err(error) = ensure_materialized(sock) {
        return fail(error);
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let Some(monitor_ctx) = sock.ctx.io_context().cloned() else {
        return fail(ETERM);
    };

    let result = with_socket(&sock.ctx, inner, move |s| async move {
        let mut stream = s.monitor();

        // Create a PAIR socket for publishing events, bind it to addr.
        let pair = std::sync::Arc::new(
            monitor_ctx.socket(omq_tokio::SocketType::Pair, omq_tokio::Options::default()),
        );
        let ep = omq_tokio::Endpoint::from_str(&addr_str)
            .map_err(|e| omq_tokio::error::Error::InvalidEndpoint(e.to_string()))?;
        pair.bind(ep).await?;

        tokio::spawn(async move {
            while let Ok(ev) = stream.recv().await {
                let Some((event_id, endpoint)) = monitor_event_to_zmq(&ev) else {
                    continue;
                };
                if events_mask != 0xFFFF && (events_mask & event_id) == 0 {
                    continue;
                }
                let msg = monitor_frame(event_id, 0, &endpoint);
                if pair.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok::<_, omq_tokio::error::Error>(())
    });

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => fail(map_omq_err(e)),
        Err(errno) => fail(errno),
    }
}

/// Configure a version-1 socket monitor; other versions return ENOTSUP.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor_versioned(
    sock_ptr: *mut c_void,
    addr: *const libc::c_char,
    events: u64,
    event_version: c_int,
    _type: c_int,
) -> c_int {
    if event_version != 1 || events > c_int::MAX as u64 {
        return fail(crate::error::ENOTSUP);
    }
    zmq_socket_monitor(sock_ptr, addr, events as c_int)
}

/// Unsupported pipe-statistics request; return -1 with ENOTSUP for a valid socket.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_monitor_pipes_stats(sock_ptr: *mut c_void) -> c_int {
    if sock_ptr.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

/// Unsupported peer-state query; return -1 with ENOTSUP for a valid socket.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_socket_get_peer_state(
    socket: *mut c_void,
    _routing_id: *const c_void,
    _routing_id_size: usize,
) -> c_int {
    if socket.is_null() {
        return fail(crate::error::ENOTSOCK);
    }
    fail(crate::error::ENOTSUP)
}

fn monitor_event_to_zmq(ev: &omq_tokio::MonitorEvent) -> Option<(u16, String)> {
    use omq_tokio::MonitorEvent;
    match ev {
        MonitorEvent::Listening { endpoint } => Some((0x0008, endpoint.to_string())),
        MonitorEvent::Accepted { endpoint, .. } => Some((0x0020, endpoint.to_string())),
        MonitorEvent::Connected { endpoint, .. } => Some((0x0001, endpoint.to_string())),
        MonitorEvent::ConnectDelayed { endpoint, .. } => Some((0x0002, endpoint.to_string())),
        MonitorEvent::HandshakeSucceeded { endpoint, .. } => Some((0x1000, endpoint.to_string())),
        MonitorEvent::HandshakeFailed { endpoint, .. } => Some((0x2000, endpoint.to_string())),
        MonitorEvent::Disconnected { endpoint, .. } => Some((0x0200, endpoint.to_string())),
        MonitorEvent::Closed => Some((0x0400, String::new())),
        _ => None,
    }
}

fn monitor_frame(event_id: u16, value: i32, endpoint: &str) -> omq_tokio::Message {
    let mut header = [0u8; 6];
    header[0..2].copy_from_slice(&event_id.to_le_bytes());
    header[2..6].copy_from_slice(&value.to_le_bytes());
    omq_tokio::Message::multipart([
        bytes::Bytes::copy_from_slice(&header),
        bytes::Bytes::copy_from_slice(endpoint.as_bytes()),
    ])
}
