//! Socket option entry points.

use crate::convert::{
    bytes_from_c, duration_from_millis, linger_from_millis, optional_duration_from_millis,
    optional_usize_from_i64,
};
use crate::error::{CLOSED, CONFIG, status_from_result};
use crate::ffi::OmqGoStatus;
use crate::socket::OmqGoSocket;
use bytes::Bytes;
use omq_tokio::Options;
use omq_tokio::options::{KeepAlive, OnMute, ReconnectPolicy, WorkloadProfile};

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
pub extern "C" fn omq_go_socket_set_max_message_parts(
    socket: *mut OmqGoSocket,
    parts: i64,
) -> OmqGoStatus {
    let max = match optional_usize_from_i64("max message parts", parts) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    if max == Some(0) {
        return OmqGoStatus::err(CONFIG, "max message parts must be greater than zero");
    }
    set_socket_option(socket, move |options| options.max_message_parts = max)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_socket_set_max_peer_subscriptions(
    socket: *mut OmqGoSocket,
    entries: i64,
) -> OmqGoStatus {
    let max = match optional_usize_from_i64("max peer subscriptions", entries) {
        Ok(value) => value,
        Err(error) => return OmqGoStatus::from_error(error),
    };
    set_socket_option(socket, move |options| options.max_peer_subscriptions = max)
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

pub(crate) fn set_socket_option<F>(socket: *mut OmqGoSocket, f: F) -> OmqGoStatus
where
    F: FnOnce(&mut Options),
{
    if socket.is_null() {
        return OmqGoStatus::err(CLOSED, "socket closed");
    }
    status_from_result(unsafe { &*socket }.set_option(f))
}
