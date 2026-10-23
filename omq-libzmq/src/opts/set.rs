//! `zmq_setsockopt` and its per-category handlers.

use super::LingerSetting;
#[allow(
    clippy::wildcard_imports,
    reason = "option code table; the lint skips test builds"
)]
use super::codes::*;
#[cfg(feature = "quic")]
use super::quic;
use super::raw::{read_bytes, read_i32, read_i64};
use super::set_security::{
    set_curve_client_option, set_curve_option, set_plain_client_option, set_plain_server_option,
    set_ws_option, set_wss_option,
};
use crate::error::fail;
use crate::socket::OmqSocket;
use bytes::Bytes;
use omq_tokio::options::{OnMute, WorkloadProfile};
use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

/// One category of `zmq_setsockopt` options; `None` when `option` is not in it.
type SetOption = fn(&Arc<OmqSocket>, c_int, *const libc::c_void, usize) -> Option<c_int>;

const SET_OPTIONS: [SetOption; 15] = [
    set_queue_option,
    set_message_option,
    set_tuning_option,
    set_reconnect_option,
    set_heartbeat_option,
    set_tcp_option,
    set_network_option,
    set_routing_option,
    set_pubsub_option,
    set_plain_server_option,
    set_plain_client_option,
    set_curve_option,
    set_curve_client_option,
    set_ws_option,
    set_wss_option,
];

/// Accepted and ignored. `ZMQ_ROUTER_HANDOVER` is always on in omq; the rest
/// are read-only or configure features omq does not implement.
const IGNORED_SET_OPTIONS: [c_int; 64] = [
    ZMQ_ROUTER_HANDOVER,
    ZMQ_AFFINITY,
    ZMQ_RATE,
    ZMQ_RECOVERY_IVL,
    ZMQ_MULTICAST_HOPS,
    ZMQ_RCVMORE,
    ZMQ_FD,
    ZMQ_EVENTS,
    ZMQ_TYPE,
    ZMQ_LAST_ENDPOINT,
    ZMQ_TCP_ACCEPT_FILTER,
    ZMQ_ROUTER_RAW,
    ZMQ_MECHANISM,
    ZMQ_TOS,
    ZMQ_IPC_FILTER_PID,
    ZMQ_IPC_FILTER_UID,
    ZMQ_IPC_FILTER_GID,
    ZMQ_CONNECT_ROUTING_ID,
    ZMQ_GSSAPI_SERVER,
    ZMQ_GSSAPI_PRINCIPAL,
    ZMQ_GSSAPI_SERVICE_PRINCIPAL,
    ZMQ_GSSAPI_PLAINTEXT,
    ZMQ_SOCKS_PROXY,
    ZMQ_BLOCKY,
    ZMQ_XPUB_MANUAL,
    ZMQ_XPUB_WELCOME_MSG,
    ZMQ_STREAM_NOTIFY,
    ZMQ_INVERT_MATCHING,
    ZMQ_XPUB_VERBOSER,
    ZMQ_THREAD_SAFE,
    ZMQ_MULTICAST_MAXTPDU,
    ZMQ_VMCI_BUFFER_SIZE,
    ZMQ_VMCI_BUFFER_MIN_SIZE,
    ZMQ_VMCI_BUFFER_MAX_SIZE,
    ZMQ_VMCI_CONNECT_TIMEOUT,
    ZMQ_USE_FD,
    ZMQ_GSSAPI_PRINCIPAL_NAMETYPE,
    ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE,
    ZMQ_BINDTODEVICE,
    ZMQ_LOOPBACK_FASTPATH,
    ZMQ_METADATA,
    ZMQ_MULTICAST_LOOP,
    ZMQ_ROUTER_NOTIFY,
    ZMQ_XPUB_MANUAL_LAST_VALUE,
    ZMQ_SOCKS_USERNAME,
    ZMQ_SOCKS_PASSWORD,
    ZMQ_IN_BATCH_SIZE,
    ZMQ_OUT_BATCH_SIZE,
    ZMQ_ONLY_FIRST_SUBSCRIBE,
    ZMQ_HELLO_MSG,
    ZMQ_DISCONNECT_MSG,
    ZMQ_PRIORITY,
    ZMQ_BUSY_POLL,
    ZMQ_HICCUP_MSG,
    ZMQ_XSUB_VERBOSE_UNSUBSCRIBE,
    ZMQ_TOPICS_COUNT,
    ZMQ_NORM_MODE,
    ZMQ_NORM_UNICAST_NACK,
    ZMQ_NORM_BUFFER_SIZE,
    ZMQ_NORM_SEGMENT_SIZE,
    ZMQ_NORM_BLOCK_SIZE,
    ZMQ_NORM_NUM_PARITY,
    ZMQ_NORM_NUM_AUTOPARITY,
    ZMQ_NORM_PUSH,
];

/// Set a socket option from caller storage; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_setsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> c_int {
    if sock.is_null() {
        return crate::error::fail(crate::error::ENOTSOCK);
    }

    // SAFETY: C callers pass null or a pointer from this API.
    let sock_arc = match unsafe { crate::handle::socket(sock) } {
        Ok(socket) => socket,
        Err(errno) => return crate::error::fail(errno),
    };

    #[cfg(feature = "quic")]
    if let Some(rc) = quic::set(sock_arc, option, optval, optvallen) {
        return rc;
    }

    for setter in SET_OPTIONS {
        if let Some(rc) = setter(sock_arc, option, optval, optvallen) {
            return rc;
        }
    }

    if IGNORED_SET_OPTIONS.contains(&option) {
        0
    } else {
        crate::error::fail(libc::EINVAL)
    }
}

/// Timeouts, high-water marks, and linger.
fn set_queue_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_SNDTIMEO => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            sock_arc
                .sndtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_RCVTIMEO => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            sock_arc
                .rcvtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_SNDHWM => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            if v <= 0 {
                return Some(fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).send_hwm = Some(v as u32);
        }
        ZMQ_RCVHWM => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            if v <= 0 {
                return Some(fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).recv_hwm = Some(v as u32);
        }
        ZMQ_LINGER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).linger = if v < 0 {
                LingerSetting::Forever
            } else {
                LingerSetting::Finite(Duration::from_millis(v as u64))
            };
        }
        _ => return None,
    }

    Some(0)
}

/// Message size limits and queue behavior.
fn set_message_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_MAXMSGSIZE => {
            let Some(v) = read_i64(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).max_message_size = if v < 0 { None } else { Some(v as usize) };
        }
        OMQ_ARENA_THRESHOLD => {
            let Some(v) = read_i64(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            if v < -1 {
                return Some(fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).arena_threshold = if v < 0 {
                None
            } else {
                let Ok(v) = usize::try_from(v) else {
                    return Some(fail(libc::EINVAL));
                };
                Some(v)
            };
        }
        ZMQ_CONFLATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).conflate = v != 0;
        }
        OMQ_ON_MUTE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            let on_mute = match v {
                OMQ_ON_MUTE_BLOCK => OnMute::Block,
                OMQ_ON_MUTE_DROP_NEWEST => OnMute::DropNewest,
                OMQ_ON_MUTE_DROP_OLDEST => OnMute::DropOldest,
                _ => return Some(fail(libc::EINVAL)),
            };
            lock_overlay!(sock_arc).on_mute = on_mute;
        }
        _ => return None,
    }

    Some(0)
}

/// OMQ compression and workload tuning.
fn set_tuning_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        OMQ_COMPRESSION_LEVEL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            if !(ZSTD_LEVEL_MIN..=ZSTD_LEVEL_MAX).contains(&v) {
                return Some(fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).compression_level = Some(v);
        }
        OMQ_COMPRESSION_DICT => {
            let Some(v) = read_bytes(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            if v.len() > COMPRESSION_DICT_MAX {
                return Some(fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).compression_dict = if v.is_empty() {
                None
            } else {
                Some(Bytes::from(v))
            };
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).compression_auto_train = v != 0;
        }
        OMQ_WORKLOAD_PROFILE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).workload_profile = match v {
                OMQ_WORKLOAD_DEFAULT => None,
                OMQ_WORKLOAD_THROUGHPUT => Some(WorkloadProfile::Throughput),
                OMQ_WORKLOAD_LATENCY => Some(WorkloadProfile::Latency),
                _ => return Some(fail(libc::EINVAL)),
            };
        }
        _ => return None,
    }

    Some(0)
}

/// Connect, handshake, and reconnect timing.
fn set_reconnect_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_RECONNECT_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).reconnect_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).reconnect_ivl_max = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_STOP => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).reconnect_stop = v;
        }
        ZMQ_HANDSHAKE_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).handshake_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_CONNECT_TIMEOUT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).connect_timeout = v;
        }
        _ => return None,
    }

    Some(0)
}

/// ZMTP heartbeat timing.
fn set_heartbeat_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_HEARTBEAT_IVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).heartbeat_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TTL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).heartbeat_ttl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).heartbeat_timeout = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        _ => return None,
    }

    Some(0)
}

/// TCP keepalive and retransmit timeout.
fn set_tcp_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_TCP_KEEPALIVE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).tcp_keepalive = v;
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).tcp_keepalive_cnt = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).tcp_keepalive_idle = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).tcp_keepalive_intvl = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_MAXRT => {
            let Some(v) = read_i32(optval, optvallen).filter(|&v| v >= 0) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).tcp_maxrt = v;
        }
        _ => return None,
    }

    Some(0)
}

/// Kernel buffers, address family, and listener behavior.
fn set_network_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_SNDBUF => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).sndbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_RCVBUF => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).rcvbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_IPV6 => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).ipv6 = v != 0;
        }
        ZMQ_IPV4ONLY => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).ipv6 = v == 0;
        }
        ZMQ_BACKLOG => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).backlog = v;
        }
        ZMQ_IMMEDIATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).immediate = v != 0;
        }
        _ => return None,
    }

    Some(0)
}

/// Identity and request/reply routing behavior.
fn set_routing_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_IDENTITY => {
            if optval.is_null() {
                return Some(crate::error::fail(libc::EFAULT));
            }
            if optvallen > 255 {
                return Some(crate::error::fail(libc::EINVAL));
            }
            // SAFETY: optval is non-null (checked above); optvallen bytes are readable.
            let bytes = unsafe { std::slice::from_raw_parts(optval.cast::<u8>(), optvallen) };
            if bytes.first() == Some(&0) {
                return Some(crate::error::fail(libc::EINVAL));
            }
            lock_overlay!(sock_arc).identity = Bytes::copy_from_slice(bytes);
        }
        ZMQ_ROUTER_MANDATORY => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).router_mandatory = v != 0;
        }
        ZMQ_PROBE_ROUTER => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).probe_router = v != 0;
        }
        ZMQ_REQ_CORRELATE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).req_correlate = v != 0;
        }
        ZMQ_REQ_RELAXED => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).req_relaxed = v != 0;
        }
        _ => return None,
    }

    Some(0)
}

/// Subscriptions and XPUB behavior.
fn set_pubsub_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    match option {
        ZMQ_SUBSCRIBE => {
            return Some(do_subscribe(sock_arc, optval, optvallen, true));
        }
        ZMQ_UNSUBSCRIBE => {
            return Some(do_subscribe(sock_arc, optval, optvallen, false));
        }
        ZMQ_XPUB_VERBOSE => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).xpub_verbose = v != 0;
        }
        ZMQ_XPUB_NODROP => {
            let Some(v) = read_i32(optval, optvallen) else {
                return Some(fail(libc::EINVAL));
            };
            lock_overlay!(sock_arc).xpub_nodrop = v != 0;
        }
        _ => return None,
    }

    Some(0)
}

fn do_subscribe(
    sock_arc: &std::sync::Arc<crate::socket::OmqSocket>,
    optval: *const libc::c_void,
    optvallen: usize,
    subscribe: bool,
) -> c_int {
    if optval.is_null() && optvallen > 0 {
        return crate::error::fail(libc::EFAULT);
    }
    let prefix = if optval.is_null() {
        Bytes::new()
    } else {
        // SAFETY: optval is non-null (checked above) with optvallen readable bytes.
        Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(optval.cast::<u8>(), optvallen)
        })
    };
    if let Err(error) = crate::socket::ensure_materialized(sock_arc) {
        return crate::error::fail(error);
    }
    let Some(inner) = sock_arc.inner.get() else {
        return crate::error::fail(crate::error::ETERM);
    };
    let result = if subscribe {
        crate::socket::with_socket(&sock_arc.ctx, inner, move |s| async move {
            s.subscribe(prefix).await
        })
    } else {
        crate::socket::with_socket(&sock_arc.ctx, inner, move |s| async move {
            s.unsubscribe(prefix).await
        })
    };
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(ref e)) => crate::error::fail(crate::error::map_omq_err(e)),
        Err(errno) => crate::error::fail(errno),
    }
}
