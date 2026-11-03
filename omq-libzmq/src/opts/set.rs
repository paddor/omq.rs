//! `zmq_setsockopt` and its per-category handlers.

use super::LingerSetting;
#[allow(
    clippy::wildcard_imports,
    reason = "option code table; the lint skips test builds"
)]
use super::codes::*;
use super::overlay;
#[cfg(feature = "quic")]
use super::quic;
use super::raw::OptIn;
use super::set_security::{
    set_curve_client_option, set_curve_option, set_plain_client_option, set_plain_server_option,
    set_ws_option, set_wss_option,
};
use crate::error::c_try;
use crate::socket::OmqSocket;
use bytes::Bytes;
use omq_tokio::options::{OnMute, WorkloadProfile};
use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

/// One category of `zmq_setsockopt` options; `Ok(false)` when `option` is not in it.
type SetOption = fn(&Arc<OmqSocket>, c_int, OptIn) -> Result<bool, c_int>;

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

/// Set a socket option from caller storage; return zero or -1 with errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_setsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> c_int {
    // SAFETY: C callers pass null or a pointer from this API.
    let sock_arc = c_try!(unsafe { crate::handle::socket(sock) });
    c_try!(set_option(sock_arc, option, OptIn::new(optval, optvallen)));
    0
}

fn set_option(sock_arc: &Arc<OmqSocket>, option: c_int, optval: OptIn) -> Result<(), c_int> {
    #[cfg(feature = "quic")]
    if quic::set(sock_arc, option, optval)? {
        return Ok(());
    }

    for setter in SET_OPTIONS {
        if setter(sock_arc, option, optval)? {
            return Ok(());
        }
    }

    if is_ignored_on_set(option) {
        Ok(())
    } else {
        Err(libc::EINVAL)
    }
}

/// Timeouts, high-water marks, and linger.
fn set_queue_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_SNDTIMEO => {
            let v = optval.i32()?;
            sock_arc
                .sndtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_RCVTIMEO => {
            let v = optval.i32()?;
            sock_arc
                .rcvtimeo_ms
                .store(i64::from(v), std::sync::atomic::Ordering::Relaxed);
        }
        ZMQ_SNDHWM => {
            let v = optval.i32()?;
            if v <= 0 {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.send_hwm = Some(v as u32);
        }
        ZMQ_RCVHWM => {
            let v = optval.i32()?;
            if v <= 0 {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.recv_hwm = Some(v as u32);
        }
        ZMQ_LINGER => {
            let v = optval.i32()?;
            overlay(sock_arc)?.linger = if v < 0 {
                LingerSetting::Forever
            } else {
                LingerSetting::Finite(Duration::from_millis(v as u64))
            };
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// Message size limits and queue behavior.
fn set_message_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_MAXMSGSIZE => {
            let v = optval.i64()?;
            overlay(sock_arc)?.max_message_size = if v < 0 { None } else { Some(v as usize) };
        }
        OMQ_ARENA_THRESHOLD => {
            let v = optval.i64()?;
            if v < -1 {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.arena_threshold = if v < 0 {
                None
            } else {
                let Ok(v) = usize::try_from(v) else {
                    return Err(libc::EINVAL);
                };
                Some(v)
            };
        }
        ZMQ_CONFLATE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.conflate = v != 0;
        }
        OMQ_ON_MUTE => {
            let v = optval.i32()?;
            let on_mute = match v {
                OMQ_ON_MUTE_BLOCK => OnMute::Block,
                OMQ_ON_MUTE_DROP_NEWEST => OnMute::DropNewest,
                OMQ_ON_MUTE_DROP_OLDEST => OnMute::DropOldest,
                _ => return Err(libc::EINVAL),
            };
            overlay(sock_arc)?.on_mute = on_mute;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// OMQ compression and workload tuning.
fn set_tuning_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        OMQ_COMPRESSION_LEVEL => {
            let v = optval.i32()?;
            if !(ZSTD_LEVEL_MIN..=ZSTD_LEVEL_MAX).contains(&v) {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.compression_level = Some(v);
        }
        OMQ_COMPRESSION_DICT => {
            let v = optval.bytes()?;
            if v.len() > COMPRESSION_DICT_MAX {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.compression_dict = if v.is_empty() {
                None
            } else {
                Some(Bytes::from(v))
            };
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let v = optval.i32()?;
            overlay(sock_arc)?.compression_auto_train = v != 0;
        }
        OMQ_WORKLOAD_PROFILE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.workload_profile = match v {
                OMQ_WORKLOAD_DEFAULT => None,
                OMQ_WORKLOAD_THROUGHPUT => Some(WorkloadProfile::Throughput),
                OMQ_WORKLOAD_LATENCY => Some(WorkloadProfile::Latency),
                _ => return Err(libc::EINVAL),
            };
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// Connect, handshake, and reconnect timing.
fn set_reconnect_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_RECONNECT_IVL => {
            let v = optval.i32()?;
            overlay(sock_arc)?.reconnect_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = optval.i32()?;
            overlay(sock_arc)?.reconnect_ivl_max = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_RECONNECT_STOP => {
            let v = optval.i32()?;
            overlay(sock_arc)?.reconnect_stop = v;
        }
        ZMQ_HANDSHAKE_IVL => {
            let v = optval.i32()?;
            overlay(sock_arc)?.handshake_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_CONNECT_TIMEOUT => {
            let v = optval.i32()?;
            overlay(sock_arc)?.connect_timeout = v;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// ZMTP heartbeat timing.
fn set_heartbeat_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_HEARTBEAT_IVL => {
            let v = optval.i32()?;
            overlay(sock_arc)?.heartbeat_ivl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = optval.i32()?;
            overlay(sock_arc)?.heartbeat_ttl = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = optval.i32()?;
            overlay(sock_arc)?.heartbeat_timeout = if v <= 0 {
                None
            } else {
                Some(Duration::from_millis(v as u64))
            };
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// TCP keepalive and retransmit timeout.
fn set_tcp_option(sock_arc: &Arc<OmqSocket>, option: c_int, optval: OptIn) -> Result<bool, c_int> {
    match option {
        ZMQ_TCP_KEEPALIVE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.tcp_keepalive = v;
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = optval.i32()?;
            overlay(sock_arc)?.tcp_keepalive_cnt = if v <= 0 { None } else { Some(v as u32) };
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.tcp_keepalive_idle = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = optval.i32()?;
            overlay(sock_arc)?.tcp_keepalive_intvl = if v <= 0 {
                None
            } else {
                Some(Duration::from_secs(v as u64))
            };
        }
        ZMQ_TCP_MAXRT => {
            let v = optval.i32()?;
            if v < 0 {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.tcp_maxrt = v;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// Kernel buffers, address family, and listener behavior.
fn set_network_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_SNDBUF => {
            let v = optval.i32()?;
            overlay(sock_arc)?.sndbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_RCVBUF => {
            let v = optval.i32()?;
            overlay(sock_arc)?.rcvbuf = if v <= 0 { None } else { Some(v as usize) };
        }
        ZMQ_IPV6 => {
            let v = optval.i32()?;
            overlay(sock_arc)?.ipv6 = v != 0;
        }
        ZMQ_IPV4ONLY => {
            let v = optval.i32()?;
            overlay(sock_arc)?.ipv6 = v == 0;
        }
        ZMQ_BACKLOG => {
            let v = optval.i32()?;
            overlay(sock_arc)?.backlog = v;
        }
        ZMQ_IMMEDIATE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.immediate = v != 0;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// Identity and request/reply routing behavior.
fn set_routing_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_IDENTITY => {
            let Some(bytes) = optval.slice() else {
                return Err(libc::EFAULT);
            };
            if bytes.len() > 255 {
                return Err(libc::EINVAL);
            }
            if bytes.first() == Some(&0) {
                return Err(libc::EINVAL);
            }
            overlay(sock_arc)?.identity = Bytes::copy_from_slice(bytes);
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = optval.i32()?;
            overlay(sock_arc)?.router_mandatory = v != 0;
        }
        ZMQ_PROBE_ROUTER => {
            let v = optval.i32()?;
            overlay(sock_arc)?.probe_router = v != 0;
        }
        ZMQ_REQ_CORRELATE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.req_correlate = v != 0;
        }
        ZMQ_REQ_RELAXED => {
            let v = optval.i32()?;
            overlay(sock_arc)?.req_relaxed = v != 0;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

/// Subscriptions and XPUB behavior.
fn set_pubsub_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    match option {
        ZMQ_SUBSCRIBE => do_subscribe(sock_arc, optval, true)?,
        ZMQ_UNSUBSCRIBE => do_subscribe(sock_arc, optval, false)?,
        ZMQ_XPUB_VERBOSE => {
            let v = optval.i32()?;
            overlay(sock_arc)?.xpub_verbose = v != 0;
        }
        ZMQ_XPUB_NODROP => {
            let v = optval.i32()?;
            overlay(sock_arc)?.xpub_nodrop = v != 0;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn do_subscribe(sock_arc: &Arc<OmqSocket>, optval: OptIn, subscribe: bool) -> Result<(), c_int> {
    let prefix = match optval.slice() {
        Some(bytes) => Bytes::copy_from_slice(bytes),
        None if optval.len == 0 => Bytes::new(),
        None => return Err(libc::EFAULT),
    };
    let inner = crate::socket::materialized(sock_arc)?;
    if subscribe {
        crate::socket::run_on_socket(sock_arc, inner, move |s| async move {
            s.subscribe(prefix).await
        })
    } else {
        crate::socket::run_on_socket(sock_arc, inner, move |s| async move {
            s.unsubscribe(prefix).await
        })
    }
}
