//! `zmq_getsockopt` and its per-category handlers.

use super::MechanismOverlay;
#[allow(
    clippy::wildcard_imports,
    reason = "option code table; the lint skips test builds"
)]
use super::codes::*;
#[cfg(feature = "quic")]
use super::quic;
use super::raw::{write_bytes, write_i32, write_i64, write_key, write_string};
use crate::error::fail;
#[cfg(unix)]
use crate::notify::NotifyHandle;
use crate::socket::OmqSocket;
use omq_tokio::SocketType;
use omq_tokio::options::{OnMute, WorkloadProfile};
use std::ffi::c_int;
use std::sync::Arc;

/// One category of `zmq_getsockopt` options; `None` when `option` is not in it.
type GetOption = fn(&Arc<OmqSocket>, c_int, *mut libc::c_void, *mut usize) -> Option<c_int>;

const GET_OPTIONS: [GetOption; 16] = [
    get_queue_option,
    get_state_option,
    get_message_option,
    get_tuning_option,
    get_reconnect_option,
    get_heartbeat_option,
    get_tcp_option,
    get_network_option,
    get_routing_option,
    get_pubsub_option,
    get_plain_option,
    get_curve_option,
    get_ws_option,
    get_wss_option,
    get_fixed_option,
    get_unsupported_option,
];

/// Write a socket option into caller storage and update its length; return zero or -1 with
/// errno.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_getsockopt(
    sock: *mut libc::c_void,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
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
    if let Some(rc) = quic::get(sock_arc, option, optval, optvallen) {
        return rc;
    }

    for getter in GET_OPTIONS {
        if let Some(rc) = getter(sock_arc, option, optval, optvallen) {
            return rc;
        }
    }

    crate::error::fail(libc::EINVAL)
}

/// Timeouts, high-water marks, and linger.
fn get_queue_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_SNDTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .sndtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_RCVTIMEO => write_i32(
            optval,
            optvallen,
            sock_arc
                .rcvtimeo_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_SNDHWM => {
            let v = lock_overlay!(sock_arc).send_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVHWM => {
            let v = lock_overlay!(sock_arc).recv_hwm.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_LINGER => {
            let v = lock_overlay!(sock_arc).linger_ms();
            write_i32(optval, optvallen, v)
        }
        _ => return None,
    };

    Some(rc)
}

/// Read-only socket state.
fn get_state_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_RCVMORE => write_i32(optval, optvallen, i32::from(rcvmore(sock_arc))),
        ZMQ_TYPE => {
            let Some(v) = zmq_type_code(sock_arc.socket_type) else {
                return Some(crate::error::fail(libc::EINVAL));
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_FD => {
            #[cfg(windows)]
            return Some(crate::error::fail(libc::ENOPROTOOPT));
            #[cfg(unix)]
            {
                let fd = sock_arc.notify.recv_fd();
                write_i32(optval, optvallen, fd)
            }
        }
        ZMQ_EVENTS => write_i32(optval, optvallen, poll_events(sock_arc)),
        ZMQ_LAST_ENDPOINT => {
            let Ok(ep) = sock_arc.last_endpoint.lock() else {
                return Some(crate::error::fail(crate::error::ETERM));
            };
            let s = ep.as_deref().unwrap_or("");
            write_string(optval, optvallen, s.as_bytes())
        }
        _ => return None,
    };

    Some(rc)
}

/// `ZMQ_RCVMORE` for whichever receive path this socket uses: ZAP handler,
/// authenticated receive, or the plain receive drain.
fn rcvmore(sock_arc: &OmqSocket) -> bool {
    if sock_arc
        .zap_handler
        .load(std::sync::atomic::Ordering::Acquire)
    {
        sock_arc.ctx.zap.has_more(sock_arc.id)
    } else if crate::send_recv::authenticated_recv_configured(sock_arc) {
        crate::send_recv::authenticated_recv_has_more(sock_arc)
    } else {
        sock_arc
            .drain_nonempty
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// libzmq's `ZMQ_TYPE` numbering; `None` for omq-only socket types.
fn zmq_type_code(socket_type: SocketType) -> Option<i32> {
    let code = match socket_type {
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
        SocketType::Server => 12,
        SocketType::Client => 13,
        SocketType::Radio => 14,
        SocketType::Dish => 15,
        SocketType::Gather => 16,
        SocketType::Scatter => 17,
        SocketType::Peer => 19,
        SocketType::Channel => 20,
        SocketType::Stream => 11,
        _ => return None,
    };

    Some(code)
}

/// `ZMQ_EVENTS` bits: POLLOUT when a send would not block, POLLIN when any
/// receive source has data.
fn poll_events(sock_arc: &OmqSocket) -> i32 {
    let mut events = if crate::send_recv::send_ready(sock_arc) {
        ZMQ_POLLOUT
    } else {
        0
    };

    let drain_nonempty = if sock_arc
        .zap_handler
        .load(std::sync::atomic::Ordering::Acquire)
    {
        sock_arc.ctx.zap.has_input(sock_arc.id)
    } else {
        sock_arc
            .drain_nonempty
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let recv_cons_has_data = sock_arc.recv_has_data();
    let authenticated_recv_has_data = crate::send_recv::authenticated_recv_has_data(sock_arc);

    let has_data = drain_nonempty || recv_cons_has_data || authenticated_recv_has_data;
    if has_data {
        events |= ZMQ_POLLIN;
    }
    events
}

/// Message size limits and queue behavior.
fn get_message_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_MAXMSGSIZE => {
            let v = lock_overlay!(sock_arc)
                .max_message_size
                .map_or(-1i64, |n| n as i64);
            write_i64(optval, optvallen, v)
        }
        OMQ_ARENA_THRESHOLD => {
            let v = lock_overlay!(sock_arc)
                .arena_threshold
                .map_or(omq_tokio::options::DEFAULT_ARENA_THRESHOLD as i64, |n| {
                    n as i64
                });
            write_i64(optval, optvallen, v)
        }
        ZMQ_CONFLATE => {
            let v = lock_overlay!(sock_arc).conflate;
            write_i32(optval, optvallen, i32::from(v))
        }
        #[expect(clippy::match_same_arms)]
        OMQ_ON_MUTE => {
            let v = match lock_overlay!(sock_arc).on_mute {
                OnMute::Block => OMQ_ON_MUTE_BLOCK,
                OnMute::DropNewest => OMQ_ON_MUTE_DROP_NEWEST,
                OnMute::DropOldest => OMQ_ON_MUTE_DROP_OLDEST,
                _ => OMQ_ON_MUTE_BLOCK,
            };
            write_i32(optval, optvallen, v)
        }
        _ => return None,
    };

    Some(rc)
}

/// OMQ compression and workload tuning.
fn get_tuning_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        OMQ_COMPRESSION_LEVEL => {
            let v = lock_overlay!(sock_arc).compression_level.unwrap_or(0);
            write_i32(optval, optvallen, v)
        }
        OMQ_COMPRESSION_DICT => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(
                optval,
                optvallen,
                ov.compression_dict.as_deref().unwrap_or(b""),
            )
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let v = lock_overlay!(sock_arc).compression_auto_train;
            write_i32(optval, optvallen, i32::from(v))
        }
        OMQ_WORKLOAD_PROFILE => {
            let v = match lock_overlay!(sock_arc).workload_profile {
                None => OMQ_WORKLOAD_DEFAULT,
                Some(WorkloadProfile::Throughput) => OMQ_WORKLOAD_THROUGHPUT,
                Some(WorkloadProfile::Latency) => OMQ_WORKLOAD_LATENCY,
            };
            write_i32(optval, optvallen, v)
        }
        _ => return None,
    };

    Some(rc)
}

/// Connect, handshake, and reconnect timing.
fn get_reconnect_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_RECONNECT_IVL => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl
                .map_or(-1, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = lock_overlay!(sock_arc)
                .reconnect_ivl_max
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RECONNECT_STOP => write_i32(optval, optvallen, lock_overlay!(sock_arc).reconnect_stop),
        ZMQ_HANDSHAKE_IVL => {
            let v = lock_overlay!(sock_arc)
                .handshake_ivl
                .map_or(DEFAULT_HANDSHAKE_IVL_MS, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_CONNECT_TIMEOUT => {
            write_i32(optval, optvallen, lock_overlay!(sock_arc).connect_timeout)
        }
        _ => return None,
    };

    Some(rc)
}

/// ZMTP heartbeat timing.
fn get_heartbeat_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_HEARTBEAT_IVL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ivl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_ttl
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = lock_overlay!(sock_arc)
                .heartbeat_timeout
                .map_or(0, |d| d.as_millis() as i32);
            write_i32(optval, optvallen, v)
        }
        _ => return None,
    };

    Some(rc)
}

/// TCP keepalive and retransmit timeout.
fn get_tcp_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_TCP_KEEPALIVE => {
            let v = lock_overlay!(sock_arc).tcp_keepalive;
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_cnt
                .map_or(-1, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_idle
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = lock_overlay!(sock_arc)
                .tcp_keepalive_intvl
                .map_or(-1, |d| d.as_secs() as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_TCP_MAXRT => {
            let v = lock_overlay!(sock_arc).tcp_maxrt;
            write_i32(optval, optvallen, v)
        }
        _ => return None,
    };

    Some(rc)
}

/// Kernel buffers, address family, and listener behavior.
fn get_network_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_SNDBUF => {
            let v = lock_overlay!(sock_arc).sndbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_RCVBUF => {
            let v = lock_overlay!(sock_arc).rcvbuf.map_or(0, |n| n as i32);
            write_i32(optval, optvallen, v)
        }
        ZMQ_IPV6 => {
            let v = lock_overlay!(sock_arc).ipv6;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_IPV4ONLY => write_i32(optval, optvallen, i32::from(!lock_overlay!(sock_arc).ipv6)),
        ZMQ_BACKLOG => write_i32(optval, optvallen, lock_overlay!(sock_arc).backlog),
        ZMQ_IMMEDIATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).immediate),
        ),
        _ => return None,
    };

    Some(rc)
}

/// Identity and request/reply routing behavior.
fn get_routing_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_IDENTITY => {
            if sock_arc.socket_type == SocketType::Stream
                && let Some(inner) = sock_arc.inner.get()
            {
                match crate::socket::with_socket(&sock_arc.ctx, inner, |socket| async move {
                    socket.last_stream_routing_id().await
                }) {
                    Ok(Ok(Some(identity))) => {
                        return Some(write_bytes(optval, optvallen, &identity));
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(error)) => return Some(fail(crate::error::map_omq_err(&error))),
                    Err(errno) => return Some(fail(errno)),
                }
            }
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, &ov.identity)
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = lock_overlay!(sock_arc).router_mandatory;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_PROBE_ROUTER => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).probe_router),
        ),
        ZMQ_REQ_CORRELATE => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_correlate),
        ),
        ZMQ_REQ_RELAXED => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).req_relaxed),
        ),
        _ => return None,
    };

    Some(rc)
}

/// XPUB behavior.
fn get_pubsub_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_XPUB_VERBOSE => {
            let v = lock_overlay!(sock_arc).xpub_verbose;
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_XPUB_NODROP => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).xpub_nodrop),
        ),
        _ => return None,
    };

    Some(rc)
}

/// Security mechanism, PLAIN role and credentials, and ZAP domain.
fn get_plain_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_MECHANISM => {
            let v = match lock_overlay!(sock_arc).mechanism {
                MechanismOverlay::Null => ZMQ_NULL,
                MechanismOverlay::PlainServer
                | MechanismOverlay::PlainServerCredentials { .. }
                | MechanismOverlay::PlainClient { .. } => ZMQ_PLAIN,
                MechanismOverlay::CurveServer { .. } | MechanismOverlay::CurveClient { .. } => {
                    ZMQ_CURVE
                }
            };
            write_i32(optval, optvallen, v)
        }
        ZMQ_PLAIN_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::PlainServer | MechanismOverlay::PlainServerCredentials { .. }
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_PLAIN_USERNAME => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref username, .. } = ov.mechanism {
                write_string(optval, optvallen, username.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::PlainClient { ref password, .. } = ov.mechanism {
                write_string(optval, optvallen, password.as_bytes())
            } else {
                write_string(optval, optvallen, b"")
            }
        }
        ZMQ_ZAP_DOMAIN => {
            let overlay = lock_overlay!(sock_arc);
            write_string(optval, optvallen, overlay.zap_domain.as_bytes())
        }
        ZMQ_ZAP_ENFORCE_DOMAIN => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).zap_enforce_domain),
        ),
        _ => return None,
    };

    Some(rc)
}

/// CURVE role and keys.
fn get_curve_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_CURVE_SERVER => {
            let v = matches!(
                lock_overlay!(sock_arc).mechanism,
                MechanismOverlay::CurveServer { .. }
            );
            write_i32(optval, optvallen, i32::from(v))
        }
        ZMQ_CURVE_PUBLICKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref public_key, .. } = ov.mechanism {
                write_key(optval, optvallen, public_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let ov = lock_overlay!(sock_arc);
            let key = match ov.mechanism {
                MechanismOverlay::CurveServer { ref secret_key, .. }
                | MechanismOverlay::CurveClient { ref secret_key, .. } => secret_key,
                _ => &[0; 32],
            };
            write_key(optval, optvallen, key)
        }
        ZMQ_CURVE_SERVERKEY => {
            let ov = lock_overlay!(sock_arc);
            if let MechanismOverlay::CurveClient { ref server_key, .. } = ov.mechanism {
                write_key(optval, optvallen, server_key)
            } else {
                write_key(optval, optvallen, &[0; 32])
            }
        }
        _ => return None,
    };

    Some(rc)
}

/// WebSocket peer limit and origin policy.
fn get_ws_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        OMQ_WS_MAX_READY_PEERS => write_i32(
            optval,
            optvallen,
            lock_overlay!(sock_arc).ws_max_ready_peers,
        ),
        OMQ_WS_ALLOWED_ORIGINS => write_bytes(
            optval,
            optvallen,
            lock_overlay!(sock_arc)
                .ws_allowed_origins
                .join("\n")
                .as_bytes(),
        ),
        _ => return None,
    };

    Some(rc)
}

/// WebSocket TLS material.
fn get_wss_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_WSS_KEY_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(
                optval,
                optvallen,
                ov.wss_key_pem
                    .as_ref()
                    .map_or(&[][..], |key| key.0.as_slice()),
            )
        }
        ZMQ_WSS_CERT_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(optval, optvallen, ov.wss_cert_pem.as_deref().unwrap_or(b""))
        }
        ZMQ_WSS_TRUST_PEM => {
            let ov = lock_overlay!(sock_arc);
            write_bytes(
                optval,
                optvallen,
                ov.wss_trust_pem.as_deref().unwrap_or(b""),
            )
        }
        ZMQ_WSS_HOSTNAME => {
            let ov = lock_overlay!(sock_arc);
            write_string(
                optval,
                optvallen,
                ov.wss_hostname.as_deref().unwrap_or("").as_bytes(),
            )
        }
        ZMQ_WSS_TRUST_SYSTEM => write_i32(
            optval,
            optvallen,
            i32::from(lock_overlay!(sock_arc).wss_trust_system),
        ),
        _ => return None,
    };

    Some(rc)
}

/// Options with fixed values: always-on behavior or unsupported features.
fn get_fixed_option(
    _sock: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_ROUTER_HANDOVER | ZMQ_BLOCKY | ZMQ_STREAM_NOTIFY => write_i32(optval, optvallen, 1),
        ZMQ_MULTICAST_MAXTPDU => write_i32(optval, optvallen, 1500),
        ZMQ_USE_FD => write_i32(optval, optvallen, -1),
        ZMQ_AFFINITY
        | ZMQ_VMCI_BUFFER_SIZE
        | ZMQ_VMCI_BUFFER_MIN_SIZE
        | ZMQ_VMCI_BUFFER_MAX_SIZE => write_i64(optval, optvallen, 0),
        ZMQ_TCP_ACCEPT_FILTER
        | ZMQ_SOCKS_PROXY
        | ZMQ_CONNECT_ROUTING_ID
        | ZMQ_GSSAPI_PRINCIPAL
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL
        | ZMQ_BINDTODEVICE
        | ZMQ_METADATA
        | ZMQ_SOCKS_USERNAME
        | ZMQ_SOCKS_PASSWORD => write_string(optval, optvallen, b""),
        ZMQ_XPUB_WELCOME_MSG | ZMQ_HELLO_MSG | ZMQ_DISCONNECT_MSG | ZMQ_HICCUP_MSG => {
            write_bytes(optval, optvallen, b"")
        }
        _ => return None,
    };

    Some(rc)
}

/// Unsupported integer options read as zero.
fn get_unsupported_option(
    _sock: &Arc<OmqSocket>,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    let rc = match option {
        ZMQ_RATE
        | ZMQ_RECOVERY_IVL
        | ZMQ_MULTICAST_HOPS
        | ZMQ_TOS
        | ZMQ_IPC_FILTER_PID
        | ZMQ_IPC_FILTER_UID
        | ZMQ_IPC_FILTER_GID
        | ZMQ_ROUTER_RAW
        | ZMQ_GSSAPI_SERVER
        | ZMQ_GSSAPI_PLAINTEXT
        | ZMQ_XPUB_MANUAL
        | ZMQ_INVERT_MATCHING
        | ZMQ_XPUB_VERBOSER
        | ZMQ_THREAD_SAFE
        | ZMQ_VMCI_CONNECT_TIMEOUT
        | ZMQ_GSSAPI_PRINCIPAL_NAMETYPE
        | ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE
        | ZMQ_LOOPBACK_FASTPATH
        | ZMQ_MULTICAST_LOOP
        | ZMQ_ROUTER_NOTIFY
        | ZMQ_XPUB_MANUAL_LAST_VALUE
        | ZMQ_IN_BATCH_SIZE
        | ZMQ_OUT_BATCH_SIZE
        | ZMQ_ONLY_FIRST_SUBSCRIBE
        | ZMQ_PRIORITY
        | ZMQ_BUSY_POLL
        | ZMQ_XSUB_VERBOSE_UNSUBSCRIBE
        | ZMQ_TOPICS_COUNT
        | ZMQ_NORM_MODE
        | ZMQ_NORM_UNICAST_NACK
        | ZMQ_NORM_BUFFER_SIZE
        | ZMQ_NORM_SEGMENT_SIZE
        | ZMQ_NORM_BLOCK_SIZE
        | ZMQ_NORM_NUM_PARITY
        | ZMQ_NORM_NUM_AUTOPARITY
        | ZMQ_NORM_PUSH => write_i32(optval, optvallen, 0),
        _ => return None,
    };

    Some(rc)
}
