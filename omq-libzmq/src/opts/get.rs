//! `zmq_getsockopt` and its per-category handlers.

use super::MechanismOverlay;
#[allow(
    clippy::wildcard_imports,
    reason = "option code table; the lint skips test builds"
)]
use super::codes::*;
use super::overlay;
#[cfg(feature = "quic")]
use super::quic;
use super::raw::OptOut;
use crate::error::c_try;
#[cfg(unix)]
use crate::notify::NotifyHandle;
use crate::socket::{DEFAULT_HWM, OmqSocket};
use omq_tokio::SocketType;
use omq_tokio::options::{OnMute, WorkloadProfile};
use std::ffi::c_int;
use std::sync::Arc;

/// One category of `zmq_getsockopt` options; `Ok(false)` when `option` is not in it.
type GetOption = fn(&Arc<OmqSocket>, c_int, OptOut) -> Result<bool, c_int>;

const GET_OPTIONS: [GetOption; 16] = [
    get_queue_option,
    get_state_option,
    get_message_option,
    get_peer_limit_option,
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
    // SAFETY: C callers pass null or a pointer from this API.
    let sock_arc = c_try!(unsafe { crate::handle::socket(sock) });
    c_try!(get_option(sock_arc, option, OptOut::new(optval, optvallen)));
    0
}

fn get_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<(), c_int> {
    #[cfg(feature = "quic")]
    if quic::get(sock_arc, option, out)? {
        return Ok(());
    }

    for getter in GET_OPTIONS {
        if getter(sock_arc, option, out)? {
            return Ok(());
        }
    }

    Err(libc::EINVAL)
}

/// Timeouts, high-water marks, and linger.
fn get_queue_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_SNDTIMEO => out.i32(
            sock_arc
                .send
                .timeout_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        ZMQ_RCVTIMEO => out.i32(
            sock_arc
                .recv
                .timeout_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
        ),
        // An unset HWM reads as the default that applies.
        ZMQ_SNDHWM => {
            let v = overlay(sock_arc)?
                .send_hwm
                .map_or(DEFAULT_HWM as i32, |n| n as i32);
            out.i32(v)
        }
        ZMQ_RCVHWM => {
            let v = overlay(sock_arc)?
                .recv_hwm
                .map_or(DEFAULT_HWM as i32, |n| n as i32);
            out.i32(v)
        }
        ZMQ_LINGER => {
            let v = overlay(sock_arc)?.linger_ms();
            out.i32(v)
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Read-only socket state.
fn get_state_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_RCVMORE => out.i32(i32::from(rcvmore(sock_arc))),
        ZMQ_TYPE => {
            let Some(v) = crate::socket::socket_type_code(sock_arc.socket_type) else {
                return Err(libc::EINVAL);
            };
            out.i32(v)
        }
        ZMQ_FD => {
            #[cfg(windows)]
            return Err(libc::ENOPROTOOPT);
            #[cfg(unix)]
            {
                crate::send_recv::fd_edge::watch(sock_arc);
                let fd = sock_arc.notify.recv_fd();
                out.i32(fd)
            }
        }
        ZMQ_EVENTS => out.i32(poll_events(sock_arc)),
        ZMQ_LAST_ENDPOINT => {
            let Ok(ep) = sock_arc.endpoints.last.lock() else {
                return Err(crate::error::ETERM);
            };
            let s = ep.as_deref().unwrap_or("");
            out.string(s.as_bytes())
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// `ZMQ_RCVMORE` for whichever receive path this socket uses: ZAP handler,
/// authenticated receive, or the plain receive drain.
fn rcvmore(sock_arc: &OmqSocket) -> bool {
    if sock_arc.is_zap_handler() {
        sock_arc.ctx.zap.has_more(sock_arc.id)
    } else if crate::send_recv::authenticated_recv_configured(sock_arc) {
        crate::send_recv::authenticated_recv_has_more(sock_arc)
    } else {
        sock_arc
            .recv
            .drain_nonempty
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// `ZMQ_EVENTS` bits: POLLOUT when a send would not block, POLLIN when any
/// receive source has data. As in libzmq, the read also consumes a pending
/// `ZMQ_FD` wake.
fn poll_events(sock_arc: &OmqSocket) -> i32 {
    let writable = crate::send_recv::send_ready(sock_arc);
    crate::send_recv::fd_edge::observe(sock_arc, writable);
    let mut events = if writable { ZMQ_POLLOUT } else { 0 };
    if crate::poll::recv_ready(sock_arc) {
        events |= ZMQ_POLLIN;
    }
    events
}

/// Message size limits and queue behavior.
fn get_message_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        ZMQ_MAXMSGSIZE => {
            let v = overlay(sock_arc)?
                .max_message_size
                .map_or(-1i64, |n| n as i64);
            out.i64(v)
        }
        OMQ_ARENA_THRESHOLD => {
            let v = overlay(sock_arc)?
                .arena_threshold
                .map_or(omq_tokio::options::DEFAULT_ARENA_THRESHOLD as i64, |n| {
                    n as i64
                });
            out.i64(v)
        }
        ZMQ_CONFLATE => {
            let v = overlay(sock_arc)?.conflate;
            out.i32(i32::from(v))
        }
        #[expect(clippy::match_same_arms)]
        OMQ_ON_MUTE => {
            let v = match overlay(sock_arc)?.on_mute {
                OnMute::Block => OMQ_ON_MUTE_BLOCK,
                OnMute::DropNewest => OMQ_ON_MUTE_DROP_NEWEST,
                OnMute::DropOldest => OMQ_ON_MUTE_DROP_OLDEST,
                _ => OMQ_ON_MUTE_BLOCK,
            };
            out.i32(v)
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Limits on what one peer may send or subscribe to; -1 is no limit.
fn get_peer_limit_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        OMQ_MAX_MESSAGE_PARTS => out.i32(overlay(sock_arc)?.limits.max_message_parts),
        OMQ_MAX_PEER_SUBSCRIPTIONS => out.i32(overlay(sock_arc)?.limits.max_peer_subscriptions),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// OMQ compression and workload tuning.
fn get_tuning_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        OMQ_COMPRESSION_LEVEL => {
            let v = overlay(sock_arc)?.compression.level.unwrap_or(0);
            out.i32(v)
        }
        OMQ_COMPRESSION_DICT => {
            let ov = overlay(sock_arc)?;
            out.bytes(ov.compression.dict.as_deref().unwrap_or(b""))
        }
        OMQ_COMPRESSION_AUTO_TRAIN => {
            let v = overlay(sock_arc)?.compression.auto_train;
            out.i32(i32::from(v))
        }
        OMQ_WORKLOAD_PROFILE => {
            let v = match overlay(sock_arc)?.workload_profile {
                None => OMQ_WORKLOAD_DEFAULT,
                Some(WorkloadProfile::Throughput) => OMQ_WORKLOAD_THROUGHPUT,
                Some(WorkloadProfile::Latency) => OMQ_WORKLOAD_LATENCY,
            };
            out.i32(v)
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Connect, handshake, and reconnect timing.
fn get_reconnect_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        ZMQ_RECONNECT_IVL => {
            let v = overlay(sock_arc)?
                .reconnect
                .ivl
                .map_or(-1, |d| d.as_millis() as i32);
            out.i32(v)
        }
        ZMQ_RECONNECT_IVL_MAX => {
            let v = overlay(sock_arc)?
                .reconnect
                .ivl_max
                .map_or(0, |d| d.as_millis() as i32);
            out.i32(v)
        }
        ZMQ_RECONNECT_STOP => out.i32(overlay(sock_arc)?.reconnect.stop),
        ZMQ_HANDSHAKE_IVL => {
            let v = overlay(sock_arc)?
                .reconnect
                .handshake_ivl
                .map_or(DEFAULT_HANDSHAKE_IVL_MS, |d| d.as_millis() as i32);
            out.i32(v)
        }
        ZMQ_CONNECT_TIMEOUT => out.i32(overlay(sock_arc)?.reconnect.connect_timeout),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// ZMTP heartbeat timing.
fn get_heartbeat_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        ZMQ_HEARTBEAT_IVL => {
            let v = overlay(sock_arc)?
                .heartbeat
                .ivl
                .map_or(0, |d| d.as_millis() as i32);
            out.i32(v)
        }
        ZMQ_HEARTBEAT_TTL => {
            let v = overlay(sock_arc)?
                .heartbeat
                .ttl
                .map_or(0, |d| d.as_millis() as i32);
            out.i32(v)
        }
        ZMQ_HEARTBEAT_TIMEOUT => {
            let v = overlay(sock_arc)?
                .heartbeat
                .timeout
                .map_or(0, |d| d.as_millis() as i32);
            out.i32(v)
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// TCP keepalive and retransmit timeout.
fn get_tcp_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_TCP_KEEPALIVE => {
            let v = overlay(sock_arc)?.tcp.keepalive;
            out.i32(v)
        }
        ZMQ_TCP_KEEPALIVE_CNT => {
            let v = overlay(sock_arc)?.tcp.cnt.map_or(-1, |n| n as i32);
            out.i32(v)
        }
        ZMQ_TCP_KEEPALIVE_IDLE => {
            let v = overlay(sock_arc)?
                .tcp
                .idle
                .map_or(-1, |d| d.as_secs() as i32);
            out.i32(v)
        }
        ZMQ_TCP_KEEPALIVE_INTVL => {
            let v = overlay(sock_arc)?
                .tcp
                .intvl
                .map_or(-1, |d| d.as_secs() as i32);
            out.i32(v)
        }
        ZMQ_TCP_MAXRT => {
            let v = overlay(sock_arc)?.tcp.maxrt;
            out.i32(v)
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Kernel buffers, address family, and listener behavior.
fn get_network_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        ZMQ_SNDBUF => {
            let v = overlay(sock_arc)?.sndbuf.map_or(0, |n| n as i32);
            out.i32(v)
        }
        ZMQ_RCVBUF => {
            let v = overlay(sock_arc)?.rcvbuf.map_or(0, |n| n as i32);
            out.i32(v)
        }
        ZMQ_IPV6 => {
            let v = overlay(sock_arc)?.ipv6;
            out.i32(i32::from(v))
        }
        ZMQ_IPV4ONLY => out.i32(i32::from(!overlay(sock_arc)?.ipv6)),
        ZMQ_BACKLOG => out.i32(overlay(sock_arc)?.backlog),
        ZMQ_IMMEDIATE => out.i32(i32::from(overlay(sock_arc)?.immediate)),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Identity and request/reply routing behavior.
fn get_routing_option(
    sock_arc: &Arc<OmqSocket>,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    match option {
        ZMQ_IDENTITY => {
            if sock_arc.socket_type == SocketType::Stream
                && let Some(inner) = sock_arc.inner.get()
            {
                match crate::socket::run_on_socket(sock_arc, inner, |socket| async move {
                    socket.last_stream_routing_id().await
                }) {
                    Ok(Some(identity)) => {
                        return out.bytes(&identity).map(|()| true);
                    }
                    Ok(None) => {}
                    Err(errno) => return Err(errno),
                }
            }
            let ov = overlay(sock_arc)?;
            out.bytes(&ov.identity)
        }
        ZMQ_ROUTER_MANDATORY => {
            let v = overlay(sock_arc)?.router_mandatory;
            out.i32(i32::from(v))
        }
        ZMQ_PROBE_ROUTER => out.i32(i32::from(overlay(sock_arc)?.probe_router)),
        ZMQ_REQ_CORRELATE => out.i32(i32::from(overlay(sock_arc)?.req_correlate)),
        ZMQ_REQ_RELAXED => out.i32(i32::from(overlay(sock_arc)?.req_relaxed)),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// XPUB behavior.
fn get_pubsub_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_XPUB_VERBOSE => {
            let v = overlay(sock_arc)?.xpub_verbose;
            out.i32(i32::from(v))
        }
        ZMQ_XPUB_NODROP => out.i32(i32::from(overlay(sock_arc)?.xpub_nodrop)),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Security mechanism, PLAIN role and credentials, and ZAP domain.
fn get_plain_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_MECHANISM => {
            let v = match overlay(sock_arc)?.mechanism {
                MechanismOverlay::Null => ZMQ_NULL,
                MechanismOverlay::PlainServer
                | MechanismOverlay::PlainServerCredentials { .. }
                | MechanismOverlay::PlainClient { .. } => ZMQ_PLAIN,
                MechanismOverlay::CurveServer { .. } | MechanismOverlay::CurveClient { .. } => {
                    ZMQ_CURVE
                }
            };
            out.i32(v)
        }
        ZMQ_PLAIN_SERVER => {
            let v = matches!(
                overlay(sock_arc)?.mechanism,
                MechanismOverlay::PlainServer | MechanismOverlay::PlainServerCredentials { .. }
            );
            out.i32(i32::from(v))
        }
        ZMQ_PLAIN_USERNAME => {
            let ov = overlay(sock_arc)?;
            if let MechanismOverlay::PlainClient { ref username, .. } = ov.mechanism {
                out.string(username.as_bytes())
            } else {
                out.string(b"")
            }
        }
        ZMQ_PLAIN_PASSWORD => {
            let ov = overlay(sock_arc)?;
            if let MechanismOverlay::PlainClient { ref password, .. } = ov.mechanism {
                out.string(password.as_bytes())
            } else {
                out.string(b"")
            }
        }
        ZMQ_ZAP_DOMAIN => {
            let overlay = overlay(sock_arc)?;
            out.string(overlay.zap.domain.as_bytes())
        }
        ZMQ_ZAP_ENFORCE_DOMAIN => out.i32(i32::from(overlay(sock_arc)?.zap.enforce)),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// CURVE role and keys.
fn get_curve_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_CURVE_SERVER => {
            let v = matches!(
                overlay(sock_arc)?.mechanism,
                MechanismOverlay::CurveServer { .. }
            );
            out.i32(i32::from(v))
        }
        ZMQ_CURVE_PUBLICKEY => {
            let ov = overlay(sock_arc)?;
            if let MechanismOverlay::CurveClient { ref public_key, .. } = ov.mechanism {
                out.key(public_key)
            } else {
                out.key(&[0; 32])
            }
        }
        ZMQ_CURVE_SECRETKEY => {
            let ov = overlay(sock_arc)?;
            let key = match ov.mechanism {
                MechanismOverlay::CurveServer { ref secret_key, .. }
                | MechanismOverlay::CurveClient { ref secret_key, .. } => secret_key,
                _ => &[0; 32],
            };
            out.key(key)
        }
        ZMQ_CURVE_SERVERKEY => {
            let ov = overlay(sock_arc)?;
            if let MechanismOverlay::CurveClient { ref server_key, .. } = ov.mechanism {
                out.key(server_key)
            } else {
                out.key(&[0; 32])
            }
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// WebSocket peer limit and origin policy.
fn get_ws_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        OMQ_WS_MAX_READY_PEERS => out.i32(overlay(sock_arc)?.ws.max_ready_peers),
        OMQ_WS_ALLOWED_ORIGINS => {
            out.bytes(overlay(sock_arc)?.ws.allowed_origins.join("\n").as_bytes())
        }
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// WebSocket TLS material.
fn get_wss_option(sock_arc: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_WSS_KEY_PEM => {
            let ov = overlay(sock_arc)?;
            out.bytes(
                ov.wss
                    .key_pem
                    .as_ref()
                    .map_or(&[][..], |key| key.0.as_slice()),
            )
        }
        ZMQ_WSS_CERT_PEM => {
            let ov = overlay(sock_arc)?;
            out.bytes(ov.wss.cert_pem.as_deref().unwrap_or(b""))
        }
        ZMQ_WSS_TRUST_PEM => {
            let ov = overlay(sock_arc)?;
            out.bytes(ov.wss.trust_pem.as_deref().unwrap_or(b""))
        }
        ZMQ_WSS_HOSTNAME => {
            let ov = overlay(sock_arc)?;
            out.string(ov.wss.hostname.as_deref().unwrap_or("").as_bytes())
        }
        ZMQ_WSS_TRUST_SYSTEM => out.i32(i32::from(overlay(sock_arc)?.wss.trust_system)),
        _ => return Ok(false),
    }?;

    Ok(true)
}

/// Options with fixed values: always-on behavior or unsupported features
/// (see the code lists in `codes`).
fn get_fixed_option(_sock: &Arc<OmqSocket>, option: c_int, out: OptOut) -> Result<bool, c_int> {
    match option {
        ZMQ_MULTICAST_MAXTPDU => out.i32(1500),
        ZMQ_USE_FD => out.i32(-1),
        _ if ONE_I32.contains(&option) => out.i32(1),
        _ if ZERO_I32.contains(&option) => out.i32(0),
        _ if ZERO_I64.contains(&option) => out.i64(0),
        _ if EMPTY_STRING.contains(&option) => out.string(b""),
        _ if EMPTY_BYTES.contains(&option) => out.bytes(b""),
        _ => return Ok(false),
    }?;

    Ok(true)
}
