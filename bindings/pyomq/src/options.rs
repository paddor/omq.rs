//! setsockopt / getsockopt dispatch.
//!
//! For options that map directly onto `omq_proto::Options`, we route
//! through the per-socket overlay and apply the change to the live
//! `Options` snapshot the backend's Socket holds. For wrapper-only
//! options (RCVTIMEO, SNDTIMEO, RCVMORE), we keep state in the overlay
//! itself.

use std::time::Duration;

use bytes::Bytes;
use omq_proto::options::{KeepAlive, OnMute, ReconnectPolicy};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Helper: turn an `i64` into a `Bound<PyAny>` for `getsockopt` returns.
fn int_to_bound(py: Python<'_>, v: i64) -> Bound<'_, PyAny> {
    // This conversion cannot fail,
    // since `v` is a valid `i64` and `PyLong` can represent any `i64`.
    v.into_pyobject(py).unwrap().into_any()
}

use crate::constants;
use crate::error::{map_err, not_implemented};
use omq_tokio as backend;

#[cfg(feature = "curve")]
fn bad_key(name: &str) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("invalid {name}"))
}

#[cfg(feature = "curve")]
fn bad_key_detail(name: &str, e: &dyn std::fmt::Display) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("invalid {name}: {e}"))
}

/// Wrapper-only state that doesn't live on `Options`.
#[derive(Clone, Debug)]
pub struct Overlay {
    pub rcvtimeo: Option<Duration>,
    pub sndtimeo: Option<Duration>,
    pub linger: Option<Duration>,
    pub send_hwm: Option<u32>,
    pub recv_hwm: Option<u32>,
    pub identity: Bytes,
    pub keepalive: KeepAlive,
    pub keepalive_idle: Option<Duration>,
    pub keepalive_intvl: Option<Duration>,
    pub keepalive_cnt: Option<u32>,
    pub max_message_size: Option<usize>,
    pub router_mandatory: bool,
    pub heartbeat_ivl: Option<Duration>,
    pub heartbeat_ttl: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub handshake_ivl: Option<Duration>,
    pub conflate: bool,
    pub reconnect_ivl: Option<Duration>,
    pub reconnect_ivl_max: Option<Duration>,
    pub send_buffer_size: Option<usize>,
    pub recv_buffer_size: Option<usize>,
    pub plain_server: bool,
    pub plain_username: Option<String>,
    pub plain_password: Option<String>,
    pub curve_server: bool,
    pub curve_publickey: Option<Vec<u8>>,
    pub curve_secretkey: Option<Vec<u8>>,
    pub curve_serverkey: Option<Vec<u8>>,
    #[cfg(feature = "curve")]
    pub curve_authenticator: Option<crate::auth::CurveAuthenticator>,
    pub on_mute: OnMute,
    pub compression_dict: Option<Bytes>,
    pub compression_auto_train: bool,
    pub reconnect_stop: i32,
}

impl Default for Overlay {
    fn default() -> Self {
        Self {
            rcvtimeo: None,
            sndtimeo: None,
            linger: None,
            send_hwm: None,
            recv_hwm: None,
            identity: Bytes::new(),
            keepalive: KeepAlive::Default,
            keepalive_idle: None,
            keepalive_intvl: None,
            keepalive_cnt: None,
            max_message_size: None,
            router_mandatory: false,
            heartbeat_ivl: None,
            heartbeat_ttl: None,
            heartbeat_timeout: None,
            handshake_ivl: None,
            conflate: false,
            reconnect_ivl: None,
            reconnect_ivl_max: None,
            send_buffer_size: None,
            recv_buffer_size: None,
            plain_server: false,
            plain_username: None,
            plain_password: None,
            curve_server: false,
            curve_publickey: None,
            curve_secretkey: None,
            curve_serverkey: None,
            #[cfg(feature = "curve")]
            curve_authenticator: None,
            on_mute: OnMute::Block,
            compression_dict: None,
            compression_auto_train: false,
            reconnect_stop: 0,
        }
    }
}

impl Overlay {
    /// Materialize an `backend::Options` from the overlay. Used
    /// when the underlying Socket is first built.
    pub fn to_options(&self) -> PyResult<backend::Options> {
        #[allow(unused_mut)]
        let mut opts = backend::Options {
            send_hwm: self.send_hwm.unwrap_or(1000),
            recv_hwm: self.recv_hwm.unwrap_or(1000),
            linger: self.linger,
            identity: self.identity.clone(),
            max_message_size: self.max_message_size,
            router_mandatory: self.router_mandatory,
            heartbeat_interval: self.heartbeat_ivl,
            heartbeat_ttl: self.heartbeat_ttl,
            heartbeat_timeout: self.heartbeat_timeout,
            handshake_timeout: self.handshake_ivl,
            conflate: self.conflate,
            tcp_keepalive: self.keepalive,
            send_buffer_size: self.send_buffer_size,
            recv_buffer_size: self.recv_buffer_size,
            reconnect: match (self.reconnect_ivl, self.reconnect_ivl_max) {
                (None, _) => ReconnectPolicy::Disabled,
                (Some(min), None) => ReconnectPolicy::Fixed(min),
                (Some(min), Some(max)) => ReconnectPolicy::Exponential { min, max },
            },
            on_mute: self.on_mute,
            compression_dict: self.compression_dict.clone(),
            compression_auto_train: self.compression_auto_train,
            arena_threshold: Some(64 * 1024),
            transmit_slot_cap: None,
            reconnect_stop_conn_refused: (self.reconnect_stop & 1) != 0,
            ..Default::default()
        };
        #[cfg(feature = "plain")]
        if self.plain_server {
            opts.mechanism = omq_proto::options::MechanismConfig::PlainServer {
                authenticator: omq_proto::proto::mechanism::Authenticator::new(|_| true),
            };
        } else if let (Some(u), Some(p)) = (&self.plain_username, &self.plain_password) {
            opts.mechanism = omq_proto::options::MechanismConfig::PlainClient {
                username: u.clone(),
                password: p.clone(),
            };
        }
        #[cfg(feature = "curve")]
        if self.curve_server {
            if let (Some(pk), Some(sk)) = (&self.curve_publickey, &self.curve_secretkey) {
                let pk_str = std::str::from_utf8(pk).map_err(|_| bad_key("CURVE_PUBLICKEY"))?;
                let sk_str = std::str::from_utf8(sk).map_err(|_| bad_key("CURVE_SECRETKEY"))?;
                let public = backend::CurvePublicKey::from_z85(pk_str)
                    .map_err(|e| bad_key_detail("CURVE_PUBLICKEY", &e))?;
                let secret = backend::CurveSecretKey::from_z85(sk_str)
                    .map_err(|e| bad_key_detail("CURVE_SECRETKEY", &e))?;
                let keypair = backend::CurveKeypair { public, secret };
                opts.mechanism = omq_proto::options::MechanismConfig::CurveServer {
                    our_keypair: keypair,
                    cookie_keyring: std::sync::Arc::new(backend::CurveCookieKeyring::new()),
                    authenticator: self
                        .curve_authenticator
                        .as_ref()
                        .map(crate::auth::build_authenticator),
                };
            }
        } else if let (Some(pk), Some(sk), Some(svk)) = (
            &self.curve_publickey,
            &self.curve_secretkey,
            &self.curve_serverkey,
        ) {
            let pk_str = std::str::from_utf8(pk).map_err(|_| bad_key("CURVE_PUBLICKEY"))?;
            let sk_str = std::str::from_utf8(sk).map_err(|_| bad_key("CURVE_SECRETKEY"))?;
            let svk_str = std::str::from_utf8(svk).map_err(|_| bad_key("CURVE_SERVERKEY"))?;
            let public = backend::CurvePublicKey::from_z85(pk_str)
                .map_err(|e| bad_key_detail("CURVE_PUBLICKEY", &e))?;
            let secret = backend::CurveSecretKey::from_z85(sk_str)
                .map_err(|e| bad_key_detail("CURVE_SECRETKEY", &e))?;
            let server_public = backend::CurvePublicKey::from_z85(svk_str)
                .map_err(|e| bad_key_detail("CURVE_SERVERKEY", &e))?;
            let keypair = backend::CurveKeypair { public, secret };
            opts.mechanism = omq_proto::options::MechanismConfig::CurveClient {
                our_keypair: keypair,
                server_public,
            };
        }
        Ok(opts)
    }

    pub fn from_options(o: &backend::Options) -> Self {
        Self {
            rcvtimeo: None,
            sndtimeo: None,
            linger: o.linger,
            send_hwm: Some(o.send_hwm),
            recv_hwm: Some(o.recv_hwm),
            identity: o.identity.clone(),
            keepalive: o.tcp_keepalive,
            keepalive_idle: None,
            keepalive_intvl: None,
            keepalive_cnt: None,
            max_message_size: o.max_message_size,
            router_mandatory: o.router_mandatory,
            heartbeat_ivl: o.heartbeat_interval,
            heartbeat_ttl: o.heartbeat_ttl,
            heartbeat_timeout: o.heartbeat_timeout,
            handshake_ivl: o.handshake_timeout,
            conflate: o.conflate,
            reconnect_ivl: match o.reconnect {
                ReconnectPolicy::Fixed(d) => Some(d),
                ReconnectPolicy::Exponential { min, .. } => Some(min),
                ReconnectPolicy::Disabled => None,
                _ => unreachable!(),
            },
            reconnect_ivl_max: match o.reconnect {
                ReconnectPolicy::Exponential { max, .. } => Some(max),
                _ => None,
            },
            send_buffer_size: o.send_buffer_size,
            recv_buffer_size: o.recv_buffer_size,
            plain_server: false,
            plain_username: None,
            plain_password: None,
            curve_server: false,
            curve_publickey: None,
            curve_secretkey: None,
            curve_serverkey: None,
            #[cfg(feature = "curve")]
            curve_authenticator: None,
            on_mute: o.on_mute,
            compression_dict: o.compression_dict.clone(),
            compression_auto_train: o.compression_auto_train,
            reconnect_stop: i32::from(o.reconnect_stop_conn_refused),
        }
    }
}

pub(crate) fn duration_from_millis(value: i64) -> Option<Duration> {
    match value {
        v if v < 0 => None,
        v => Some(Duration::from_millis(v as u64)),
    }
}

fn as_ms(d: Option<Duration>) -> i64 {
    match d {
        Some(d) => d.as_millis().min(i64::MAX as u128) as i64,
        None => -1,
    }
}

fn parse_hwm(value: &Bound<'_, PyAny>) -> PyResult<Option<u32>> {
    match value.extract::<i64>()? {
        n if n < 0 => Err(pyo3::exceptions::PyValueError::new_err(
            "HWM must be non-negative",
        )),
        0 => Ok(None),
        n if n > u32::MAX as i64 => Err(pyo3::exceptions::PyValueError::new_err(
            "HWM exceeds u32::MAX",
        )),
        n => Ok(Some(n as u32)),
    }
}

pub fn setsockopt(
    sock: &crate::socket::SocketInner,
    py: Python<'_>,
    option: i32,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let mut ov = sock.overlay.lock().unwrap();
    match option {
        constants::LINGER => {
            ov.linger = duration_from_millis(value.extract::<i64>()?);
        }
        constants::SNDHWM => {
            ov.send_hwm = parse_hwm(value)?;
        }
        constants::RCVHWM => {
            ov.recv_hwm = parse_hwm(value)?;
        }
        constants::IDENTITY => {
            let v: &[u8] = value.extract()?;
            ov.identity = Bytes::copy_from_slice(v);
        }
        constants::SUBSCRIBE => {
            let v: &[u8] = value.extract()?;
            let bytes = Bytes::copy_from_slice(v);
            sock.subscriptions.lock().unwrap().push(bytes.clone());
            drop(ov);
            if let Some(s) = sock
                .blocking_materialized
                .read()
                .unwrap()
                .as_ref()
                .map(|m| m.socket.clone())
            {
                return py.detach(|| s.subscribe(bytes)).map_err(map_err);
            }
            let ctx = sock.ctx.clone();
            let s = sock.ensure_socket()?;
            let r =
                py.detach(|| ctx.with_socket(&s, move |s| async move { s.subscribe(bytes).await }));
            return r.map_err(map_err);
        }
        constants::UNSUBSCRIBE => {
            let v: &[u8] = value.extract()?;
            let bytes = Bytes::copy_from_slice(v);
            sock.subscriptions.lock().unwrap().retain(|p| p != &bytes);
            drop(ov);
            if let Some(s) = sock
                .blocking_materialized
                .read()
                .unwrap()
                .as_ref()
                .map(|m| m.socket.clone())
            {
                return py.detach(|| s.unsubscribe(bytes)).map_err(map_err);
            }
            let ctx = sock.ctx.clone();
            let s = sock.ensure_socket()?;
            let r = py
                .detach(|| ctx.with_socket(&s, move |s| async move { s.unsubscribe(bytes).await }));
            return r.map_err(map_err);
        }
        constants::RCVTIMEO => {
            ov.rcvtimeo = duration_from_millis(value.extract::<i64>()?);
        }
        constants::SNDTIMEO => {
            ov.sndtimeo = duration_from_millis(value.extract::<i64>()?);
        }
        constants::ROUTER_MANDATORY => {
            ov.router_mandatory = value.extract::<i64>()? != 0;
        }
        constants::MAXMSGSIZE => {
            ov.max_message_size = match value.extract::<i64>()? {
                v if v < 0 => None,
                v => Some(v as usize),
            };
        }
        constants::RECONNECT_IVL => {
            ov.reconnect_ivl = duration_from_millis(value.extract::<i64>()?);
        }
        constants::RECONNECT_IVL_MAX => {
            ov.reconnect_ivl_max = duration_from_millis(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_IVL => {
            ov.heartbeat_ivl = duration_from_millis(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_TTL => {
            ov.heartbeat_ttl = duration_from_millis(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_TIMEOUT => {
            ov.heartbeat_timeout = duration_from_millis(value.extract::<i64>()?);
        }
        constants::HANDSHAKE_IVL => {
            ov.handshake_ivl = duration_from_millis(value.extract::<i64>()?);
        }
        constants::CONFLATE => {
            ov.conflate = value.extract::<i64>()? != 0;
        }
        // TCP keepalive (group C, the only one in v0.1).
        constants::TCP_KEEPALIVE => match value.extract::<i64>()? {
            -1 => ov.keepalive = KeepAlive::Default,
            0 => ov.keepalive = KeepAlive::Disabled,
            _ => {
                let idle = ov.keepalive_idle.unwrap_or(Duration::from_secs(60));
                let intvl = ov.keepalive_intvl.unwrap_or(Duration::from_secs(10));
                let cnt = ov.keepalive_cnt.unwrap_or(3);
                ov.keepalive = KeepAlive::Enabled { idle, intvl, cnt };
            }
        },
        constants::TCP_KEEPALIVE_IDLE => {
            ov.keepalive_idle = match value.extract::<i64>()? {
                v if v < 0 => None,
                v => Some(Duration::from_secs(v as u64)),
            };
            promote_keepalive(&mut ov);
        }
        constants::TCP_KEEPALIVE_INTVL => {
            ov.keepalive_intvl = match value.extract::<i64>()? {
                v if v < 0 => None,
                v => Some(Duration::from_secs(v as u64)),
            };
            promote_keepalive(&mut ov);
        }
        constants::TCP_KEEPALIVE_CNT => {
            ov.keepalive_cnt = match value.extract::<i64>()? {
                v if v < 0 => None,
                v => Some(v as u32),
            };
            promote_keepalive(&mut ov);
        }
        constants::SNDBUF => {
            let v = value.extract::<i64>()?;
            ov.send_buffer_size = if v <= 0 { None } else { Some(v as usize) };
        }
        constants::RCVBUF => {
            let v = value.extract::<i64>()?;
            ov.recv_buffer_size = if v <= 0 { None } else { Some(v as usize) };
        }
        constants::PLAIN_SERVER => {
            ov.plain_server = value.extract::<i64>()? != 0;
        }
        constants::PLAIN_USERNAME => {
            let v: &[u8] = value.extract()?;
            ov.plain_username = Some(String::from_utf8_lossy(v).into_owned());
        }
        constants::PLAIN_PASSWORD => {
            let v: &[u8] = value.extract()?;
            ov.plain_password = Some(String::from_utf8_lossy(v).into_owned());
        }
        constants::CURVE_SERVER => {
            ov.curve_server = value.extract::<i64>()? != 0;
        }
        constants::CURVE_PUBLICKEY => {
            let v: &[u8] = value.extract()?;
            ov.curve_publickey = Some(v.to_vec());
        }
        constants::CURVE_SECRETKEY => {
            let v: &[u8] = value.extract()?;
            ov.curve_secretkey = Some(v.to_vec());
        }
        constants::CURVE_SERVERKEY => {
            let v: &[u8] = value.extract()?;
            ov.curve_serverkey = Some(v.to_vec());
        }
        constants::OMQ_ON_MUTE => {
            ov.on_mute = match value.extract::<i64>()? {
                0 => OnMute::Block,
                1 => OnMute::DropNewest,
                2 => OnMute::DropOldest,
                v => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "OMQ_ON_MUTE must be 0 (Block), 1 (DropNewest), or 2 (DropOldest), \
                         got {v}"
                    )));
                }
            };
        }
        constants::OMQ_COMPRESSION_DICT => {
            let v: &[u8] = value.extract()?;
            if v.is_empty() {
                ov.compression_dict = None;
            } else {
                const DICT_MAX: usize = 64 * 1024 - 4;
                if v.len() > DICT_MAX {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "compression dict must be at most {DICT_MAX} bytes, got {}",
                        v.len()
                    )));
                }
                ov.compression_dict = Some(Bytes::copy_from_slice(v));
            }
        }
        constants::OMQ_COMPRESSION_AUTO_TRAIN => {
            ov.compression_auto_train = value.extract::<i64>()? != 0;
        }
        // No-ops accepted for source-compat with pyzmq:
        constants::IMMEDIATE
        | constants::IPV6
        | constants::IPV4ONLY
        | constants::RATE
        | constants::CONNECT_TIMEOUT
        | constants::XPUB_VERBOSE
        | constants::PROBE_ROUTER
        | constants::REQ_CORRELATE
        | constants::REQ_RELAXED
        | constants::ROUTER_HANDOVER
        | constants::TCP_ACCEPT_FILTER
        | constants::TCP_MAXRT
        | constants::MULTICAST_HOPS
        | constants::RECOVERY_IVL
        | constants::ZAP_DOMAIN => {}
        constants::RECONNECT_STOP => {
            let v: i32 = value.extract()?;
            sock.overlay.lock().unwrap().reconnect_stop = v;
        }
        constants::AFFINITY => return Err(not_implemented("AFFINITY")),
        constants::BACKLOG => return Err(not_implemented("BACKLOG")),
        constants::TYPE | constants::RCVMORE => return Err(not_implemented("read-only option")),
        other => {
            return Err(not_implemented(&format!("option id {other}")));
        }
    }
    Ok(())
}

/// Once any of {KEEPALIVE_IDLE, _INTVL, _CNT} is set, flip the policy
/// from `Default` to `Enabled` so the next connect / accept actually
/// applies it. Idempotent.
fn promote_keepalive(ov: &mut Overlay) {
    let idle = ov.keepalive_idle.unwrap_or(Duration::from_secs(60));
    let intvl = ov.keepalive_intvl.unwrap_or(Duration::from_secs(10));
    let cnt = ov.keepalive_cnt.unwrap_or(3);
    ov.keepalive = KeepAlive::Enabled { idle, intvl, cnt };
}

pub fn getsockopt<'py>(
    sock: &crate::socket::SocketInner,
    py: Python<'py>,
    option: i32,
) -> PyResult<Bound<'py, PyAny>> {
    drop(sock.overlay.lock().unwrap()); // ensure poison-detection on every call path
    match option {
        constants::TYPE => {
            let socket_type = sock.socket_type;
            let option_value: i32 = match socket_type {
                backend::SocketType::Pair => constants::PAIR,
                backend::SocketType::Pub => constants::PUB,
                backend::SocketType::Sub => constants::SUB,
                backend::SocketType::Req => constants::REQ,
                backend::SocketType::Rep => constants::REP,
                backend::SocketType::Dealer => constants::DEALER,
                backend::SocketType::Router => constants::ROUTER,
                backend::SocketType::Pull => constants::PULL,
                backend::SocketType::Push => constants::PUSH,
                backend::SocketType::XPub => constants::XPUB,
                backend::SocketType::XSub => constants::XSUB,
                backend::SocketType::Stream => constants::STREAM,
                backend::SocketType::Server => constants::SERVER,
                backend::SocketType::Client => constants::CLIENT,
                backend::SocketType::Radio => constants::RADIO,
                backend::SocketType::Dish => constants::DISH,
                backend::SocketType::Gather => constants::GATHER,
                backend::SocketType::Scatter => constants::SCATTER,
                backend::SocketType::Peer => constants::PEER,
                backend::SocketType::Channel => constants::CHANNEL,
                _ => -1,
            };
            Ok(int_to_bound(py, option_value as i64))
        }
        constants::RCVMORE => {
            let more = !sock.rxbuf.lock().unwrap().is_empty();
            Ok(int_to_bound(py, more as i64))
        }
        constants::IDENTITY => {
            let id = sock.overlay.lock().unwrap().identity.clone();
            Ok(PyBytes::new(py, &id).into_any())
            // Cast to PyAny via `into_any()` keeps the type uniform with the
            // numeric branches above.
        }
        constants::LINGER => {
            let v = as_ms(sock.overlay.lock().unwrap().linger);
            Ok(int_to_bound(py, v))
        }
        constants::SNDHWM => {
            let v = sock.overlay.lock().unwrap().send_hwm.unwrap_or(0) as i64;
            Ok(int_to_bound(py, v))
        }
        constants::RCVHWM => {
            let v = sock.overlay.lock().unwrap().recv_hwm.unwrap_or(0) as i64;
            Ok(int_to_bound(py, v))
        }
        constants::RCVTIMEO => {
            let v = as_ms(sock.overlay.lock().unwrap().rcvtimeo);
            Ok(int_to_bound(py, v))
        }
        constants::SNDTIMEO => {
            let v = as_ms(sock.overlay.lock().unwrap().sndtimeo);
            Ok(int_to_bound(py, v))
        }
        constants::ROUTER_MANDATORY => {
            let v = sock.overlay.lock().unwrap().router_mandatory as i64;
            Ok(int_to_bound(py, v))
        }
        constants::MAXMSGSIZE => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .max_message_size
                .map(|n| n as i64)
                .unwrap_or(-1);
            Ok(int_to_bound(py, v))
        }
        constants::TCP_KEEPALIVE => {
            let v: i64 = match sock.overlay.lock().unwrap().keepalive {
                KeepAlive::Default => -1,
                KeepAlive::Disabled => 0,
                KeepAlive::Enabled { .. } => 1,
                _ => unreachable!(),
            };
            Ok(int_to_bound(py, v))
        }
        constants::TCP_KEEPALIVE_IDLE => {
            let v = match sock.overlay.lock().unwrap().keepalive {
                KeepAlive::Enabled { idle, .. } => idle.as_secs() as i64,
                _ => -1,
            };
            Ok(int_to_bound(py, v))
        }
        constants::TCP_KEEPALIVE_INTVL => {
            let v = match sock.overlay.lock().unwrap().keepalive {
                KeepAlive::Enabled { intvl, .. } => intvl.as_secs() as i64,
                _ => -1,
            };
            Ok(int_to_bound(py, v))
        }
        constants::TCP_KEEPALIVE_CNT => {
            let v = match sock.overlay.lock().unwrap().keepalive {
                KeepAlive::Enabled { cnt, .. } => cnt as i64,
                _ => -1,
            };
            Ok(int_to_bound(py, v))
        }
        constants::RECONNECT_IVL => {
            let v = as_ms(sock.overlay.lock().unwrap().reconnect_ivl);
            Ok(int_to_bound(py, v))
        }
        constants::RECONNECT_IVL_MAX => {
            let v = as_ms(sock.overlay.lock().unwrap().reconnect_ivl_max);
            Ok(int_to_bound(py, v))
        }
        constants::HEARTBEAT_IVL => {
            let v = as_ms(sock.overlay.lock().unwrap().heartbeat_ivl);
            Ok(int_to_bound(py, v))
        }
        constants::HEARTBEAT_TTL => {
            let v = as_ms(sock.overlay.lock().unwrap().heartbeat_ttl);
            Ok(int_to_bound(py, v))
        }
        constants::HEARTBEAT_TIMEOUT => {
            let v = as_ms(sock.overlay.lock().unwrap().heartbeat_timeout);
            Ok(int_to_bound(py, v))
        }
        constants::HANDSHAKE_IVL => {
            let v = as_ms(sock.overlay.lock().unwrap().handshake_ivl);
            Ok(int_to_bound(py, v))
        }
        constants::CONFLATE => {
            let v = sock.overlay.lock().unwrap().conflate as i64;
            Ok(int_to_bound(py, v))
        }
        constants::SNDBUF => {
            let v = sock.overlay.lock().unwrap().send_buffer_size.unwrap_or(0) as i64;
            Ok(int_to_bound(py, v))
        }
        constants::RCVBUF => {
            let v = sock.overlay.lock().unwrap().recv_buffer_size.unwrap_or(0) as i64;
            Ok(int_to_bound(py, v))
        }
        constants::PLAIN_SERVER => {
            let v = sock.overlay.lock().unwrap().plain_server as i64;
            Ok(int_to_bound(py, v))
        }
        constants::PLAIN_USERNAME => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .plain_username
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, v.as_bytes()).into_any())
        }
        constants::PLAIN_PASSWORD => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .plain_password
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, v.as_bytes()).into_any())
        }
        constants::CURVE_SERVER => {
            let v = sock.overlay.lock().unwrap().curve_server as i64;
            Ok(int_to_bound(py, v))
        }
        constants::CURVE_PUBLICKEY => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .curve_publickey
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, &v).into_any())
        }
        constants::CURVE_SECRETKEY => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .curve_secretkey
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, &v).into_any())
        }
        constants::CURVE_SERVERKEY => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .curve_serverkey
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, &v).into_any())
        }
        constants::OMQ_ON_MUTE => {
            let v: i64 = match sock.overlay.lock().unwrap().on_mute {
                OnMute::Block => 0,
                OnMute::DropNewest => 1,
                OnMute::DropOldest => 2,
                _ => unreachable!(),
            };
            Ok(int_to_bound(py, v))
        }
        constants::OMQ_COMPRESSION_DICT => {
            let v = sock
                .overlay
                .lock()
                .unwrap()
                .compression_dict
                .clone()
                .unwrap_or_default();
            Ok(PyBytes::new(py, &v).into_any())
        }
        constants::OMQ_COMPRESSION_AUTO_TRAIN => {
            let v = sock.overlay.lock().unwrap().compression_auto_train as i64;
            Ok(int_to_bound(py, v))
        }
        // Compat no-ops: return sensible defaults.
        constants::MECHANISM => Ok(int_to_bound(py, 0_i64)),
        constants::RATE
        | constants::CONNECT_TIMEOUT
        | constants::XPUB_VERBOSE
        | constants::PROBE_ROUTER
        | constants::REQ_CORRELATE
        | constants::REQ_RELAXED
        | constants::ROUTER_HANDOVER
        | constants::TCP_MAXRT
        | constants::MULTICAST_HOPS
        | constants::RECOVERY_IVL => Ok(int_to_bound(py, 0_i64)),
        constants::RECONNECT_STOP => Ok(int_to_bound(
            py,
            i64::from(sock.overlay.lock().unwrap().reconnect_stop),
        )),
        constants::FD => {
            sock.materialize()?;
            let materialized_guard = sock.materialized.read().unwrap();
            let materialized = materialized_guard.as_ref().unwrap();
            materialized.recv_ready.arm_persistent();
            Ok(int_to_bound(py, materialized.recv_ready.fd() as i64))
        }
        constants::EVENTS => {
            let has_data = if !sock.rxbuf.lock().unwrap().is_empty() {
                true
            } else {
                let materialized_guard = sock.materialized.read().unwrap();
                materialized_guard
                    .as_ref()
                    .is_some_and(|materialized| !materialized.recv_cons.lock().unwrap().is_empty())
            };
            let flags: i64 = if has_data { 1 } else { 0 };
            Ok(int_to_bound(py, flags))
        }
        constants::ZAP_DOMAIN | constants::TCP_ACCEPT_FILTER | constants::LAST_ENDPOINT => {
            Ok(PyBytes::new(py, b"").into_any())
        }
        other => Err(not_implemented(&format!(
            "getsockopt for option id {other}"
        ))),
    }
}
