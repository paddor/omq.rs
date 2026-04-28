//! setsockopt / getsockopt dispatch.
//!
//! For options that map directly onto `omq_proto::Options`, we route
//! through the per-socket overlay and apply the change to the live
//! `Options` snapshot the backend's Socket holds. For wrapper-only
//! options (RCVTIMEO, SNDTIMEO, RCVMORE), we keep state in the overlay
//! itself.

use std::time::Duration;

use bytes::Bytes;
use omq_proto::options::{KeepAlive, ReconnectPolicy};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Helper: turn an `i64` into a `Bound<PyAny>` for `getsockopt` returns.
fn int_to_bound<T: pyo3::IntoPy<PyObject>>(py: Python<'_>, v: T) -> Bound<'_, PyAny> {
    v.into_py(py).into_bound(py)
}

use omq_compio as backend;
use crate::constants;
use crate::error::{map_err, not_implemented};
// SocketInner accessed via fully-qualified path; no top-level import.

/// Wrapper-only state that doesn't live on `Options`.
#[derive(Clone, Debug, Default)]
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
}

impl Overlay {
    /// Materialise an `omq_compio::Options` from the overlay. Used
    /// when the underlying Socket is first built.
    pub fn to_options(&self) -> backend::Options {
        let mut o = backend::Options::default();
        o.send_hwm = self.send_hwm;
        o.recv_hwm = self.recv_hwm;
        o.linger = self.linger;
        o.identity = self.identity.clone();
        o.max_message_size = self.max_message_size;
        o.router_mandatory = self.router_mandatory;
        o.heartbeat_interval = self.heartbeat_ivl;
        o.heartbeat_ttl = self.heartbeat_ttl;
        o.heartbeat_timeout = self.heartbeat_timeout;
        o.handshake_timeout = self.handshake_ivl;
        o.conflate = self.conflate;
        o.tcp_keepalive = self.keepalive;
        o.reconnect = match (self.reconnect_ivl, self.reconnect_ivl_max) {
            (None, _) => ReconnectPolicy::Disabled,
            (Some(min), None) => ReconnectPolicy::Fixed(min),
            (Some(min), Some(max)) => ReconnectPolicy::Exponential { min, max },
        };
        o
    }

    pub fn from_options(o: &backend::Options) -> Self {
        Self {
            rcvtimeo: None,
            sndtimeo: None,
            linger: o.linger,
            send_hwm: o.send_hwm,
            recv_hwm: o.recv_hwm,
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
            },
            reconnect_ivl_max: match o.reconnect {
                ReconnectPolicy::Exponential { max, .. } => Some(max),
                _ => None,
            },
        }
    }
}

fn ms(value: i64) -> Option<Duration> {
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

pub fn setsockopt(
    sock: &crate::socket::SocketInner,
    py: Python<'_>,
    option: i32,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let mut ov = sock.overlay.lock().unwrap();
    match option {
        constants::LINGER => {
            ov.linger = ms(value.extract::<i64>()?);
        }
        constants::SNDHWM => {
            ov.send_hwm = match value.extract::<i64>()? {
                0 => None,
                n => Some(n as u32),
            };
        }
        constants::RCVHWM => {
            ov.recv_hwm = match value.extract::<i64>()? {
                0 => None,
                n => Some(n as u32),
            };
        }
        constants::IDENTITY => {
            let v: &[u8] = value.extract()?;
            ov.identity = Bytes::copy_from_slice(v);
        }
        constants::SUBSCRIBE => {
            let v: &[u8] = value.extract()?;
            let bytes = Bytes::copy_from_slice(v);
            // `ensure_id` re-locks `sock.overlay` to materialise on
            // first call. Drop the overlay guard we acquired at the
            // top of `setsockopt` before calling into it.
            drop(ov);
            let id = sock.ensure_id()?;
            let r = py.allow_threads(|| {
                crate::runtime::with_socket(id, move |s| async move { s.subscribe(bytes).await })
            });
            return match r {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(map_err(e)),
                Err(_) => Err(map_err(omq_proto::error::Error::Closed)),
            };
        }
        constants::UNSUBSCRIBE => {
            let v: &[u8] = value.extract()?;
            let bytes = Bytes::copy_from_slice(v);
            drop(ov);
            let id = sock.ensure_id()?;
            let r = py.allow_threads(|| {
                crate::runtime::with_socket(id, move |s| async move { s.unsubscribe(bytes).await })
            });
            return match r {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(map_err(e)),
                Err(_) => Err(map_err(omq_proto::error::Error::Closed)),
            };
        }
        constants::RCVTIMEO => {
            ov.rcvtimeo = ms(value.extract::<i64>()?);
        }
        constants::SNDTIMEO => {
            ov.sndtimeo = ms(value.extract::<i64>()?);
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
            ov.reconnect_ivl = ms(value.extract::<i64>()?);
        }
        constants::RECONNECT_IVL_MAX => {
            ov.reconnect_ivl_max = ms(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_IVL => {
            ov.heartbeat_ivl = ms(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_TTL => {
            ov.heartbeat_ttl = ms(value.extract::<i64>()?);
        }
        constants::HEARTBEAT_TIMEOUT => {
            ov.heartbeat_timeout = ms(value.extract::<i64>()?);
        }
        constants::HANDSHAKE_IVL => {
            ov.handshake_ivl = ms(value.extract::<i64>()?);
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
        // Group B no-ops accepted for source-compat with pyzmq:
        constants::IMMEDIATE | constants::IPV6 => {}
        // Group C "not implemented in v0.1".
        constants::AFFINITY => return Err(not_implemented("AFFINITY")),
        constants::BACKLOG => return Err(not_implemented("BACKLOG")),
        constants::TYPE | constants::RCVMORE => {
            return Err(not_implemented("read-only option"))
        }
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
            let st = sock.socket_type;
            let v: i32 = match st {
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
                _ => -1,
            };
            Ok(int_to_bound(py, v))
        }
        constants::RCVMORE => {
            let more = !sock.rxbuf.lock().unwrap().is_empty();
            Ok(int_to_bound(py, more as i64))
        }
        constants::IDENTITY => {
            let id = sock.overlay.lock().unwrap().identity.clone();
            Ok(PyBytes::new_bound(py, &id).into_any())
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
        other => Err(not_implemented(&format!("getsockopt for option id {other}"))),
    }
}
