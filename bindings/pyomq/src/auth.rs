//! NULL, PLAIN, and CURVE authenticator bridges from Python policies.

#[cfg(feature = "curve")]
use std::collections::HashSet;
use std::fmt;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::peer_info::PeerInfo;
use crate::socket::SocketInner;

/// Two modes of CURVE client authentication stored in the Overlay
/// before socket materialization.
#[cfg(feature = "curve")]
pub(crate) enum CurveAuthenticator {
    AllowedKeys(HashSet<[u8; 32]>),
    Callback(Py<PyAny>),
}

/// PLAIN server policy stored before socket materialization.
#[cfg(feature = "plain")]
pub(crate) enum PlainAuthenticator {
    Credentials(Vec<(String, String)>),
    Callback(Py<PyAny>),
}

#[cfg(feature = "plain")]
impl Clone for PlainAuthenticator {
    fn clone(&self) -> Self {
        match self {
            Self::Credentials(credentials) => Self::Credentials(credentials.clone()),
            Self::Callback(callback) => Python::attach(|py| Self::Callback(callback.clone_ref(py))),
        }
    }
}

#[cfg(feature = "plain")]
impl fmt::Debug for PlainAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credentials(credentials) => f
                .debug_tuple("Credentials")
                .field(&format_args!("{} entries", credentials.len()))
                .finish(),
            Self::Callback(_) => f.write_str("Callback(<callable>)"),
        }
    }
}

#[cfg(feature = "curve")]
impl Clone for CurveAuthenticator {
    fn clone(&self) -> Self {
        match self {
            Self::AllowedKeys(keys) => Self::AllowedKeys(keys.clone()),
            Self::Callback(cb) => Python::attach(|py| Self::Callback(cb.clone_ref(py))),
        }
    }
}

#[cfg(feature = "curve")]
impl fmt::Debug for CurveAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllowedKeys(keys) => write!(f, "AllowedKeys({} keys)", keys.len()),
            Self::Callback(_) => f.write_str("Callback(<callable>)"),
        }
    }
}

/// Context-wide policy installed by `pyomq.auth`, called as
/// `callback(mechanism, domain, peer)`. It covers the socket's server
/// mechanism unless an explicit CURVE or PLAIN policy is set.
pub(crate) struct ZapAuthenticator(Py<PyAny>);

impl Clone for ZapAuthenticator {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl fmt::Debug for ZapAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ZapAuthenticator(<callable>)")
    }
}

/// Build the handshake authenticator for `mechanism` (`"NULL"`, `"PLAIN"`,
/// or `"CURVE"`) and the socket's ZAP domain.
pub(crate) fn build_zap_authenticator(
    auth: &ZapAuthenticator,
    mechanism: &'static str,
    domain: &[u8],
) -> omq_proto::proto::mechanism::Authenticator {
    let callback = Python::attach(|py| auth.0.clone_ref(py));
    let domain = domain.to_vec();
    omq_proto::proto::mechanism::Authenticator::new(move |peer| {
        Python::attach(|py| {
            let Ok(info) = Py::new(py, PeerInfo::from_mechanism(py, peer)) else {
                return false;
            };
            let domain = PyBytes::new(py, &domain);
            admitted(py, callback.call1(py, (mechanism, domain, info)))
        })
    })
}

/// Convert a pyomq-level `CurveAuthenticator` into the `omq_proto`
/// `Authenticator` closure consumed by the CURVE handshake.
#[cfg(feature = "curve")]
pub(crate) fn build_authenticator(
    auth: &CurveAuthenticator,
) -> omq_proto::proto::mechanism::Authenticator {
    match auth {
        CurveAuthenticator::AllowedKeys(keys) => {
            let keys = keys.clone();
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                keys.contains(&peer.public_key)
            })
        }
        CurveAuthenticator::Callback(cb) => {
            let cb = Python::attach(|py| cb.clone_ref(py));
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                Python::attach(|py| {
                    let Ok(info) = Py::new(py, PeerInfo::from_mechanism(py, peer)) else {
                        return false;
                    };
                    admitted(py, cb.call1(py, (info,)))
                })
            })
        }
    }
}

#[cfg(feature = "plain")]
pub(crate) fn build_plain_authenticator(
    auth: &PlainAuthenticator,
) -> omq_proto::proto::mechanism::Authenticator {
    match auth {
        PlainAuthenticator::Credentials(credentials) => {
            omq_proto::Authenticator::plain_credentials(credentials.clone())
        }
        PlainAuthenticator::Callback(callback) => {
            let callback = Python::attach(|py| callback.clone_ref(py));
            omq_proto::Authenticator::new(move |peer| {
                Python::attach(|py| {
                    let Ok(info) = Py::new(py, PeerInfo::from_mechanism(py, peer)) else {
                        return false;
                    };
                    admitted(py, callback.call1(py, (info,)))
                })
            })
        }
    }
}

#[cfg(feature = "plain")]
pub(crate) fn set_plain_auth_impl(inner: &SocketInner, auth: &Bound<'_, PyAny>) -> PyResult<()> {
    let policy = if auth.is_callable() {
        PlainAuthenticator::Callback(auth.clone().unbind())
    } else {
        let credentials: Vec<(String, String)> = auth
            .try_iter()
            .and_then(|items| items.map(|item| item?.extract()).collect())
            .map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(concat!(
                    "set_plain_auth expects an iterable of ",
                    "(username, password) pairs or a callable",
                ))
            })?;
        for (username, password) in &credentials {
            if username.len() > 255
                || password.len() > 255
                || !username.bytes().all(|byte| byte.is_ascii_graphic())
                || !password.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "PLAIN credentials must contain at most 255 ASCII VCHAR bytes",
                ));
            }
        }
        PlainAuthenticator::Credentials(credentials)
    };
    store_before_use(inner, "PLAIN authentication", |ov| {
        ov.plain_authenticator = Some(policy);
    })
}

/// Shared implementation for `Socket::set_curve_auth` and
/// `AsyncSocket::set_curve_auth`.
#[cfg(feature = "curve")]
pub(crate) fn set_curve_auth_impl(inner: &SocketInner, auth: &Bound<'_, PyAny>) -> PyResult<()> {
    let policy = if auth.is_none() {
        None
    } else if auth.is_callable() {
        Some(CurveAuthenticator::Callback(auth.clone().unbind()))
    } else {
        let iter = auth.try_iter().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "set_curve_auth expects an iterable of Z85 keys, a callable, or None",
            )
        })?;
        let mut keys = HashSet::new();
        for item in iter {
            let item = item?;
            let z85_bytes: &[u8] = item.extract()?;
            let z85_str = std::str::from_utf8(z85_bytes).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("key must be valid Z85 ASCII")
            })?;
            let pk = omq_tokio::CurvePublicKey::from_z85(z85_str)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            keys.insert(*pk.as_bytes());
        }
        Some(CurveAuthenticator::AllowedKeys(keys))
    };
    store_before_use(inner, "CURVE authentication", |ov| {
        ov.curve_authenticator = policy;
    })
}

/// Shared implementation of the private `_set_zap_auth` used by `pyomq.auth`.
pub(crate) fn set_zap_auth_impl(inner: &SocketInner, callback: &Bound<'_, PyAny>) -> PyResult<()> {
    if !callback.is_callable() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "_set_zap_auth expects a callable",
        ));
    }
    let policy = ZapAuthenticator(callback.clone().unbind());
    store_before_use(inner, "A pyomq.auth authenticator", |ov| {
        ov.zap_authenticator = Some(policy);
    })
}

/// Store an admission policy unless the socket is already in use. The lock
/// order matches materialization: socket slots, then the overlay.
fn store_before_use(
    inner: &SocketInner,
    what: &str,
    store: impl FnOnce(&mut crate::options::Overlay),
) -> PyResult<()> {
    let materialized = inner.materialized.read().unwrap();
    let blocking_materialized = inner.blocking_materialized.read().unwrap();
    if materialized.is_some() || blocking_materialized.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{what} must be configured before socket use"
        )));
    }
    store(&mut inner.overlay.lock().unwrap());
    Ok(())
}

/// Reads a Python policy result. An exception or a falsy value rejects the
/// peer.
fn admitted(py: Python<'_>, result: PyResult<Py<PyAny>>) -> bool {
    match result {
        Ok(value) => value.is_truthy(py).unwrap_or(false),
        Err(error) => {
            error.restore(py);
            false
        }
    }
}
