//! PLAIN and CURVE authenticator bridges from Python policies.

use std::collections::HashSet;
use std::fmt;

use pyo3::prelude::*;

use crate::peer_info::PeerInfo;
use crate::socket::SocketInner;

/// Two modes of CURVE client authentication stored in the Overlay
/// before socket materialization.
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

impl Clone for CurveAuthenticator {
    fn clone(&self) -> Self {
        match self {
            Self::AllowedKeys(keys) => Self::AllowedKeys(keys.clone()),
            Self::Callback(cb) => Python::attach(|py| Self::Callback(cb.clone_ref(py))),
        }
    }
}

impl fmt::Debug for CurveAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllowedKeys(keys) => write!(f, "AllowedKeys({} keys)", keys.len()),
            Self::Callback(_) => f.write_str("Callback(<callable>)"),
        }
    }
}

/// Convert a pyomq-level `CurveAuthenticator` into the `omq_proto`
/// `Authenticator` closure consumed by the CURVE handshake.
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
                    let info = Py::new(
                        py,
                        PeerInfo::from_raw(py, &peer.public_key, peer.identity.as_ref()),
                    );
                    let info = match info {
                        Ok(i) => i,
                        Err(_) => return false,
                    };
                    match cb.call1(py, (info,)) {
                        Ok(val) => val.is_truthy(py).unwrap_or(false),
                        Err(e) => {
                            e.restore(py);
                            false
                        }
                    }
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
                    let info = Py::new(
                        py,
                        PeerInfo::from_plain(
                            py,
                            peer.username.as_deref(),
                            peer.password.as_deref(),
                            peer.peer_address.as_deref(),
                        ),
                    );
                    let Ok(info) = info else {
                        return false;
                    };
                    match callback.call1(py, (info,)) {
                        Ok(value) => value.is_truthy(py).unwrap_or(false),
                        Err(error) => {
                            error.restore(py);
                            false
                        }
                    }
                })
            })
        }
    }
}

#[cfg(feature = "plain")]
pub(crate) fn set_plain_auth_impl(inner: &SocketInner, auth: &Bound<'_, PyAny>) -> PyResult<()> {
    let materialized = inner.materialized.read().unwrap();
    let blocking_materialized = inner.blocking_materialized.read().unwrap();
    if materialized.is_some() || blocking_materialized.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "PLAIN authentication must be configured before socket use",
        ));
    }
    let policy = if auth.is_callable() {
        PlainAuthenticator::Callback(auth.clone().unbind())
    } else {
        let credentials: Vec<(String, String)> = auth.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "set_plain_auth expects an iterable of (username, password) pairs or a callable",
            )
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
    inner.overlay.lock().unwrap().plain_authenticator = Some(policy);
    Ok(())
}

/// Shared implementation for `Socket::set_curve_auth` and
/// `AsyncSocket::set_curve_auth`.
pub(crate) fn set_curve_auth_impl(inner: &SocketInner, auth: &Bound<'_, PyAny>) -> PyResult<()> {
    let mut ov = inner.overlay.lock().unwrap();
    if auth.is_none() {
        ov.curve_authenticator = None;
        return Ok(());
    }
    if auth.is_callable() {
        ov.curve_authenticator = Some(CurveAuthenticator::Callback(auth.clone().unbind()));
        return Ok(());
    }
    let iter = auth.try_iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "set_curve_auth expects an iterable of Z85 keys, a callable, or None",
        )
    })?;
    let mut keys = HashSet::new();
    for item in iter {
        let item = item?;
        let z85_bytes: &[u8] = item.extract()?;
        let z85_str = std::str::from_utf8(z85_bytes)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("key must be valid Z85 ASCII"))?;
        let pk = omq_tokio::CurvePublicKey::from_z85(z85_str)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        keys.insert(*pk.as_bytes());
    }
    ov.curve_authenticator = Some(CurveAuthenticator::AllowedKeys(keys));
    Ok(())
}
