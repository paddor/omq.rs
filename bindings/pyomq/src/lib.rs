//! pyomq native module. The Python-facing surface is in `python/pyomq/`;
//! this module exposes the native classes that re-export it imports.

#[cfg(feature = "curve")]
mod auth;
mod constants;
mod context;
mod conversions;
mod dispatch;
mod error;
mod notify;
mod options;
#[cfg(feature = "curve")]
mod peer_info;
mod runtime;
mod socket;
mod socket_async;

use std::sync::Arc;

use pyo3::prelude::*;

/// Extractor that accepts either a sync `Socket` or an `AsyncSocket`,
/// yielding the shared `SocketInner`.
struct AnySocket(Arc<socket::SocketInner>);

impl<'py> FromPyObject<'py, 'py> for AnySocket {
    type Error = PyErr;

    fn extract(obj: pyo3::Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = obj.cast::<socket::Socket>() {
            return Ok(Self(s.borrow().inner.clone()));
        }
        if let Ok(s) = obj.cast::<socket_async::AsyncSocket>() {
            return Ok(Self(s.borrow().inner.clone()));
        }
        Err(pyo3::exceptions::PyTypeError::new_err(
            "expected a Socket or AsyncSocket",
        ))
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    socket::register_atfork();
    constants::register(m)?;
    error::register(py, m)?;
    m.add_class::<context::Context>()?;
    m.add_class::<context::AsyncContext>()?;
    m.add_class::<socket::Monitor>()?;
    m.add_class::<socket::Socket>()?;
    m.add_class::<socket_async::AsyncSocket>()?;
    m.add_function(wrap_pyfunction!(backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(wait_any, m)?)?;
    m.add_function(wrap_pyfunction!(native_proxy, m)?)?;
    m.add_function(wrap_pyfunction!(has_feature, m)?)?;
    #[cfg(feature = "curve")]
    m.add_class::<peer_info::PeerInfo>()?;
    #[cfg(feature = "curve")]
    {
        m.add_function(wrap_pyfunction!(curve_keypair, m)?)?;
        m.add_function(wrap_pyfunction!(curve_public, m)?)?;
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (sockets, timeout_ms=None))]
fn wait_any(
    py: Python<'_>,
    sockets: &Bound<'_, pyo3::types::PySequence>,
    timeout_ms: Option<u64>,
) -> PyResult<Vec<u64>> {
    let mut entries = Vec::with_capacity(sockets.len()?);
    for item in sockets.try_iter()? {
        let s = item?.extract::<AnySocket>()?;
        let id = s.0.ensure_id()?;
        entries.push((id, s.0.clone()));
    }
    Ok(py.detach(|| runtime::wait_any(entries, timeout_ms)))
}

#[pyfunction]
#[pyo3(signature = (frontend, backend, capture=None, control=None))]
fn native_proxy(
    py: Python<'_>,
    frontend: &Bound<'_, socket::Socket>,
    backend: &Bound<'_, socket::Socket>,
    capture: Option<&Bound<'_, socket::Socket>>,
    control: Option<&Bound<'_, socket::Socket>>,
) -> PyResult<()> {
    let fe = frontend.borrow().inner.clone();
    let be = backend.borrow().inner.clone();
    fe.ensure_blocking_id()?;
    be.ensure_blocking_id()?;
    let cap = match capture {
        Some(c) => {
            let inner = c.borrow().inner.clone();
            inner.ensure_blocking_id()?;
            Some(inner)
        }
        None => None,
    };
    let ctrl = match control {
        Some(c) => {
            let inner = c.borrow().inner.clone();
            inner.ensure_blocking_id()?;
            Some(inner)
        }
        None => None,
    };
    let fe_sock = fe.ensure_blocking_socket()?;
    let be_sock = be.ensure_blocking_socket()?;
    let cap_sock = cap
        .as_ref()
        .map(|inner| inner.ensure_blocking_socket())
        .transpose()?;
    let ctrl_sock = ctrl
        .as_ref()
        .map(|inner| inner.ensure_blocking_socket())
        .transpose()?;
    let ctx = fe.ctx.clone();
    py.detach(|| runtime::proxy_handles(&ctx, fe_sock, be_sock, cap_sock, ctrl_sock))
        .map_err(error::map_err)?;
    Ok(())
}

#[cfg(feature = "curve")]
#[pyfunction]
fn curve_keypair(py: Python<'_>) -> PyResult<(Bound<'_, PyAny>, Bound<'_, PyAny>)> {
    let kp = omq_proto::CurveKeypair::generate();
    let pub_z85 = kp.public.to_z85();
    let sec_z85 = kp.secret.to_z85();
    let pub_bytes = pyo3::types::PyBytes::new(py, pub_z85.as_bytes());
    let sec_bytes = pyo3::types::PyBytes::new(py, sec_z85.as_bytes());
    Ok((pub_bytes.into_any(), sec_bytes.into_any()))
}

#[cfg(feature = "curve")]
#[pyfunction]
fn curve_public<'py>(py: Python<'py>, secret_z85: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let secret_str = std::str::from_utf8(secret_z85).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("secret key must be valid UTF-8 Z85")
    })?;
    let sk = omq_proto::CurveSecretKey::from_z85(secret_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let pk = sk.derive_public();
    let pub_z85 = pk.to_z85();
    Ok(pyo3::types::PyBytes::new(py, pub_z85.as_bytes()).into_any())
}

#[pyfunction]
fn has_feature(name: &str) -> bool {
    match name {
        "ipc" | "inproc" => true,
        #[cfg(feature = "curve")]
        "curve" => true,
        #[cfg(feature = "plain")]
        "plain" => true,
        #[cfg(feature = "lz4")]
        "lz4" => true,
        _ => false,
    }
}

#[pyfunction]
fn backend_name() -> &'static str {
    "tokio"
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
