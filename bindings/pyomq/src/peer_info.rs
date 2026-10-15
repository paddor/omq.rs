use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Peer information from an admission snapshot or an authenticator callback.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerInfo {
    public_key: Py<PyBytes>,
    identity: Option<Py<PyBytes>>,
    peer_address: Option<String>,
    username: Option<String>,
    password: Option<String>,
    snapshot: Option<omq_proto::Identity>,
}

#[pymethods]
impl PeerInfo {
    /// Locally verified facts; absent on pre-admission authentication callbacks.
    #[getter]
    fn authentication(&self) -> Option<PeerAuthentication> {
        self.snapshot
            .as_ref()
            .map(|id| PeerAuthentication(id.peer_info().authentication.clone()))
    }

    /// Transport details for diagnostics, not application identity or authorization.
    #[getter]
    fn diagnostics(&self) -> Option<PeerDiagnostics> {
        self.snapshot
            .as_ref()
            .map(|id| PeerDiagnostics(id.peer_info().diagnostics.clone()))
    }

    #[getter]
    fn public_key(&self, py: Python<'_>) -> Py<PyBytes> {
        self.public_key.clone_ref(py)
    }

    #[getter]
    fn identity(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.identity.as_ref().map(|id| id.clone_ref(py))
    }

    #[getter]
    fn peer_address(&self) -> Option<&str> {
        self.peer_address.as_deref()
    }

    #[getter]
    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    #[getter]
    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

impl PeerInfo {
    #[cfg(feature = "curve")]
    pub(crate) fn from_raw(
        py: Python<'_>,
        raw: &[u8; 32],
        identity: Option<&bytes::Bytes>,
    ) -> Self {
        let pk = omq_proto::CurvePublicKey::from_bytes(*raw);
        let z85 = pk.to_z85();
        Self {
            public_key: PyBytes::new(py, z85.as_bytes()).unbind(),
            identity: identity.map(|id| PyBytes::new(py, id).unbind()),
            peer_address: None,
            username: None,
            password: None,
            snapshot: None,
        }
    }

    #[cfg(feature = "plain")]
    pub(crate) fn from_plain(
        py: Python<'_>,
        username: Option<&str>,
        password: Option<&str>,
        peer_address: Option<&str>,
    ) -> Self {
        Self {
            public_key: PyBytes::new(py, b"").unbind(),
            identity: None,
            peer_address: peer_address.map(str::to_owned),
            username: username.map(str::to_owned),
            password: password.map(str::to_owned),
            snapshot: None,
        }
    }
}

/// Opaque sender handle retaining the peer snapshot from message admission.
#[pyclass(frozen, module = "pyomq._native")]
pub struct Identity(pub(crate) omq_proto::Identity);

#[pymethods]
impl Identity {
    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.as_ref())
    }

    #[getter]
    fn peer_info(&self, py: Python<'_>) -> PeerInfo {
        let peer = self.0.peer_info();
        PeerInfo {
            public_key: PyBytes::new(py, &curve_z85(&peer.authentication)).unbind(),
            identity: peer
                .peer_identity
                .as_ref()
                .map(|id| PyBytes::new(py, id).unbind()),
            peer_address: peer.peer_address.map(|address| address.to_string()),
            username: None,
            password: None,
            snapshot: Some(self.0.clone()),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut hash);
        hash.finish()
    }
}

fn curve_z85(auth: &omq_proto::PeerAuthentication) -> Vec<u8> {
    #[cfg(feature = "curve")]
    if let Some(omq_proto::PeerPublicKey::Curve(key)) = &auth.public_key {
        return omq_proto::CurvePublicKey::from_bytes(*key)
            .to_z85()
            .into_bytes();
    }
    let _ = auth;
    Vec::new()
}

/// Authentication facts verified locally, independent of peer-advertised metadata.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerAuthentication(std::sync::Arc<omq_proto::PeerAuthentication>);

#[pymethods]
impl PeerAuthentication {
    #[getter]
    fn mechanism(&self) -> Option<&str> {
        self.0
            .mechanism
            .as_ref()
            .and_then(|name| name.as_str().ok())
    }

    #[getter]
    fn public_key_type(&self) -> Option<&str> {
        match &self.0.public_key {
            Some(omq_proto::PeerPublicKey::Curve(_)) => Some("CURVE"),
            _ => None,
        }
    }

    /// Raw public key bytes (32 bytes for CURVE), not Z85 text.
    #[getter]
    fn public_key<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.0.public_key {
            Some(omq_proto::PeerPublicKey::Curve(key)) => Some(PyBytes::new(py, key)),
            _ => None,
        }
    }

    #[getter]
    fn user_id<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.0.user_id.as_ref().map(|value| PyBytes::new(py, value))
    }

    /// A fresh dictionary; changing it cannot change the snapshot.
    #[getter]
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let result = pyo3::types::PyDict::new(py);
        for (name, value) in &self.0.metadata {
            result.set_item(name, PyBytes::new(py, value))?;
        }
        Ok(result)
    }
}

/// Admission-time transport details; addresses can change after admission.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerDiagnostics(omq_proto::PeerDiagnostics);

#[pymethods]
impl PeerDiagnostics {
    #[getter]
    fn endpoint(&self) -> &str {
        &self.0.endpoint
    }

    #[getter]
    fn local_address(&self) -> Option<String> {
        self.0.local_address.as_ref().map(ToString::to_string)
    }

    #[getter]
    fn remote_address(&self) -> Option<String> {
        self.0.remote_address.as_ref().map(ToString::to_string)
    }
}

pub(crate) fn received_from<'py>(
    py: Python<'py>,
    mut message: omq_proto::Message,
    copy: bool,
) -> PyResult<(Identity, Bound<'py, pyo3::types::PyList>)> {
    let identity = message
        .pop_identity()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("message has no sender snapshot"))?;
    let parts = if copy {
        crate::conversions::parts_to_pylist(py, message)?
    } else {
        crate::conversions::message_to_frame_list(py, message)?
    };
    Ok((Identity(identity), parts))
}
