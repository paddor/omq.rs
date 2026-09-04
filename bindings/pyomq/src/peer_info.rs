use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Peer information passed to a Python authenticator callback.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerInfo {
    public_key: Py<PyBytes>,
    identity: Option<Py<PyBytes>>,
    peer_address: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

#[pymethods]
impl PeerInfo {
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
        }
    }
}
