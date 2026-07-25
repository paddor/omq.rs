//! Python Frame/Message object backed by `bytes::Bytes`.

use bytes::Bytes;
use pyo3::basic::CompareOp;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyMemoryView};

#[pyclass(module = "pyomq._native")]
pub struct Frame {
    data: Bytes,
    more: bool,
}

impl Frame {
    pub(crate) fn from_bytes(data: Bytes) -> Self {
        Self { data, more: false }
    }

    pub(crate) fn from_bytes_more(data: Bytes, more: bool) -> Self {
        Self { data, more }
    }

    pub(crate) fn bytes_clone(&self) -> Bytes {
        self.data.clone()
    }
}

#[pymethods]
impl Frame {
    #[new]
    #[pyo3(signature = (data=None, track=false, copy=None, copy_threshold=None))]
    fn new(
        data: Option<&Bound<'_, PyAny>>,
        track: bool,
        copy: Option<bool>,
        copy_threshold: Option<usize>,
    ) -> PyResult<Self> {
        let _ = (track, copy, copy_threshold);
        match data {
            Some(data) => Ok(Self::from_bytes(crate::conversions::bytes_from_pyany(
                data,
            )?)),
            None => Ok(Self::from_bytes(Bytes::new())),
        }
    }

    #[getter]
    fn bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    #[getter]
    fn buffer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMemoryView>> {
        let bytes = PyBytes::new(py, &self.data).into_any();
        PyMemoryView::from(&bytes)
    }

    #[getter]
    fn more(&self) -> bool {
        self.more
    }

    #[getter]
    fn tracker<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        py.None().bind(py).clone()
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    fn __len__(&self) -> usize {
        self.data.len()
    }

    fn __bool__(&self) -> bool {
        !self.data.is_empty()
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let bytes_repr = PyBytes::new(py, &self.data).repr()?.to_string();
        Ok(format!("<zmq.Frame({bytes_repr})>"))
    }

    fn __richcmp__<'py>(
        &self,
        py: Python<'py>,
        other: &Bound<'py, PyAny>,
        op: CompareOp,
    ) -> PyResult<Bound<'py, PyAny>> {
        if matches!(op, CompareOp::Eq | CompareOp::Ne)
            && let Ok(other_frame) = other.cast::<Self>()
        {
            let eq = self.data == other_frame.borrow().data;
            let result = if matches!(op, CompareOp::Eq) { eq } else { !eq };
            return Ok(PyBool::new(py, result).to_owned().into_any());
        }
        Ok(py.NotImplemented().bind(py).clone())
    }
}
