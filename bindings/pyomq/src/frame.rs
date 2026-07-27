//! Python Frame/Message object backed by `bytes::Bytes`.

use bytes::Bytes;
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyBufferError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyMemoryView};
use std::ffi::{c_int, c_void};

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
    fn buffer<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
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

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("view is null"));
        }
        if flags & ffi::PyBUF_WRITABLE == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("Frame is not writable"));
        }

        let frame = slf.borrow();
        let data = frame.data.as_ref();
        // SAFETY: `view` is non-null. `PyBuffer_FillInfo` stores a new
        // reference to `slf`, so `frame.data` outlives the exported view.
        // The C API takes `*mut c_void`, but the buffer is marked read-only
        // and `Bytes` is immutable.
        let result = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                data.as_ptr().cast::<c_void>().cast_mut(),
                data.len() as ffi::Py_ssize_t,
                1,
                flags,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(PyErr::fetch(slf.py()))
        }
    }
}
