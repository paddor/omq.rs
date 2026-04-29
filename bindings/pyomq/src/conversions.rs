//! Bytes <-> Message conversion. Hot path; avoid copies.

use bytes::Bytes;
use omq_proto::message::{Message, Payload};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

/// Owner that holds a Python `bytes` object alive while exposing its
/// backing storage as `&[u8]`. Lets us construct `bytes::Bytes` via
/// `Bytes::from_owner(...)` without copying the payload, since
/// Python's `bytes` storage is stable for the object's lifetime.
///
/// SAFETY:
/// - `bytes` in Python is immutable, so the buffer pointer is stable.
/// - `Py<PyBytes>` is `Send + Sync` (it's just a refcounted handle;
///   actual access requires the GIL but we never re-touch the Python
///   object after construction).
/// - The captured `ptr` and `len` come from `as_bytes()` under the
///   GIL at construction time, and remain valid as long as the
///   `Py<PyBytes>` keeps the object alive.
struct PyBytesOwner {
    _py_bytes: Py<PyBytes>,
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for PyBytesOwner {}
unsafe impl Sync for PyBytesOwner {}

impl PyBytesOwner {
    fn from_pybytes(b: &Bound<'_, PyBytes>) -> Self {
        let s = b.as_bytes();
        Self {
            ptr: s.as_ptr(),
            len: s.len(),
            _py_bytes: b.clone().unbind(),
        }
    }
}

impl AsRef<[u8]> for PyBytesOwner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Build a `Bytes` from a Python `bytes`-like object. Uses the
/// zero-copy `Bytes::from_owner` path when the input is an immutable
/// `bytes`; falls back to `Bytes::copy_from_slice` for `bytearray` /
/// `memoryview` / other buffer-protocol types whose backing storage
/// might be mutable or transient.
pub fn bytes_from_pyany(b: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(pb) = b.downcast::<PyBytes>() {
        return Ok(Bytes::from_owner(PyBytesOwner::from_pybytes(pb)));
    }
    let view: &[u8] = b.extract()?;
    Ok(Bytes::copy_from_slice(view))
}

/// Build a multipart `Message` from a Python list/tuple of bytes-like.
pub fn message_from_pylist(parts: &Bound<'_, PyAny>) -> PyResult<Message> {
    let mut msg = Message::new();
    let it = parts.iter()?;
    for part in it {
        let part: Bound<'_, PyAny> = part?;
        msg.push_part(Payload::from_bytes(bytes_from_pyany(&part)?));
    }
    Ok(msg)
}

/// Return a Python list of bytes - one per message frame.
pub fn parts_to_pylist<'py>(py: Python<'py>, msg: Message) -> Bound<'py, PyList> {
    let parts = msg.into_parts();
    let items: Vec<Bound<'py, PyBytes>> =
        parts.into_iter().map(|p| PyBytes::new_bound(py, &p.coalesce())).collect();
    PyList::new_bound(py, items)
}
