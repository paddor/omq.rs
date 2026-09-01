//! Bytes <-> Message conversion. Hot path; avoid copies.

use bytes::Bytes;
use omq_proto::message::Message;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use crate::frame::Frame;

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

/// Owner that holds a Python buffer export alive while exposing its
/// backing storage as `&[u8]`.
///
/// SAFETY:
/// - `PyBuffer<u8>` pins the exporter according to Python's buffer
///   protocol until release/drop.
/// - We only construct this for C-contiguous `u8` buffers.
/// - `copy=False` callers are responsible for not mutating the backing
///   object until the send completes, matching PyZMQ's zero-copy contract.
struct PyBufferOwner {
    _buffer: PyBuffer<u8>,
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for PyBufferOwner {}
unsafe impl Sync for PyBufferOwner {}

impl AsRef<[u8]> for PyBufferOwner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Build a `Bytes` from a Python bytes-like object. Immutable `bytes`
/// use zero-copy ownership. Buffer-protocol objects copy by default,
/// and use zero-copy ownership for C-contiguous `u8` buffers only when
/// the caller requested `copy=False`.
pub fn bytes_from_pyany(b: &Bound<'_, PyAny>, copy: bool) -> PyResult<Bytes> {
    if let Ok(frame) = b.cast::<Frame>() {
        return Ok(frame.borrow().bytes_clone());
    }
    if let Ok(pb) = b.cast::<PyBytes>() {
        return Ok(Bytes::from_owner(PyBytesOwner::from_pybytes(pb)));
    }
    if let Ok(buffer) = PyBuffer::<u8>::get(b) {
        if !copy && buffer.as_slice(b.py()).is_some() {
            return Ok(Bytes::from_owner(PyBufferOwner {
                ptr: buffer.buf_ptr().cast(),
                len: buffer.len_bytes(),
                _buffer: buffer,
            }));
        }
        return Ok(Bytes::from(buffer.to_vec(b.py())?));
    }
    let view: &[u8] = b.extract()?;
    Ok(Bytes::copy_from_slice(view))
}

pub fn routing_id_from_pyany(b: &Bound<'_, PyAny>) -> u32 {
    b.cast::<Frame>()
        .map(|frame| frame.borrow().routing_id_value())
        .unwrap_or(0)
}

/// Build a multipart `Message` from a Python list/tuple of bytes-like.
pub fn message_from_pylist(parts: &Bound<'_, PyAny>, copy: bool) -> PyResult<Message> {
    let it = parts.try_iter()?;
    let mut collected = Vec::new();
    let mut routing_id = 0;
    for part in it {
        let part = part?;
        routing_id = routing_id.max(routing_id_from_pyany(&part));
        collected.push(bytes_from_pyany(&part, copy)?);
    }
    let message = match collected.len() {
        0 => Message::new(),
        1 => Message::single(collected.into_iter().next().unwrap()),
        _ => Message::multipart(collected),
    };
    Ok(if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id)
    })
}

/// Return a Python list of bytes - one per message frame.
pub fn parts_to_pylist<'py>(py: Python<'py>, msg: Message) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, msg.iter().map(|b| PyBytes::new(py, &b)))
}

pub fn frames_to_pylist<'py>(py: Python<'py>, parts: Vec<Bytes>) -> PyResult<Bound<'py, PyList>> {
    frames_to_pylist_routed(py, parts, 0)
}

fn frames_to_pylist_routed<'py>(
    py: Python<'py>,
    parts: Vec<Bytes>,
    routing_id: u32,
) -> PyResult<Bound<'py, PyList>> {
    let len = parts.len();
    let frames = parts
        .into_iter()
        .enumerate()
        .map(|(idx, part)| {
            Bound::new(
                py,
                Frame::from_bytes_more_routing(
                    part,
                    idx + 1 < len,
                    if idx == 0 { routing_id } else { 0 },
                ),
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, frames)
}

pub fn message_to_frame_list<'py>(py: Python<'py>, msg: Message) -> PyResult<Bound<'py, PyList>> {
    let routing_id = msg.routing_id().unwrap_or(0);
    frames_to_pylist_routed(py, msg.iter().collect(), routing_id)
}
