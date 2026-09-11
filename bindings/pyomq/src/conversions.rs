//! Bytes <-> Message conversion. Hot path; avoid copies.

use bytes::Bytes;
use omq_proto::message::Message;
use pyo3::buffer::PyUntypedBuffer;
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
/// - `PyUntypedBuffer` pins the exporter according to Python's buffer
///   protocol until release/drop.
/// - We only construct this for contiguous buffers, viewed as raw bytes.
/// - `copy=False` callers are responsible for not mutating the backing
///   object until the send completes, matching PyZMQ's zero-copy contract.
struct PyBufferOwner {
    _buffer: PyUntypedBuffer,
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
/// and use zero-copy ownership for contiguous buffers only when
/// the caller requested `copy=False`.
pub fn bytes_from_pyany(b: &Bound<'_, PyAny>, copy: bool) -> PyResult<Bytes> {
    if let Ok(frame) = b.cast::<Frame>() {
        return Ok(frame.borrow().bytes_clone());
    }
    if let Ok(pb) = b.cast::<PyBytes>() {
        return Ok(Bytes::from_owner(PyBytesOwner::from_pybytes(pb)));
    }
    let buffer = PyUntypedBuffer::get(b)?;
    if !buffer.is_c_contiguous() && !buffer.is_fortran_contiguous() {
        return Err(pyo3::exceptions::PyBufferError::new_err(
            "buffer must be contiguous",
        ));
    }
    if buffer.len_bytes() == 0 {
        return Ok(Bytes::new());
    }
    if !copy {
        return Ok(Bytes::from_owner(PyBufferOwner {
            ptr: buffer.buf_ptr().cast(),
            len: buffer.len_bytes(),
            _buffer: buffer,
        }));
    }
    // SAFETY: the export pins a contiguous allocation; the GIL remains held
    // for this copy. Interpret raw bytes independently of element format.
    let view =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr().cast::<u8>(), buffer.len_bytes()) };
    Ok(Bytes::copy_from_slice(view))
}

pub(crate) fn payload_with_tracker(
    b: &Bound<'_, PyAny>,
    copy: bool,
    track: bool,
) -> PyResult<(Bytes, Option<Py<PyAny>>)> {
    let (data, source) = payload_with_completion(b, copy, track)?;
    let tracker = source
        .map(|source| crate::tracker::from_source(b.py(), source))
        .transpose()?;
    Ok((data, tracker))
}

/// Raw buffers contribute tokens; Frames contribute their existing trackers.
fn payload_with_completion(
    b: &Bound<'_, PyAny>,
    copy: bool,
    track: bool,
) -> PyResult<(Bytes, Option<Py<PyAny>>)> {
    if let Ok(frame) = b.cast::<Frame>() {
        let frame = frame.borrow();
        let tracker = frame.tracker_clone(b.py());
        if track && tracker.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Not a tracked message",
            ));
        }
        return Ok((frame.bytes_clone(), tracker));
    }
    let data = bytes_from_pyany(b, copy)?;
    if track && !copy {
        let (data, token) = crate::tracker::track_buffer(b.py(), data)?;
        Ok((data, Some(token.into_any())))
    } else {
        Ok((data, None))
    }
}

pub fn routing_id_from_pyany(b: &Bound<'_, PyAny>) -> u32 {
    b.cast::<Frame>()
        .map(|frame| frame.borrow().routing_id_value())
        .unwrap_or(0)
}

pub fn group_from_pyany(b: &Bound<'_, PyAny>) -> Option<String> {
    b.cast::<Frame>()
        .ok()
        .and_then(|frame| frame.borrow().group_value())
}

/// Build a multipart `Message` from a Python list/tuple of bytes-like.
pub fn message_from_pylist(
    parts: &Bound<'_, PyAny>,
    copy: bool,
    track: bool,
) -> PyResult<(Message, Option<Py<PyAny>>)> {
    let it = parts.try_iter()?;
    let mut collected = Vec::new();
    let mut routing_id = 0;
    let mut trackers = Vec::new();
    for part in it {
        let part = part?;
        routing_id = routing_id.max(routing_id_from_pyany(&part));
        let (data, tracker) = payload_with_completion(&part, copy, track)?;
        collected.push(data);
        if let Some(tracker) = tracker {
            trackers.push(tracker);
        }
    }
    let message = match collected.len() {
        0 => Message::new(),
        1 => Message::single(collected.into_iter().next().unwrap()),
        _ => Message::multipart(collected),
    };
    let message = if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id)
    };
    let tracker = if track && !copy && trackers.is_empty() {
        Some(crate::tracker::finished(parts.py())?)
    } else {
        crate::tracker::aggregate(parts.py(), trackers)?
    };
    Ok((message, tracker))
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
