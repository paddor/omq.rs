//! Bytes <-> Message conversion. Hot path; avoid copies.

use bytes::Bytes;
use omq_proto::message::Message;
use pyo3::buffer::PyUntypedBuffer;
use pyo3::exceptions::{PyBufferError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyList};

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
    // The export pins the allocation. The critical section also prevents a
    // free-threaded exporter from mutating its bytes while we copy them.
    Ok(pyo3::sync::critical_section::with_critical_section(
        b,
        || {
            // SAFETY: the buffer is contiguous and remains exported for this copy.
            let view = unsafe {
                std::slice::from_raw_parts(buffer.buf_ptr().cast::<u8>(), buffer.len_bytes())
            };
            Bytes::copy_from_slice(view)
        },
    ))
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

/// Copy one received frame directly into a writable Python buffer.
pub fn copy_into_pybuffer(
    target: &Bound<'_, PyAny>,
    data: &[u8],
    nbytes: usize,
) -> PyResult<usize> {
    if let Ok(bytearray) = target.cast::<PyByteArray>() {
        pyo3::sync::critical_section::with_critical_section(bytearray, || {
            let limit = if nbytes == 0 {
                bytearray.len()
            } else {
                nbytes.min(bytearray.len())
            };
            let copy_len = data.len().min(limit);
            // SAFETY: the bytearray's critical section prevents concurrent
            // resize or access while the mutable slice is live.
            (unsafe { bytearray.as_bytes_mut() })[..copy_len].copy_from_slice(&data[..copy_len]);
        });
        return Ok(data.len());
    }
    let buffer = PyUntypedBuffer::get(target)?;
    if buffer.readonly() {
        return Err(PyTypeError::new_err(
            "recv_into() requires a writable buffer",
        ));
    }
    if !buffer.is_c_contiguous() {
        return Err(PyBufferError::new_err(
            "recv_into() requires a contiguous buffer",
        ));
    }
    let limit = if nbytes == 0 {
        buffer.len_bytes()
    } else {
        nbytes.min(buffer.len_bytes())
    };
    let copy_len = data.len().min(limit);
    pyo3::sync::critical_section::with_critical_section(target, || {
        // SAFETY: the exported buffer is writable and C-contiguous, and both
        // source and destination remain valid for the duration of this copy.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.buf_ptr().cast::<u8>(), copy_len);
        }
    });
    Ok(data.len())
}

/// Build a multipart `Message` from a Python iterable of bytes-like values.
pub fn message_from_pyiterable(
    parts: &Bound<'_, PyAny>,
    copy: bool,
    track: bool,
) -> PyResult<(Message, Option<Py<PyAny>>)> {
    let it = parts.try_iter()?;
    let mut collected = Vec::new();
    let mut routing_id = None;
    let mut trackers = Vec::new();
    for part in it {
        let part = part?;
        let part_routing_id = routing_id_from_pyany(&part);
        if part_routing_id != 0 {
            if routing_id.is_some_and(|current| current != part_routing_id) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "multipart frames have conflicting routing IDs",
                ));
            }
            routing_id = Some(part_routing_id);
        }
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
    let message = match routing_id {
        Some(id) => message.with_routing_id(id),
        None => message,
    };
    let tracker = if track && !copy && trackers.is_empty() {
        Some(crate::tracker::finished(parts.py())?)
    } else {
        crate::tracker::aggregate(parts.py(), trackers)?
    };
    Ok((message, tracker))
}

/// Build the single grouped body accepted by a RADIO socket.
pub fn radio_message_from_pyiterable(
    parts: &Bound<'_, PyAny>,
    copy: bool,
    track: bool,
) -> PyResult<(Message, Option<Py<PyAny>>)> {
    let mut parts = parts.try_iter()?;
    let part = parts
        .next()
        .transpose()?
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("RADIO requires one body"))?;
    if parts.next().transpose()?.is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "RADIO requires exactly one message part",
        ));
    }
    let group = group_from_pyany(&part).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(
            "RADIO requires a group; pass group= or set Frame.group",
        )
    })?;
    let (body, tracker) = payload_with_tracker(&part, copy, track)?;
    Ok((Message::with_group(group, body), tracker))
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
