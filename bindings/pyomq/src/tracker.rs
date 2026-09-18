//! Completion follows buffer ownership, never queue admission or peer delivery.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pyo3::prelude::*;

#[derive(Debug, Default)]
pub(crate) struct Completion {
    done: Mutex<bool>,
    changed: Condvar,
}

impl Completion {
    fn finish(&self) {
        *self.done.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

#[pyclass(frozen, module = "pyomq._native")]
#[derive(Debug)]
pub(crate) struct ReleaseToken(Arc<Completion>);

#[pymethods]
impl ReleaseToken {
    #[getter]
    fn done(&self) -> bool {
        *self.0.done.lock().unwrap()
    }

    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout = timeout
            .map(Duration::try_from_secs_f64)
            .transpose()
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("invalid timeout"))?;
        Ok(py.detach(|| {
            let guard = self.0.done.lock().unwrap();
            let guard = if let Some(timeout) = timeout {
                self.0
                    .changed
                    .wait_timeout_while(guard, timeout, |done| !*done)
                    .unwrap()
                    .0
            } else {
                self.0.changed.wait_while(guard, |done| !*done).unwrap()
            };
            *guard
        }))
    }
}

struct FinishOnDrop(Arc<Completion>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0.finish();
    }
}

// Fields drop in declaration order: release the exporter before notifying waiters.
struct TrackedBytes {
    data: Bytes,
    _finish: FinishOnDrop,
}

impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

pub(crate) fn track(py: Python<'_>, data: Bytes) -> PyResult<(Bytes, Py<PyAny>)> {
    let (data, token) = track_buffer(py, data)?;
    Ok((data, from_source(py, token.into_any())?))
}

/// Multipart conversion collects tokens before building one Python tracker.
pub(crate) fn track_buffer(py: Python<'_>, data: Bytes) -> PyResult<(Bytes, Py<ReleaseToken>)> {
    let state = Arc::new(Completion::default());
    let token = Py::new(py, ReleaseToken(state.clone()))?;
    Ok((
        Bytes::from_owner(TrackedBytes {
            data,
            _finish: FinishOnDrop(state),
        }),
        token,
    ))
}

pub(crate) fn from_source(py: Python<'_>, source: Py<PyAny>) -> PyResult<Py<PyAny>> {
    if !source.bind(py).is_instance_of::<ReleaseToken>() {
        // Sending an existing Frame must return its original tracker.
        return Ok(source);
    }
    Ok(py
        .import("pyomq._tracker")?
        .getattr("_from_sources")?
        .call1((source,))?
        .unbind())
}

pub(crate) fn finished(py: Python<'_>) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("pyomq._tracker")?
        .getattr("_FINISHED_TRACKER")?
        .unbind())
}

pub(crate) fn aggregate(py: Python<'_>, trackers: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
    if trackers.is_empty() {
        return Ok(None);
    }
    if trackers.len() == 1 {
        return trackers
            .into_iter()
            .next()
            .map(|source| from_source(py, source))
            .transpose();
    }
    let args = pyo3::types::PyTuple::new(py, trackers)?;
    Ok(Some(
        py.import("pyomq._tracker")?
            .getattr("_from_sources")?
            .call1(args)?
            .unbind(),
    ))
}
