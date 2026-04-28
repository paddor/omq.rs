//! pyomq native module. The Python-facing surface is in `python/pyomq/`;
//! this module exposes the native classes that re-export it imports.

mod constants;
mod context;
mod conversions;
mod error;
mod options;
mod runtime;
mod socket;
mod socket_async;

use pyo3::prelude::*;

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    constants::register(m)?;
    error::register(py, m)?;
    m.add_class::<context::Context>()?;
    m.add_class::<context::AsyncContext>()?;
    m.add_class::<socket::Socket>()?;
    m.add_class::<socket_async::AsyncSocket>()?;
    m.add_function(wrap_pyfunction!(backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}

#[pyfunction]
fn backend_name() -> &'static str {
    "compio"
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
