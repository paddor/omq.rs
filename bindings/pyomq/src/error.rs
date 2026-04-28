//! ZMQError exception class + Error → errno mapping.

use omq_proto::error::Error;
use pyo3::create_exception;
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

create_exception!(_native, ZMQError, PyOSError, "Base class for pyomq errors.");

// libzmq's ETERM constant. POSIX errno 156 isn't standard so libzmq
// picks a value in the range that doesn't collide on supported OSes.
pub const ETERM: i32 = 156;

/// Map an `omq_proto::Error` to a `PyErr` carrying the right errno.
pub fn map_err(e: Error) -> PyErr {
    let (errno, msg) = match e {
        Error::Closed => (ETERM, "context terminated".to_string()),
        Error::Timeout => (libc::EAGAIN, "operation timed out".into()),
        Error::HandshakeFailed(m) => (libc::EPROTO, m),
        Error::Unroutable => (libc::EHOSTUNREACH, "host unreachable".into()),
        Error::Protocol(m) => (libc::EPROTO, m),
        Error::UnsupportedScheme(m) => (libc::EINVAL, m),
        Error::UnsupportedZmtpVersion { major, minor } => (
            libc::EPROTO,
            format!("unsupported ZMTP version {major}.{minor}"),
        ),
        Error::MessageTooLarge { size, max } => {
            (libc::EMSGSIZE, format!("message {size} > {max}"))
        }
        Error::InvalidEndpoint(m) => (libc::EINVAL, m),
        Error::IdentityCollision(_) => (libc::EADDRINUSE, "identity collision".into()),
        Error::Io(io) => (
            io.raw_os_error().unwrap_or(libc::EIO),
            io.to_string(),
        ),
        _ => (libc::EIO, "internal error".into()),
    };
    let py_err = ZMQError::new_err(msg);
    Python::with_gil(|py| {
        let inst = py_err.value_bound(py);
        let _ = inst.setattr("errno", errno);
    });
    py_err
}

pub fn timeout_err() -> PyErr {
    let e = ZMQError::new_err("operation would block");
    Python::with_gil(|py| {
        let inst = e.value_bound(py);
        let _ = inst.setattr("errno", libc::EAGAIN);
    });
    e
}

pub fn not_implemented(name: &str) -> PyErr {
    let e = ZMQError::new_err(format!("option {name} is not implemented in pyomq v0.1"));
    Python::with_gil(|py| {
        let inst = e.value_bound(py);
        let _ = inst.setattr("errno", libc::ENOSYS);
    });
    e
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ZMQError", py.get_type_bound::<ZMQError>())?;
    Ok(())
}
