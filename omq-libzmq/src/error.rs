//! Thread-local errno and error code definitions.

use std::cell::Cell;
use std::ffi::c_int;

thread_local! {
    static ERRNO: Cell<c_int> = const { Cell::new(0) };
}

pub(crate) fn set_errno(e: c_int) {
    ERRNO.with(|c| c.set(e));
}

pub(crate) fn fail(e: c_int) -> c_int {
    set_errno(e);
    -1
}

// ZMQ-specific error codes (HAUSNUMERO = 156384712).
pub(crate) const ENOTSUP: c_int = 156_384_713;
pub(crate) const EPROTONOSUPPORT: c_int = 156_384_714;
pub(crate) const ENOBUFS: c_int = 156_384_715;
pub(crate) const ENETDOWN: c_int = 156_384_716;
pub(crate) const EADDRINUSE: c_int = 156_384_717;
pub(crate) const EADDRNOTAVAIL: c_int = 156_384_718;
pub(crate) const ECONNREFUSED: c_int = 156_384_719;
pub(crate) const EINPROGRESS: c_int = 156_384_720;
pub(crate) const ENOTSOCK: c_int = 156_384_721;
pub(crate) const EMSGSIZE: c_int = 156_384_722;
pub(crate) const ETERM: c_int = 156_384_765;
pub(crate) const EFSM: c_int = 156_384_763;
pub(crate) const ENOCOMPATPROTO: c_int = 156_384_764;

#[unsafe(no_mangle)]
pub extern "C" fn zmq_errno() -> c_int {
    ERRNO.with(std::cell::Cell::get)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_strerror(errnum: c_int) -> *const libc::c_char {
    match errnum {
        ETERM => c"Context was terminated".as_ptr(),
        ENOTSUP => c"Operation not supported".as_ptr(),
        EFSM => c"Operation cannot be accomplished in current state".as_ptr(),
        ENOCOMPATPROTO => c"The protocol is not compatible with the socket type".as_ptr(),
        ENOTSOCK => c"Not a socket".as_ptr(),
        EMSGSIZE => c"Message too large".as_ptr(),
        EADDRINUSE => c"Address already in use".as_ptr(),
        EADDRNOTAVAIL => c"Address not available".as_ptr(),
        ECONNREFUSED => c"Connection refused".as_ptr(),
        EINPROGRESS => c"Operation in progress".as_ptr(),
        ENOBUFS => c"No buffer space available".as_ptr(),
        ENETDOWN => c"Network is down".as_ptr(),
        EPROTONOSUPPORT => c"Protocol not supported".as_ptr(),
        // SAFETY: libc::strerror is safe for any c_int; returns a static string.
        _ => unsafe { libc::strerror(errnum) },
    }
}

pub(crate) fn map_io_err(e: &std::io::Error) -> c_int {
    if let Some(raw) = e.raw_os_error() {
        return raw;
    }
    match e.kind() {
        std::io::ErrorKind::AddrInUse => EADDRINUSE,
        std::io::ErrorKind::ConnectionRefused => ECONNREFUSED,
        std::io::ErrorKind::TimedOut => libc::ETIMEDOUT,
        std::io::ErrorKind::WouldBlock => libc::EAGAIN,
        _ => libc::EIO,
    }
}

pub(crate) fn map_omq_err(e: &omq_tokio::error::Error) -> c_int {
    use omq_tokio::error::Error;
    match e {
        Error::WouldBlock | Error::Timeout => libc::EAGAIN,
        Error::Closed => ETERM,
        Error::InvalidEndpoint(_) | Error::Config(_) => libc::EINVAL,
        Error::UnsupportedScheme(_) | Error::HandshakeFailed(_) | Error::Protocol(_) => {
            EPROTONOSUPPORT
        }
        Error::Unroutable => libc::EHOSTUNREACH,
        Error::MessageTooLarge { .. } => EMSGSIZE,
        Error::Io(io_e) => map_io_err(io_e),
        _ => libc::EIO,
    }
}
