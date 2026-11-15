//! The `ZMQException` hierarchy and errno conversion.

use std::ffi::{CStr, c_int};

use ext_php_rs::class::RegisteredClass;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::zend::ce;

/// Base class of every exception the extension throws.
#[php_class]
#[php(name = "ZMQException")]
#[php(extends(ce = ce::exception, stub = "Exception"))]
#[derive(Debug, Default)]
pub struct ZmqException;

/// Context creation and option failures.
#[php_class]
#[php(name = "ZMQContextException")]
#[php(extends(ZmqException))]
#[php(flags = ClassFlags::Final)]
#[derive(Debug, Default)]
pub struct ZmqContextException;

/// Socket call failures; the code is the errno.
#[php_class]
#[php(name = "ZMQSocketException")]
#[php(extends(ZmqException))]
#[php(flags = ClassFlags::Final)]
#[derive(Debug, Default)]
pub struct ZmqSocketException;

/// `ZMQPoll` failures.
#[php_class]
#[php(name = "ZMQPollException")]
#[php(extends(ZmqException))]
#[php(flags = ClassFlags::Final)]
#[derive(Debug, Default)]
pub struct ZmqPollException;

/// `ZMQDevice` failures.
#[php_class]
#[php(name = "ZMQDeviceException")]
#[php(extends(ZmqException))]
#[php(flags = ClassFlags::Final)]
#[derive(Debug, Default)]
pub struct ZmqDeviceException;

/// The errno of the last failed `zmq_*` call on this thread.
pub(crate) fn last_errno() -> c_int {
    omq_zmq::zmq_errno()
}

fn strerror(code: c_int) -> String {
    let ptr = omq_zmq::zmq_strerror(code);
    if ptr.is_null() {
        return format!("errno {code}");
    }

    // SAFETY: zmq_strerror returns a static NUL-terminated string.
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

/// `"<what>: <strerror(code)>"` as an exception of class `T` with `code`.
pub(crate) fn exception<T: RegisteredClass>(what: &str, code: c_int) -> PhpException {
    PhpException::new(
        format!("{what}: {}", strerror(code)),
        code,
        T::get_metadata().ce(),
    )
}

/// A `ZMQSocketException` for the last errno.
pub(crate) fn socket_error(what: &str) -> PhpException {
    exception::<ZmqSocketException>(what, last_errno())
}

/// A `ZMQSocketException` without an errno, for invalid arguments.
pub(crate) fn socket_misuse(message: &str) -> PhpException {
    PhpException::new(
        message.to_owned(),
        0,
        ZmqSocketException::get_metadata().ce(),
    )
}
