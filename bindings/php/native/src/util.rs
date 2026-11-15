//! Value conversions and a scoped `zmq_msg_t`.

use std::ffi::{CString, c_int};

use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use ext_php_rs::exception::PhpException;
use ext_php_rs::zend::ce;

use crate::errors::socket_misuse;

/// Run `f` over the bytes of a PHP value: strings without a copy, other
/// scalars through PHP's string conversion.
pub(crate) fn with_bytes<T>(value: &Zval, f: impl FnOnce(&[u8]) -> T) -> PhpResult<T> {
    if let Some(string) = value.zend_str() {
        return Ok(f(string.as_bytes()));
    }
    if value.is_null() {
        return Ok(f(&[]));
    }

    // Scalars convert as PHP would; arrays, objects, and resources are a
    // TypeError, as with ext-zmq's string parameters.
    let string = value
        .is_scalar()
        .then(|| value.coerce_to_string())
        .flatten()
        .ok_or_else(|| {
            let kind = value.get_type();
            PhpException::new(
                format!("Argument must be of type string, {kind} given"),
                0,
                ce::type_error(),
            )
        })?;
    Ok(f(string.as_bytes()))
}

/// A PHP integer as a C int.
pub(crate) fn c_int_arg(value: i64, what: &str) -> PhpResult<c_int> {
    c_int::try_from(value).map_err(|_| socket_misuse(&format!("{what} is out of range")))
}

/// An endpoint for the C API; NUL bytes are rejected.
pub(crate) fn c_endpoint(dsn: &str) -> PhpResult<CString> {
    CString::new(dsn).map_err(|_| socket_misuse("Endpoint contains a NUL byte"))
}

const ZMQ_MSG_WORDS: usize = 64 / size_of::<usize>();

/// An initialized `zmq_msg_t`, closed on drop.
#[derive(Debug)]
pub(crate) struct Msg([usize; ZMQ_MSG_WORDS]);

impl Msg {
    pub(crate) fn new() -> Self {
        let mut msg = Self([0; ZMQ_MSG_WORDS]);
        // zmq_msg_init only fails for a null pointer.
        omq_zmq::zmq_msg_init(msg.as_mut_ptr());
        msg
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut omq_zmq::OmqMsgRepr {
        self.0.as_mut_ptr().cast()
    }

    fn as_ptr(&self) -> *const omq_zmq::OmqMsgRepr {
        self.0.as_ptr().cast()
    }

    pub(crate) fn more(&self) -> bool {
        omq_zmq::zmq_msg_more(self.as_ptr()) != 0
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        omq_zmq::zmq_msg_close(self.as_mut_ptr());
    }
}
