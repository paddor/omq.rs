//! Typed socket options, following ext-zmq's per-option PHP types.

use std::ffi::{c_int, c_void};

use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendStr, Zval};

use crate::errors::{socket_error, socket_misuse};
use crate::util::{c_int_arg, with_bytes};

const ZMQ_AFFINITY: c_int = 4;
const ZMQ_FD: c_int = 14;
const ZMQ_MAXMSGSIZE: c_int = 22;
const ZMQ_SNDHWM: c_int = 23;
const ZMQ_RCVHWM: c_int = 24;
/// ext-zmq's alias that sets both high-water marks.
const SOCKOPT_HWM: c_int = 2001;
const STRING_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Int,
    Int64,
    Uint64,
    /// Raw bytes, returned as is: identities and subscriptions.
    Bytes,
    /// A C string, returned without its terminator.
    Text,
}

fn kind(key: c_int) -> Kind {
    match key {
        ZMQ_AFFINITY => Kind::Uint64,
        ZMQ_MAXMSGSIZE => Kind::Int64,
        5..=7 | 61 | 72 => Kind::Bytes,
        32 | 38 | 45 | 46 | 48..=50 | 55 | 63 | 64 | 68 | 92 => Kind::Text,
        _ => Kind::Int,
    }
}

fn long(value: &Zval) -> PhpResult<i64> {
    if let Some(long) = value.long() {
        return Ok(long);
    }

    value
        .coerce_to_string()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| socket_misuse("Expected an integer option value"))
}

fn set_raw(socket: *mut c_void, key: c_int, value: *const c_void, len: usize) -> PhpResult<()> {
    if omq_zmq::zmq_setsockopt(socket, key, value, len) != 0 {
        return Err(socket_error("Failed to set socket option"));
    }

    Ok(())
}

fn set_int(socket: *mut c_void, key: c_int, value: c_int) -> PhpResult<()> {
    set_raw(socket, key, (&raw const value).cast(), size_of::<c_int>())
}

/// `ZMQSocket::setSockOpt`.
pub(crate) fn set(socket: *mut c_void, key: i64, value: &Zval) -> PhpResult<()> {
    let key = c_int_arg(key, "key")?;
    match key {
        SOCKOPT_HWM => {
            let hwm = c_int_arg(long(value)?, "value")?;
            set_int(socket, ZMQ_SNDHWM, hwm)?;
            return set_int(socket, ZMQ_RCVHWM, hwm);
        }
        ZMQ_FD => return Err(socket_misuse("ZMQ::SOCKOPT_FD is read-only")),
        _ => {}
    }

    match kind(key) {
        Kind::Int => set_int(socket, key, c_int_arg(long(value)?, "value")?),
        Kind::Int64 => {
            let value = long(value)?;
            set_raw(socket, key, (&raw const value).cast(), size_of::<i64>())
        }
        Kind::Uint64 => {
            let value =
                u64::try_from(long(value)?).map_err(|_| socket_misuse("value is negative"))?;
            set_raw(socket, key, (&raw const value).cast(), size_of::<u64>())
        }
        Kind::Bytes | Kind::Text => with_bytes(value, |bytes| {
            set_raw(socket, key, bytes.as_ptr().cast(), bytes.len())
        })?,
    }
}

fn get_raw(socket: *mut c_void, key: c_int, value: *mut c_void, len: &mut usize) -> PhpResult<()> {
    if omq_zmq::zmq_getsockopt(socket, key, value, len) != 0 {
        return Err(socket_error("Failed to get socket option"));
    }

    Ok(())
}

fn get_bytes(socket: *mut c_void, key: c_int, text: bool) -> PhpResult<Zval> {
    let mut buffer = [0_u8; STRING_CAPACITY];
    let mut len = buffer.len();
    get_raw(socket, key, buffer.as_mut_ptr().cast(), &mut len)?;
    let mut bytes = &buffer[..len.min(buffer.len())];
    if text && let Some(stripped) = bytes.strip_suffix(&[0]) {
        bytes = stripped;
    }

    let mut zval = Zval::new();
    zval.set_zend_string(ZendStr::new(bytes, false));
    Ok(zval)
}

/// `ZMQSocket::getSockOpt`.
pub(crate) fn get(socket: *mut c_void, key: i64) -> PhpResult<Zval> {
    let key = c_int_arg(key, "key")?;
    if key == ZMQ_FD {
        return crate::fd_stream::socket_fd_stream(socket);
    }

    let key = if key == SOCKOPT_HWM { ZMQ_SNDHWM } else { key };
    let mut zval = Zval::new();
    match kind(key) {
        Kind::Int => {
            let mut value: c_int = 0;
            let mut len = size_of::<c_int>();
            get_raw(socket, key, (&raw mut value).cast(), &mut len)?;
            zval.set_long(i64::from(value));
        }
        Kind::Int64 => {
            let mut value: i64 = 0;
            let mut len = size_of::<i64>();
            get_raw(socket, key, (&raw mut value).cast(), &mut len)?;
            zval.set_long(value);
        }
        Kind::Uint64 => {
            let mut value: u64 = 0;
            let mut len = size_of::<u64>();
            get_raw(socket, key, (&raw mut value).cast(), &mut len)?;
            zval.set_long(i64::try_from(value).unwrap_or(i64::MAX));
        }
        Kind::Bytes => return get_bytes(socket, key, false),
        Kind::Text => return get_bytes(socket, key, true),
    }

    Ok(zval)
}
