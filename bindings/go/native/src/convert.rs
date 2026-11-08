//! Foreign values and scalar option conversion.

use omq_tokio::{Endpoint, Error, SocketType};
use std::ffi::{CStr, CString, c_char};
use std::slice;
use std::str::FromStr;
use std::time::Duration;

pub(crate) fn string_to_raw(value: String) -> *mut c_char {
    let value = value.replace('\0', "\\0");
    CString::new(value).expect("nul removed").into_raw()
}

pub(crate) fn str_from_c(ptr: *const c_char) -> Result<&'static str, Error> {
    if ptr.is_null() {
        return Err(Error::Config("null string pointer".to_string()));
    }
    let value = unsafe { CStr::from_ptr(ptr) };
    value
        .to_str()
        .map_err(|error| Error::Config(format!("invalid UTF-8 string: {error}")))
}

pub(crate) fn bytes_from_c<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Error::Config("null byte pointer".to_string()));
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

pub(crate) fn bytes_from_c_mut<'a>(ptr: *mut u8, len: usize) -> Result<&'a mut [u8], Error> {
    if len == 0 {
        return Ok(&mut []);
    }
    if ptr.is_null() {
        return Err(Error::Config("null byte pointer".to_string()));
    }
    Ok(unsafe { slice::from_raw_parts_mut(ptr, len) })
}

pub(crate) fn endpoint_from_c(ptr: *const c_char) -> Result<Endpoint, Error> {
    Endpoint::from_str(str_from_c(ptr)?)
}

pub(crate) fn socket_type_from_i32(value: i32) -> Result<SocketType, Error> {
    match value {
        1 => Ok(SocketType::Pair),
        2 => Ok(SocketType::Pub),
        3 => Ok(SocketType::Sub),
        4 => Ok(SocketType::Req),
        5 => Ok(SocketType::Rep),
        6 => Ok(SocketType::Dealer),
        7 => Ok(SocketType::Router),
        8 => Ok(SocketType::Pull),
        9 => Ok(SocketType::Push),
        10 => Ok(SocketType::XPub),
        11 => Ok(SocketType::XSub),
        12 => Ok(SocketType::Stream),
        13 => Ok(SocketType::Server),
        14 => Ok(SocketType::Client),
        15 => Ok(SocketType::Radio),
        16 => Ok(SocketType::Dish),
        17 => Ok(SocketType::Gather),
        18 => Ok(SocketType::Scatter),
        19 => Ok(SocketType::Peer),
        20 => Ok(SocketType::Channel),
        _ => Err(Error::Config(format!("unknown socket type {value}"))),
    }
}

pub(crate) fn duration_from_timeout_millis(timeout_millis: i64) -> Option<Duration> {
    if timeout_millis < 0 {
        None
    } else {
        Some(Duration::from_millis(timeout_millis as u64))
    }
}

pub(crate) fn duration_from_millis(millis: i64) -> Result<Duration, Error> {
    if millis < 0 {
        return Err(Error::Config("duration must be non-negative".to_string()));
    }
    Ok(Duration::from_millis(millis as u64))
}

pub(crate) fn optional_duration_from_millis(millis: i64) -> Result<Option<Duration>, Error> {
    if millis < 0 {
        Ok(None)
    } else {
        duration_from_millis(millis).map(Some)
    }
}

pub(crate) fn optional_usize_from_i64(name: &str, value: i64) -> Result<Option<usize>, Error> {
    if value < 0 {
        Ok(None)
    } else {
        usize::try_from(value)
            .map(Some)
            .map_err(|_| Error::Config(format!("{name} exceeds usize")))
    }
}

pub(crate) fn linger_from_millis(millis: i64) -> Option<Duration> {
    if millis < 0 {
        None
    } else {
        Some(Duration::from_millis(millis as u64))
    }
}

pub(crate) fn duration_from_nonnegative_millis(millis: i64) -> Result<Duration, Error> {
    if millis < 0 {
        return Err(Error::Config("timeout must be non-negative".to_string()));
    }
    Ok(Duration::from_millis(millis as u64))
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_string_free(value: *mut c_char) {
    if value.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(value));
    }
}
