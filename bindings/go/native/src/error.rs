//! Native error mapping and reporting.

use crate::convert::string_to_raw;
use crate::ffi::OmqGoStatus;
use omq_proto::TrySendError;
use omq_tokio::Error;
use std::ffi::CString;
use std::ptr;

pub(crate) const OK: i32 = 0;

pub(crate) const AGAIN: i32 = 1;

pub(crate) const CLOSED: i32 = 2;

pub(crate) const TIMEOUT: i32 = 3;

pub(crate) const CANCELED: i32 = 4;

const INVALID_ENDPOINT: i32 = 5;

const UNSUPPORTED_SCHEME: i32 = 6;

const PROTOCOL: i32 = 7;

pub(crate) const CONFIG: i32 = 8;

const IO: i32 = 9;

const UNROUTABLE: i32 = 10;

const MESSAGE_TOO_LARGE: i32 = 11;

pub(crate) const ERROR: i32 = 99;

impl OmqGoStatus {
    pub(crate) fn ok() -> Self {
        Self {
            code: OK,
            message: ptr::null_mut(),
        }
    }

    pub(crate) fn err(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: string_to_raw(message.into()),
        }
    }

    pub(crate) fn from_error(error: Error) -> Self {
        match error {
            Error::InvalidEndpoint(message) => Self::err(INVALID_ENDPOINT, message),
            Error::UnsupportedScheme(message) => Self::err(UNSUPPORTED_SCHEME, message),
            Error::UnsupportedZmtpVersion { major, minor } => Self::err(
                PROTOCOL,
                format!("unsupported ZMTP version: {major}.{minor}"),
            ),
            Error::Protocol(message) | Error::HandshakeFailed(message) => {
                Self::err(PROTOCOL, message)
            }
            Error::Closed => Self::err(CLOSED, "socket closed"),
            Error::Timeout => Self::err(TIMEOUT, "operation timed out"),
            Error::MessageTooLarge { size, max } => Self::err(
                MESSAGE_TOO_LARGE,
                format!("message too large: {size} bytes exceeds max {max}"),
            ),
            Error::Unroutable => Self::err(UNROUTABLE, "no route to peer"),
            Error::WouldBlock => Self::err(AGAIN, "operation would block"),
            Error::Config(message) => Self::err(CONFIG, message),
            Error::Io(error) => Self::err(IO, error.to_string()),
            _ => Self::err(ERROR, error.to_string()),
        }
    }

    pub(crate) fn from_try_send(error: TrySendError) -> Self {
        match error {
            TrySendError::Full(_) => Self::err(AGAIN, "send queue full"),
            TrySendError::Closed => Self::err(CLOSED, "socket closed"),
            TrySendError::Error(error) => Self::from_error(error),
        }
    }
}

pub(crate) fn status_code_from_error(error: &Error) -> i32 {
    match error {
        Error::InvalidEndpoint(_) => INVALID_ENDPOINT,
        Error::UnsupportedScheme(_) => UNSUPPORTED_SCHEME,
        Error::UnsupportedZmtpVersion { .. } | Error::Protocol(_) | Error::HandshakeFailed(_) => {
            PROTOCOL
        }
        Error::Closed => CLOSED,
        Error::Timeout => TIMEOUT,
        Error::MessageTooLarge { .. } => MESSAGE_TOO_LARGE,
        Error::Unroutable => UNROUTABLE,
        Error::WouldBlock => AGAIN,
        Error::Config(_) => CONFIG,
        Error::Io(_) => IO,
        _ => ERROR,
    }
}

pub(crate) fn status_from_result(result: Result<(), Error>) -> OmqGoStatus {
    match result {
        Ok(()) => OmqGoStatus::ok(),
        Err(error) => OmqGoStatus::from_error(error),
    }
}

pub(crate) fn empty_cstring() -> CString {
    CString::new("").expect("empty string contains no NUL")
}

pub(crate) fn cstring_lossy(message: &str) -> CString {
    let bytes: Vec<u8> = message
        .as_bytes()
        .iter()
        .copied()
        .filter(|byte| *byte != 0)
        .collect();
    CString::new(bytes).unwrap_or_else(|_| empty_cstring())
}
