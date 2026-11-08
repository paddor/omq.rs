//! Native error mapping and reporting.

use jni::JNIEnv;
use jni::objects::{JObject, JThrowable, JValue};
use omq_tokio::Error;
use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(crate) const RECV_RING_STATUS_OK: i32 = 0;

const RECV_RING_STATUS_TIMEOUT: i32 = 1;

pub(crate) const RECV_RING_STATUS_CLOSED: i32 = 2;

const RECV_RING_STATUS_INVALID_ENDPOINT: i32 = 3;

const RECV_RING_STATUS_PROTOCOL: i32 = 4;

pub(crate) const RECV_RING_STATUS_ERROR: i32 = 5;

const MAX_ERROR_MESSAGE_BYTES: usize = 4095;

thread_local! {
    static FFM_LAST_ERROR_CODE: Cell<i32> = const { Cell::new(RECV_RING_STATUS_OK) };
    static FFM_LAST_ERROR_MESSAGE: RefCell<CString> = RefCell::new(empty_cstring());
}

pub(crate) fn empty_cstring() -> CString {
    CString::new("").expect("empty string contains no NUL")
}

pub(crate) fn cstring_lossy(message: &str) -> CString {
    let mut bytes = Vec::with_capacity(message.len().min(MAX_ERROR_MESSAGE_BYTES));
    for ch in message.chars() {
        if ch == '\0' {
            continue;
        }
        let mut encoded = [0; 4];
        let chunk = ch.encode_utf8(&mut encoded).as_bytes();
        if bytes.len() + chunk.len() > MAX_ERROR_MESSAGE_BYTES {
            break;
        }
        bytes.extend_from_slice(chunk);
    }
    CString::new(bytes).unwrap_or_else(|_| empty_cstring())
}

pub(crate) fn set_ffm_last_error(code: i32, message: impl AsRef<str>) {
    FFM_LAST_ERROR_CODE.with(|slot| slot.set(code));
    FFM_LAST_ERROR_MESSAGE.with(|slot| *slot.borrow_mut() = cstring_lossy(message.as_ref()));
}

pub(crate) fn clear_ffm_last_error() {
    set_ffm_last_error(RECV_RING_STATUS_OK, "");
}

pub(crate) fn recv_ring_status(error: &Error) -> i32 {
    match error {
        Error::Timeout | Error::WouldBlock => RECV_RING_STATUS_TIMEOUT,
        Error::Closed => RECV_RING_STATUS_CLOSED,
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            RECV_RING_STATUS_INVALID_ENDPOINT
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            RECV_RING_STATUS_PROTOCOL
        }
        _ => RECV_RING_STATUS_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_last_error_code() -> i32 {
    FFM_LAST_ERROR_CODE.with(Cell::get)
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_java_last_error_message() -> i64 {
    FFM_LAST_ERROR_MESSAGE.with(|slot| slot.borrow().as_ptr() as i64)
}

pub(crate) fn guard<R>(
    env: &mut JNIEnv<'_>,
    default: R,
    body: impl FnOnce(&mut JNIEnv<'_>) -> R,
) -> R {
    match catch_unwind(AssertUnwindSafe(|| body(env))) {
        Ok(value) => value,
        Err(_) => {
            throw_java(env, "io/omq/OMQException", "native OMQ panic");
            default
        }
    }
}

pub(crate) fn throw_java(env: &mut JNIEnv<'_>, class: &str, message: impl AsRef<str>) {
    let _ = env.throw_new(class, message.as_ref());
}

pub(crate) fn throw_java_default(env: &mut JNIEnv<'_>, class: &str) {
    let result = env
        .new_object(class, "()V", &[])
        .and_then(|throwable| env.throw(JThrowable::from(throwable)));
    if result.is_err() {
        throw_java(env, "io/omq/OMQException", class);
    }
}

fn throw_transport_java(
    env: &mut JNIEnv<'_>,
    class: &str,
    operation: &str,
    endpoint: &str,
    detail: &str,
) {
    let result = (|| {
        let operation = env.new_string(operation)?;
        let endpoint = env.new_string(endpoint)?;
        let detail = env.new_string(detail)?;
        let throwable = env.new_object(
            class,
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
            &[
                JValue::Object(&operation),
                JValue::Object(&endpoint),
                JValue::Object(&detail),
            ],
        )?;
        env.throw(JThrowable::from(throwable))
    })();

    if result.is_err() {
        let _ = env.throw_new(
            "io/omq/OMQException",
            format!("{operation} failed for {endpoint}: {detail}"),
        );
    }
}

fn is_name_resolution_error(error: &std::io::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    matches!(error.kind(), std::io::ErrorKind::NotFound)
        || text.contains("lookup")
        || text.contains("no address")
        || text.contains("no addresses")
        || text.contains("no such host")
        || text.contains("name or service")
        || text.contains("nodename")
        || text.contains("host not found")
        || text.contains("temporary failure in name resolution")
        || error.raw_os_error() == Some(11001)
}

pub(crate) fn throw_omq_for_endpoint(
    env: &mut JNIEnv<'_>,
    error: Error,
    operation: &str,
    endpoint: &str,
) {
    if let Error::Io(io_error) = error {
        let class = if is_name_resolution_error(&io_error) {
            "io/omq/NameResolutionException"
        } else {
            match operation {
                "bind" => "io/omq/BindException",
                "connect" => "io/omq/ConnectException",
                _ => "io/omq/TransportException",
            }
        };
        throw_transport_java(env, class, operation, endpoint, &io_error.to_string());
    } else {
        throw_omq(env, error);
    }
}

pub(crate) fn throw_omq(env: &mut JNIEnv<'_>, error: Error) {
    let class = match error {
        Error::Timeout | Error::WouldBlock => "io/omq/TimeoutException",
        Error::Closed => "io/omq/ClosedException",
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            "io/omq/InvalidEndpointException"
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            "io/omq/ProtocolException"
        }
        _ => "io/omq/OMQException",
    };
    throw_java(env, class, error.to_string());
}

fn exception_class(error: &Error) -> &'static str {
    match error {
        Error::Timeout | Error::WouldBlock => "io/omq/TimeoutException",
        Error::Closed => "io/omq/ClosedException",
        Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_) => {
            "io/omq/InvalidEndpointException"
        }
        Error::Protocol(_) | Error::HandshakeFailed(_) | Error::UnsupportedZmtpVersion { .. } => {
            "io/omq/ProtocolException"
        }
        _ => "io/omq/OMQException",
    }
}

pub(crate) fn exception_object<'local>(
    env: &mut JNIEnv<'local>,
    error: Error,
) -> jni::errors::Result<JObject<'local>> {
    let message = env.new_string(error.to_string())?;
    env.new_object(
        exception_class(&error),
        "(Ljava/lang/String;)V",
        &[JValue::Object(&message)],
    )
}

pub(crate) fn runtime_exception_object<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> jni::errors::Result<JObject<'local>> {
    let message = env.new_string(message)?;
    env.new_object(
        "io/omq/OMQException",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&message)],
    )
}

pub(crate) fn jni_error(error: jni::errors::Error) -> Error {
    Error::Config(format!("JNI error: {error}"))
}

#[cfg(test)]
mod tests;
