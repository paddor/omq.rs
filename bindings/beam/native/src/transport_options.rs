//! WSS, WebSocket origin, and QUIC socket options. IDs match omq-libzmq's
//! `zmq.h`. Like other staged options, they apply when the socket
//! materializes.

use omq_tokio::Options;

#[cfg(feature = "ws")]
const WSS_KEY_PEM: i64 = 103;
#[cfg(feature = "ws")]
const WSS_CERT_PEM: i64 = 104;
#[cfg(feature = "ws")]
const WSS_TRUST_PEM: i64 = 105;
#[cfg(feature = "ws")]
const WSS_HOSTNAME: i64 = 106;
#[cfg(feature = "ws")]
const WSS_TRUST_SYSTEM: i64 = 107;
#[cfg(feature = "ws")]
const WS_ALLOWED_ORIGINS: i64 = 1009;
#[cfg(feature = "quic")]
const QUIC_CERT_PEM: i64 = 1011;
#[cfg(feature = "quic")]
const QUIC_KEY_PEM: i64 = 1012;
#[cfg(feature = "quic")]
const QUIC_TRUST_PEM: i64 = 1013;
#[cfg(feature = "quic")]
const QUIC_SERVER_NAME: i64 = 1014;
#[cfg(feature = "quic")]
const QUIC_TRUST_SYSTEM: i64 = 1015;
#[cfg(feature = "webtransport")]
const QUIC_ALLOWED_ORIGINS: i64 = 1017;
#[cfg(feature = "webtransport")]
const QUIC_WEBTRANSPORT_PATH: i64 = 1019;

/// A transport option value for `getsockopt`.
pub(crate) enum Value {
    Int(i64),
    Bytes(Vec<u8>),
}

/// Applies a transport option. `None` when `option` is not one, or its
/// transport is not built.
pub(crate) fn set(
    options: &mut Options,
    option: i64,
    int_value: i64,
    bytes: &[u8],
) -> Option<Result<(), String>> {
    #[cfg(feature = "ws")]
    if let Some(result) = set_ws(options, option, int_value, bytes) {
        return Some(result);
    }

    #[cfg(feature = "quic")]
    if let Some(result) = set_quic(options, option, int_value, bytes) {
        return Some(result);
    }

    None
}

/// Reads a transport option. `None` when `option` is not one, or its
/// transport is not built.
pub(crate) fn get(options: &Options, option: i64) -> Option<Value> {
    #[cfg(feature = "ws")]
    if let Some(value) = get_ws(options, option) {
        return Some(value);
    }

    #[cfg(feature = "quic")]
    if let Some(value) = get_quic(options, option) {
        return Some(value);
    }

    None
}

#[cfg(feature = "ws")]
fn set_ws(
    options: &mut Options,
    option: i64,
    int_value: i64,
    bytes: &[u8],
) -> Option<Result<(), String>> {
    let tls = &mut options.wss_tls;
    match option {
        WSS_KEY_PEM => tls.server_key_pem = pem(bytes),
        WSS_CERT_PEM => tls.server_cert_pem = pem(bytes),
        WSS_TRUST_PEM => tls.trust_pem = pem(bytes),
        WSS_HOSTNAME => match text("WSS_HOSTNAME", bytes) {
            Ok(name) => tls.hostname = name,
            Err(error) => return Some(Err(error)),
        },
        WSS_TRUST_SYSTEM => tls.trust_system = int_value != 0,
        WS_ALLOWED_ORIGINS => match origins("OMQ_WS_ALLOWED_ORIGINS", bytes) {
            Ok(origins) => options.ws.allowed_origins = origins,
            Err(error) => return Some(Err(error)),
        },
        _ => return None,
    }

    Some(Ok(()))
}

#[cfg(feature = "ws")]
fn get_ws(options: &Options, option: i64) -> Option<Value> {
    let tls = &options.wss_tls;
    let value = match option {
        WSS_KEY_PEM => bytes_value(tls.server_key_pem.as_deref()),
        WSS_CERT_PEM => bytes_value(tls.server_cert_pem.as_deref()),
        WSS_TRUST_PEM => bytes_value(tls.trust_pem.as_deref()),
        WSS_HOSTNAME => bytes_value(tls.hostname.as_deref().map(str::as_bytes)),
        WSS_TRUST_SYSTEM => Value::Int(i64::from(tls.trust_system)),
        WS_ALLOWED_ORIGINS => Value::Bytes(options.ws.allowed_origins.join("\n").into_bytes()),
        _ => return None,
    };

    Some(value)
}

#[cfg(feature = "quic")]
fn set_quic(
    options: &mut Options,
    option: i64,
    int_value: i64,
    bytes: &[u8],
) -> Option<Result<(), String>> {
    let quic = &mut options.quic;
    match option {
        QUIC_CERT_PEM => quic.server_cert_pem = pem(bytes),
        QUIC_KEY_PEM => quic.server_key_pem = pem(bytes),
        QUIC_TRUST_PEM => quic.trust_pem = pem(bytes),
        QUIC_SERVER_NAME => match text("OMQ_QUIC_SERVER_NAME", bytes) {
            Ok(name) => quic.server_name = name,
            Err(error) => return Some(Err(error)),
        },
        QUIC_TRUST_SYSTEM => quic.trust_system = int_value != 0,
        #[cfg(feature = "webtransport")]
        QUIC_ALLOWED_ORIGINS => match origins("OMQ_QUIC_ALLOWED_ORIGINS", bytes) {
            Ok(origins) => quic.allowed_origins = origins,
            Err(error) => return Some(Err(error)),
        },
        #[cfg(feature = "webtransport")]
        QUIC_WEBTRANSPORT_PATH => match webtransport_path(bytes) {
            Ok(path) => quic.webtransport_path = path,
            Err(error) => return Some(Err(error)),
        },
        _ => return None,
    }

    Some(Ok(()))
}

#[cfg(feature = "quic")]
fn get_quic(options: &Options, option: i64) -> Option<Value> {
    let quic = &options.quic;
    let value = match option {
        QUIC_CERT_PEM => bytes_value(quic.server_cert_pem.as_deref()),
        QUIC_KEY_PEM => bytes_value(quic.server_key_pem.as_deref()),
        QUIC_TRUST_PEM => bytes_value(quic.trust_pem.as_deref()),
        QUIC_SERVER_NAME => bytes_value(quic.server_name.as_deref().map(str::as_bytes)),
        QUIC_TRUST_SYSTEM => Value::Int(i64::from(quic.trust_system)),
        #[cfg(feature = "webtransport")]
        QUIC_ALLOWED_ORIGINS => Value::Bytes(quic.allowed_origins.join("\n").into_bytes()),
        #[cfg(feature = "webtransport")]
        QUIC_WEBTRANSPORT_PATH => Value::Bytes(quic.webtransport_path.clone().into_bytes()),
        _ => return None,
    };

    Some(value)
}

/// Empty clears the setting.
fn pem(bytes: &[u8]) -> Option<Vec<u8>> {
    (!bytes.is_empty()).then(|| bytes.to_vec())
}

fn bytes_value(bytes: Option<&[u8]>) -> Value {
    Value::Bytes(bytes.unwrap_or_default().to_vec())
}

/// UTF-8 text; empty clears the setting.
fn text(name: &str, bytes: &[u8]) -> Result<Option<String>, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| format!("{name} must be UTF-8"))?;
    Ok((!text.is_empty()).then(|| text.to_owned()))
}

/// Newline-separated exact origins; empty admits no browser.
#[cfg(any(feature = "ws", feature = "webtransport"))]
fn origins(name: &str, bytes: &[u8]) -> Result<Vec<String>, String> {
    use omq_tokio::proto::web_address::normalize_ws_origin;

    let Some(text) = text(name, bytes)? else {
        return Ok(Vec::new());
    };

    text.split('\n')
        .map(|origin| match normalize_ws_origin(origin) {
            Ok(_) => Ok(origin.to_owned()),
            Err(_) => Err(format!("{name}: invalid origin {origin:?}")),
        })
        .collect()
}

#[cfg(feature = "webtransport")]
fn webtransport_path(bytes: &[u8]) -> Result<String, String> {
    use omq_tokio::endpoint::Host;
    use omq_tokio::proto::web_address::validate_ws_address;

    let name = "OMQ_QUIC_WEBTRANSPORT_PATH";
    let path = std::str::from_utf8(bytes).map_err(|_| format!("{name} must be UTF-8"))?;
    validate_ws_address(&Host::Name("localhost".into()), path)
        .map_err(|_| format!("{name}: invalid path {path:?}"))?;

    Ok(path.to_owned())
}
