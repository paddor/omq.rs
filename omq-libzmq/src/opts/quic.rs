//! `OMQ_QUIC_*` options. All are fixed once the backend socket exists:
//! later changes fail with `EBUSY` instead of silently not applying.

use std::ffi::c_int;

use super::QuicOverlay;
use super::raw::{
    none_if_empty, read_bytes, read_i32, read_string, write_bytes, write_i32, write_string,
};
use crate::error::fail;

#[cfg(feature = "webtransport")]
use super::codes::{OMQ_QUIC_ALLOWED_ORIGINS, OMQ_QUIC_WEBTRANSPORT_PATH};
use super::codes::{
    OMQ_QUIC_CERT_PEM, OMQ_QUIC_KEY_PEM, OMQ_QUIC_MAX_READY_PEERS, OMQ_QUIC_SERVER_NAME,
    OMQ_QUIC_STREAM_WINDOW, OMQ_QUIC_TRUST_PEM, OMQ_QUIC_TRUST_SYSTEM,
};

fn is_quic_option(option: c_int) -> bool {
    #[cfg(feature = "webtransport")]
    if matches!(
        option,
        OMQ_QUIC_ALLOWED_ORIGINS | OMQ_QUIC_WEBTRANSPORT_PATH
    ) {
        return true;
    }
    matches!(
        option,
        OMQ_QUIC_CERT_PEM
            | OMQ_QUIC_KEY_PEM
            | OMQ_QUIC_TRUST_PEM
            | OMQ_QUIC_SERVER_NAME
            | OMQ_QUIC_TRUST_SYSTEM
            | OMQ_QUIC_STREAM_WINDOW
            | OMQ_QUIC_MAX_READY_PEERS
    )
}

/// `None` when `option` is not a QUIC option.
pub(super) fn set(
    sock: &crate::socket::OmqSocket,
    option: c_int,
    optval: *const libc::c_void,
    optvallen: usize,
) -> Option<c_int> {
    if !is_quic_option(option) {
        return None;
    }
    if sock.inner.get().is_some() {
        return Some(fail(libc::EBUSY));
    }
    let Ok(mut overlay) = sock.overlay.lock() else {
        return Some(fail(crate::error::ETERM));
    };
    let quic = &mut overlay.quic;
    let ok = match option {
        OMQ_QUIC_CERT_PEM => set_pem(&mut quic.cert_pem, optval, optvallen),
        OMQ_QUIC_KEY_PEM => set_pem(&mut quic.key_pem, optval, optvallen),
        OMQ_QUIC_TRUST_PEM => set_pem(&mut quic.trust_pem, optval, optvallen),
        OMQ_QUIC_SERVER_NAME => read_string(optval, optvallen).map(|name| {
            quic.server_name = (!name.is_empty()).then_some(name);
        }),
        OMQ_QUIC_TRUST_SYSTEM => {
            read_i32(optval, optvallen).map(|value| quic.trust_system = value != 0)
        }
        OMQ_QUIC_STREAM_WINDOW => set_window(quic, optval, optvallen),
        OMQ_QUIC_MAX_READY_PEERS => read_i32(optval, optvallen)
            .filter(|&value| value > 0)
            .map(|value| quic.max_ready_peers = value),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_ALLOWED_ORIGINS => set_origins(quic, optval, optvallen),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_WEBTRANSPORT_PATH => set_path(quic, optval, optvallen),
        _ => unreachable!("checked QUIC option"),
    };
    Some(if ok.is_some() { 0 } else { fail(libc::EINVAL) })
}

fn set_pem(slot: &mut Option<Vec<u8>>, optval: *const libc::c_void, len: usize) -> Option<()> {
    read_bytes(optval, len).map(|pem| *slot = none_if_empty(pem))
}

fn set_window(quic: &mut QuicOverlay, optval: *const libc::c_void, len: usize) -> Option<()> {
    use omq_tokio::options::QuicOptions;
    let value = read_i32(optval, len)?;
    let window = u32::try_from(value).ok()?;
    (QuicOptions::MIN_STREAM_WINDOW..=QuicOptions::MAX_STREAM_WINDOW)
        .contains(&window)
        .then(|| quic.stream_window = value)
}

#[cfg(feature = "webtransport")]
fn set_origins(quic: &mut QuicOverlay, optval: *const libc::c_void, len: usize) -> Option<()> {
    let value = read_string(optval, len)?;
    let origins: Vec<String> = if value.is_empty() {
        Vec::new()
    } else {
        value.split('\n').map(str::to_owned).collect()
    };
    origins
        .iter()
        .all(|origin| omq_tokio::proto::web_address::normalize_ws_origin(origin).is_ok())
        .then(|| quic.allowed_origins = origins)
}

#[cfg(feature = "webtransport")]
fn set_path(quic: &mut QuicOverlay, optval: *const libc::c_void, len: usize) -> Option<()> {
    let path = read_string(optval, len)?;
    let host = omq_tokio::endpoint::Host::Name("localhost".into());
    omq_tokio::proto::web_address::validate_ws_address(&host, &path)
        .ok()
        .map(|()| quic.webtransport_path = path)
}

/// `None` when `option` is not a QUIC option.
pub(super) fn get(
    sock: &crate::socket::OmqSocket,
    option: c_int,
    optval: *mut libc::c_void,
    optvallen: *mut usize,
) -> Option<c_int> {
    if !is_quic_option(option) {
        return None;
    }
    let Ok(overlay) = sock.overlay.lock() else {
        return Some(fail(crate::error::ETERM));
    };
    let quic = &overlay.quic;
    let bytes =
        |value: &Option<Vec<u8>>| write_bytes(optval, optvallen, value.as_deref().unwrap_or(b""));
    Some(match option {
        OMQ_QUIC_CERT_PEM => bytes(&quic.cert_pem),
        OMQ_QUIC_KEY_PEM => bytes(&quic.key_pem),
        OMQ_QUIC_TRUST_PEM => bytes(&quic.trust_pem),
        OMQ_QUIC_SERVER_NAME => write_string(
            optval,
            optvallen,
            quic.server_name.as_deref().unwrap_or("").as_bytes(),
        ),
        OMQ_QUIC_TRUST_SYSTEM => write_i32(optval, optvallen, i32::from(quic.trust_system)),
        OMQ_QUIC_STREAM_WINDOW => write_i32(optval, optvallen, quic.stream_window),
        OMQ_QUIC_MAX_READY_PEERS => write_i32(optval, optvallen, quic.max_ready_peers),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_ALLOWED_ORIGINS => write_bytes(
            optval,
            optvallen,
            quic.allowed_origins.join("\n").as_bytes(),
        ),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_WEBTRANSPORT_PATH => {
            write_string(optval, optvallen, quic.webtransport_path.as_bytes())
        }
        _ => unreachable!("checked QUIC option"),
    })
}
