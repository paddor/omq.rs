//! `OMQ_QUIC_*` options. All are fixed once the backend socket exists:
//! later changes fail with `EBUSY` instead of silently not applying.

use std::ffi::c_int;

use super::overlay;
#[cfg(feature = "webtransport")]
use super::raw::parse_origins;
use super::raw::{OptIn, OptOut, none_if_empty};

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

/// `Ok(false)` when `option` is not a QUIC option.
pub(super) fn set(
    sock: &crate::socket::OmqSocket,
    option: c_int,
    optval: OptIn,
) -> Result<bool, c_int> {
    if !is_quic_option(option) {
        return Ok(false);
    }
    if sock.inner.get().is_some() {
        return Err(libc::EBUSY);
    }
    let mut overlay = overlay(sock)?;
    let quic = &mut overlay.quic;
    match option {
        OMQ_QUIC_CERT_PEM => quic.cert_pem = none_if_empty(optval.bytes()?),
        OMQ_QUIC_KEY_PEM => quic.key_pem = none_if_empty(optval.bytes()?),
        OMQ_QUIC_TRUST_PEM => quic.trust_pem = none_if_empty(optval.bytes()?),
        OMQ_QUIC_SERVER_NAME => {
            let name = optval.string()?;
            quic.server_name = (!name.is_empty()).then_some(name);
        }
        OMQ_QUIC_TRUST_SYSTEM => quic.trust_system = optval.i32()? != 0,
        OMQ_QUIC_STREAM_WINDOW => quic.stream_window = stream_window(optval)?,
        OMQ_QUIC_MAX_READY_PEERS => {
            let value = optval.i32()?;
            if value <= 0 {
                return Err(libc::EINVAL);
            }
            quic.max_ready_peers = value;
        }
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_ALLOWED_ORIGINS => quic.allowed_origins = parse_origins(&optval.string()?)?,
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_WEBTRANSPORT_PATH => quic.webtransport_path = webtransport_path(optval)?,
        _ => unreachable!("checked QUIC option"),
    }
    Ok(true)
}

fn stream_window(optval: OptIn) -> Result<i32, c_int> {
    use omq_tokio::options::QuicOptions;
    let value = optval.i32()?;
    let window = u32::try_from(value).map_err(|_| libc::EINVAL)?;
    if !(QuicOptions::MIN_STREAM_WINDOW..=QuicOptions::MAX_STREAM_WINDOW).contains(&window) {
        return Err(libc::EINVAL);
    }
    Ok(value)
}

#[cfg(feature = "webtransport")]
fn webtransport_path(optval: OptIn) -> Result<String, c_int> {
    let path = optval.string()?;
    let host = omq_tokio::endpoint::Host::Name("localhost".into());
    omq_tokio::proto::web_address::validate_ws_address(&host, &path).map_err(|_| libc::EINVAL)?;
    Ok(path)
}

/// `Ok(false)` when `option` is not a QUIC option.
pub(super) fn get(
    sock: &crate::socket::OmqSocket,
    option: c_int,
    out: OptOut,
) -> Result<bool, c_int> {
    if !is_quic_option(option) {
        return Ok(false);
    }
    let overlay = overlay(sock)?;
    let quic = &overlay.quic;
    let bytes = |value: &Option<Vec<u8>>| out.bytes(value.as_deref().unwrap_or(b""));
    match option {
        OMQ_QUIC_CERT_PEM => bytes(&quic.cert_pem),
        OMQ_QUIC_KEY_PEM => bytes(&quic.key_pem),
        OMQ_QUIC_TRUST_PEM => bytes(&quic.trust_pem),
        OMQ_QUIC_SERVER_NAME => out.string(quic.server_name.as_deref().unwrap_or("").as_bytes()),
        OMQ_QUIC_TRUST_SYSTEM => out.i32(i32::from(quic.trust_system)),
        OMQ_QUIC_STREAM_WINDOW => out.i32(quic.stream_window),
        OMQ_QUIC_MAX_READY_PEERS => out.i32(quic.max_ready_peers),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_ALLOWED_ORIGINS => out.bytes(quic.allowed_origins.join("\n").as_bytes()),
        #[cfg(feature = "webtransport")]
        OMQ_QUIC_WEBTRANSPORT_PATH => out.string(quic.webtransport_path.as_bytes()),
        _ => unreachable!("checked QUIC option"),
    }?;
    Ok(true)
}
