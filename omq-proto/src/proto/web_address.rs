//! ASCII authority, resource, and browser-origin validation shared by the
//! WebSocket and WebTransport carriers.

use std::net::{IpAddr, Ipv6Addr};

use crate::endpoint::Host;
use crate::{Error, Result};

/// Longest accepted resource or origin; equals the WS HTTP head ceiling.
const MAX_HTTP_BYTES: usize = 4096;

fn invalid() -> Error {
    Error::InvalidEndpoint("invalid WebSocket authority, resource, or origin".into())
}

pub(crate) fn authority(value: &str) -> Result<(Host, Option<u16>)> {
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (ip, tail) = rest.split_once(']').ok_or_else(invalid)?;
        let ip = ip.parse::<Ipv6Addr>().map_err(|_| invalid())?;

        let port = if tail.is_empty() {
            None
        } else {
            Some(tail.strip_prefix(':').ok_or_else(invalid)?)
        };

        (Host::Ip(IpAddr::V6(ip)), port)
    } else {
        let (name, port) = value
            .split_once(':')
            .map_or((value, None), |(name, port)| (name, Some(port)));
        validate_name(name)?;
        let host = name
            .parse::<IpAddr>()
            .map_or_else(|_| Host::Name(name.to_ascii_lowercase()), Host::Ip);

        (host, port)
    };

    let port = port
        .map(|port| {
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }

            port.parse::<u16>().map_err(|_| invalid())
        })
        .transpose()?;

    Ok((host, port))
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 253
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err(invalid());
    }

    Ok(())
}

/// Validate typed WS/WSS addresses before formatting an HTTP request or DNS lookup.
pub fn validate_ws_address(host: &Host, path: &str) -> Result<()> {
    if let Host::Name(name) = host {
        validate_name(name)?;
    }

    if path.len() > MAX_HTTP_BYTES
        || !path.starts_with('/')
        || path
            .bytes()
            .any(|byte| !(0x21..=0x7e).contains(&byte) || byte == b'#')
    {
        return Err(invalid());
    }

    Ok(())
}

/// Normalize a serialized HTTP(S) browser origin for exact allowlist matching.
/// Reject opaque/null origins, lists, paths, credentials, and non-ASCII hosts.
pub fn normalize_ws_origin(origin: &str) -> Result<String> {
    if origin.len() > MAX_HTTP_BYTES {
        return Err(invalid());
    }

    let (scheme, value) = origin.split_once("://").ok_or_else(invalid)?;
    let (scheme, default_port) = if scheme.eq_ignore_ascii_case("http") {
        ("http", 80)
    } else if scheme.eq_ignore_ascii_case("https") {
        ("https", 443)
    } else {
        return Err(invalid());
    };

    let (host, port) = authority(value)?;
    Ok(format!(
        "{scheme}://{host}:{}",
        port.unwrap_or(default_port)
    ))
}

#[cfg(test)]
mod tests;
