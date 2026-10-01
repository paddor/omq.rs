//! Bounded HTTP setup, separate from TLS and established-stream I/O.
//!
//! Resources match the exact configured path/query, without decoding or prefix
//! matching. A present Origin must match the normalized explicit allowlist;
//! native clients may omit it. Origin and proxy headers grant no authentication.
//! Native profiles select the configured `ZWS2.0/NULL`, `/PLAIN`, or `/CURVE`.
//! Browser-compatible `ZWS2.0` still runs OMQ's NULL/PLAIN ZMTP handshake.
//! No extensions are negotiated. Bytes following the HTTP head remain input
//! for the codec, including when the first frame arrived with the upgrade.

use bytes::Bytes;
use omq_proto::proto::ws_handshake::{self, MAX_HTTP_BYTES, UpgradeRequest};
use omq_proto::{Error, MechanismSetup, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub(super) struct ServerPolicy {
    path: String,
    protocol: &'static str,
    allowed_origins: Vec<String>,
}

impl ServerPolicy {
    pub(super) fn new(path: &str, options: &omq_proto::Options) -> Result<Self> {
        Ok(Self {
            path: path.into(),
            protocol: mechanism_subprotocol(&options.mechanism),
            allowed_origins: options
                .ws
                .allowed_origins
                .iter()
                .map(|origin| ws_handshake::normalize_ws_origin(origin))
                .collect::<Result<_>>()?,
        })
    }

    fn select(&self, request: &UpgradeRequest) -> Result<&'static str> {
        if request.path != self.path {
            return Err(failed("WebSocket resource mismatch"));
        }
        if let Some(origin) = &request.origin {
            let origin = ws_handshake::normalize_ws_origin(origin)
                .map_err(|_| failed("invalid WebSocket Origin"))?;
            if !self.allowed_origins.contains(&origin) {
                return Err(failed("WebSocket Origin not allowed"));
            }
        }
        if request
            .subprotocols
            .iter()
            .any(|offer| offer == self.protocol)
        {
            return Ok(self.protocol);
        }
        // OMQ's existing browser profile still runs configured NULL/PLAIN
        // authentication. It is not RFC 45's identity-first bare profile.
        if matches!(self.protocol, "ZWS2.0/NULL" | "ZWS2.0/PLAIN")
            && request.subprotocols.iter().any(|offer| offer == "ZWS2.0")
        {
            return Ok("ZWS2.0");
        }
        Err(failed(
            "no offered WebSocket profile matches configured mechanism",
        ))
    }
}

fn failed(reason: &str) -> Error {
    Error::HandshakeFailed(reason.into())
}

pub(super) fn mechanism_subprotocol(mechanism: &MechanismSetup) -> &'static str {
    use omq_proto::proto::greeting::MechanismName;
    match mechanism.wire_name() {
        MechanismName::NULL => "ZWS2.0/NULL",
        MechanismName::PLAIN => "ZWS2.0/PLAIN",
        MechanismName::CURVE => "ZWS2.0/CURVE",
        _ => unreachable!("unsupported configured mechanism"),
    }
}

async fn read_head(stream: &mut (impl AsyncRead + Unpin)) -> Result<(Bytes, Bytes)> {
    let mut buffer = vec![0; MAX_HTTP_BYTES];
    let mut total: usize = 0;
    loop {
        let start = total.saturating_sub(3);
        let read = stream.read(&mut buffer[total..]).await.map_err(Error::Io)?;
        if read == 0 {
            return Err(failed("connection closed during HTTP upgrade"));
        }
        total += read;
        if let Some(end) = buffer[start..total]
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
        {
            let end = start + end + 4;
            buffer.truncate(total);
            let mut head = Bytes::from(buffer);
            let leftover = head.split_off(end);
            return Ok((head, leftover));
        }
        if total == MAX_HTTP_BYTES {
            return Err(failed("HTTP upgrade head too large"));
        }
    }
}

pub(super) async fn accept(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    policy: &ServerPolicy,
) -> Result<Bytes> {
    let (head, leftover) = read_head(stream).await?;
    let request = ws_handshake::parse_client_upgrade(&head)?;
    let protocol = policy.select(&request)?;
    let response = ws_handshake::format_server_upgrade(
        &ws_handshake::compute_ws_accept(&request.key),
        protocol,
    );
    stream.write_all(&response).await.map_err(Error::Io)?;
    stream.flush().await.map_err(Error::Io)?;
    Ok(leftover)
}

pub(super) async fn connect(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    host: &str,
    path: &str,
    protocol: &str,
) -> Result<Bytes> {
    let key = ws_handshake::generate_ws_key();
    let request = ws_handshake::format_client_upgrade(host, path, &key, protocol);
    if request.len() > MAX_HTTP_BYTES {
        return Err(failed("HTTP upgrade head too large"));
    }
    stream.write_all(&request).await.map_err(Error::Io)?;
    stream.flush().await.map_err(Error::Io)?;
    let (head, leftover) = read_head(stream).await?;
    if ws_handshake::parse_server_upgrade(&head, &key)? != protocol {
        return Err(failed("server selected an unoffered WebSocket profile"));
    }
    Ok(leftover)
}

#[cfg(test)]
mod tests;
