//! Encode and decode WebTransport CONNECT requests and responses.

use std::{str::FromStr, sync::Arc};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use url::Url;

use super::{Frame, VarInt, WebTransportVersion, qpack};
use thiserror::Error;

const MAX_CONNECT_FRAME_SIZE: usize = 64 * 1024;
const WEBTRANSPORT_PROTOCOL: &str = "webtransport-h3";
const LEGACY_WEBTRANSPORT_PROTOCOL: &str = "webtransport";

// Errors that can occur while processing a CONNECT request.
#[derive(Error, Debug, Clone)]
/// Errors returned while encoding or decoding WebTransport CONNECT messages.
pub enum ConnectError {
    #[error("unexpected end of input")]
    /// Input ended before the complete frame/header block was available.
    UnexpectedEnd,

    #[error("qpack error")]
    /// QPACK header block decoding failed.
    QpackError(#[from] qpack::DecodeError),

    #[error("unexpected frame {0:?}")]
    /// Received an unexpected HTTP/3 frame type.
    UnexpectedFrame(Frame),

    #[error("invalid method")]
    /// `:method` could not be parsed as an HTTP method.
    InvalidMethod,

    #[error("invalid url")]
    /// URL reconstruction from pseudo-headers failed.
    InvalidUrl(#[from] url::ParseError),

    #[error("invalid authority")]
    /// Reconstructed URL authority contained unsupported user information.
    InvalidAuthority,

    #[error("URL fragments are not valid in an HTTP request target")]
    /// The request target contained a URI fragment.
    InvalidFragment,

    #[error("invalid status")]
    /// `:status` could not be parsed as a valid HTTP status code.
    InvalidStatus,

    #[error("CONNECT frame exceeds the 64 KiB implementation limit")]
    /// CONNECT frame exceeded the implementation's bounded decode limit.
    MessageTooLong,

    #[error("expected 200, got: {0:?}")]
    /// Response status was not successful.
    WrongStatus(Option<http::StatusCode>),

    #[error("expected connect, got: {0:?}")]
    /// Request method was not CONNECT.
    WrongMethod(Option<http::method::Method>),

    #[error("expected https, got: {0:?}")]
    /// Request scheme was not HTTPS.
    WrongScheme(Option<String>),

    #[error("expected authority header")]
    /// Required `:authority` pseudo-header was missing.
    WrongAuthority,

    #[error("unsupported WebTransport CONNECT protocol: {0:?}")]
    /// CONNECT protocol was neither supported WebTransport token.
    WrongProtocol(Option<String>),

    #[error("expected path header")]
    /// Required `:path` pseudo-header was missing.
    WrongPath,

    #[error("non-200 status: {0:?}")]
    /// Peer returned an explicit non-2xx HTTP response status.
    ErrorStatus(http::StatusCode),

    #[error("io error: {0}")]
    /// I/O error while reading or writing frames.
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for ConnectError {
    fn from(err: std::io::Error) -> Self {
        ConnectError::Io(Arc::new(err))
    }
}

impl WebTransportVersion {
    /// Extended CONNECT `:protocol` token used by this version.
    pub fn upgrade_token(self) -> &'static str {
        match self {
            Self::Draft02 | Self::Draft07 => LEGACY_WEBTRANSPORT_PROTOCOL,
            Self::Draft16 => WEBTRANSPORT_PROTOCOL,
        }
    }
}

#[derive(Debug)]
/// Decoded WebTransport CONNECT request metadata.
pub struct ConnectRequest {
    /// Target URL reconstructed from pseudo-headers.
    pub url: Url,
    /// Browser Origin header, if supplied.
    pub origin: Option<String>,
    /// Wire version selected by HTTP/3 SETTINGS negotiation.
    pub version: WebTransportVersion,
}

impl ConnectRequest {
    /// Decode a CONNECT request from an in-memory frame buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ConnectError> {
        let (typ, mut data) = Frame::read(buf).map_err(|_| ConnectError::UnexpectedEnd)?;
        if typ != Frame::HEADERS {
            return Err(ConnectError::UnexpectedFrame(typ));
        }

        // The frame payload should be complete, so an additional UnexpectedEnd is unexpected here.

        let headers = qpack::Headers::decode(&mut data)?;
        headers.validate_pseudo_headers(&[
            ":method",
            ":scheme",
            ":authority",
            ":path",
            ":protocol",
        ])?;

        let scheme = match headers.get(":scheme") {
            Some("https") => "https",
            Some(scheme) => Err(ConnectError::WrongScheme(Some(scheme.to_string())))?,
            None => return Err(ConnectError::WrongScheme(None)),
        };

        let authority = headers
            .get(":authority")
            .ok_or(ConnectError::WrongAuthority)?;

        let path_and_query = headers.get(":path").ok_or(ConnectError::WrongPath)?;

        let method = headers.get(":method");
        match method
            .map(|method| method.try_into().map_err(|_| ConnectError::InvalidMethod))
            .transpose()?
        {
            Some(http::Method::CONNECT) => (),
            o => return Err(ConnectError::WrongMethod(o)),
        };

        let protocol = headers.get(":protocol");
        // Draft-02 and draft-07 share the legacy token; SETTINGS tell them apart.
        let version = match protocol {
            Some(WEBTRANSPORT_PROTOCOL) => WebTransportVersion::Draft16,
            Some(LEGACY_WEBTRANSPORT_PROTOCOL) => WebTransportVersion::Draft02,
            _ => return Err(ConnectError::WrongProtocol(protocol.map(str::to_owned))),
        };

        let url = Url::parse(&format!("{scheme}://{authority}{path_and_query}"))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ConnectError::InvalidAuthority);
        }
        if url.fragment().is_some() {
            return Err(ConnectError::InvalidFragment);
        }

        let origin = headers.get("origin").map(str::to_owned);
        Ok(Self {
            url,
            origin,
            version,
        })
    }

    /// Read and decode a CONNECT request from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, ConnectError> {
        let frame = read_frame(stream).await?;
        Self::decode(&mut frame.as_slice())
    }

    /// Encode this CONNECT request into a HEADERS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut headers = qpack::Headers::default();
        headers.set(":method", "CONNECT");
        headers.set(":scheme", self.url.scheme());
        headers.set(":authority", self.url.authority());
        let path_and_query = match self.url.query() {
            Some(query) => format!("{}?{}", self.url.path(), query),
            None => self.url.path().to_string(),
        };
        headers.set(":path", &path_and_query);
        headers.set(":protocol", self.version.upgrade_token());
        if let Some(origin) = &self.origin {
            headers.set("origin", origin);
        }
        encode_headers_frame(buf, &headers);
    }

    /// Encode and write this CONNECT request to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), ConnectError> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }
}

#[derive(Debug)]
/// Decoded WebTransport CONNECT response metadata.
pub struct ConnectResponse {
    /// HTTP status returned by the peer.
    pub status: http::status::StatusCode,
}

impl ConnectResponse {
    /// Decode a CONNECT response from an in-memory frame buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, ConnectError> {
        let (typ, mut data) = Frame::read(buf).map_err(|_| ConnectError::UnexpectedEnd)?;
        if typ != Frame::HEADERS {
            return Err(ConnectError::UnexpectedFrame(typ));
        }

        let headers = qpack::Headers::decode(&mut data)?;
        headers.validate_pseudo_headers(&[":status"])?;

        let status = headers
            .get(":status")
            .ok_or(ConnectError::WrongStatus(None))
            .and_then(|status| {
                http::StatusCode::from_str(status).map_err(|_| ConnectError::InvalidStatus)
            })?;

        Ok(Self { status })
    }

    /// Read and decode a CONNECT response from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, ConnectError> {
        let frame = read_frame(stream).await?;
        Self::decode(&mut frame.as_slice())
    }

    /// Encode this CONNECT response into a HEADERS frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut headers = qpack::Headers::default();
        headers.set(":status", self.status.as_str());
        encode_headers_frame(buf, &headers);
    }

    /// Encode and write this CONNECT response to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), ConnectError> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }
}

fn encode_headers_frame<B: BufMut>(buf: &mut B, headers: &qpack::Headers) {
    // Encode headers into a temporary payload so we can prefix it with the frame length.
    let mut payload = Vec::new();
    headers.encode(&mut payload);

    Frame::HEADERS.encode(buf);
    VarInt::from_u32(payload.len() as u32).encode(buf);
    buf.put_slice(&payload);
}

async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>, ConnectError> {
    loop {
        let typ = VarInt::read(stream)
            .await
            .map_err(|_| ConnectError::UnexpectedEnd)?;
        let length = VarInt::read(stream)
            .await
            .map_err(|_| ConnectError::UnexpectedEnd)?;
        let length =
            usize::try_from(length.into_inner()).map_err(|_| ConnectError::MessageTooLong)?;
        if length > MAX_CONNECT_FRAME_SIZE {
            return Err(ConnectError::MessageTooLong);
        }

        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;
        let typ = Frame(typ);
        if typ.is_grease() {
            continue;
        }

        let mut frame = Vec::with_capacity(typ.0.size() + VarInt::MAX_SIZE + length);
        typ.encode(&mut frame);
        VarInt::try_from(length)
            .map_err(|_| ConnectError::MessageTooLong)?
            .encode(&mut frame);
        frame.extend_from_slice(&payload);
        return Ok(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_uses_current_upgrade_token() {
        let request = ConnectRequest {
            url: Url::parse("https://example.com/chat").unwrap(),
            origin: None,
            version: WebTransportVersion::Draft16,
        };
        let mut encoded = Vec::new();
        request.encode(&mut encoded);

        let mut source = encoded.as_slice();
        let (typ, mut payload) = Frame::read(&mut source).unwrap();
        assert_eq!(typ, Frame::HEADERS);
        let headers = qpack::Headers::decode(&mut payload).unwrap();
        assert_eq!(headers.get(":protocol"), Some("webtransport-h3"));
    }

    #[test]
    fn request_roundtrips() {
        let request = ConnectRequest {
            url: Url::parse("https://example.com:4433/chat?room=alpha").unwrap(),
            origin: Some("https://app.example.com".into()),
            version: WebTransportVersion::Draft16,
        };
        let mut encoded = Vec::new();
        request.encode(&mut encoded);

        let decoded = ConnectRequest::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.url, request.url);
        assert_eq!(decoded.origin, request.origin);
        assert_eq!(decoded.version, request.version);
    }

    #[test]
    fn legacy_request_uses_legacy_upgrade_token() {
        let request = ConnectRequest {
            url: Url::parse("https://example.com/chat").unwrap(),
            origin: None,
            version: WebTransportVersion::Draft02,
        };
        let mut encoded = Vec::new();
        request.encode(&mut encoded);
        let decoded = ConnectRequest::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.version, WebTransportVersion::Draft02);
    }

    #[test]
    fn draft07_request_uses_legacy_upgrade_token() {
        assert_eq!(WebTransportVersion::Draft07.upgrade_token(), "webtransport");
        assert_eq!(
            WebTransportVersion::Draft07.upgrade_token(),
            WebTransportVersion::Draft02.upgrade_token()
        );
    }

    #[test]
    fn response_preserves_rejection_status() {
        let response = ConnectResponse {
            status: http::StatusCode::FORBIDDEN,
        };
        let mut encoded = Vec::new();
        response.encode(&mut encoded);

        let decoded = ConnectResponse::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.status, http::StatusCode::FORBIDDEN);
    }
}
