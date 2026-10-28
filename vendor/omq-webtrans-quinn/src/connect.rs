//! HTTP/3 CONNECT request/response handling for WebTransport sessions.

use omq_webtrans_proto::{ConnectRequest, ConnectResponse, VarInt, WebTransportVersion};

use thiserror::Error;
use url::Url;

#[derive(Error, Debug, Clone)]
pub enum ConnectError {
    #[error("quic stream was closed early")]
    UnexpectedEnd,

    #[error("protocol error: {0}")]
    ProtoError(#[from] omq_webtrans_proto::ConnectError),

    #[error("connection error")]
    ConnectionError(#[from] quinn::ConnectionError),

    #[error("read error")]
    ReadError(#[from] quinn::ReadError),

    #[error("write error")]
    WriteError(#[from] quinn::WriteError),

    #[error("http error status: {0}")]
    ErrorStatus(http::StatusCode),

    #[error("WebTransport CONNECT token does not match negotiated SETTINGS version")]
    VersionMismatch,
}

pub struct Connect {
    // The CONNECT request sent by the client.
    request: ConnectRequest,

    // Keep references to send/recv streams so they remain open until drop.
    send: quinn::SendStream,

    #[allow(dead_code)]
    recv: quinn::RecvStream,
}

impl Connect {
    pub async fn accept(
        conn: &quinn::Connection,
        version: WebTransportVersion,
    ) -> Result<Self, ConnectError> {
        // Accept the stream used for the HTTP CONNECT request.
        // Any other request type is treated as an error.
        let (send, mut recv) = conn.accept_bi().await?;

        let mut request = omq_webtrans_proto::ConnectRequest::read(&mut recv).await?;
        tracing::debug!("received CONNECT request: {request:?}");
        if request.version.upgrade_token() != version.upgrade_token() {
            return Err(ConnectError::VersionMismatch);
        }
        // Draft-02 and draft-07 share a token, so SETTINGS pick the version.
        request.version = version;

        // The request decoded successfully, so we can respond.
        Ok(Self {
            request,
            send,
            recv,
        })
    }

    // Called by the server to send a response to the client.
    pub async fn respond(&mut self, status: http::StatusCode) -> Result<(), ConnectError> {
        let resp = ConnectResponse { status };

        tracing::debug!("sending CONNECT response: {resp:?}");
        resp.write(&mut self.send).await?;

        Ok(())
    }

    pub async fn reject(&mut self, status: http::StatusCode) -> Result<(), ConnectError> {
        self.respond(status).await?;
        self.send
            .finish()
            .map_err(|_| ConnectError::UnexpectedEnd)?;
        // Once the response and FIN are queued, a peer may immediately close
        // the rejected connection. Waiting here keeps the control streams alive
        // long enough to avoid racing the response, but the resulting stop or
        // connection-close status does not invalidate the rejection.
        let _ = self.send.stopped().await;
        Ok(())
    }

    pub async fn open(
        conn: &quinn::Connection,
        url: Url,
        origin: Option<String>,
        version: WebTransportVersion,
    ) -> Result<Self, ConnectError> {
        // Create a stream for sending the CONNECT request.
        let (mut send, mut recv) = conn.open_bi().await?;

        // Create a CONNECT request to send using HTTP/3.
        let request = ConnectRequest {
            url,
            origin,
            version,
        };

        tracing::debug!("sending CONNECT request: {request:?}");
        request.write(&mut send).await?;

        let response = omq_webtrans_proto::ConnectResponse::read(&mut recv).await?;
        tracing::debug!("received CONNECT response: {response:?}");

        // Return an error if the response is not 200 OK.
        if response.status != http::StatusCode::OK {
            return Err(ConnectError::ErrorStatus(response.status));
        }

        Ok(Self {
            request,
            send,
            recv,
        })
    }

    // The session ID is the stream ID of the CONNECT request.
    pub fn session_id(&self) -> VarInt {
        let stream_id = quinn::VarInt::from(self.send.id());
        VarInt::try_from(stream_id.into_inner()).unwrap()
    }

    // The URL from the CONNECT request.
    pub fn url(&self) -> &Url {
        &self.request.url
    }

    /// Origin supplied with the CONNECT request, if any.
    pub fn origin(&self) -> Option<&str> {
        self.request.origin.as_deref()
    }

    pub(super) fn into_inner(self) -> (quinn::SendStream, quinn::RecvStream) {
        (self.send, self.recv)
    }
}
