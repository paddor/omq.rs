//! HTTP/3 SETTINGS exchange for WebTransport over Quinn.

use futures::try_join;

use omq_webtrans_proto::WebTransportVersion;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum SettingsError {
    #[error("quic stream was closed early")]
    UnexpectedEnd,

    #[error("protocol error: {0}")]
    ProtoError(#[from] omq_webtrans_proto::SettingsError),

    #[error("WebTransport is not supported by peer settings: {0}")]
    WebTransportUnsupported(String),

    #[error("connection error")]
    ConnectionError(#[from] quinn::ConnectionError),

    #[error("read error")]
    ReadError(#[from] quinn::ReadError),

    #[error("write error")]
    WriteError(#[from] quinn::WriteError),
}

pub struct Settings {
    version: WebTransportVersion,
    // Keep references to send/recv streams so they remain open until drop.
    #[allow(dead_code)]
    send: quinn::SendStream,

    #[allow(dead_code)]
    recv: quinn::RecvStream,
}

impl Settings {
    // Establish the HTTP/3 SETTINGS exchange.
    pub async fn connect(
        conn: &quinn::Connection,
        peer_is_server: bool,
    ) -> Result<Self, SettingsError> {
        let recv = Self::accept(conn, peer_is_server);
        let send = Self::open(conn);

        // Run both tasks concurrently until one errors or both complete.
        let (send, (recv, version)) = try_join!(send, recv)?;
        Ok(Self {
            send,
            recv,
            version,
        })
    }

    async fn accept(
        conn: &quinn::Connection,
        peer_is_server: bool,
    ) -> Result<(quinn::RecvStream, WebTransportVersion), SettingsError> {
        let mut recv = conn.accept_uni().await?;
        let settings = omq_webtrans_proto::Settings::read(&mut recv).await?;

        tracing::debug!("received SETTINGS frame: {settings:?}");

        let version = settings
            .negotiated_version(peer_is_server, crate::RESET_STREAM_AT_SUPPORTED)
            .ok_or_else(|| SettingsError::WebTransportUnsupported(format!("{settings:?}")))?;
        Ok((recv, version))
    }

    async fn open(conn: &quinn::Connection) -> Result<quinn::SendStream, SettingsError> {
        let mut settings = omq_webtrans_proto::Settings::default();
        settings.enable_webtransport(crate::RESET_STREAM_AT_SUPPORTED);

        tracing::debug!("sending SETTINGS frame: {settings:?}");

        let mut send = conn.open_uni().await?;
        settings.write(&mut send).await?;

        Ok(send)
    }

    pub(crate) fn version(&self) -> WebTransportVersion {
        self.version
    }
}
