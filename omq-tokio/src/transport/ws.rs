//! WebSocket bind/connect glue for OMQ's ZMTP profile.
//!
//! Performs the HTTP upgrade handshake and returns a raw byte stream
//! (`WsTransport`). The Connection codec in omq-proto handles WS
//! framing internally via `ws_role`.
//!
//! WS/WSS shares the finite setup deadline through DNS, TCP, TLS, HTTP, and
//! ZMTP READY. TLS verifies certificate chains and server names by default;
//! custom trust and name overrides are explicit. Mutual TLS is unsupported.
//! Compression is available on eligible plain `lz4+ws`; WSS and CURVE disable
//! OMQ compression. Encoding stays on connection drivers or fan-out workers.

use tokio::net::TcpStream;

use omq_proto::error::{Error, Result};
use omq_proto::proto::ws_handshake;

mod insecure;
mod listener;
mod upgrade;
pub(crate) use listener::{WsListener, bind};

pub(crate) enum WsTransport {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    TlsServer(tokio_rustls::server::TlsStream<TcpStream>),
}

impl WsTransport {
    pub(crate) fn migrate(self) -> std::io::Result<Self> {
        match self {
            Self::Plain(tcp) => {
                let std = tcp.into_std()?;
                Ok(Self::Plain(TcpStream::from_std(std)?))
            }
            other => Ok(other),
        }
    }
}

impl std::fmt::Debug for WsTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("WsTransport::Plain"),
            Self::Tls(_) => f.write_str("WsTransport::Tls"),
            Self::TlsServer(_) => f.write_str("WsTransport::TlsServer"),
        }
    }
}

crate::transport::delegate_async_io!(WsTransport {
    Plain,
    Tls,
    TlsServer
});

// ---- Bind / Accept / Connect ----

/// An upgraded WebSocket stream plus any bytes read past the HTTP headers
/// (may contain WS frames).
pub(crate) struct WsUpgraded {
    pub transport: WsTransport,
    pub leftover: bytes::Bytes,
}

async fn accept(
    stream: TcpStream,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
    policy: &upgrade::ServerPolicy,
) -> Result<WsUpgraded> {
    let _ = stream.set_nodelay(true);
    let mut transport = if let Some(acc) = tls_acceptor {
        let tls = acc.accept(stream).await.map_err(Error::Io)?;
        WsTransport::TlsServer(tls)
    } else {
        WsTransport::Plain(stream)
    };

    let leftover = upgrade::accept(&mut transport, policy).await?;

    Ok(WsUpgraded {
        transport,
        leftover,
    })
}

/// Address and scheme of one WS/WSS dial.
pub(crate) struct WsTarget<'a> {
    pub(crate) host: &'a omq_proto::endpoint::Host,
    pub(crate) port: u16,
    pub(crate) path: &'a str,
    /// `wss://`: wrap the TCP stream in TLS before the HTTP upgrade.
    pub(crate) tls: bool,
}

pub(crate) async fn connect(
    target: WsTarget<'_>,
    wss_tls: &omq_proto::options::WssTls,
    mechanism: &omq_proto::MechanismSetup,
) -> Result<WsUpgraded> {
    let WsTarget {
        host,
        port,
        path,
        tls,
    } = target;

    ws_handshake::validate_ws_address(host, path)?;
    let addrs = crate::transport::dns::connect_addrs(host, port).await?;
    let stream = crate::transport::tcp::connect_any(addrs).await?;
    let _ = stream.set_nodelay(true);

    let mut transport = if tls {
        WsTransport::Tls(connect_tls(wss_tls, host, stream).await?)
    } else {
        WsTransport::Plain(stream)
    };

    let host_header = format!("{host}:{port}");
    let leftover = upgrade::connect(
        &mut transport,
        &host_header,
        path,
        upgrade::mechanism_subprotocol(mechanism),
    )
    .await?;

    Ok(WsUpgraded {
        transport,
        leftover,
    })
}

/// TLS setup and handshake of one dial. A failure is a handshake failure:
/// the dialer retries it and publishes it to monitors. As an I/O or
/// protocol error it stayed invisible behind `ConnectDelayed`.
async fn connect_tls(
    wss_tls: &omq_proto::options::WssTls,
    host: &omq_proto::endpoint::Host,
    stream: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    fn failed(error: &dyn std::fmt::Display) -> Error {
        Error::HandshakeFailed(format!("TLS: {error}"))
    }
    let connector = build_tls_connector(wss_tls).map_err(|error| failed(&error))?;
    let domain = crate::transport::tls::server_name(host, wss_tls.hostname.as_deref())
        .map_err(|error| failed(&error))?;
    connector
        .connect(domain, stream)
        .await
        .map_err(|error| failed(&error))
}

fn build_tls_connector(wss_tls: &omq_proto::options::WssTls) -> Result<tokio_rustls::TlsConnector> {
    let config = if wss_tls.accept_invalid_certs {
        insecure::client_config()
    } else {
        crate::transport::tls::verified_client_config(
            wss_tls.trust_system,
            wss_tls.trust_pem.as_deref(),
        )?
    };
    Ok(tokio_rustls::TlsConnector::from(std::sync::Arc::new(
        config,
    )))
}
