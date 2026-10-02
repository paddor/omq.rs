//! WebSocket bind/connect glue (ZWS/2.0, RFC 45).
//!
//! Performs the HTTP upgrade handshake and returns a raw byte stream
//! (`WsTransport`). The Connection codec in omq-proto handles WS
//! framing internally via `ws_role`.

use std::net::SocketAddr;

use tokio::net::TcpStream;

use omq_proto::error::{Error, Result};
use omq_proto::proto::ws_handshake;

mod insecure;
mod listener;
mod upgrade;
pub(crate) use listener::{AcceptSetup, WsListener, bind};

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

impl tokio::io::AsyncRead for WsTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for WsTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Tls(s) => std::pin::Pin::new(s).poll_write_vectored(cx, bufs),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
            Self::TlsServer(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::TlsServer(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ---- Bind / Accept / Connect ----

/// Result of a WebSocket accept: the upgraded stream + any leftover
/// bytes read past the HTTP headers (may contain WS frames).
pub(crate) struct WsAccepted {
    pub transport: WsTransport,
    pub leftover: bytes::Bytes,
}

async fn accept(
    stream: TcpStream,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
    policy: &upgrade::ServerPolicy,
) -> Result<WsAccepted> {
    let _ = stream.set_nodelay(true);
    let mut transport = if let Some(acc) = tls_acceptor {
        let tls = acc.accept(stream).await.map_err(Error::Io)?;
        WsTransport::TlsServer(tls)
    } else {
        WsTransport::Plain(stream)
    };

    let leftover = upgrade::accept(&mut transport, policy).await?;

    Ok(WsAccepted {
        transport,
        leftover,
    })
}

/// Result of a WebSocket connect: the upgraded stream + any leftover
/// bytes read past the HTTP response headers.
pub(crate) struct WsConnected {
    pub transport: WsTransport,
    pub leftover: bytes::Bytes,
}

pub(crate) async fn connect(
    host: &omq_proto::endpoint::Host,
    port: u16,
    path: &str,
    tls: bool,
    wss_tls: &omq_proto::options::WssTls,
    mechanism: &omq_proto::MechanismSetup,
) -> Result<WsConnected> {
    ws_handshake::validate_ws_address(host, path)?;
    let addrs = match host {
        omq_proto::endpoint::Host::Wildcard => {
            return Err(Error::InvalidEndpoint(
                "cannot connect to wildcard host".into(),
            ));
        }
        omq_proto::endpoint::Host::Ip(ip) => vec![SocketAddr::new(*ip, port)],
        omq_proto::endpoint::Host::Name(name) => crate::transport::dns::resolve(name, port).await?,
        _ => unreachable!(),
    };
    let stream = connect_any_resolved(addrs).await?;
    let _ = stream.set_nodelay(true);

    let mut transport = if tls {
        let connector = build_tls_connector(wss_tls)?;
        let domain = crate::transport::tls::server_name(host, wss_tls.hostname.as_deref())?;
        let tls_stream = connector.connect(domain, stream).await.map_err(Error::Io)?;
        WsTransport::Tls(tls_stream)
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

    Ok(WsConnected {
        transport,
        leftover,
    })
}

async fn connect_any_resolved(addrs: Vec<SocketAddr>) -> Result<TcpStream> {
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(Error::Io(last_err.unwrap_or_else(|| {
        std::io::Error::other("no addresses to connect")
    })))
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
