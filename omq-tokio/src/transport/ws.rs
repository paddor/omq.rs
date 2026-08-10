//! WebSocket bind/connect glue (ZWS/2.0, RFC 45).
//!
//! Performs the HTTP upgrade handshake and returns a raw byte stream
//! (`WsTransport`). The Connection codec in omq-proto handles WS
//! framing internally via `ws_role`.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};
use omq_proto::proto::ws_handshake;

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

fn mechanism_subprotocol(
    #[cfg_attr(not(feature = "plain"), expect(unused_variables))]
    mechanism: &omq_proto::MechanismSetup,
) -> &'static str {
    #[cfg(feature = "plain")]
    {
        use omq_proto::MechanismSetup;
        match mechanism {
            MechanismSetup::PlainClient { .. } | MechanismSetup::PlainServer { .. } => {
                return "ZWS2.0/PLAIN";
            }
            _ => {}
        }
    }
    "ZWS2.0/NULL"
}

fn is_known_subprotocol(s: &str) -> bool {
    matches!(s, "ZWS2.0" | "ZWS2.0/NULL" | "ZWS2.0/PLAIN")
}

fn ws_err(e: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(e.to_string()))
}

// ---- Bind / Accept / Connect ----

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    endpoint: Endpoint,
    pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
}

impl WsListener {
    pub(crate) fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

pub(crate) async fn bind(
    endpoint: &Endpoint,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> Result<WsListener> {
    use std::net::{IpAddr, Ipv4Addr};
    let (host, port, path) = match endpoint {
        Endpoint::Ws { host, port, path } | Endpoint::Wss { host, port, path } => {
            (host, *port, path)
        }
        other => {
            return Err(Error::InvalidEndpoint(format!(
                "WS transport got non-WS endpoint: {other}"
            )));
        }
    };
    let addr = match host {
        Host::Wildcard => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        Host::Ip(ip) => SocketAddr::new(*ip, port),
        Host::Name(name) => tokio::net::lookup_host(format!("{name}:{port}"))
            .await
            .map_err(Error::Io)?
            .next()
            .ok_or_else(|| Error::InvalidEndpoint(format!("DNS lookup failed for {name}")))?,
        _ => unreachable!(),
    };
    let listener = super::tcp::reuse_addr_bind(addr)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    let resolved = match endpoint {
        Endpoint::Ws { .. } => Endpoint::Ws {
            host: Host::Ip(local.ip()),
            port: local.port(),
            path: path.clone(),
        },
        Endpoint::Wss { .. } => Endpoint::Wss {
            host: Host::Ip(local.ip()),
            port: local.port(),
            path: path.clone(),
        },
        _ => unreachable!(),
    };
    Ok(WsListener {
        inner: listener,
        endpoint: resolved,
        tls_acceptor,
    })
}

/// Result of a WebSocket accept: the upgraded stream + any leftover
/// bytes read past the HTTP headers (may contain WS frames).
pub(crate) struct WsAccepted {
    pub transport: WsTransport,
    pub leftover: bytes::Bytes,
}

pub(crate) async fn accept(
    stream: TcpStream,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
) -> Result<WsAccepted> {
    let _ = stream.set_nodelay(true);
    let mut transport = if let Some(acc) = tls_acceptor {
        let tls = acc.accept(stream).await.map_err(Error::Io)?;
        WsTransport::TlsServer(tls)
    } else {
        WsTransport::Plain(stream)
    };

    let mut buf = vec![0u8; 4096];
    let mut total = 0;
    loop {
        let n = transport.read(&mut buf[total..]).await.map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::HandshakeFailed(
                "connection closed during HTTP upgrade".into(),
            ));
        }
        total += n;
        if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if total >= buf.len() {
            return Err(Error::HandshakeFailed("HTTP request too large".into()));
        }
    }

    let header_end = buf[..total]
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap()
        + 4;

    let upgrade = ws_handshake::parse_client_upgrade(&buf[..header_end])?;
    let chosen = upgrade
        .subprotocols
        .iter()
        .find(|p| is_known_subprotocol(p))
        .cloned()
        .unwrap_or_else(|| "ZWS2.0/NULL".to_string());

    let accept_value = ws_handshake::compute_ws_accept(&upgrade.key);
    let response = ws_handshake::format_server_upgrade(&accept_value, &chosen);
    transport.write_all(&response).await.map_err(Error::Io)?;

    let leftover = if header_end < total {
        bytes::Bytes::copy_from_slice(&buf[header_end..total])
    } else {
        bytes::Bytes::new()
    };

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
    let addrs = match host {
        omq_proto::endpoint::Host::Wildcard => {
            return Err(Error::InvalidEndpoint(
                "cannot connect to wildcard host".into(),
            ));
        }
        omq_proto::endpoint::Host::Ip(ip) => vec![SocketAddr::new(*ip, port)],
        omq_proto::endpoint::Host::Name(name) => {
            let addrs: Vec<_> = tokio::net::lookup_host(format!("{name}:{port}"))
                .await
                .map_err(Error::Io)?
                .collect();
            if addrs.is_empty() {
                return Err(Error::InvalidEndpoint(format!(
                    "DNS lookup failed for {name}"
                )));
            }
            addrs
        }
        _ => unreachable!(),
    };
    let stream = connect_any_resolved(addrs).await?;
    let _ = stream.set_nodelay(true);

    let mut transport = if tls {
        let connector = build_tls_connector(wss_tls)?;
        let host_str = wss_tls.hostname.clone().unwrap_or_else(|| match host {
            omq_proto::endpoint::Host::Name(n) => n.clone(),
            omq_proto::endpoint::Host::Ip(ip) => ip.to_string(),
            omq_proto::endpoint::Host::Wildcard => "localhost".into(),
            _ => unreachable!(),
        });
        let domain = rustls_pki_types::ServerName::try_from(host_str).map_err(ws_err)?;
        let tls_stream = connector.connect(domain, stream).await.map_err(Error::Io)?;
        WsTransport::Tls(tls_stream)
    } else {
        WsTransport::Plain(stream)
    };

    let host_header = format!("{host}:{port}");
    let key = ws_handshake::generate_ws_key();
    let subprotocol = mechanism_subprotocol(mechanism);
    let request = ws_handshake::format_client_upgrade(&host_header, path, &key, subprotocol);
    transport.write_all(&request).await.map_err(Error::Io)?;

    let mut buf = vec![0u8; 4096];
    let mut total = 0;
    loop {
        let n = transport.read(&mut buf[total..]).await.map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::HandshakeFailed(
                "connection closed during HTTP upgrade".into(),
            ));
        }
        total += n;
        if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if total >= buf.len() {
            return Err(Error::HandshakeFailed("HTTP response too large".into()));
        }
    }

    let header_end = buf[..total]
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap()
        + 4;

    ws_handshake::parse_server_upgrade(&buf[..header_end], &key)?;

    let leftover = if header_end < total {
        bytes::Bytes::copy_from_slice(&buf[header_end..total])
    } else {
        bytes::Bytes::new()
    };

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
    use rustls_pki_types::CertificateDer;
    use rustls_pki_types::pem::PemObject;
    use std::sync::Arc;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    if wss_tls.accept_invalid_certs {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerify));
    } else {
        let mut roots = rustls::RootCertStore::empty();
        if wss_tls.trust_system {
            let cert_result = rustls_native_certs::load_native_certs();
            if cert_result.certs.is_empty() && !cert_result.errors.is_empty() {
                return Err(crate::Error::Io(std::io::Error::other(format!(
                    "failed to load system certificates: {:?}",
                    cert_result.errors
                ))));
            }
            for cert in cert_result.certs {
                let _ = roots.add(cert);
            }
        }
        if let Some(trust_pem) = wss_tls.trust_pem.as_deref() {
            for cert in CertificateDer::pem_slice_iter(trust_pem) {
                let cert = cert.map_err(|e| Error::Protocol(format!("invalid trust PEM: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| Error::Protocol(format!("invalid trust certificate: {e}")))?;
            }
        }
        config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
    }
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

pub(crate) fn build_tls_acceptor(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<tokio_rustls::TlsAcceptor> {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use std::sync::Arc;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<_> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::Protocol(format!("invalid cert PEM: {e}")))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|e| Error::Protocol(format!("invalid key PEM: {e}")))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::Protocol(format!("TLS config: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &[rustls_pki_types::CertificateDer<'_>],
        _: &rustls_pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::CryptoProvider::get_default()
            .map(|p| p.signature_verification_algorithms.supported_schemes())
            .unwrap_or_default()
    }
}
