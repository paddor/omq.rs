//! Transport dispatch types and bind / connect helpers.
//!
//! `AnyStream` is the common byte-stream half (TCP or IPC); inproc
//! has its own non-byte-stream Message-channel pair carried inside
//! `AnyConn::Inproc`. `AnyListener` wraps the same three transports
//! on the bind side. `bind_any` / `connect_any` are the dispatch
//! entry points the socket actor calls from its bind / dial paths.

use std::sync::Arc;

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

use crate::transport::{
    InprocConn, InprocPeerSnapshot, IpcTransport, Listener as _, PeerIdent, TcpTransport,
    Transport as _, inproc as inproc_transport,
};

/// Per-attempt settings for carriers with TLS/HTTP setup.
#[cfg(any(feature = "ws", feature = "quic"))]
#[derive(Debug, Clone, Copy)]
pub(super) struct CarrierConnect<'a> {
    pub(super) options: &'a omq_proto::Options,
    /// Absolute setup deadline of this attempt.
    #[cfg_attr(not(feature = "quic"), expect(dead_code))]
    pub(super) deadline: Option<std::time::Instant>,
    /// Data IO runtimes; QUIC connects on the peer's assigned runtime.
    #[cfg(feature = "quic")]
    pub(super) io_pool: &'a crate::context::IoPoolHandle,
}

/// What `bind_any` / `connect_any` hand back. Either a byte-stream
/// (TCP / IPC - runs the ZMTP codec via `ConnectionDriver`) or a
/// pre-paired Message channel (inproc - runs the codec-less
/// `InprocPeerDriver`).
pub(crate) enum AnyConn {
    ByteStream {
        stream: AnyStream,
        peer_ident: PeerIdent,
        leftover: bytes::Bytes,
        setup: Option<crate::transport::setup::SetupState>,
    },
    Inproc {
        conn: InprocConn,
        peer_ident: PeerIdent,
    },
}

impl std::fmt::Debug for AnyConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByteStream { peer_ident, .. } => f
                .debug_struct("AnyConn::ByteStream")
                .field("peer_ident", peer_ident)
                .finish(),
            Self::Inproc { peer_ident, .. } => f
                .debug_struct("AnyConn::Inproc")
                .field("peer_ident", peer_ident)
                .finish(),
        }
    }
}

impl AnyConn {
    pub(crate) fn peer_ident(&self) -> &PeerIdent {
        match self {
            Self::ByteStream { peer_ident, .. } | Self::Inproc { peer_ident, .. } => peer_ident,
        }
    }
}

pub(super) struct BoundListener {
    pub(super) listener: AnyListener,
    pub(super) endpoint: Endpoint,
}

pub(super) enum AnyListener {
    Tcp(crate::transport::tcp::TcpListener),
    Inproc(crate::transport::InprocListener),
    Ipc(crate::transport::ipc::IpcListener),
    #[cfg(target_os = "linux")]
    Shm(crate::transport::ipc::IpcListener, Endpoint),
    #[cfg(feature = "ws")]
    Ws(crate::transport::ws::WsListener),
    #[cfg(feature = "quic")]
    Quic(Box<crate::transport::quic::QuicListener>),
}

impl AnyListener {
    pub(super) fn local_endpoint(&self) -> &Endpoint {
        match self {
            Self::Tcp(l) => l.local_endpoint(),
            Self::Inproc(l) => l.local_endpoint(),
            Self::Ipc(l) => l.local_endpoint(),
            #[cfg(target_os = "linux")]
            Self::Shm(_, endpoint) => endpoint,
            #[cfg(feature = "ws")]
            Self::Ws(l) => l.local_endpoint(),
            #[cfg(feature = "quic")]
            Self::Quic(l) => l.local_endpoint(),
        }
    }

    pub(super) async fn accept(&mut self) -> Result<AnyConn> {
        match self {
            Self::Tcp(l) => l
                .accept()
                .await
                .map(|(s, peer_ident)| byte_stream(AnyStream::Tcp(s), peer_ident)),
            Self::Inproc(l) => {
                let peer_ident = PeerIdent::Inproc(l.name().to_string());
                let conn = l.accept().await?;
                Ok(AnyConn::Inproc { conn, peer_ident })
            }
            #[cfg(target_os = "linux")]
            Self::Shm(l, _) => l
                .accept()
                .await
                .map(|(s, peer_ident)| byte_stream(AnyStream::Ipc(s), peer_ident)),
            Self::Ipc(l) => l
                .accept()
                .await
                .map(|(s, peer_ident)| byte_stream(AnyStream::Ipc(s), peer_ident)),
            #[cfg(feature = "ws")]
            Self::Ws(l) => {
                let (accepted, addr, setup) = l.accept().await?;
                Ok(AnyConn::ByteStream {
                    stream: AnyStream::Ws(Box::new(accepted.transport)),
                    peer_ident: PeerIdent::Socket(addr),
                    leftover: accepted.leftover,
                    setup: Some(setup),
                })
            }
            #[cfg(feature = "quic")]
            Self::Quic(l) => {
                let (stream, addr, setup) = l.accept().await?;
                Ok(AnyConn::ByteStream {
                    stream: AnyStream::Quic(Box::new(stream)),
                    peer_ident: PeerIdent::Socket(addr),
                    leftover: bytes::Bytes::new(),
                    setup: Some(setup),
                })
            }
        }
    }
}

/// Listener settings for carriers with TLS/HTTP setup.
#[cfg(any(feature = "ws", feature = "quic"))]
pub(super) struct CarrierAccept<'a> {
    pub(super) options: &'a omq_proto::Options,
    pub(super) setup: crate::transport::setup::AcceptSetup,
}

/// What a socket brings to inproc binds and connects.
pub(super) struct InprocSetup {
    pub(super) registry: Arc<inproc_transport::InprocRegistry>,
    pub(super) snapshot: InprocPeerSnapshot,
    pub(super) recv: inproc_transport::RecvConfig,
}

/// Bind dispatch: route an endpoint to its transport's listener and wrap it.
///
/// `lz4+tcp://` reuses the TCP listener; the per-connection
/// transform is installed by the actor based on the endpoint scheme.
pub(super) async fn bind_any(
    endpoint: &Endpoint,
    inproc: &InprocSetup,
    #[cfg(any(feature = "ws", feature = "quic"))] carrier: CarrierAccept<'_>,
) -> Result<BoundListener> {
    if endpoint.is_tcp_family() {
        let listener = AnyListener::Tcp(TcpTransport::bind(&endpoint.underlying_tcp()).await?);
        let resolved = endpoint.rewrap_tcp(listener.local_endpoint().clone());
        return Ok(BoundListener {
            listener,
            endpoint: resolved,
        });
    }

    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        return bind_ws(endpoint, carrier).await;
    }

    #[cfg(feature = "quic")]
    if endpoint.is_quic_family() {
        let mut quic_listener =
            crate::transport::quic::bind(endpoint, carrier.setup, carrier.options).await?;
        let resolved = quic_listener.local_endpoint().clone();
        quic_listener.set_monitor_endpoint(resolved.clone());
        return Ok(BoundListener {
            listener: AnyListener::Quic(Box::new(quic_listener)),
            endpoint: resolved,
        });
    }

    #[cfg(target_os = "linux")]
    if let Endpoint::Shm(path) = endpoint {
        let listener = crate::transport::ipc::bind_shm(&Endpoint::Ipc(path.clone()))?;
        return Ok(BoundListener {
            listener: AnyListener::Shm(listener, endpoint.clone()),
            endpoint: endpoint.clone(),
        });
    }

    let listener = match endpoint {
        Endpoint::Inproc { name } => AnyListener::Inproc(inproc_transport::bind(
            inproc.registry.clone(),
            name,
            inproc.snapshot.clone(),
            inproc.recv,
        )?),
        Endpoint::Ipc(_) => AnyListener::Ipc(IpcTransport::bind(endpoint).await?),
        other => return Err(Error::UnsupportedScheme(other.scheme().to_string())),
    };
    let resolved = listener.local_endpoint().clone();
    Ok(BoundListener {
        listener,
        endpoint: resolved,
    })
}

/// Bind a WS/WSS listener. WSS needs the server certificate and key.
#[cfg(feature = "ws")]
async fn bind_ws(endpoint: &Endpoint, carrier: CarrierAccept<'_>) -> Result<BoundListener> {
    let plain = endpoint.underlying_ws();
    let wss_tls = &carrier.options.wss_tls;
    let tls_acc = if matches!(plain, Endpoint::Wss { .. }) {
        let cert = wss_tls.server_cert_pem.as_deref().ok_or_else(|| {
            Error::Protocol("wss:// bind requires server_cert_pem in WssTls options".into())
        })?;
        let key = wss_tls.server_key_pem.as_deref().ok_or_else(|| {
            Error::Protocol("wss:// bind requires server_key_pem in WssTls options".into())
        })?;
        Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
            crate::transport::tls::server_config(cert, key)?,
        )))
    } else {
        None
    };

    let mut ws_listener =
        crate::transport::ws::bind(&plain, tls_acc, carrier.setup, carrier.options).await?;
    // Report the bound port and keep any wrapper (e.g. lz4+ws://).
    let resolved = endpoint.rewrap_ws(ws_listener.local_endpoint().clone());
    ws_listener.set_monitor_endpoint(resolved.clone());
    Ok(BoundListener {
        listener: AnyListener::Ws(ws_listener),
        endpoint: resolved,
    })
}

/// Validate connect-side DNS synchronously before registering a dialer.
/// No socket connect happens here; reconnect attempts resolve again later.
pub(super) async fn preflight_connect_endpoint_resolution(endpoint: &Endpoint) -> Result<()> {
    if endpoint.is_tcp_family() {
        let plain = endpoint.underlying_tcp();
        let Endpoint::Tcp { host, port } = &plain else {
            unreachable!();
        };
        return preflight_connect_host(host, *port).await;
    }
    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        let plain = endpoint.underlying_ws();
        let (host, port) = match &plain {
            Endpoint::Ws { host, port, .. } | Endpoint::Wss { host, port, .. } => (host, *port),
            _ => unreachable!(),
        };
        return preflight_connect_host(host, port).await;
    }
    #[cfg(feature = "quic")]
    if let Endpoint::Quic { host, port } = endpoint {
        return preflight_connect_host(host, *port).await;
    }
    match endpoint {
        Endpoint::Inproc { .. } | Endpoint::Ipc(_) | Endpoint::Shm(_) => Ok(()),
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
    }
}

async fn preflight_connect_host(host: &Host, port: u16) -> Result<()> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(_) => Ok(()),
        Host::Name(name) => crate::transport::dns::resolve(name, port).await.map(|_| ()),
        _ => unreachable!(),
    }
}

/// Connect dispatch (single attempt). Used under `dial_with_backoff`.
pub(super) async fn connect_any(
    endpoint: &Endpoint,
    inproc: &InprocSetup,
    #[cfg(any(feature = "ws", feature = "quic"))] carrier: CarrierConnect<'_>,
) -> Result<AnyConn> {
    if endpoint.is_tcp_family() {
        let s = TcpTransport::connect(&endpoint.underlying_tcp()).await?;
        return Ok(byte_stream(
            AnyStream::Tcp(s),
            peer_ident_for_endpoint(endpoint),
        ));
    }

    #[cfg(feature = "ws")]
    if endpoint.is_ws_family() {
        return connect_ws(endpoint, carrier).await;
    }

    #[cfg(feature = "quic")]
    if endpoint.is_quic_family() {
        let stream = crate::transport::quic::connect(
            endpoint,
            carrier.options,
            carrier.deadline,
            carrier.io_pool,
        )
        .await?;
        let peer_ident = PeerIdent::Socket(stream.remote_address());
        return Ok(byte_stream(AnyStream::Quic(Box::new(stream)), peer_ident));
    }

    // SHM sets up over a Unix stream; the actor wraps it after connect.
    #[cfg(target_os = "linux")]
    if let Endpoint::Shm(path) = endpoint {
        let stream = IpcTransport::connect(&Endpoint::Ipc(path.clone())).await?;
        return Ok(byte_stream(
            AnyStream::Ipc(stream),
            peer_ident_for_endpoint(endpoint),
        ));
    }

    match endpoint {
        Endpoint::Inproc { name } => {
            let conn = inproc_transport::connect(
                &inproc.registry,
                name,
                inproc.snapshot.clone(),
                inproc.recv,
            )
            .await?;
            Ok(AnyConn::Inproc {
                conn,
                peer_ident: PeerIdent::Inproc(name.clone()),
            })
        }
        Endpoint::Ipc(_) => {
            let s = IpcTransport::connect(endpoint).await?;
            Ok(byte_stream(
                AnyStream::Ipc(s),
                peer_ident_for_endpoint(endpoint),
            ))
        }
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
    }
}

/// Connect a WS/WSS transport. Bytes read past the HTTP upgrade belong to
/// the ZMTP stream.
#[cfg(feature = "ws")]
async fn connect_ws(endpoint: &Endpoint, carrier: CarrierConnect<'_>) -> Result<AnyConn> {
    let plain = endpoint.underlying_ws();
    let (host, port, path) = match &plain {
        Endpoint::Ws {
            host, port, path, ..
        }
        | Endpoint::Wss {
            host, port, path, ..
        } => (host, *port, path.as_str()),
        _ => unreachable!(),
    };

    let target = crate::transport::ws::WsTarget {
        host,
        port,
        path,
        tls: matches!(plain, Endpoint::Wss { .. }),
    };

    let connected =
        crate::transport::ws::connect(target, &carrier.options.wss_tls, &carrier.options.mechanism)
            .await?;
    Ok(AnyConn::ByteStream {
        stream: AnyStream::Ws(Box::new(connected.transport)),
        peer_ident: peer_ident_for_endpoint(endpoint),
        leftover: connected.leftover,
        setup: None,
    })
}

/// A connected byte stream with no buffered bytes or carrier setup state.
fn byte_stream(stream: AnyStream, peer_ident: PeerIdent) -> AnyConn {
    AnyConn::ByteStream {
        stream,
        peer_ident,
        leftover: bytes::Bytes::new(),
        setup: None,
    }
}

pub(super) fn peer_ident_for_endpoint(endpoint: &Endpoint) -> PeerIdent {
    match endpoint {
        Endpoint::Tcp { host, port } => PeerIdent::Path(format!("{host}:{port}")),
        Endpoint::Inproc { name } => PeerIdent::Inproc(name.clone()),
        other => PeerIdent::Path(other.to_string()),
    }
}

pub(super) fn peer_ident_socket_addr(ident: &PeerIdent) -> Option<std::net::SocketAddr> {
    match ident {
        PeerIdent::Socket(a) => Some(*a),
        _ => None,
    }
}

pub(super) use omq_proto::message::generated_identity;

mod stream;

pub(crate) use stream::{AnyReadHalf, AnyStream, AnyWriteHalf, Migratable};

#[cfg(test)]
mod tests;
