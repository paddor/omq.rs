//! Transport dispatch types and bind / connect helpers.
//!
//! `AnyStream` is the common byte-stream half (TCP or IPC); inproc
//! has its own non-byte-stream Message-channel pair carried inside
//! `AnyConn::Inproc`. `AnyListener` wraps the same three transports
//! on the bind side. `bind_any` / `connect_any` are the dispatch
//! entry points the socket actor calls from its bind / dial paths.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};

use crate::transport::{
    InprocConn, InprocPeerSnapshot, IpcTransport, Listener as _, PeerIdent, TcpTransport,
    Transport as _, inproc as inproc_transport,
};

/// Byte-stream dispatch across TCP-shaped transports (TCP and IPC).
/// Inproc does NOT go through this - it skips the ZMTP codec entirely
/// and uses its own Message-typed channel pair (see `AnyConn`).
#[derive(Debug)]
pub(crate) enum AnyStream {
    Tcp(TcpStream),
    Ipc(UnixStream),
}

impl AnyStream {
    /// Apply per-socket TCP options (currently just keepalive). No-op
    /// for non-TCP variants. Called from the actor on every accepted /
    /// connected stream so the option lives for the connection's
    /// lifetime.
    pub(crate) fn apply_tcp_options(&self, options: &omq_proto::Options) -> std::io::Result<()> {
        match self {
            Self::Tcp(s) => options.tcp_keepalive.apply(s),
            Self::Ipc(_) => Ok(()),
        }
    }
}

impl AsyncRead for AnyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AnyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            Self::Ipc(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::Ipc(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// What `bind_any` / `connect_any` hand back. Either a byte-stream
/// (TCP / IPC - runs the ZMTP codec via `ConnectionDriver`) or a
/// pre-paired Message channel (inproc - runs the codec-less
/// `InprocPeerDriver`).
pub(crate) enum AnyConn {
    ByteStream { stream: AnyStream, peer_ident: PeerIdent },
    Inproc { conn: InprocConn, peer_ident: PeerIdent },
}

impl std::fmt::Debug for AnyConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByteStream { peer_ident, .. } => {
                f.debug_struct("AnyConn::ByteStream").field("peer_ident", peer_ident).finish()
            }
            Self::Inproc { peer_ident, .. } => {
                f.debug_struct("AnyConn::Inproc").field("peer_ident", peer_ident).finish()
            }
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

pub(super) enum AnyListener {
    Tcp(crate::transport::tcp::TcpListener),
    Inproc(crate::transport::InprocListener),
    Ipc(crate::transport::ipc::IpcListener),
}

impl AnyListener {
    pub(super) fn local_endpoint(&self) -> &Endpoint {
        match self {
            Self::Tcp(l) => l.local_endpoint(),
            Self::Inproc(l) => l.local_endpoint(),
            Self::Ipc(l) => l.local_endpoint(),
        }
    }

    pub(super) async fn accept(&mut self) -> Result<AnyConn> {
        match self {
            Self::Tcp(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Tcp(s),
                peer_ident,
            }),
            Self::Inproc(l) => {
                let peer_ident = PeerIdent::Inproc(l.name().to_string());
                let conn = l.accept().await?;
                Ok(AnyConn::Inproc { conn, peer_ident })
            }
            Self::Ipc(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Ipc(s),
                peer_ident,
            }),
        }
    }
}

/// Bind dispatch: route an endpoint to its transport's listener and wrap it.
///
/// `lz4+tcp://` and `zstd+tcp://` reuse the TCP listener; the per-connection
/// transform is installed by the actor based on the endpoint scheme.
pub(super) async fn bind_any(
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
) -> Result<AnyListener> {
    if endpoint.is_tcp_family() {
        return Ok(AnyListener::Tcp(
            TcpTransport::bind(&endpoint.underlying_tcp()).await?,
        ));
    }
    match endpoint {
        Endpoint::Inproc { name } => {
            Ok(AnyListener::Inproc(inproc_transport::bind(name, snapshot.clone())?))
        }
        Endpoint::Ipc(_) => Ok(AnyListener::Ipc(IpcTransport::bind(endpoint).await?)),
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
    }
}

/// Connect dispatch (single attempt). Used under `dial_with_backoff`.
pub(super) async fn connect_any(
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
) -> Result<AnyConn> {
    if endpoint.is_tcp_family() {
        let s = TcpTransport::connect(&endpoint.underlying_tcp()).await?;
        let peer_ident = peer_ident_for_endpoint(endpoint);
        return Ok(AnyConn::ByteStream { stream: AnyStream::Tcp(s), peer_ident });
    }
    match endpoint {
        Endpoint::Inproc { name } => {
            let conn = inproc_transport::connect(name, snapshot.clone()).await?;
            Ok(AnyConn::Inproc {
                conn,
                peer_ident: PeerIdent::Inproc(name.clone()),
            })
        }
        Endpoint::Ipc(_) => {
            let s = IpcTransport::connect(endpoint).await?;
            let peer_ident = peer_ident_for_endpoint(endpoint);
            Ok(AnyConn::ByteStream { stream: AnyStream::Ipc(s), peer_ident })
        }
        other => Err(Error::UnsupportedScheme(other.scheme().to_string())),
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

/// Synthesize a routing identity when a peer didn't provide one. libzmq's
/// ROUTER assigns a 5-byte random id; we use the peer's sequence id so it
/// stays stable for the lifetime of the connection and can't collide
/// across peers.
pub(super) fn generated_identity(peer_id: u64) -> bytes::Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(0); // libzmq-style leading null marks "auto-generated"
    buf.extend_from_slice(&peer_id.to_be_bytes());
    bytes::Bytes::from(buf)
}
