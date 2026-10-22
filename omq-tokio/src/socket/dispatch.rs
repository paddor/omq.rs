//! Transport dispatch types and bind / connect helpers.
//!
//! `AnyStream` is the common byte-stream half (TCP or IPC); inproc
//! has its own non-byte-stream Message-channel pair carried inside
//! `AnyConn::Inproc`. `AnyListener` wraps the same three transports
//! on the bind side. `bind_any` / `connect_any` are the dispatch
//! entry points the socket actor calls from its bind / dial paths.

use std::io;
use std::io::IoSlice;
use std::io::Write;
use std::pin::Pin;
#[cfg(test)]
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Caller-side TCP writer for the latency profile. It uses a duplicated
/// nonblocking descriptor, so sends do not enter the connection driver's
/// reactor loop.
#[derive(Debug)]
pub(crate) struct DirectTcpWriter {
    state: Mutex<DirectWriteState>,
}

#[derive(Debug)]
pub(crate) struct DirectWriteState<W = std::net::TcpStream> {
    stream: W,
    ownership: crate::engine::write_ownership::WriteOwnership,
}

impl DirectTcpWriter {
    pub(crate) fn new(stream: std::net::TcpStream) -> Self {
        Self {
            state: Mutex::new(DirectWriteState {
                stream,
                ownership: crate::engine::write_ownership::WriteOwnership::new(),
            }),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, DirectWriteState> {
        self.state.lock().expect("direct writer state")
    }

    pub(crate) fn claim_driver(&self) -> bool {
        self.lock().ownership.claim_driver()
    }

    /// Queue observations must happen under the same lock as send admission.
    pub(crate) fn publish_idle(&self, empty: impl FnOnce() -> bool) {
        self.lock().ownership.publish_idle(empty);
    }
}

impl<W: Write> DirectWriteState<W> {
    pub(crate) fn is_closed(&self) -> bool {
        self.ownership.is_closed()
    }

    pub(crate) fn close(&mut self) {
        self.ownership.close();
    }

    pub(crate) fn queued(&mut self) {
        self.ownership.claim_driver();
    }

    /// Called with admission locked. Ineligible means enqueue the owned message.
    pub(crate) fn try_send(
        &mut self,
        slot: &crate::engine::transmit_slot::PeerTransmitSlot,
        message: &omq_proto::Message,
    ) -> crate::engine::transmit_slot::TryFrameResult {
        use crate::engine::transmit_slot::TryFrameResult;
        if self.is_closed() {
            return TryFrameResult::Dead;
        }
        if !self.ownership.is_idle() {
            return TryFrameResult::Ineligible;
        }
        let result = slot.try_encode_without_signal(message);
        if result != TryFrameResult::Ok {
            return result;
        }
        // Admission succeeded. A subsequent peer failure must not become a
        // socket-closed error, nor cause the caller to retry accepted bytes.
        match slot.try_direct_write(|slices| self.try_write(slices)) {
            Ok(()) if slot.is_empty() => {}
            Ok(()) => {
                self.queued();
                slot.data_signal.mark();
            }
            Err(_) => {
                self.close();
                slot.data_signal.mark();
            }
        }
        TryFrameResult::Ok
    }

    fn try_write(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        let written = match slices {
            [one] => self.stream.write(one),
            _ => self.stream.write_vectored(slices),
        };
        match written {
            Ok(0) => Err(io::Error::new(io::ErrorKind::WriteZero, "tcp write")),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e),
        }
    }
}

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

use crate::transport::ipc::IpcStream;
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

/// Re-register a stream with the current thread's I/O reactor. Each
/// `current_thread` tokio runtime has its own reactor; a stream
/// accepted on one thread must be migrated before a driver on another
/// thread can poll it.
pub(crate) trait Migratable: Sized {
    fn migrate(self) -> io::Result<Self>;
}

/// Byte-stream dispatch across TCP-shaped transports (TCP, IPC, WS).
/// Inproc does NOT go through this - it skips the ZMTP codec entirely
/// and uses its own Message-typed channel pair (see `AnyConn`).
#[derive(Debug)]
pub(crate) enum AnyStream {
    #[cfg(test)]
    Memory(tokio::io::DuplexStream),
    Tcp(TcpStream),
    Ipc(IpcStream),
    #[cfg(target_os = "linux")]
    Shm(crate::transport::shm::ShmStream),
    #[cfg(feature = "ws")]
    Ws(Box<crate::transport::ws::WsTransport>),
    #[cfg(feature = "quic")]
    Quic(Box<crate::transport::quic::QuicStream>),
}

impl AnyStream {
    /// Capture observations before the transport is split or migrated.
    pub(crate) fn addresses(&self) -> (Option<PeerIdent>, Option<PeerIdent>) {
        match self {
            Self::Tcp(stream) => (
                stream.local_addr().ok().map(PeerIdent::Socket),
                stream.peer_addr().ok().map(PeerIdent::Socket),
            ),
            #[cfg(unix)]
            Self::Ipc(stream) => (
                ipc_address(stream.local_addr()),
                ipc_address(stream.peer_addr()),
            ),
            #[cfg(feature = "ws")]
            Self::Ws(stream) => {
                use crate::transport::ws::WsTransport;
                let tcp = match stream.as_ref() {
                    WsTransport::Plain(tcp) => tcp,
                    WsTransport::Tls(tls) => tls.get_ref().0,
                    WsTransport::TlsServer(tls) => tls.get_ref().0,
                };
                (
                    tcp.local_addr().ok().map(PeerIdent::Socket),
                    tcp.peer_addr().ok().map(PeerIdent::Socket),
                )
            }
            #[cfg(feature = "quic")]
            Self::Quic(stream) => (
                stream.local_address().map(PeerIdent::Socket),
                Some(PeerIdent::Socket(stream.remote_address())),
            ),
            // SHM addresses are captured from its IPC rendezvous before wrapping.
            #[cfg(target_os = "linux")]
            Self::Shm(_) => (None, None),
            #[cfg(target_os = "windows")]
            Self::Ipc(_) => (None, None),
            #[cfg(test)]
            Self::Memory(_) => (None, None),
        }
    }
}

#[cfg(unix)]
fn ipc_address(address: io::Result<tokio::net::unix::SocketAddr>) -> Option<PeerIdent> {
    let address = address.ok()?;
    if let Some(path) = address.as_pathname() {
        return Some(PeerIdent::Path(path.to_string_lossy().into_owned()));
    }
    #[cfg(target_os = "linux")]
    if let Some(name) = address.as_abstract_name() {
        return Some(PeerIdent::Path(format!(
            "@{}",
            String::from_utf8_lossy(name)
        )));
    }
    None
}

impl Migratable for AnyStream {
    fn migrate(self) -> io::Result<Self> {
        match self {
            #[cfg(test)]
            Self::Memory(stream) => Ok(Self::Memory(stream)),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Ok(Self::Shm(s.migrate()?)),
            Self::Tcp(s) => {
                let std = s.into_std()?;
                Ok(Self::Tcp(TcpStream::from_std(std)?))
            }
            #[cfg(unix)]
            Self::Ipc(s) => {
                let std = s.into_std()?;
                Ok(Self::Ipc(IpcStream::from_std(std)?))
            }
            #[cfg(target_os = "windows")]
            Self::Ipc(s) => Ok(Self::Ipc(s)),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Ok(Self::Ws(Box::new(s.migrate()?))),
            // Quinn streams hold no reactor registration; the endpoint's
            // UDP socket stays on the runtime that created it.
            #[cfg(feature = "quic")]
            Self::Quic(s) => Ok(Self::Quic(s)),
        }
    }
}

impl AnyStream {
    /// Split the stream while retaining a TCP write-half fast path.
    pub(crate) fn split(self, fast_write: bool) -> (AnyReadHalf, AnyWriteHalf) {
        match self {
            #[cfg(test)]
            Self::Memory(stream) => {
                let (reader, writer) = tokio::io::split(Self::Memory(stream));
                (AnyReadHalf::Other(reader), AnyWriteHalf::Other(writer))
            }
            Self::Tcp(stream) => {
                let (reader, writer) = stream.into_split();
                (
                    AnyReadHalf::Tcp(reader),
                    AnyWriteHalf::Tcp { writer, fast_write },
                )
            }
            #[cfg(target_os = "linux")]
            Self::Shm(stream) => {
                let (reader, writer) = tokio::io::split(Self::Shm(stream));
                (AnyReadHalf::Other(reader), AnyWriteHalf::Other(writer))
            }
            Self::Ipc(stream) => {
                let (reader, writer) = tokio::io::split(Self::Ipc(stream));
                (AnyReadHalf::Other(reader), AnyWriteHalf::Other(writer))
            }
            #[cfg(feature = "ws")]
            Self::Ws(ws) => {
                let (reader, writer) = tokio::io::split(Self::Ws(ws));
                (AnyReadHalf::Other(reader), AnyWriteHalf::Other(writer))
            }
            #[cfg(feature = "quic")]
            Self::Quic(quic) => {
                let (reader, writer) = quic.into_split();
                (AnyReadHalf::Quic(reader), AnyWriteHalf::Quic(writer))
            }
        }
    }

    /// Apply per-socket TCP options (keepalive, user timeout). No-op
    /// for non-TCP variants. Called from the actor on every accepted /
    /// connected stream so the option lives for the connection's
    /// lifetime.
    pub(crate) fn apply_tcp_options(&self, options: &omq_proto::Options) -> std::io::Result<()> {
        match self {
            #[cfg(test)]
            Self::Memory(_) => Ok(()),
            Self::Tcp(s) => {
                options.tcp_keepalive.apply(s)?;
                options.apply_tcp_user_timeout(s)?;
                options.apply_socket_buffers(s)?;
                Ok(())
            }
            #[cfg(unix)]
            Self::Ipc(s) => options.apply_socket_buffers(s),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => s.apply_options(options),
            #[cfg(target_os = "windows")]
            Self::Ipc(_) => Ok(()),
            #[cfg(feature = "ws")]
            Self::Ws(_) => Ok(()),
            #[cfg(feature = "quic")]
            Self::Quic(_) => Ok(()),
        }
    }
}

pub(crate) enum AnyReadHalf {
    Tcp(OwnedReadHalf),
    Other(tokio::io::ReadHalf<AnyStream>),
    #[cfg(feature = "quic")]
    Quic(crate::transport::quic::QuicRecvHalf),
}

impl AsyncRead for AnyReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp(reader) => Pin::new(reader).poll_read(cx, buf),
            Self::Other(reader) => Pin::new(reader).poll_read(cx, buf),
            #[cfg(feature = "quic")]
            Self::Quic(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

pub(crate) enum AnyWriteHalf {
    Tcp {
        writer: OwnedWriteHalf,
        fast_write: bool,
    },
    Other(tokio::io::WriteHalf<AnyStream>),
    #[cfg(feature = "quic")]
    Quic(crate::transport::quic::QuicSendHalf),
}

impl AsyncWrite for AnyWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Tcp { writer, fast_write } => {
                if *fast_write {
                    match writer.try_write(buf) {
                        Ok(n) => return Poll::Ready(Ok(n)),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Pin::new(writer).poll_write(cx, buf)
            }
            Self::Other(writer) => Pin::new(writer).poll_write(cx, buf),
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Pin::new(writer).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Tcp { writer, fast_write } => {
                if *fast_write {
                    match writer.try_write_vectored(bufs) {
                        Ok(n) => return Poll::Ready(Ok(n)),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Pin::new(writer).poll_write_vectored(cx, bufs)
            }
            Self::Other(writer) => Pin::new(writer).poll_write_vectored(cx, bufs),
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Pin::new(writer).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp { writer, .. } => writer.is_write_vectored(),
            Self::Other(writer) => writer.is_write_vectored(),
            #[cfg(feature = "quic")]
            Self::Quic(writer) => writer.is_write_vectored(),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp { writer, .. } => Pin::new(writer).poll_flush(cx),
            Self::Other(writer) => Pin::new(writer).poll_flush(cx),
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp { writer, .. } => Pin::new(writer).poll_shutdown(cx),
            Self::Other(writer) => Pin::new(writer).poll_shutdown(cx),
            #[cfg(feature = "quic")]
            Self::Quic(writer) => Pin::new(writer).poll_shutdown(cx),
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
            #[cfg(test)]
            Self::Memory(s) => Pin::new(s).poll_read(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "quic")]
            Self::Quic(s) => Pin::new(s).poll_read(cx, buf),
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
            #[cfg(test)]
            Self::Memory(s) => Pin::new(s).poll_write(cx, buf),
            Self::Ipc(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "quic")]
            Self::Quic(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(test)]
            Self::Memory(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Ipc(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(feature = "quic")]
            Self::Quic(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(s) => s.is_write_vectored(),
            #[cfg(test)]
            Self::Memory(s) => s.is_write_vectored(),
            Self::Ipc(s) => s.is_write_vectored(),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => s.is_write_vectored(),
            #[cfg(feature = "ws")]
            Self::Ws(s) => s.is_write_vectored(),
            #[cfg(feature = "quic")]
            Self::Quic(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(test)]
            Self::Memory(s) => Pin::new(s).poll_flush(cx),
            Self::Ipc(s) => Pin::new(s).poll_flush(cx),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "quic")]
            Self::Quic(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(test)]
            Self::Memory(s) => Pin::new(s).poll_shutdown(cx),
            Self::Ipc(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "ws")]
            Self::Ws(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "quic")]
            Self::Quic(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
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
            Self::Tcp(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Tcp(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
                setup: None,
            }),
            Self::Inproc(l) => {
                let peer_ident = PeerIdent::Inproc(l.name().to_string());
                let conn = l.accept().await?;
                Ok(AnyConn::Inproc { conn, peer_ident })
            }
            #[cfg(target_os = "linux")]
            Self::Shm(l, _) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Ipc(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
                setup: None,
            }),
            Self::Ipc(l) => l.accept().await.map(|(s, peer_ident)| AnyConn::ByteStream {
                stream: AnyStream::Ipc(s),
                peer_ident,
                leftover: bytes::Bytes::new(),
                setup: None,
            }),
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

/// Bind dispatch: route an endpoint to its transport's listener and wrap it.
///
/// `lz4+tcp://` reuses the TCP listener; the per-connection
/// transform is installed by the actor based on the endpoint scheme.
pub(super) async fn bind_any(
    inproc_registry: &std::sync::Arc<inproc_transport::InprocRegistry>,
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
    recv: &inproc_transport::RecvConfig,
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
            inproc_registry.clone(),
            name,
            snapshot.clone(),
            *recv,
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
    inproc_registry: &inproc_transport::InprocRegistry,
    endpoint: &Endpoint,
    snapshot: &InprocPeerSnapshot,
    recv: &inproc_transport::RecvConfig,
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
            let conn =
                inproc_transport::connect(inproc_registry, name, snapshot.clone(), *recv).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use omq_proto::proto::SocketType;
    use tokio::io::AsyncWrite;
    #[cfg(unix)]
    use tokio::net::UnixStream;

    fn snapshot() -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: SocketType::Pair,
            identity: Bytes::new(),
        }
    }

    async fn bind_result_for_test(endpoint: &Endpoint) -> Result<BoundListener> {
        #[cfg(any(feature = "ws", feature = "quic"))]
        let carrier_options = omq_proto::Options::default();
        let inproc_registry = Arc::new(inproc_transport::InprocRegistry::new());
        bind_any(
            &inproc_registry,
            endpoint,
            &snapshot(),
            &inproc_transport::RecvConfig {
                direct: false,
                send_hwm: 1000,
            },
            #[cfg(any(feature = "ws", feature = "quic"))]
            CarrierAccept {
                options: &carrier_options,
                setup: crate::transport::setup::AcceptSetup {
                    admission: crate::transport::setup::Admission::new(128),
                    timeout: std::time::Duration::from_secs(30),
                    cancel: tokio_util::sync::CancellationToken::new(),
                    monitor: crate::socket::monitor::MonitorPublisher::new(
                        omq_proto::options::DEFAULT_MONITOR_CAPACITY,
                    ),
                    io_pool: crate::context::IoPoolHandle::none(),
                },
            },
        )
        .await
    }

    async fn bind_for_test(endpoint: &Endpoint) -> BoundListener {
        bind_result_for_test(endpoint).await.unwrap()
    }

    #[test]
    fn any_stream_tcp_reports_write_vectored() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let tcp = TcpStream::connect(addr).await.unwrap();
            assert!(tcp.is_write_vectored());
            let any = AnyStream::Tcp(tcp);
            assert!(any.is_write_vectored());
        });
    }

    #[cfg(unix)]
    #[test]
    fn any_stream_ipc_reports_write_vectored() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = std::env::temp_dir();
            let path = dir.join(format!("omq-test-writev-{}.sock", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let client = UnixStream::connect(&path).await.unwrap();
            let _ = listener.accept().await.unwrap();
            assert!(client.is_write_vectored());
            let any = AnyStream::Ipc(client);
            assert!(any.is_write_vectored());
            let _ = std::fs::remove_file(&path);
        });
    }

    #[tokio::test]
    async fn bind_any_returns_resolved_tcp_endpoint() {
        let endpoint = Endpoint::Tcp {
            host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            port: 0,
        };
        let bound = bind_for_test(&endpoint).await;

        match bound.endpoint {
            Endpoint::Tcp {
                host: Host::Ip(ip),
                port,
            } => {
                assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                assert_ne!(port, 0);
            }
            other => panic!("expected resolved TCP endpoint, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bind_any_rejects_unresolved_tcp_name() {
        let endpoint = Endpoint::Tcp {
            host: Host::Name("omq-bind-preflight.invalid".into()),
            port: 5555,
        };

        assert!(bind_result_for_test(&endpoint).await.is_err());
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn bind_any_returns_resolved_ws_endpoint() {
        let endpoint = Endpoint::Ws {
            host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            port: 0,
            path: "/z".into(),
        };
        let bound = bind_for_test(&endpoint).await;

        match bound.listener.local_endpoint() {
            Endpoint::Ws {
                host: Host::Ip(ip),
                port,
                path,
            } => {
                assert_eq!(*ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                assert_ne!(*port, 0);
                assert_eq!(path, "/z");
            }
            other => panic!("expected resolved WS listener endpoint, got {other:?}"),
        }
        match bound.endpoint {
            Endpoint::Ws {
                host: Host::Ip(ip),
                port,
                path,
            } => {
                assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                assert_ne!(port, 0);
                assert_eq!(path, "/z");
            }
            other => panic!("expected resolved WS endpoint, got {other:?}"),
        }
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn bind_any_rejects_unresolved_ws_name() {
        let endpoint = Endpoint::Ws {
            host: Host::Name("omq-bind-ws-preflight.invalid".into()),
            port: 5555,
            path: "/z".into(),
        };

        assert!(bind_result_for_test(&endpoint).await.is_err());
    }

    #[cfg(all(feature = "lz4", feature = "ws"))]
    #[tokio::test]
    async fn bind_any_returns_resolved_lz4_ws_endpoint() {
        let endpoint = Endpoint::Lz4Ws {
            host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            port: 0,
            path: "/lz4".into(),
        };
        let bound = bind_for_test(&endpoint).await;

        match bound.endpoint {
            Endpoint::Lz4Ws {
                host: Host::Ip(ip),
                port,
                path,
            } => {
                assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                assert_ne!(port, 0);
                assert_eq!(path, "/lz4");
            }
            other => panic!("expected resolved LZ4 WS endpoint, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connect_preflight_rejects_tcp_wildcard() {
        let endpoint = Endpoint::Tcp {
            host: Host::Wildcard,
            port: 5555,
        };

        assert!(matches!(
            preflight_connect_endpoint_resolution(&endpoint).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_preflight_rejects_unresolved_tcp_name() {
        let endpoint = Endpoint::Tcp {
            host: Host::Name("omq-connect-preflight.invalid".into()),
            port: 5555,
        };

        assert!(
            preflight_connect_endpoint_resolution(&endpoint)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn connect_preflight_accepts_named_tcp_without_socket_connect() {
        let endpoint = Endpoint::Tcp {
            host: Host::Name("127.0.0.1".into()),
            port: 9,
        };

        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .unwrap();
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn connect_preflight_accepts_named_lz4_tcp_without_socket_connect() {
        let endpoint = Endpoint::Lz4Tcp {
            host: Host::Name("127.0.0.1".into()),
            port: 9,
        };

        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .unwrap();
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn connect_preflight_rejects_unresolved_ws_name() {
        let endpoint = Endpoint::Ws {
            host: Host::Name("omq-connect-ws-preflight.invalid".into()),
            port: 5555,
            path: "/z".into(),
        };

        assert!(
            preflight_connect_endpoint_resolution(&endpoint)
                .await
                .is_err()
        );
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn connect_preflight_accepts_named_ws_without_socket_connect() {
        let endpoint = Endpoint::Ws {
            host: Host::Name("127.0.0.1".into()),
            port: 9,
            path: "/z".into(),
        };

        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .unwrap();
    }

    #[cfg(all(feature = "lz4", feature = "ws"))]
    #[tokio::test]
    async fn connect_preflight_accepts_named_lz4_ws_without_socket_connect() {
        let endpoint = Endpoint::Lz4Ws {
            host: Host::Name("127.0.0.1".into()),
            port: 9,
            path: "/z".into(),
        };

        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod direct_tests;
