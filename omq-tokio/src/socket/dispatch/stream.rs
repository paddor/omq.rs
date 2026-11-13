//! `AnyStream`: one byte stream over every ZMTP carrier, with its split
//! halves and reactor migration.

use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::transport::PeerIdent;
use crate::transport::ipc::IpcStream;

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
            Self::Tcp(stream) => {
                let (reader, writer) = stream.into_split();
                (
                    AnyReadHalf::Tcp(reader),
                    AnyWriteHalf::Tcp { writer, fast_write },
                )
            }
            #[cfg(feature = "quic")]
            Self::Quic(quic) => {
                let (reader, writer) = quic.into_split();
                (AnyReadHalf::Quic(reader), AnyWriteHalf::Quic(writer))
            }
            // Other carriers share one lock-backed split.
            stream => {
                let (reader, writer) = tokio::io::split(stream);
                (AnyReadHalf::Other(reader), AnyWriteHalf::Other(writer))
            }
        }
    }

    /// Apply per-socket TCP options (keepalive, user timeout, buffer
    /// sizes) to TCP and to the TCP stream under a WebSocket. Called from
    /// the actor on every accepted / connected stream so the option lives
    /// for the connection's lifetime.
    pub(crate) fn apply_tcp_options(&self, options: &omq_proto::Options) -> std::io::Result<()> {
        fn apply_tcp(stream: &TcpStream, options: &omq_proto::Options) -> std::io::Result<()> {
            // One rejected option must not skip the others.
            let keepalive = options.tcp_keepalive.apply(stream);
            let user_timeout = options.apply_tcp_user_timeout(stream);
            let buffers = options.apply_socket_buffers(stream);
            keepalive.and(user_timeout).and(buffers)
        }
        match self {
            #[cfg(test)]
            Self::Memory(_) => Ok(()),
            Self::Tcp(s) => apply_tcp(s, options),
            #[cfg(unix)]
            Self::Ipc(s) => options.apply_socket_buffers(s),
            #[cfg(target_os = "linux")]
            Self::Shm(s) => s.apply_options(options),
            #[cfg(target_os = "windows")]
            Self::Ipc(_) => Ok(()),
            #[cfg(feature = "ws")]
            Self::Ws(s) => apply_tcp(s.tcp(), options),
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

crate::transport::delegate_async_io!(AnyStream {
    #[cfg(test)]
    Memory,
    Tcp,
    Ipc,
    #[cfg(target_os = "linux")]
    Shm,
    #[cfg(feature = "ws")]
    Ws,
    #[cfg(feature = "quic")]
    Quic,
});
