//! IPC transport: `ipc://path` (Unix filesystem), `ipc://@name` (Linux abstract),
//! or `ipc://name` (Windows named pipes).
//!
//! **Unix filesystem** (`ipc://path`):
//! Listener removes stale socket file at its path on bind and drop.
//!
//! **Linux abstract namespace** (`ipc://@name`):
//! Uses leading-null `sockaddr_un`. Abstract sockets carry no filesystem entry
//! and are torn down by the kernel when the last fd closes.
//!
//! **Windows named pipes** (`ipc://name`):
//! Uses `tokio::net::windows::named_pipe` for byte-stream IPC.

#[cfg(unix)]
use std::path::{Path, PathBuf};

#[cfg(unix)]
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};

#[cfg(target_os = "windows")]
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};

use omq_proto::endpoint::{Endpoint, IpcPath};
use omq_proto::error::{Error, Result};

use super::{Listener, PeerIdent, Transport};

/// Platform-specific IPC stream type.
#[cfg(unix)]
pub type IpcStream = UnixStream;

#[cfg(target_os = "windows")]
#[derive(Debug)]
pub enum IpcStream {
    Server(NamedPipeServer),
    Client(NamedPipeClient),
}

#[cfg(target_os = "windows")]
impl std::fmt::Display for IpcStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server(_) => write!(f, "NamedPipeServer"),
            Self::Client(_) => write!(f, "NamedPipeClient"),
        }
    }
}

#[cfg(target_os = "windows")]
impl tokio::io::AsyncRead for IpcStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Server(server) => std::pin::Pin::new(server).poll_read(cx, buf),
            Self::Client(client) => std::pin::Pin::new(client).poll_read(cx, buf),
        }
    }
}

#[cfg(target_os = "windows")]
impl tokio::io::AsyncWrite for IpcStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Server(server) => std::pin::Pin::new(server).poll_write(cx, buf),
            Self::Client(client) => std::pin::Pin::new(client).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Server(server) => std::pin::Pin::new(server).poll_flush(cx),
            Self::Client(client) => std::pin::Pin::new(client).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Server(server) => std::pin::Pin::new(server).poll_shutdown(cx),
            Self::Client(client) => std::pin::Pin::new(client).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Server(server) => std::pin::Pin::new(server).poll_write_vectored(cx, bufs),
            Self::Client(client) => std::pin::Pin::new(client).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Server(server) => server.is_write_vectored(),
            Self::Client(client) => client.is_write_vectored(),
        }
    }
}

#[derive(Debug)]
pub struct IpcTransport;

// ============================================================================
// Platform-specific Transport implementations
// ============================================================================

#[cfg(unix)]
impl Transport for IpcTransport {
    type Stream = IpcStream;
    type Listener = IpcListener;

    fn scheme() -> &'static str {
        "ipc"
    }

    async fn bind(endpoint: &Endpoint) -> Result<Self::Listener> {
        match endpoint {
            Endpoint::Ipc(IpcPath::Filesystem(p)) => bind_filesystem_unix(endpoint, p).await,
            Endpoint::Ipc(IpcPath::Abstract(name)) => bind_abstract_unix(endpoint, name),
            other => Err(Error::InvalidEndpoint(format!(
                "ipc transport got non-ipc endpoint: {other}"
            ))),
        }
    }

    async fn connect(endpoint: &Endpoint) -> Result<Self::Stream> {
        let stream = match endpoint {
            Endpoint::Ipc(IpcPath::Filesystem(p)) => UnixStream::connect(p).await?,
            Endpoint::Ipc(IpcPath::Abstract(name)) => connect_abstract_unix(name)?,
            other => {
                return Err(Error::InvalidEndpoint(format!(
                    "ipc transport got non-ipc endpoint: {other}"
                )));
            }
        };
        tune_unix_buffers(&stream);
        Ok(stream)
    }
}

#[cfg(target_os = "windows")]
impl Transport for IpcTransport {
    type Stream = IpcStream;
    type Listener = IpcListener;

    fn scheme() -> &'static str {
        "ipc"
    }

    fn bind(endpoint: &Endpoint) -> impl Future<Output = Result<Self::Listener>> + Send {
        std::future::ready(match endpoint {
            Endpoint::Ipc(IpcPath::NamedPipe(name)) => bind_named_pipe_windows(endpoint, name),
            other => Err(Error::InvalidEndpoint(format!(
                "ipc transport got non-ipc endpoint: {other}"
            ))),
        })
    }

    async fn connect(endpoint: &Endpoint) -> Result<Self::Stream> {
        match endpoint {
            Endpoint::Ipc(IpcPath::NamedPipe(name)) => connect_named_pipe_windows(name).await,
            other => Err(Error::InvalidEndpoint(format!(
                "ipc transport got non-ipc endpoint: {other}"
            ))),
        }
    }
}

// ============================================================================
// Platform-specific Listener structs
// ============================================================================

/// Bound IPC listener (Unix).
/// For filesystem-path binds, removes the socket file on drop;
/// abstract-namespace binds carry no filesystem entry and need no cleanup.
#[cfg(unix)]
#[derive(Debug)]
pub struct IpcListener {
    inner: TokioUnixListener,
    endpoint: Endpoint,
    cleanup_path: Option<PathBuf>,
    ident: PeerIdent,
}

/// Bound IPC listener (Windows).
/// Stores the pipe name to create new server instances for each connection.
#[cfg(target_os = "windows")]
#[derive(Debug)]
pub struct IpcListener {
    inner: Option<NamedPipeServer>,
    endpoint: Endpoint,
    pipe_path: String,
    name: String,
}

#[cfg(unix)]
impl Listener for IpcListener {
    type Stream = IpcStream;

    fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    async fn accept(&mut self) -> Result<(Self::Stream, PeerIdent)> {
        let (stream, _addr) = self.inner.accept().await?;
        tune_unix_buffers(&stream);
        Ok((stream, self.ident.clone()))
    }
}

#[cfg(target_os = "windows")]
impl Listener for IpcListener {
    type Stream = IpcStream;

    fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    async fn accept(&mut self) -> Result<(Self::Stream, PeerIdent)> {
        // Use existing server if available, otherwise create a new one
        if self.inner.is_none() {
            let server = ServerOptions::new().create(&self.pipe_path)?;
            self.inner = Some(server);
        }

        // Unwrap is safe because we just ensured inner is Some
        #[allow(unused_mut)]
        let mut server = self.inner.take().unwrap();
        // Wait for client connection
        server.connect().await?;
        // Prepare the next instance before handing this one to the connection driver.
        // If creation fails, keep the accepted connection and retry on the next accept.
        self.inner = ServerOptions::new().create(&self.pipe_path).ok();
        Ok((
            IpcStream::Server(server),
            PeerIdent::Path(self.name.clone()),
        ))
    }
}

#[cfg(unix)]
impl Drop for IpcListener {
    fn drop(&mut self) {
        if let Some(path) = &self.cleanup_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(target_os = "windows")]
impl Drop for IpcListener {
    fn drop(&mut self) {
        // NamedPipeServer handles cleanup via Drop impl
    }
}

// ============================================================================
// Unix-specific helpers
// ============================================================================

#[cfg(unix)]
#[expect(clippy::unused_async)]
async fn bind_filesystem_unix(endpoint: &Endpoint, path: &Path) -> Result<IpcListener> {
    // Best-effort cleanup of any stale socket at this path. Ignore
    // failure: the real bind below surfaces a precise error if the
    // path is unusable.
    let _ = std::fs::remove_file(path);
    let listener = TokioUnixListener::bind(path)?;
    Ok(IpcListener {
        inner: listener,
        endpoint: endpoint.clone(),
        cleanup_path: Some(path.to_path_buf()),
        ident: PeerIdent::Path(path.display().to_string()),
    })
}

#[cfg(target_os = "linux")]
fn bind_abstract_unix(endpoint: &Endpoint, name: &str) -> Result<IpcListener> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr as StdSockAddr, UnixListener as StdListener};

    let addr = StdSockAddr::from_abstract_name(name.as_bytes())
        .map_err(|e| Error::InvalidEndpoint(format!("abstract ipc name {name:?}: {e}")))?;
    let std_listener = StdListener::bind_addr(&addr)?;
    std_listener.set_nonblocking(true)?;
    let inner = TokioUnixListener::from_std(std_listener)?;
    Ok(IpcListener {
        inner,
        endpoint: endpoint.clone(),
        cleanup_path: None,
        ident: PeerIdent::Path(format!("@{name}")),
    })
}

#[cfg(not(target_os = "linux"))]
#[cfg(unix)]
fn bind_abstract_unix(_endpoint: &Endpoint, _name: &str) -> Result<IpcListener> {
    Err(Error::UnsupportedScheme(
        "ipc abstract namespace is Linux-only".into(),
    ))
}

#[cfg(target_os = "linux")]
fn connect_abstract_unix(name: &str) -> Result<IpcStream> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr as StdSockAddr, UnixStream as StdStream};

    let addr = StdSockAddr::from_abstract_name(name.as_bytes())
        .map_err(|e| Error::InvalidEndpoint(format!("abstract ipc name {name:?}: {e}")))?;
    let std_stream = StdStream::connect_addr(&addr)?;
    std_stream.set_nonblocking(true)?;
    Ok(UnixStream::from_std(std_stream)?)
}

#[cfg(not(target_os = "linux"))]
#[cfg(unix)]
fn connect_abstract_unix(_name: &str) -> Result<IpcStream> {
    Err(Error::UnsupportedScheme(
        "ipc abstract namespace is Linux-only".into(),
    ))
}

#[cfg(unix)]
const IPC_BUF_SIZE: u32 = 1024 * 1024;

#[cfg(unix)]
fn tune_unix_buffers(stream: &IpcStream) {
    let sock = socket2::SockRef::from(stream);
    let _ = sock.set_send_buffer_size(IPC_BUF_SIZE as usize);
    let _ = sock.set_recv_buffer_size(IPC_BUF_SIZE as usize);
}

// ============================================================================
// Windows bind/connect implementation
// ============================================================================

#[cfg(target_os = "windows")]
const ERROR_PIPE_BUSY: i32 = 231;

#[cfg(target_os = "windows")]
fn bind_named_pipe_windows(endpoint: &Endpoint, name: &str) -> Result<IpcListener> {
    // Construct the full named pipe path
    let pipe_path = format!(r"\\.\pipe\{name}");

    // Create the first named pipe server using ServerOptions
    let server = ServerOptions::new().create(&pipe_path)?;

    Ok(IpcListener {
        inner: Some(server),
        endpoint: endpoint.clone(),
        pipe_path,
        name: name.to_string(),
    })
}

#[cfg(target_os = "windows")]
async fn connect_named_pipe_windows(name: &str) -> Result<IpcStream> {
    // Construct the full named pipe path
    let pipe_path = format!(r"\\.\pipe\{name}");

    let client = loop {
        match ClientOptions::new().open(&pipe_path) {
            Ok(client) => break client,
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Err(e) => return Err(e.into()),
        }
    };

    Ok(IpcStream::Client(client))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[cfg(unix)]
    fn temp_ipc(name: &str) -> Endpoint {
        let short_name: String = name.chars().take(8).collect();
        let path = std::path::PathBuf::from(format!(
            "/tmp/omq-ipc-{short_name}-{}.sock",
            std::process::id()
        ));
        Endpoint::Ipc(IpcPath::Filesystem(path))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_connect_accept_roundtrip() {
        let ep = temp_ipc("basic");
        let mut listener = IpcTransport::bind(&ep).await.unwrap();
        let ep2 = ep.clone();
        let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

        let (mut server_side, peer) = listener.accept().await.unwrap();
        let mut client_side = connect.await.unwrap().unwrap();
        assert!(matches!(peer, PeerIdent::Path(_)));

        client_side.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        server_side.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn abstract_bind_connect_roundtrip() {
        // Random suffix avoids cross-test collisions in the abstract namespace.
        let name = format!(
            "omq-ipc-abs-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        );
        let ep = Endpoint::Ipc(IpcPath::Abstract(name));
        let mut listener = IpcTransport::bind(&ep).await.unwrap();
        let ep2 = ep.clone();
        let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

        let (mut server_side, peer) = listener.accept().await.unwrap();
        let mut client_side = connect.await.unwrap().unwrap();
        match peer {
            PeerIdent::Path(p) => assert!(p.starts_with('@')),
            other => panic!("expected abstract peer ident, got {other:?}"),
        }

        client_side.write_all(b"abstract").await.unwrap();
        let mut buf = [0u8; 8];
        server_side.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abstract");
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn abstract_rejected_off_linux() {
        #[cfg(unix)]
        {
            let ep = Endpoint::Ipc(IpcPath::Abstract("foo".into()));
            assert!(matches!(
                IpcTransport::bind(&ep).await,
                Err(Error::UnsupportedScheme(_))
            ));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_cleans_up_stale_socket() {
        let ep = temp_ipc("stale");
        // First bind creates the socket file.
        {
            let _l = IpcTransport::bind(&ep).await.unwrap();
        }
        // Drop removed the file. Re-bind should still succeed.
        let _l = IpcTransport::bind(&ep).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn listener_drop_removes_socket_file() {
        let ep = temp_ipc("drop");
        let path = match &ep {
            Endpoint::Ipc(IpcPath::Filesystem(p)) => p.clone(),
            _ => unreachable!(),
        };
        {
            let _l = IpcTransport::bind(&ep).await.unwrap();
            assert!(path.exists());
        }
        assert!(!path.exists(), "drop should have removed the socket file");
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn windows_named_pipe_bind_connect_roundtrip() {
        let name = format!("omq-pipe-{}-{}", std::process::id(), rand::random::<u32>());
        let ep = Endpoint::Ipc(IpcPath::NamedPipe(name));

        // Bind server
        let mut listener = IpcTransport::bind(&ep).await.unwrap();

        // Connect client in background
        let ep2 = ep.clone();
        let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

        // Accept connection
        let (mut server_side, peer) = listener.accept().await.unwrap();
        let mut client_side = connect.await.unwrap().unwrap();

        // Verify peer identity contains the pipe name
        assert!(matches!(peer, PeerIdent::Path(_)));

        // Roundtrip data
        client_side.write_all(b"windows").await.unwrap();
        let mut buf = [0u8; 7];
        server_side.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"windows");
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn windows_named_pipe_accepts_concurrent_clients() {
        let name = format!("omq-pipe-{}-{}", std::process::id(), rand::random::<u32>());
        let ep = Endpoint::Ipc(IpcPath::NamedPipe(name));
        let mut listener = IpcTransport::bind(&ep).await.unwrap();

        let ep1 = ep.clone();
        let ep2 = ep.clone();
        let connect1 = tokio::spawn(async move { IpcTransport::connect(&ep1).await });
        let connect2 = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

        let (mut server1, _) = listener.accept().await.unwrap();
        let (mut server2, _) = listener.accept().await.unwrap();
        let mut client1 = connect1.await.unwrap().unwrap();
        let mut client2 = connect2.await.unwrap().unwrap();

        client1.write_all(b"hello").await.unwrap();
        client2.write_all(b"hello").await.unwrap();

        let mut buf1 = [0u8; 5];
        let mut buf2 = [0u8; 5];
        server1.read_exact(&mut buf1).await.unwrap();
        server2.read_exact(&mut buf2).await.unwrap();

        assert_eq!(&buf1, b"hello");
        assert_eq!(&buf2, b"hello");
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn windows_pipe_name_in_error() {
        // Invalid pipe name should be rejected at parse time
        let result: Result<Endpoint> = "ipc://CON".parse();
        assert!(result.is_err());
    }
}
