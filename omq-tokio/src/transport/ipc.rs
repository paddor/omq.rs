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

/// Platform-specific IPC stream type: one end of a named pipe.
#[cfg(target_os = "windows")]
#[derive(Debug)]
pub enum IpcStream {
    /// Pipe accepted by a listener.
    Server(NamedPipeServer),
    /// Pipe opened by a dialer.
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
super::delegate_async_io!(IpcStream { Server, Client });

/// Interprocess transport using Unix sockets or Windows named pipes.
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
    socket_file: Option<SocketFile>,
    ident: PeerIdent,
}

/// The socket file a filesystem bind created. A later bind may replace the
/// path; drop then leaves the newer file in place.
#[cfg(unix)]
#[derive(Debug)]
struct SocketFile {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
impl SocketFile {
    fn new(path: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::symlink_metadata(path)?;
        Ok(Self {
            // Absolute, so that a later change of the working directory
            // does not make drop look for the file elsewhere.
            path: std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf()),
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    fn remove_if_unchanged(&self) {
        use std::os::unix::fs::MetadataExt;
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| metadata.dev() == self.dev && metadata.ino() == self.ino)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
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
        if let Some(file) = &self.socket_file {
            file.remove_if_unchanged();
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
    // libzmq unlinks whatever is at the path. That deletes regular files
    // and leaves a live listener running but unreachable.
    remove_stale_socket(path)?;

    listen_filesystem_unix(endpoint, path)
}

#[cfg(unix)]
fn listen_filesystem_unix(endpoint: &Endpoint, path: &Path) -> Result<IpcListener> {
    let listener = TokioUnixListener::bind(path)?;
    Ok(IpcListener {
        inner: listener,
        endpoint: endpoint.clone(),
        socket_file: Some(SocketFile::new(path)?),
        ident: PeerIdent::Path(path.display().to_string()),
    })
}

/// Bind an `shm://` rendezvous socket. As for IPC, a filesystem path is
/// replaced only when it holds a stale socket.
#[cfg(target_os = "linux")]
pub(crate) fn bind_shm(endpoint: &Endpoint) -> Result<IpcListener> {
    match endpoint {
        Endpoint::Ipc(IpcPath::Filesystem(path)) => {
            remove_stale_socket(path)?;
            listen_filesystem_unix(endpoint, path)
        }
        Endpoint::Ipc(IpcPath::Abstract(name)) => bind_abstract_unix(endpoint, name),
        other => Err(Error::InvalidEndpoint(format!(
            "shm transport got non-ipc endpoint: {other}"
        ))),
    }
}

/// Remove `path` if it is a socket nobody accepts on; fail if anything else
/// occupies it. A nonblocking connect never waits on a full backlog.
#[cfg(unix)]
fn remove_stale_socket(path: &Path) -> Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let refused = || -> std::io::Result<bool> {
        let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        socket.set_nonblocking(true)?;
        Ok(socket
            .connect(&socket2::SockAddr::unix(path)?)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused))
    };

    if metadata.file_type().is_socket() && refused()? {
        std::fs::remove_file(path)?;
        return Ok(());
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        format!("socket path {} is in use", path.display()),
    )
    .into())
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
        socket_file: None,
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
    use std::os::unix::ffi::OsStrExt;

    // socket2 reads a leading NUL as an abstract name.
    let mut raw = Vec::with_capacity(1 + name.len());
    raw.push(0);
    raw.extend_from_slice(name.as_bytes());
    let addr = socket2::SockAddr::unix(std::ffi::OsStr::from_bytes(&raw))
        .map_err(|e| Error::InvalidEndpoint(format!("abstract ipc name {name:?}: {e}")))?;
    // Nonblocking before the connect: a blocking one waits without limit
    // while the listener's backlog is full, on a runtime thread.
    let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    socket.connect(&addr)?;
    Ok(UnixStream::from_std(socket.into())?)
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

    // The first instance must be new: if another server already owns the
    // name, connecting clients would reach it instead of us.
    let server = ServerOptions::new()
        .first_pipe_instance(true)
        .create(&pipe_path)?;

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
mod tests;
