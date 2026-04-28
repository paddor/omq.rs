//! Unix-domain-socket transport: `ipc://path` and `ipc://@name`.
//!
//! Filesystem paths work on every Unix; the listener removes any stale
//! socket file at its path on bind and on drop so repeated binds in
//! tests don't fail with `EADDRINUSE`. Matches libzmq's
//! `ZMQ_IPC_FILTER_PID`-free default.
//!
//! Linux abstract namespace (`ipc://@name`, leading-null sockaddr_un)
//! is also supported. Abstract sockets carry no filesystem entry and
//! are torn down by the kernel when the last fd referencing them
//! closes -- nothing for the listener to clean up. On non-Linux
//! platforms `ipc://@name` is rejected with `UnsupportedScheme`.

use std::path::{Path, PathBuf};

use tokio::net::{UnixListener as TokioUnixListener, UnixStream};

use omq_proto::endpoint::{Endpoint, IpcPath};
use omq_proto::error::{Error, Result};

use super::{Listener, PeerIdent, Transport};

#[derive(Debug)]
pub struct IpcTransport;

impl Transport for IpcTransport {
    type Stream = UnixStream;
    type Listener = IpcListener;

    fn scheme() -> &'static str {
        "ipc"
    }

    async fn bind(endpoint: &Endpoint) -> Result<Self::Listener> {
        match endpoint {
            Endpoint::Ipc(IpcPath::Filesystem(p)) => bind_filesystem(endpoint, p).await,
            Endpoint::Ipc(IpcPath::Abstract(name)) => bind_abstract(endpoint, name),
            other => Err(Error::InvalidEndpoint(format!(
                "ipc transport got non-ipc endpoint: {other}"
            ))),
        }
    }

    async fn connect(endpoint: &Endpoint) -> Result<Self::Stream> {
        match endpoint {
            Endpoint::Ipc(IpcPath::Filesystem(p)) => Ok(UnixStream::connect(p).await?),
            Endpoint::Ipc(IpcPath::Abstract(name)) => connect_abstract(name),
            other => Err(Error::InvalidEndpoint(format!(
                "ipc transport got non-ipc endpoint: {other}"
            ))),
        }
    }
}

async fn bind_filesystem(endpoint: &Endpoint, path: &Path) -> Result<IpcListener> {
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
fn bind_abstract(endpoint: &Endpoint, name: &str) -> Result<IpcListener> {
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
fn bind_abstract(_endpoint: &Endpoint, _name: &str) -> Result<IpcListener> {
    Err(Error::UnsupportedScheme(
        "ipc abstract namespace is Linux-only".into(),
    ))
}

#[cfg(target_os = "linux")]
fn connect_abstract(name: &str) -> Result<UnixStream> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr as StdSockAddr, UnixStream as StdStream};

    let addr = StdSockAddr::from_abstract_name(name.as_bytes())
        .map_err(|e| Error::InvalidEndpoint(format!("abstract ipc name {name:?}: {e}")))?;
    let std_stream = StdStream::connect_addr(&addr)?;
    std_stream.set_nonblocking(true)?;
    Ok(UnixStream::from_std(std_stream)?)
}

#[cfg(not(target_os = "linux"))]
fn connect_abstract(_name: &str) -> Result<UnixStream> {
    Err(Error::UnsupportedScheme(
        "ipc abstract namespace is Linux-only".into(),
    ))
}

/// Bound IPC listener. For filesystem-path binds, removes the socket
/// file on drop; abstract-namespace binds carry no filesystem entry
/// and need no cleanup.
#[derive(Debug)]
pub struct IpcListener {
    inner: TokioUnixListener,
    endpoint: Endpoint,
    /// `Some(path)` for filesystem binds; `None` for abstract.
    cleanup_path: Option<PathBuf>,
    /// Stable PeerIdent surfaced on every accept (the bound address).
    ident: PeerIdent,
}

impl Listener for IpcListener {
    type Stream = UnixStream;

    fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    async fn accept(&mut self) -> Result<(Self::Stream, PeerIdent)> {
        let (stream, _addr) = self.inner.accept().await?;
        // Accepted peer addresses on Unix are usually anonymous; surface
        // the bound address for monitor events instead.
        Ok((stream, self.ident.clone()))
    }
}

impl Drop for IpcListener {
    fn drop(&mut self) {
        if let Some(path) = &self.cleanup_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn temp_ipc(name: &str) -> Endpoint {
        let mut dir = std::env::temp_dir();
        dir.push(format!("omq-ipc-{name}-{}.sock", std::process::id()));
        Endpoint::Ipc(IpcPath::Filesystem(dir))
    }

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
        let name = format!("omq-ipc-abs-{}-{}", std::process::id(), rand::random::<u32>());
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
        let ep = Endpoint::Ipc(IpcPath::Abstract("foo".into()));
        assert!(matches!(
            IpcTransport::bind(&ep).await,
            Err(Error::UnsupportedScheme(_))
        ));
    }

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
}
