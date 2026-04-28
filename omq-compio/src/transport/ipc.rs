//! Unix-domain-socket transport: `ipc://path` and `ipc://@name`.
//!
//! Filesystem paths work on every Unix; bind removes any stale
//! socket file at the path so repeated binds in tests don't fail
//! with `EADDRINUSE`. The connection driver is shared with TCP via
//! `transport::driver::run_connection`.
//!
//! Linux abstract namespace (`ipc://@name`, leading-null sockaddr_un)
//! is supported via std::os::unix::net + `from_std`. On non-Linux
//! platforms `ipc://@name` is rejected.

use std::path::PathBuf;

use compio::net::{UnixListener, UnixStream};

use omq_proto::endpoint::{Endpoint, IpcPath};
use omq_proto::error::{Error, Result};

pub use crate::transport::driver::DriverCommand as IpcDriverCommand;

/// A bound IPC listener plus the socket path we may need to clean up.
#[derive(Debug)]
pub struct IpcListener {
    pub inner: UnixListener,
    pub cleanup_path: Option<PathBuf>,
}

impl Drop for IpcListener {
    fn drop(&mut self) {
        if let Some(path) = &self.cleanup_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub async fn bind(endpoint: &Endpoint) -> Result<IpcListener> {
    match endpoint {
        Endpoint::Ipc(IpcPath::Filesystem(p)) => {
            // Best-effort cleanup of any stale socket at this path.
            let _ = std::fs::remove_file(p);
            let listener = UnixListener::bind(p).await.map_err(Error::Io)?;
            Ok(IpcListener {
                inner: listener,
                cleanup_path: Some(p.clone()),
            })
        }
        Endpoint::Ipc(IpcPath::Abstract(name)) => bind_abstract(name),
        other => Err(Error::InvalidEndpoint(format!(
            "ipc transport got non-ipc endpoint: {other}"
        ))),
    }
}

pub async fn connect(endpoint: &Endpoint) -> Result<UnixStream> {
    match endpoint {
        Endpoint::Ipc(IpcPath::Filesystem(p)) => UnixStream::connect(p).await.map_err(Error::Io),
        Endpoint::Ipc(IpcPath::Abstract(name)) => connect_abstract(name),
        other => Err(Error::InvalidEndpoint(format!(
            "ipc transport got non-ipc endpoint: {other}"
        ))),
    }
}

#[cfg(target_os = "linux")]
fn bind_abstract(name: &str) -> Result<IpcListener> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr as StdSockAddr, UnixListener as StdListener};

    let addr = StdSockAddr::from_abstract_name(name.as_bytes())
        .map_err(|e| Error::InvalidEndpoint(format!("abstract ipc name {name:?}: {e}")))?;
    let std_listener = StdListener::bind_addr(&addr).map_err(Error::Io)?;
    std_listener.set_nonblocking(true).map_err(Error::Io)?;
    let inner = UnixListener::from_std(std_listener).map_err(Error::Io)?;
    Ok(IpcListener {
        inner,
        cleanup_path: None,
    })
}

#[cfg(not(target_os = "linux"))]
fn bind_abstract(_name: &str) -> Result<IpcListener> {
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
    let std_stream = StdStream::connect_addr(&addr).map_err(Error::Io)?;
    std_stream.set_nonblocking(true).map_err(Error::Io)?;
    UnixStream::from_std(std_stream).map_err(Error::Io)
}

#[cfg(not(target_os = "linux"))]
fn connect_abstract(_name: &str) -> Result<UnixStream> {
    Err(Error::UnsupportedScheme(
        "ipc abstract namespace is Linux-only".into(),
    ))
}
