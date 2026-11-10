//! TCP transport.
//!
//! Binds `tcp://host:port`, supports IPv4 and IPv6 (bracketed), wildcard (`*`
//! for bind), DNS names, and port 0 (OS-assigned). `TCP_NODELAY` is always on
//! to match libzmq's default; Nagle is not what you want for ZMQ-style
//! message traffic.

use std::io;
use std::net::SocketAddr;

use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

use super::{Listener, PeerIdent, Transport};

/// TCP transport with DNS resolution and nonblocking stream I/O.
#[derive(Debug)]
pub struct TcpTransport;

impl Transport for TcpTransport {
    type Stream = TcpStream;
    type Listener = TcpListener;

    fn scheme() -> &'static str {
        "tcp"
    }

    async fn bind(endpoint: &Endpoint) -> Result<Self::Listener> {
        let Endpoint::Tcp { host, port } = endpoint else {
            return Err(Error::InvalidEndpoint(format!(
                "TCP transport got non-TCP endpoint: {endpoint}"
            )));
        };

        let addr = super::dns::bind_addr(host, *port).await?;
        let listener = reuse_addr_bind(addr)?;
        let local = listener.local_addr()?;
        let bound = Endpoint::Tcp {
            host: Host::Ip(local.ip()),
            port: local.port(),
        };

        Ok(TcpListener {
            inner: listener,
            endpoint: bound,
        })
    }

    async fn connect(endpoint: &Endpoint) -> Result<Self::Stream> {
        let Endpoint::Tcp { host, port } = endpoint else {
            return Err(Error::InvalidEndpoint(format!(
                "TCP transport got non-TCP endpoint: {endpoint}"
            )));
        };

        let stream = connect_any(super::dns::connect_addrs(host, *port).await?).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}

/// Bound TCP listener.
#[derive(Debug)]
pub struct TcpListener {
    inner: TokioTcpListener,
    endpoint: Endpoint,
}

impl Listener for TcpListener {
    type Stream = TcpStream;

    fn local_endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    async fn accept(&mut self) -> Result<(Self::Stream, PeerIdent)> {
        let (stream, peer) = self.inner.accept().await?;
        stream.set_nodelay(true)?;
        Ok((stream, PeerIdent::Socket(peer)))
    }
}

pub(crate) fn reuse_addr_bind(addr: SocketAddr) -> Result<TokioTcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };

    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    // On Windows, SO_REUSEADDR lets a second socket bind a port that is
    // still listening. Elsewhere it only allows rebinding past TIME_WAIT.
    #[cfg(not(windows))]
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;

    Ok(TokioTcpListener::from_std(std::net::TcpListener::from(
        sock,
    ))?)
}

/// Dial `addrs` in order and return the first stream that connects.
pub(crate) async fn connect_any(addrs: Vec<SocketAddr>) -> Result<TcpStream> {
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }

    Err(Error::Io(last_err.unwrap_or_else(|| {
        io::Error::other("no addresses to connect")
    })))
}

#[cfg(test)]
mod tests;
