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
        // A peer that already reset can fail this. That is no accept
        // error: the listener must not back off for it.
        let _ = stream.set_nodelay(true);
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
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback_port_zero() -> Endpoint {
        Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        }
    }

    #[tokio::test]
    async fn bind_connect_accept() {
        let mut listener = TcpTransport::bind(&loopback_port_zero()).await.unwrap();
        let local = listener.local_endpoint().clone();
        let Endpoint::Tcp { port, .. } = local else {
            panic!()
        };
        assert!(port != 0, "OS should assign a port");

        let connect_target = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port,
        };
        let connect = tokio::spawn(async move { TcpTransport::connect(&connect_target).await });
        let (_srv_stream, peer) = listener.accept().await.unwrap();
        let _cli_stream = connect.await.unwrap().unwrap();
        match peer {
            PeerIdent::Socket(_) => {}
            _ => panic!("expected Socket peer ident"),
        }
    }

    #[tokio::test]
    async fn bind_rejects_non_tcp_endpoint() {
        let ep = Endpoint::Inproc { name: "x".into() };
        assert!(matches!(
            TcpTransport::bind(&ep).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_rejects_wildcard() {
        let ep = Endpoint::Tcp {
            host: Host::Wildcard,
            port: 5555,
        };
        assert!(matches!(
            TcpTransport::connect(&ep).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_tries_later_resolved_address() {
        let bad = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let bad_addr = bad.local_addr().unwrap();
        let listener = TokioTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let good_addr = listener.local_addr().unwrap();
        drop(bad);

        let connect = tokio::spawn(connect_any(vec![bad_addr, good_addr]));
        let (_accepted, _) = listener.accept().await.unwrap();
        let stream = connect.await.unwrap().unwrap();

        assert_eq!(stream.peer_addr().unwrap(), good_addr);
    }
}
