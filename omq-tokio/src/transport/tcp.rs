//! TCP transport.
//!
//! Binds `tcp://host:port`, supports IPv4 and IPv6 (bracketed), wildcard (`*`
//! for bind), DNS names, and port 0 (OS-assigned). `TCP_NODELAY` is always on
//! to match libzmq's default; Nagle is not what you want for ZMQ-style
//! message traffic.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

use super::{Listener, PeerIdent, Transport};

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
        let addr = resolve_bind(host, *port).await?;
        let listener = TokioTcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let bound = Endpoint::Tcp { host: Host::Ip(local.ip()), port: local.port() };
        Ok(TcpListener { inner: listener, endpoint: bound })
    }

    async fn connect(endpoint: &Endpoint) -> Result<Self::Stream> {
        let Endpoint::Tcp { host, port } = endpoint else {
            return Err(Error::InvalidEndpoint(format!(
                "TCP transport got non-TCP endpoint: {endpoint}"
            )));
        };
        let addr = resolve_connect(host, *port).await?;
        let stream = TcpStream::connect(addr).await?;
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

async fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => resolve_first(&format!("{name}:{port}")).await,
    }
}

async fn resolve_connect(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => resolve_first(&format!("{name}:{port}")).await,
    }
}

async fn resolve_first(s: &str) -> Result<SocketAddr> {
    let mut it = tokio::net::lookup_host(s).await?;
    it.next()
        .ok_or_else(|| Error::Io(io::Error::other(format!("no addresses for {s}"))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback_port_zero() -> Endpoint {
        Endpoint::Tcp { host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)), port: 0 }
    }

    #[tokio::test]
    async fn bind_connect_accept() {
        let mut listener = TcpTransport::bind(&loopback_port_zero()).await.unwrap();
        let local = listener.local_endpoint().clone();
        let Endpoint::Tcp { port, .. } = local else { panic!() };
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
        let ep = Endpoint::Tcp { host: Host::Wildcard, port: 5555 };
        assert!(matches!(
            TcpTransport::connect(&ep).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }
}
