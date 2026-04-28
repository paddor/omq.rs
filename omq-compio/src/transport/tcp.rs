//! TCP bind/connect glue. Driver lives in `transport::driver`.

use std::net::SocketAddr;

use compio::net::{TcpListener, TcpStream};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

pub use crate::transport::driver::DriverCommand as TcpDriverCommand;

fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio".into(),
        )),
    }
}

fn resolve_connect(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio".into(),
        )),
    }
}

pub async fn bind(endpoint: &Endpoint) -> Result<(TcpListener, SocketAddr)> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint(format!(
            "tcp transport got non-tcp endpoint: {endpoint}"
        )));
    };
    let addr = resolve_bind(host, *port)?;
    let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    Ok((listener, local))
}

pub async fn connect(endpoint: &Endpoint) -> Result<TcpStream> {
    let Endpoint::Tcp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint(format!(
            "tcp transport got non-tcp endpoint: {endpoint}"
        )));
    };
    let addr = resolve_connect(host, *port)?;
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    stream.set_nodelay(true).map_err(Error::Io)?;
    Ok(stream)
}
