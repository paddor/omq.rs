//! UDP transport for RADIO/DISH (compio backend).
//!
//! Connectionless, datagram-based, no ZMTP handshake. DISH binds and
//! receives; RADIO connects and sends. Wire format per datagram is
//! `flags(0x01) | group_size | group | body`. Matches the omq-tokio
//! transport bit-for-bit.

use std::io;
use std::net::{IpAddr, SocketAddr};

use bytes::Bytes;
use compio::net::UdpSocket;

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

/// Maximum UDP payload that's safe to send unfragmented on a 1500-byte
/// MTU minus IPv6 + UDP header allowance.
pub const MAX_DATAGRAM_SIZE: usize = 65_507;

/// `flags` byte: bit 0 set marks a data datagram. Other bits reserved.
pub const FLAG_DATA: u8 = 0x01;

/// Encode `[group, body]` as a single UDP datagram.
pub fn encode_datagram(group: &[u8], body: &[u8]) -> Result<Bytes> {
    if group.len() > u8::MAX as usize {
        return Err(Error::Protocol(format!(
            "RADIO group name too long ({} bytes; max 255)",
            group.len()
        )));
    }
    let mut out = Vec::with_capacity(2 + group.len() + body.len());
    out.push(FLAG_DATA);
    out.push(group.len() as u8);
    out.extend_from_slice(group);
    out.extend_from_slice(body);
    Ok(Bytes::from(out))
}

/// Decode a wire datagram into `(group, body)`. Returns `None` for
/// malformed datagrams so DISH drops them silently per RFC 48.
pub fn decode_datagram(data: &[u8]) -> Option<(Bytes, Bytes)> {
    if data.len() < 2 {
        return None;
    }
    if data[0] & FLAG_DATA == 0 {
        return None;
    }
    let glen = data[1] as usize;
    if data.len() < 2 + glen {
        return None;
    }
    let group = Bytes::copy_from_slice(&data[2..2 + glen]);
    let body = Bytes::copy_from_slice(&data[2 + glen..]);
    Some((group, body))
}

/// Bind a UDP socket for a DISH endpoint. `udp://*:port` resolves to
/// `0.0.0.0`. Hostnames are rejected - pass an IP literal or `*`.
pub async fn bind(endpoint: &Endpoint) -> Result<UdpSocket> {
    let (host, port) = parts(endpoint)?;
    let ip = match host {
        Host::Ip(ip) => *ip,
        Host::Wildcard => IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        Host::Name(_) => {
            return Err(Error::InvalidEndpoint(
                "udp bind requires an IP literal or *, not a hostname".into(),
            ));
        }
    };
    let sock = UdpSocket::bind(SocketAddr::new(ip, port))
        .await
        .map_err(Error::Io)?;
    Ok(sock)
}

/// Open and "connect" a UDP socket to the RADIO target so we can use
/// `send` rather than `send_to`. Hostname resolution is done by the
/// stdlib lookup; failure surfaces as `io::Error`.
pub async fn connect(endpoint: &Endpoint) -> Result<UdpSocket> {
    let (host, port) = parts(endpoint)?;
    let target = host_to_lookup(host, port)?;
    let local = match target {
        SocketAddr::V4(_) => "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse::<SocketAddr>().unwrap(),
    };
    let sock = UdpSocket::bind(local).await.map_err(Error::Io)?;
    sock.connect(target).await.map_err(Error::Io)?;
    Ok(sock)
}

fn parts(endpoint: &Endpoint) -> Result<(&Host, u16)> {
    match endpoint {
        Endpoint::Udp { host, port, .. } => Ok((host, *port)),
        other => Err(Error::InvalidEndpoint(format!(
            "udp transport got non-udp endpoint: {other}"
        ))),
    }
}

fn host_to_lookup(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "udp connect cannot target a wildcard host".into(),
        )),
        Host::Name(name) => {
            let mut addrs = std::net::ToSocketAddrs::to_socket_addrs(&(name.as_str(), port))
                .map_err(|e| Error::Io(io::Error::new(e.kind(), format!("{name}: {e}"))))?;
            addrs
                .next()
                .ok_or_else(|| Error::InvalidEndpoint(format!("udp: no address for {name}")))
        }
    }
}
