//! UDP transport for RADIO / DISH.
//!
//! Connectionless, datagram-based, no ZMTP handshake. DISH binds and
//! receives; RADIO connects and sends. Wire format per datagram is
//! `flags(0x01) | group_size | group | body`.
//!
//! Multicast is intentionally out of scope for this slice; the listener
//! takes whatever address is bound and lets the OS handle routing.

use std::io;
use std::net::{IpAddr, SocketAddr};

use bytes::Bytes;
use tokio::net::UdpSocket;

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

/// Maximum UDP payload that's safe to send unfragmented on a 1500-byte
/// MTU link minus a generous IPv6 + UDP header allowance. 65507 =
/// 65535 - 20 IPv4 - 8 UDP. The kernel enforces lower limits per route.
pub const MAX_DATAGRAM_SIZE: usize = 65_507;

/// `flags` byte: bit 0 set marks a data datagram. Reserved bits stay
/// zero (UDP RADIO/DISH has no other framing today).
pub const FLAG_DATA: u8 = 0x01;

/// Encode `[group, body]` as a single UDP datagram. Errors when
/// `group` exceeds 255 bytes (it doesn't fit the 1-byte length field).
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

/// Decode a wire datagram into `(group, body)`. Returns `Ok(None)` for
/// malformed datagrams (wrong flags byte, truncated header, group
/// overruns the buffer) so DISH can drop them silently per RFC 48.
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

/// Bind a UDP socket for a DISH endpoint. Wildcard (`udp://*:port`) is
/// resolved to `0.0.0.0`; named hosts are not resolved here -- pass an
/// IP literal or `*`.
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
    let sock = UdpSocket::bind(SocketAddr::new(ip, port)).await?;
    Ok(sock)
}

/// Open an unbound UDP socket and "connect" it to the RADIO target so
/// we can use `send` rather than `send_to`. Hostname resolution is
/// delegated to the std lookup; failure surfaces as `io::Error`.
pub async fn connect(endpoint: &Endpoint) -> Result<UdpSocket> {
    let (host, port) = parts(endpoint)?;
    let target = host_to_lookup(host, port)?;
    // Bind to ephemeral local; let the OS pick the address family.
    let local = match target {
        SocketAddr::V4(_) => "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse::<SocketAddr>().unwrap(),
    };
    let sock = UdpSocket::bind(local).await?;
    sock.connect(target).await?;
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
            // Synchronous lookup; UDP connect paths run inside the dial
            // task so a brief block here is acceptable.
            let mut addrs = std::net::ToSocketAddrs::to_socket_addrs(&(name.as_str(), port))
                .map_err(|e| Error::Io(io::Error::new(e.kind(), format!("{name}: {e}"))))?;
            addrs
                .next()
                .ok_or_else(|| Error::InvalidEndpoint(format!("udp: no address for {name}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datagram_roundtrip() {
        let bytes = encode_datagram(b"weather", b"hot today").unwrap();
        let (group, body) = decode_datagram(&bytes).unwrap();
        assert_eq!(&group[..], b"weather");
        assert_eq!(&body[..], b"hot today");
    }

    #[test]
    fn datagram_empty_group() {
        let bytes = encode_datagram(b"", b"body").unwrap();
        let (group, body) = decode_datagram(&bytes).unwrap();
        assert_eq!(group.len(), 0);
        assert_eq!(&body[..], b"body");
    }

    #[test]
    fn datagram_rejects_oversized_group() {
        let big = vec![b'g'; 256];
        assert!(matches!(
            encode_datagram(&big, b"x"),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(decode_datagram(&[]).is_none());
        assert!(decode_datagram(&[0x01]).is_none());
        // glen=10 but only 5 bytes follow.
        let bytes = [0x01, 10, b'h', b'i', b'!'];
        assert!(decode_datagram(&bytes).is_none());
    }

    #[test]
    fn decode_rejects_wrong_flag() {
        // flags=0 isn't a data datagram.
        let bytes = [0x00, 0x00];
        assert!(decode_datagram(&bytes).is_none());
    }

    #[tokio::test]
    async fn bind_connect_roundtrip() {
        use std::net::Ipv4Addr;

        let bind_ep = Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        };
        let listener = bind(&bind_ep).await.unwrap();
        let local = listener.local_addr().unwrap();

        let connect_ep = Endpoint::Udp {
            group: None,
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: local.port(),
        };
        let sender = connect(&connect_ep).await.unwrap();

        let dgram = encode_datagram(b"news", b"hello").unwrap();
        sender.send(&dgram).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let (n, _from) = listener.recv_from(&mut buf).await.unwrap();
        let (group, body) = decode_datagram(&buf[..n]).unwrap();
        assert_eq!(&group[..], b"news");
        assert_eq!(&body[..], b"hello");
    }
}
