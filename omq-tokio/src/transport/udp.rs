//! UDP transport for RADIO / DISH.
//!
//! Connectionless, datagram-based, no ZMTP handshake. DISH binds and
//! receives; RADIO connects and sends. Wire format per datagram is
//! `flags(0x01) | group_size | group | body`.
//!
//! A DISH bind on a multicast address joins that group on the default
//! interface, as libzmq does. RADIO sends to a group like to any address;
//! the OS default hop limit of one keeps the datagrams on the local link.

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
/// `group` exceeds 255 bytes (it doesn't fit the 1-byte length field)
/// or the datagram exceeds [`MAX_DATAGRAM_SIZE`].
pub fn encode_datagram(group: &[u8], body: &[u8]) -> Result<Bytes> {
    if group.len() > u8::MAX as usize {
        return Err(Error::Protocol(format!(
            "RADIO group name too long ({} bytes; max 255)",
            group.len()
        )));
    }

    // No socket can send it, and an egress limit could never grant it.
    let total = 2 + group.len() + body.len();
    if total > MAX_DATAGRAM_SIZE {
        return Err(Error::Protocol(format!(
            "RADIO datagram too large ({total} bytes; max {MAX_DATAGRAM_SIZE})"
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
    let (group, body) = split_datagram(data)?;
    Some((Bytes::copy_from_slice(group), Bytes::copy_from_slice(body)))
}

/// Borrow `(group, body)` from a wire datagram without copying, so a
/// receiver can drop unjoined groups first. Malformed datagrams give `None`.
pub fn split_datagram(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&flags, rest) = data.split_first()?;
    let (&glen, rest) = rest.split_first()?;

    if flags != FLAG_DATA {
        return None;
    }

    rest.split_at_checked(usize::from(glen))
}

/// Bind a UDP socket for a DISH endpoint. Wildcard (`udp://*:port`) is
/// resolved to `0.0.0.0`; named hosts are not resolved here -- pass an
/// IP literal or `*`. A multicast address binds the wildcard address and
/// joins that group.
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
        _ => unreachable!(),
    };

    if ip.is_multicast() {
        return bind_multicast(ip, port);
    }
    let sock = UdpSocket::bind(SocketAddr::new(ip, port)).await?;
    Ok(sock)
}

/// Bind the wildcard address and join `group` on the default interface.
/// Address reuse lets several DISH sockets of one host share the port; each
/// receives its own copy.
fn bind_multicast(group: IpAddr, port: u16) -> Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let (domain, any) = match group {
        IpAddr::V4(_) => (Domain::IPV4, IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
        IpAddr::V6(_) => (Domain::IPV6, IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)),
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    // BSD kernels share a wildcard port only with this option.
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::new(any, port).into())?;
    match group {
        IpAddr::V4(group) => {
            socket.join_multicast_v4(&group, &std::net::Ipv4Addr::UNSPECIFIED)?;
        }
        IpAddr::V6(group) => socket.join_multicast_v6(&group, 0)?,
    }
    Ok(UdpSocket::from_std(socket.into())?)
}

/// Open an unbound UDP socket and "connect" it to the RADIO target so
/// we can use `send` rather than `send_to`. Hostnames resolve through the
/// shared DNS workers; failure surfaces as `io::Error`.
pub async fn connect(endpoint: &Endpoint) -> Result<UdpSocket> {
    let (host, port) = parts(endpoint)?;
    let target = super::dns::connect_addrs(host, port).await?[0];

    // Bind to ephemeral local; let the OS pick the address family.
    let local = match target {
        SocketAddr::V4(_) => "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse::<SocketAddr>().unwrap(),
    };

    let sock = UdpSocket::bind(local).await?;
    grow_send_buffer(&sock);
    sock.connect(target).await?;

    Ok(sock)
}

/// BSD kernels reject datagrams larger than the send buffer, which starts at
/// 9216 bytes on macOS. Grow it to fit `MAX_DATAGRAM_SIZE`; never shrink it.
fn grow_send_buffer(sock: &UdpSocket) {
    let sock = socket2::SockRef::from(sock);
    if sock
        .send_buffer_size()
        .is_ok_and(|size| size < MAX_DATAGRAM_SIZE)
    {
        let _ = sock.set_send_buffer_size(MAX_DATAGRAM_SIZE);
    }
}

fn parts(endpoint: &Endpoint) -> Result<(&Host, u16)> {
    match endpoint {
        Endpoint::Udp { host, port, .. } => Ok((host, *port)),
        other => Err(Error::InvalidEndpoint(format!(
            "udp transport got non-udp endpoint: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests;
