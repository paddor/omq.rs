//! Linux listener sockets: one UDP socket per data IO thread on one port,
//! so receiving and demultiplexing datagrams spreads over the IO threads.
//!
//! The sockets form one `SO_REUSEPORT` group. A classic BPF program picks
//! the group member from the first byte of the packet's destination
//! connection ID, and each member's Quinn endpoint issues connection IDs
//! whose first byte selects that member. A client's first Initials carry a
//! random connection ID and spread across members. The kernel's default
//! 4-tuple hash would not spread them: OMQ connectors share one UDP socket
//! per IO thread, so all connections from one peer thread share a 4-tuple.

use std::io;
use std::net::{SocketAddr, UdpSocket};

use socket2::{Domain, Protocol, Socket, Type};

use super::config;

/// Length of the connection IDs listener endpoints issue.
const CID_LEN: usize = 8;

/// Bind `count` sockets to `addr` as one steered reuseport group, in
/// member order. `Ok(None)` means the group could not be set up and the
/// caller binds one plain socket instead.
pub(super) fn bind(addr: SocketAddr, count: usize) -> io::Result<Option<Vec<UdpSocket>>> {
    let Ok(count) = u8::try_from(count) else {
        return Ok(None);
    };
    // Any socket of the same user that sets SO_REUSEPORT may join a group.
    // A plain bind first fails like any bind when the address is taken, and
    // resolves port 0.
    let addr = UdpSocket::bind(addr)?.local_addr()?;
    Ok(group(addr, count).ok())
}

fn group(addr: SocketAddr, count: u8) -> io::Result<Vec<UdpSocket>> {
    let mut sockets = Vec::with_capacity(count.into());
    for _ in 0..count {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_port(true)?;
        socket.bind(&addr.into())?;
        if sockets.is_empty() {
            // The program applies to the whole group.
            steer(&socket, count)?;
        }
        sockets.push(socket.into());
    }
    Ok(sockets)
}

/// Endpoint config for group member `index` of `count`.
pub(super) fn endpoint_config(index: usize, count: usize) -> quinn::EndpointConfig {
    let index = u8::try_from(index).expect("reuseport member index fits u8");
    let count = u8::try_from(count).expect("reuseport group size fits u8");
    let mut config = config::endpoint();
    config.cid_generator(move || Box::new(SteeredCids { index, count }));
    config
}

/// Random connection IDs whose first byte modulo `count` is `index`.
#[derive(Debug)]
struct SteeredCids {
    index: u8,
    count: u8,
}

impl quinn::ConnectionIdGenerator for SteeredCids {
    fn generate_cid(&mut self) -> quinn::ConnectionId {
        let mut bytes: [u8; CID_LEN] = rand::random();
        bytes[0] = steered_byte(bytes[0], self.index, self.count);
        quinn::ConnectionId::new(&bytes)
    }

    fn cid_len(&self) -> usize {
        CID_LEN
    }

    fn cid_lifetime(&self) -> Option<std::time::Duration> {
        None
    }
}

/// Map a random byte to one whose value modulo `count` is `index`.
fn steered_byte(random: u8, index: u8, count: u8) -> u8 {
    let slots = 256 / u16::from(count);
    let byte = u16::from(random) % slots * u16::from(count) + u16::from(index);
    u8::try_from(byte).expect("steered byte stays below 256")
}

#[expect(clippy::cast_possible_truncation, reason = "BPF opcodes fit u16")]
const fn op(bits: u32) -> u16 {
    bits as u16
}

fn insn(code: u32, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code: op(code),
        jt,
        jf,
        k,
    }
}

/// Attach the connection ID steering program to the socket's group. The
/// program sees the UDP payload. A long header (high bit of byte 0 set)
/// carries the destination connection ID from byte 6, a short header from
/// byte 1. It returns the member index; loads past the packet end return
/// 0, and indices outside the group fall back to the kernel's hash.
fn steer(socket: &Socket, count: u8) -> io::Result<()> {
    use libc::{
        BPF_A, BPF_ABS, BPF_ALU, BPF_B, BPF_JA, BPF_JMP, BPF_JSET, BPF_K, BPF_LD, BPF_MOD, BPF_RET,
    };
    let mut program = [
        insn(BPF_LD | BPF_B | BPF_ABS, 0, 0, 0),
        insn(BPF_JMP | BPF_JSET | BPF_K, 0, 2, 0x80),
        insn(BPF_LD | BPF_B | BPF_ABS, 0, 0, 6),
        insn(BPF_JMP | BPF_JA, 0, 0, 1),
        insn(BPF_LD | BPF_B | BPF_ABS, 0, 0, 1),
        insn(BPF_ALU | BPF_MOD | BPF_K, 0, 0, count.into()),
        insn(BPF_RET | BPF_A, 0, 0, 0),
    ];
    let fprog = libc::sock_fprog {
        len: u16::try_from(program.len()).expect("short BPF program"),
        filter: program.as_mut_ptr(),
    };
    nix::sys::socket::setsockopt(
        socket,
        nix::sys::socket::sockopt::AttachReusePortCbpf,
        &fprog,
    )
    .map_err(io::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steered_bytes_select_their_member() {
        for count in 2..=u8::MAX {
            for index in 0..count {
                for random in [0, 1, 127, 128, 254, 255] {
                    assert_eq!(steered_byte(random, index, count) % count, index);
                }
            }
        }
    }

    #[test]
    fn listener_endpoints_issue_steered_cids() {
        use quinn::ConnectionIdGenerator;
        let mut cids = SteeredCids { index: 2, count: 3 };
        for _ in 0..64 {
            let cid = cids.generate_cid();
            assert_eq!(cid.len(), CID_LEN);
            assert_eq!(cid[0] % 3, 2);
        }
    }

    #[test]
    fn group_binds_one_port_and_resolves_port_zero() {
        let sockets = bind("127.0.0.1:0".parse().unwrap(), 3)
            .unwrap()
            .expect("reuseport group");
        let addr = sockets[0].local_addr().unwrap();
        assert_ne!(addr.port(), 0);
        for socket in &sockets {
            assert_eq!(socket.local_addr().unwrap(), addr);
        }
    }

    #[test]
    fn program_steers_by_destination_cid_not_by_sender() {
        let sockets = bind("127.0.0.1:0".parse().unwrap(), 2)
            .unwrap()
            .expect("reuseport group");
        let addr = sockets[0].local_addr().unwrap();
        for socket in &sockets {
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(1)))
                .unwrap();
        }
        // One sender, so the kernel's 4-tuple hash would pick one member.
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        let short = |cid0| vec![0x40, cid0, 7, 7, 7, 7, 7, 7, 7, 7];
        let long = |cid0| vec![0xc0, 0, 0, 0, 1, 8, cid0, 7, 7, 7, 7, 7, 7, 7];
        for (packet, member) in [(short(4), 0), (short(5), 1), (long(9), 1), (long(10), 0)] {
            client.send_to(&packet, addr).unwrap();
            let mut buf = [0; 64];
            let n = sockets[member].recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], &packet[..]);
        }
    }

    #[test]
    fn group_refuses_a_taken_address() {
        let sockets = bind("127.0.0.1:0".parse().unwrap(), 2)
            .unwrap()
            .expect("reuseport group");
        let addr = sockets[0].local_addr().unwrap();
        let err = bind(addr, 2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    }
}
