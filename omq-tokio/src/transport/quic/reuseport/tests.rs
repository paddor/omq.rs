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
