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
    for bytes in [[0x00, 0x00], [0x03, 0x00], [0x81, 0x00]] {
        assert!(decode_datagram(&bytes).is_none());
    }
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

#[tokio::test]
async fn connect_sends_datagrams_above_the_macos_default_buffer() {
    use std::net::Ipv4Addr;

    let host = Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let listener = bind(&Endpoint::Udp {
        group: None,
        host: host.clone(),
        port: 0,
    })
    .await
    .unwrap();
    let sender = connect(&Endpoint::Udp {
        group: None,
        host,
        port: listener.local_addr().unwrap().port(),
    })
    .await
    .unwrap();

    let body = vec![7u8; 16 * 1024];
    sender
        .send(&encode_datagram(b"g", &body).unwrap())
        .await
        .unwrap();

    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
    let (n, _from) = listener.recv_from(&mut buf).await.unwrap();
    assert_eq!(decode_datagram(&buf[..n]).unwrap().1, body);
}
