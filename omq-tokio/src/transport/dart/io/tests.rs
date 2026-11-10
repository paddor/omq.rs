use super::*;
use omq_proto::dart::{Datagram, decode, encode_sequenced_data};

#[test]
fn send_windows_preserve_exact_lengths_and_fallback_boundaries() {
    assert_eq!(send_window(17 * 64, 17, 64), (17 * 64, 64));
    assert_eq!(send_window(17 * 63 + 3, 17, 64), (17 * 63 + 3, 64));
    assert_eq!(send_window(17 * 64, 17, 1), (17, 1));
    assert_eq!(send_window(17 * 63 + 3, 17, 1), (17, 1));
    assert_eq!(send_window(3, 17, 1), (3, 1));
}

#[test]
fn gro_boundaries_short_final_segment_and_metadata_are_preserved() {
    let mut batch = ReceiveBatch::new(64);
    batch.count = 1;
    let mut expected = Vec::new();
    let mut length = 0;

    for (sequence, body) in [&[1, 2][..], &[3, 4][..], &[][..]].into_iter().enumerate() {
        let size =
            encode_sequenced_data(1, sequence as u64, body, None, &mut batch.storage[length..])
                .unwrap();
        expected.push(batch.storage[length..length + size].to_vec());
        length += size;
    }

    let meta = &mut batch.metadata[0];
    meta.len = length;
    meta.stride = expected[0].len();
    meta.addr = "127.0.0.1:1".parse().unwrap();
    meta.ecn = Some(EcnCodepoint::Ce);
    meta.dst_ip = Some("127.0.0.1".parse().unwrap());

    let datagrams: Vec<_> = batch.datagrams().collect();
    assert_eq!(datagrams.len(), 3);
    assert!(expected[2].len() < expected[0].len());

    for (datagram, packet) in datagrams.into_iter().zip(&expected) {
        assert_eq!(datagram.bytes, packet);
        assert_eq!(datagram.ecn, Some(EcnCodepoint::Ce));
        assert_eq!(datagram.destination, Some("127.0.0.1".parse().unwrap()));
        assert_eq!(datagram.source.port(), 1);
        assert!(matches!(decode(datagram.bytes), Some(Datagram::Data(_))));
    }

    assert_eq!(batch.rejected_buffers(), 0);
    // The worker stops after a budget and resumes the same batch.
    for expected in &expected {
        assert!(batch.has_pending_datagrams());
        assert_eq!(batch.peek_datagram().unwrap().bytes, expected);
        // Peeking after a runtime yield preserves the same segment.
        assert_eq!(batch.peek_datagram().unwrap().bytes, expected);
        assert_eq!(batch.peek_datagram().unwrap().ecn, Some(EcnCodepoint::Ce));
        batch.advance_datagram();
    }

    assert!(!batch.has_pending_datagrams());
    assert!(batch.pop_datagram().is_none());
}

#[test]
fn full_arenas_invalid_strides_and_oversized_segments_are_rejected() {
    let mut batch = ReceiveBatch::new(64);
    batch.count = 1;

    for (length, stride) in [
        (0, 0),
        (1, 0),
        (MAX_PACKED_DATAGRAM + 1, MAX_PACKED_DATAGRAM + 1),
        (batch.slot_size, 1),
        (batch.slot_size + 1, 17),
    ] {
        batch.metadata[0].len = length;
        batch.metadata[0].stride = stride;
        assert_eq!(batch.datagrams().count(), 0);
        assert_eq!(batch.rejected_buffers(), 1);
    }

    batch.metadata[0].len = MAX_PACKED_DATAGRAM * 64;
    batch.metadata[0].stride = MAX_PACKED_DATAGRAM;
    assert_eq!(batch.datagrams().count(), 64);
    assert_eq!(batch.rejected_buffers(), 0);
}

#[tokio::test]
async fn loopback_offload_datagrams_keep_individual_lengths() {
    let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let sender = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let target = receiver.local_addr().unwrap();
    let wire = [0, 1, 2, 0, 3, 4, 0];
    let mut offset = 0;

    while offset < wire.len() {
        sender.writable().await.unwrap();
        match sender.try_send_segments(SegmentBatch::new(target, &wire[offset..], 3)) {
            Ok(count) => offset = (offset + count * 3).min(wire.len()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("send: {error}"),
        }
    }

    let mut batch = ReceiveBatch::new(receiver.gro_segments());
    let mut actual = Vec::new();

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while actual.len() < 3 {
            receiver.readable().await.unwrap();
            match batch.try_receive(&receiver) {
                Ok(_) => {
                    actual.extend(batch.datagrams().map(|datagram| datagram.bytes.to_vec()));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("receive: {error}"),
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(actual, [vec![0, 1, 2], vec![0, 3, 4], vec![0]]);
    assert_eq!(
        batch.try_receive(&receiver).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(batch.received_buffers(), 0);
    assert_eq!(batch.datagrams().count(), 0);
}

#[tokio::test]
async fn ipv6_destination_and_ecn_metadata_are_preserved() {
    let receiver = DartIo::new(std::net::UdpSocket::bind("[::1]:0").unwrap()).unwrap();
    let sender = DartIo::new(std::net::UdpSocket::bind("[::1]:0").unwrap()).unwrap();
    let target = receiver.local_addr().unwrap();
    sender.writable().await.unwrap();

    assert_eq!(
        sender
            .try_send_segments(SegmentBatch {
                ecn: Some(EcnCodepoint::Ect0),
                ..SegmentBatch::new(target, &[0, 7], 2)
            })
            .unwrap(),
        1
    );

    let mut batch = ReceiveBatch::new(receiver.gro_segments());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            receiver.readable().await.unwrap();
            match batch.try_receive(&receiver) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("receive: {error}"),
            }
        }
    })
    .await
    .unwrap();

    let datagram = batch.datagrams().next().unwrap();
    assert_eq!(datagram.bytes, &[0, 7]);
    assert!(datagram.source.is_ipv6());
    assert_eq!(datagram.source.port(), sender.local_addr().unwrap().port());

    if receiver.ecn_receive_supported(datagram.source.ip()) == Some(true) {
        assert_eq!(datagram.ecn, Some(EcnCodepoint::Ect0));
    }

    if let Some(destination) = datagram.destination {
        assert_eq!(destination, target.ip());
    }
}

#[tokio::test]
async fn oversized_wire_datagrams_are_discarded() {
    let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .send_to(&[0; 2048], receiver.local_addr().unwrap())
        .unwrap();
    let mut batch = ReceiveBatch::new(receiver.gro_segments());

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            receiver.readable().await.unwrap();
            match batch.try_receive(&receiver) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("receive: {error}"),
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(batch.received_buffers(), 1);
    assert_eq!(batch.rejected_buffers(), 1);
    assert_eq!(batch.datagrams().count(), 0);
}

#[tokio::test]
async fn explicit_spin_can_receive_before_reactor_readiness_refreshes() {
    let receiver = DartIo::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut batch = ReceiveBatch::new(receiver.gro_segments());
    assert_eq!(
        batch.try_receive(&receiver).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    sender
        .send_to(&[0, 7], receiver.local_addr().unwrap())
        .unwrap();
    // No runtime await has occurred since clearing cached readiness.
    batch.try_receive_spinning(&receiver).unwrap();
    assert_eq!(batch.pop_datagram().unwrap().bytes, &[0, 7]);
}
