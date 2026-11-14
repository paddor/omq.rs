use super::*;

fn cost(targets: usize) -> FanOutCost {
    FanOutCost {
        targets,
        copy_budget: 8 * 1024,
    }
}

#[test]
fn arena_batch_when_total_copy_fits_budget() {
    let mut encoder = FanOutEncoder::new();
    let msg = Message::from(Bytes::from_static(&[0x11; 64]));

    let batch = encoder.build(&msg, cost(8));

    assert!(matches!(batch, FanOutFrame::Arena(_)));
    assert_eq!(encoder.chunks, [] as [Bytes; 0]);
}

#[test]
fn chunk_batch_when_total_copy_exceeds_budget() {
    let mut encoder = FanOutEncoder::new();
    let msg = Message::from(Bytes::from(vec![0x22; 4 * 1024]));

    let batch = encoder.build(&msg, cost(8));

    assert!(matches!(batch, FanOutFrame::Chunks(_)));
    assert_eq!(encoder.chunks.len(), 2);
    assert_eq!(encoder.chunks[0].len(), 9);
    assert_eq!(encoder.chunks[1].len(), 4 * 1024);
}

#[test]
fn chunk_batch_for_large_single_peer_message() {
    let mut encoder = FanOutEncoder::new();
    let msg = Message::from(Bytes::from(vec![0x33; 128 * 1024]));

    let batch = encoder.build(&msg, cost(1));

    assert!(matches!(batch, FanOutFrame::Chunks(_)));
    assert_ne!(encoder.chunks, [] as [Bytes; 0]);
}

#[test]
fn arena_batch_when_total_copy_equals_budget() {
    let mut encoder = FanOutEncoder::new();
    let msg = Message::from(Bytes::from(vec![0x44; 254]));

    let batch = encoder.build(&msg, cost(32));

    assert!(matches!(batch, FanOutFrame::Arena(_)));
    assert_eq!(encoder.chunks, [] as [Bytes; 0]);
}

#[test]
fn gathered_multipart_keeps_all_chunks_in_one_publication() {
    for parts in [600, 1025] {
        let msg = Message::multipart((0..parts).map(|_| Bytes::from_static(&[0x5a; 1024])));
        let mut expected = bytes::BytesMut::new();
        crate::proto::frame::encode_message_flat(&msg, &mut expected);
        let mut encoder = FanOutEncoder::new();
        let frame = encoder.build(&msg, cost(2));

        let FanOutFrame::Chunks(encoded_chunks) = frame else {
            panic!("large multipart must gather");
        };

        assert!(encoded_chunks.len() > 1024);
        let actual: Vec<u8> = encoded_chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        assert_eq!(actual, expected.as_ref());
        encoder.clear();
        assert!(encoder.frames().is_empty());
    }
}

#[test]
fn message_wire_len_counts_short_and_long_headers() {
    let msg = Message::multipart([vec![0; 255], vec![0; 256], Vec::new()]);
    assert_eq!(message_wire_len(&msg), (2 + 255) + (9 + 256) + 2);
}
