use super::*;

#[test]
fn drain_owned_moves_arena_bytes_and_reserves_again() {
    let mut eq = FrameBuffer::new();
    eq.frame(&Message::single("abc"));
    eq.frame(&Message::single("defg"));
    let arena_start = eq.arena_bytes().as_ptr() as usize;

    let mut chunks = Vec::new();
    eq.drain_owned(&mut chunks, 16);
    assert_eq!(chunks.len(), 1);
    assert_eq!(&chunks[0][..], b"\x00\x03abc\x00\x04defg");
    assert_eq!(
        chunks[0].as_ptr() as usize,
        arena_start,
        "arena bytes were copied"
    );
    assert!(eq.is_empty());
    assert_eq!(eq.total_bytes(), 0);

    let large = vec![7u8; ARENA_INITIAL_CAP];
    eq.frame(&Message::single(Bytes::from(large.clone())));
    eq.frame(&Message::single("h"));
    // The first chunk still holds the old arena allocation.
    assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
    chunks.clear();
    eq.drain_owned(&mut chunks, 16);
    let wire: Vec<u8> = chunks
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    let mut expected = vec![0x02];
    expected.extend_from_slice(&(large.len() as u64).to_be_bytes());
    expected.extend_from_slice(&large);
    expected.extend_from_slice(b"\x00\x01h");
    assert_eq!(wire, expected);
}

#[test]
fn drain_owned_reuses_arena_only_after_chunks_drop() {
    let mut eq = FrameBuffer::new();
    let mut chunks = Vec::new();
    eq.frame(&Message::single("first"));
    let first_arena = eq.arena_bytes().as_ptr() as usize;
    eq.drain_owned(&mut chunks, 16);

    // A chunk still references the first arena: allocate another.
    eq.frame(&Message::single("second"));
    let second_arena = eq.arena_bytes().as_ptr() as usize;
    assert_ne!(second_arena, first_arena);
    let mut second = Vec::new();
    eq.drain_owned(&mut second, 16);

    // Dropping the first arena's chunks makes it reusable.
    chunks.clear();
    eq.frame(&Message::single("third"));
    assert_eq!(eq.arena_bytes().as_ptr() as usize, first_arena);
    assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
    eq.drain_owned(&mut chunks, 16);
    assert_eq!(&chunks[0][..], b"\x00\x05third");
    assert_eq!(&second[0][..], b"\x00\x06second");
}

#[test]
fn drain_owned_bounds_retired_arenas_by_count_and_capacity() {
    let mut eq = FrameBuffer::new();
    let mut in_flight = Vec::new();
    for _ in 0..3 * RETIRED_ARENAS {
        eq.frame(&Message::single("x"));
        eq.drain_owned(&mut in_flight, 16);
        assert!(eq.retired.len() <= RETIRED_ARENAS);
    }
    let mut large = FrameBuffer::with_config(ARENA_THRESHOLD, 1024 * 1024);
    for _ in 0..8 {
        large.frame(&Message::single("x"));
        large.drain_owned(&mut in_flight, 16);
        assert!(large.retired_bytes <= RETIRED_ARENA_BYTES);
        assert_eq!(
            large.retired_bytes,
            large
                .retired
                .iter()
                .map(|(_, capacity)| capacity)
                .sum::<usize>()
        );
    }
    // Evicted arenas stay valid for the chunks that still hold them.
    assert!(in_flight.iter().all(|chunk| &chunk[..] == b"\x00\x01x"));
    in_flight.clear();
    eq.frame(&Message::single("y"));
    assert_eq!(eq.retired.len(), RETIRED_ARENAS - 1);
}

#[test]
fn lazy_config_defers_arena_allocation_until_encode() {
    let mut eq = FrameBuffer::with_config_lazy(ARENA_THRESHOLD, ARENA_INITIAL_CAP);
    assert_eq!(eq.arena.capacity(), 0);

    eq.frame(&Message::single("abc"));

    assert!(eq.arena.capacity() >= ARENA_INITIAL_CAP);
    assert_ne!(eq.arena_bytes(), []);
}

fn wire_bytes(eq: &FrameBuffer) -> Vec<u8> {
    let mut slices = [IoSlice::new(&[]); 64];
    let count = eq.io_slices(&mut slices);
    slices[..count]
        .iter()
        .flat_map(|slice| slice.iter().copied())
        .collect()
}

#[test]
fn io_slices_borrow_large_payloads_in_wire_order() {
    let mut eq = FrameBuffer::new();
    let large = Bytes::from(vec![2; 128 * 1024]);
    eq.frame(&Message::single("a"));
    eq.frame(&Message::single(large.clone()));
    eq.frame(&Message::single("b"));

    let mut slices = [IoSlice::new(&[]); 8];
    let count = eq.io_slices(&mut slices);
    // [a + large header], large body, [b]
    assert_eq!(count, 3);
    assert_eq!(slices[1].as_ptr(), large.as_ptr(), "payload was copied");

    let mut expected = FrameBuffer::new();
    expected.frame(&Message::single("a"));
    expected.frame(&Message::single(large));
    expected.frame(&Message::single("b"));
    let mut chunks = Vec::new();
    expected.drain(&mut chunks, 1024);
    let drained: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    assert_eq!(wire_bytes(&eq), drained);
}

#[test]
fn advance_consumes_partial_writes_across_entries() {
    let mut eq = FrameBuffer::new();
    eq.frame(&Message::single("head"));
    eq.frame(&Message::single(Bytes::from(vec![9; 8 * 1024])));
    eq.frame(&Message::single("tail"));
    let full = wire_bytes(&eq);

    let mut written = 0;
    for step in [1, 7, 4000, 4100, 1] {
        eq.advance(step);
        written += step;
        assert_eq!(wire_bytes(&eq), full[written..]);
        assert_eq!(eq.total_bytes(), full.len() - written);
    }
    eq.advance(full.len() - written);
    assert!(eq.is_empty());
    assert_eq!(eq.total_bytes(), 0);

    // The arena is reusable after a full write.
    eq.frame(&Message::single("next"));
    assert!(eq.has_arena_only());
    assert_eq!(eq.arena_bytes(), b"\x00\x04next");
}

#[test]
fn advance_through_arena_only_queue() {
    let mut eq = FrameBuffer::new();
    eq.frame(&Message::single("abcdef"));
    eq.advance(3);
    assert_eq!(wire_bytes(&eq), b"bcdef");
    assert!(eq.has_arena_only());
    assert_eq!(eq.arena_bytes(), b"bcdef");
    eq.advance(5);
    assert!(eq.is_empty());
}

#[test]
fn put_back_partial_write() {
    let mut eq = FrameBuffer::new();
    let msg = Message::from(Bytes::from_static(&[0xAB; 100]));
    eq.frame_gather(&msg);
    assert!(!eq.is_empty());

    let mut buf = Vec::new();
    eq.drain(&mut buf, 1024);
    let total: usize = buf.iter().map(Bytes::len).sum();
    assert!(total > 0);

    eq.put_back_unwritten(buf, 5);
    assert!(!eq.is_empty());

    let mut buf2 = Vec::new();
    eq.drain(&mut buf2, 1024);
    let remaining: usize = buf2.iter().map(Bytes::len).sum();
    assert_eq!(remaining, total - 5);
}

#[test]
fn arena_and_gather_ordering() {
    let mut eq = FrameBuffer::new();
    let small = Message::from(Bytes::from_static(&[1; 64]));
    let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

    eq.frame_inline(&small);
    eq.frame_gather(&large);
    eq.frame_inline(&small);

    let mut buf = Vec::new();
    eq.drain(&mut buf, 1024);

    // First chunk: small message frame + large message header (coalesced)
    assert!(buf[0].len() > 64);
    assert!(buf.len() >= 3);
}

#[test]
fn gather_headers_share_arena() {
    let mut eq = FrameBuffer::new();
    let large = Message::from(Bytes::from(vec![0xCC; 128 * 1024]));

    eq.frame_gather(&large);
    eq.frame_gather(&large);

    let mut buf = Vec::new();
    eq.drain(&mut buf, 1024);

    // 2 messages × (1 header chunk + 1 payload chunk) = 4 chunks
    assert_eq!(buf.len(), 4);
    // Both header chunks are slices of the same arena allocation
    assert_eq!(buf[0].len(), 9); // long frame header
    assert_eq!(buf[2].len(), 9);
}

#[test]
fn mixed_coalesces_header_with_small() {
    let mut eq = FrameBuffer::new();
    let small = Message::from(Bytes::from_static(&[1; 64]));
    let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

    eq.frame_inline(&small);
    eq.frame_gather(&large);

    let mut buf = Vec::new();
    eq.drain(&mut buf, 1024);

    // small frame (2 + 64 = 66 bytes) + large header (9 bytes) = 75 bytes
    // coalesced into one arena chunk
    assert_eq!(buf.len(), 2);
    assert_eq!(buf[0].len(), 66 + 9);
    assert_eq!(buf[1].len(), 128 * 1024);
}

#[test]
fn empty_after_drain() {
    let mut eq = FrameBuffer::new();
    let msg = Message::from(Bytes::from_static(&[1; 64]));
    eq.frame_inline(&msg);
    assert!(!eq.is_empty());

    let mut buf = Vec::new();
    eq.drain(&mut buf, 1024);
    assert!(eq.is_empty());
}

#[test]
fn has_arena_only_small_message() {
    let mut eq = FrameBuffer::one_shot();
    assert!(!eq.has_arena_only());

    let msg = Message::from(Bytes::from_static(&[0xAA; 64]));
    eq.frame(&msg);
    assert!(eq.has_arena_only());

    let raw = eq.uncommitted_arena();
    assert_eq!(raw.len(), eq.total_bytes());
    assert_ne!(raw, []);
}

#[test]
fn has_arena_only_false_for_gather() {
    let mut eq = FrameBuffer::one_shot();
    let large = Message::from(Bytes::from(vec![0xBB; 128 * 1024]));
    eq.frame(&large);
    assert!(!eq.has_arena_only());
}

#[test]
fn rep_frame_keeps_identity_off_wire() {
    let mut eq = FrameBuffer::one_shot();
    let body = Message::single(Bytes::from_static(b"reply"));
    eq.frame_rep(&Bytes::from_static(b"peer-id"), &body);

    let mut actual = Vec::new();
    eq.drain(&mut actual, 1024);
    let actual: Vec<u8> = actual
        .into_iter()
        .flat_map(|chunk| chunk.to_vec())
        .collect();

    let expected_message = Message::multipart([Bytes::new(), Bytes::from_static(b"reply")]);
    let mut expected = BytesMut::new();
    crate::proto::frame::encode_message_flat(&expected_message, &mut expected);
    assert_eq!(actual, expected.as_ref());
}

#[test]
fn take_arena_bytes_round_trip() {
    let mut eq = FrameBuffer::one_shot();
    let msg = Message::from(Bytes::from_static(&[0xCC; 32]));
    eq.frame(&msg);
    assert!(eq.has_arena_only());

    let frozen = eq.take_arena_bytes();
    assert!(!frozen.is_empty());
    assert!(eq.is_empty());
    assert_eq!(eq.total_bytes(), 0);

    let mut eq2 = FrameBuffer::new();
    eq2.push_pre_framed(&frozen);
    let mut buf = Vec::new();
    eq2.drain(&mut buf, 1024);
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0], frozen);
}

#[test]
fn arena_only_matches_drain_output() {
    let mut eq1 = FrameBuffer::one_shot();
    let mut eq2 = FrameBuffer::one_shot();
    let msg = Message::from(Bytes::from_static(&[0xDD; 100]));

    eq1.frame(&msg);
    eq2.frame(&msg);

    let raw = eq1.uncommitted_arena().to_vec();
    eq1.clear_arena();

    let mut chunks = Vec::new();
    eq2.drain(&mut chunks, 1024);

    let drained: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
    assert_eq!(raw, drained);
}

#[test]
fn pop_front_entry_drops_oldest_committed_chunk() {
    let mut eq = FrameBuffer::one_shot();
    eq.push_raw(vec![Bytes::from_static(b"first")]);
    eq.push_raw(vec![Bytes::from_static(b"second")]);

    assert!(eq.pop_front_entry());

    let mut chunks = Vec::new();
    eq.drain(&mut chunks, 1024);
    assert_eq!(chunks, vec![Bytes::from_static(b"second")]);
    assert!(eq.is_empty());
}
