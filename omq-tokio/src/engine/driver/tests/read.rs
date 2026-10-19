use super::*;

#[test]
fn recv_buf_pool_reuses_bounded_buffers() {
    let pool = RecvBufPool::new();
    let mut buf = pool.take(256 * 1024);
    buf.extend_from_slice(&[7; 1024]);
    let capacity = buf.capacity();
    pool.give(buf);

    assert_eq!(pool.inner.lock().unwrap().buffers.len(), 1);
    let reused = pool.take(128 * 1024);
    assert_eq!(reused.len(), 0);
    assert!(reused.capacity() >= capacity);
    assert_eq!(pool.inner.lock().unwrap().retained_bytes, 0);
}

#[test]
fn recv_buf_pool_does_not_retain_huge_buffers() {
    let pool = RecvBufPool::new();
    pool.give(BytesMut::with_capacity(RECV_POOL_MAX_BUFFER_BYTES + 1));
    let guard = pool.inner.lock().unwrap();
    assert_eq!(guard.buffers.len(), 0);
    assert_eq!(guard.retained_bytes, 0);
}

#[test]
fn recv_buf_pool_grows_before_returning_a_larger_lease() {
    let pool = RecvBufPool::new();
    pool.give(BytesMut::with_capacity(128 * 1024));
    let buffer = pool.take(192 * 1024);
    assert!(buffer.is_empty());
    assert!(buffer.capacity() >= 192 * 1024);
}

#[test]
fn recv_buf_pool_caps_total_retained_bytes() {
    const BUFFER_BYTES: usize = 1024 * 1024;

    let pool = RecvBufPool::new();
    let buffer_count = (RECV_POOL_MAX_RETAINED_BYTES / BUFFER_BYTES) + 2;
    for _ in 0..buffer_count {
        pool.give(BytesMut::with_capacity(BUFFER_BYTES));
    }
    let guard = pool.inner.lock().unwrap();
    assert!(guard.retained_bytes <= RECV_POOL_MAX_RETAINED_BYTES);
    assert_eq!(
        guard.buffers.len(),
        RECV_POOL_MAX_RETAINED_BYTES / BUFFER_BYTES
    );
}

#[test]
fn recv_buf_pool_waits_for_last_bytes_clone() {
    let pool = RecvBufPool::new();
    let mut buf = pool.take(4 * 1024 * 1024);
    buf.extend_from_slice(b"payload");
    let payload = pool.wrap(buf);
    let clone = payload.clone();

    drop(payload);
    assert_eq!(pool.inner.lock().unwrap().buffers.len(), 0);

    drop(clone);
    let guard = pool.inner.lock().unwrap();
    assert_eq!(guard.buffers.len(), 1);
    assert!(guard.retained_bytes >= 4 * 1024 * 1024);
}

#[test]
fn recv_buf_pool_does_not_outlive_connection_owner() {
    let pool = RecvBufPool::new();
    let weak = Arc::downgrade(&pool);
    let payload = pool.wrap(pool.take(4 * 1024 * 1024));

    drop(pool);
    assert!(weak.upgrade().is_none());

    drop(payload);
}

#[test]
fn recv_buf_pool_accepts_concurrent_last_owner_drops() {
    const BUFFER_COUNT: usize = 16;
    const BUFFER_BYTES: usize = 1024 * 1024;

    let pool = RecvBufPool::new();
    let payloads = (0..BUFFER_COUNT)
        .map(|_| pool.wrap(pool.take(BUFFER_BYTES)))
        .collect::<Vec<_>>();

    std::thread::scope(|scope| {
        for payload in payloads {
            scope.spawn(move || drop(payload));
        }
    });

    let guard = pool.inner.lock().unwrap();
    assert_eq!(guard.buffers.len(), BUFFER_COUNT);
    assert_eq!(guard.retained_bytes, BUFFER_COUNT * BUFFER_BYTES);
}

#[tokio::test]
async fn accepted_large_read_keeps_one_allocation_through_completion() {
    let declared = RECV_POOL_MAX_BUFFER_BYTES * 2;
    let (_, mut pull) = ready_connections(
        ConnectionConfig::new(Role::Client, SocketType::Push),
        ConnectionConfig::new(Role::Server, SocketType::Pull)
            .max_message_size(declared + size_of::<omq_proto::Payload>()),
    );
    let prefix = b"buffered prefix";
    let mut wire = vec![0x02];
    wire.extend_from_slice(&(declared as u64).to_be_bytes());
    wire.extend_from_slice(prefix);
    pull.handle_input(Bytes::from(wire)).unwrap();
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let pool = RecvBufPool::new();
    let mut pending = PendingLargeRead::begin(&mut pull, &config, &pool)
        .unwrap()
        .unwrap();
    let ReceiveBody::Stream { buf, pooled } = &pending.buf else {
        panic!("expected an owned receive buffer");
    };
    assert!(!pooled);
    assert_eq!(buf.capacity(), declared);
    let allocation = buf.as_ptr();
    let mut reader = tokio::io::repeat(b'!').take((declared - prefix.len()) as u64);
    while !pending.complete() {
        pending.read(&mut reader).await.unwrap();
        let ReceiveBody::Stream { buf, .. } = &pending.buf else {
            unreachable!()
        };
        assert_eq!(buf.as_ptr(), allocation, "receive body moved during a read");
        assert_eq!(buf.capacity(), declared);
    }
    pending.finish(&mut pull, &pool).unwrap();
    let message = pull.poll_message().unwrap();
    let body = message.part_bytes(0).unwrap();
    assert_eq!(body.as_ptr(), allocation);
    assert_eq!(&body[..prefix.len()], prefix);
    assert!(body[prefix.len()..].iter().all(|byte| *byte == b'!'));
}

#[tokio::test]
async fn unpooled_large_read_grows_with_received_bytes() {
    let declared = RECV_POOL_MAX_BUFFER_BYTES * 2;
    let prefix = b"buffered prefix";
    let (_, mut pull) = ready_push_pull_connections();
    let mut wire = vec![0x02];
    wire.extend_from_slice(&(declared as u64).to_be_bytes());
    wire.extend_from_slice(prefix);
    pull.handle_input(Bytes::from(wire)).unwrap();
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let pool = RecvBufPool::new();
    let mut pending = PendingLargeRead::begin(&mut pull, &config, &pool)
        .unwrap()
        .unwrap();
    let ReceiveBody::Stream { buf, pooled } = &pending.buf else {
        panic!("expected an unpooled receive buffer");
    };
    assert!(!pooled);
    assert_eq!(buf.as_ref(), prefix);
    assert!(
        buf.capacity() <= prefix.len() + 64 * 1024,
        "a header allocated the entire declared body: {} bytes",
        buf.capacity()
    );

    let mut reader = tokio::io::repeat(b'!').take((declared - prefix.len()) as u64);
    while !pending.complete() {
        let before = pending.filled();
        let n = pending.read(&mut reader).await.unwrap();
        assert!(n > 0 && n <= 64 * 1024);
        assert_eq!(pending.filled(), before + n);
    }
    pending.finish(&mut pull, &pool).unwrap();
    let message = pull.poll_message().unwrap();
    let body = message.part_bytes(0).unwrap();
    assert_eq!(body.len(), declared);
    assert_eq!(&body[..prefix.len()], prefix);
    assert!(body[prefix.len()..].iter().all(|byte| *byte == b'!'));
    assert_eq!(pool.inner.lock().unwrap().retained_bytes, 0);
}

#[tokio::test]
async fn unpooled_large_declaration_keeps_partial_read_bounded() {
    let declared = isize::MAX as usize;
    let (_, mut pull) = ready_push_pull_connections();
    let mut wire = vec![0x02];
    wire.extend_from_slice(&(declared as u64).to_be_bytes());
    pull.handle_input(Bytes::from(wire)).unwrap();
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let pool = RecvBufPool::new();
    let mut pending = PendingLargeRead::begin(&mut pull, &config, &pool)
        .unwrap()
        .unwrap();
    let mut reader = &b"partial body"[..];
    assert_eq!(pending.read(&mut reader).await.unwrap(), 12);
    assert_eq!(pending.target, declared);
    assert!(!pending.complete());
    let ReceiveBody::Stream { buf, .. } = &pending.buf else {
        panic!("expected an unpooled receive buffer");
    };
    assert_eq!(buf.as_ref(), b"partial body");
    assert!(buf.capacity() <= 64 * 1024);
    assert_eq!(
        pending.read(&mut reader).await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(pull.poll_message().is_none());
}

#[tokio::test]
async fn large_message_direct_read_preserves_buffered_prefix() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 632_558;

    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
    push.send_message(&Message::single(Bytes::from(payload.clone())))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

    pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
    let mut reader = ScriptedReader::new(
        wire.slice(prefix_wire_bytes..),
        [3, 1, 65_537, 8_191, 262_147],
    );
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
        .await
        .unwrap();

    let msg = pull.poll_message().expect("large message decoded");
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
    assert_eq!(reader.pos, reader.data.len());
}

#[tokio::test]
async fn read_stream_reuses_prefix_buffer_after_large_frame_consumes_it() {
    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = vec![0x5a; 1024 * 1024];
    push.send_message(&Message::single(Bytes::from(payload.clone())))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let mut read = StreamRead::new(ReceiveProfile::Throughput);
    read.buffer.reserve(READ_BUF_MAX);
    read.buffer.extend_from_slice(&wire[..READ_BUF_MAX]);
    let original = read.buffer.as_ptr();
    let mut reader = ScriptedReader::new(wire.slice(READ_BUF_MAX..), [4093, 65537, 8191]);
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();
    read.handle_input(READ_BUF_MAX, &mut pull, &config, &mut last_input)
        .unwrap();
    assert!(read.buffer.is_empty());
    assert!(read.buffer.capacity() >= READ_BUF_MAX);
    assert_eq!(
        read.buffer.as_ptr(),
        original,
        "released prefix allocation should be reused"
    );
    assert!(read.has_pending_body());
    while read.has_pending_body() {
        assert!(read.read(&mut reader).await.unwrap() > 0);
        read.finish_pending(&mut pull, &config).unwrap();
    }
    let message = pull.poll_message().expect("complete large frame");
    assert_eq!(message.part_bytes(0).unwrap().as_ref(), payload);
}

#[tokio::test]
async fn large_message_direct_read_preserves_fragmented_buffered_prefix() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 632_558;

    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = patterned_payload(MSG_SIZE, 42);
    push.send_message(&Message::single(Bytes::from(payload.clone())))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

    pull.handle_input(wire.slice(..5)).unwrap();
    pull.handle_input(wire.slice(5..17)).unwrap();
    pull.handle_input(wire.slice(17..prefix_wire_bytes))
        .unwrap();

    let mut reader = ScriptedReader::new(
        wire.slice(prefix_wire_bytes..),
        [4, 3, 1, 65_537, 8_191, 262_147],
    );
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
        .await
        .unwrap();

    let msg = pull.poll_message().expect("large message decoded");
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
    assert_eq!(reader.pos, reader.data.len());
}

#[tokio::test]
async fn large_message_direct_read_preserves_many_chunk_buffered_prefix() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 632_558;

    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = patterned_payload(MSG_SIZE, 43);
    push.send_message(&Message::single(Bytes::from(payload.clone())))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

    feed_input_in_chunks(
        &mut pull,
        &wire,
        prefix_wire_bytes,
        [4096, 8192, 16_384, 32_768, 65_536, 131_072],
    );

    let mut reader = ScriptedReader::new(
        wire.slice(prefix_wire_bytes..),
        [4, 3, 1, 65_537, 8_191, 262_147],
    );
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
        .await
        .unwrap();

    let msg = pull.poll_message().expect("large message decoded");
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
    assert_eq!(reader.pos, reader.data.len());
}

#[tokio::test]
async fn large_message_direct_read_uses_uninitialized_spare_capacity() {
    const MSG_SIZE: usize = RECV_POOL_MAX_BUFFER_BYTES + 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 64;

    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = patterned_payload(MSG_SIZE, 45);
    push.send_message(&Message::single(Bytes::from(payload.clone())))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

    pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
    let mut reader = UninitProbeReader {
        inner: ScriptedReader::new(wire.slice(prefix_wire_bytes..), [128, 4093, 65_537]),
    };
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
        .await
        .unwrap();

    let msg = pull.poll_message().expect("large message decoded");
    assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn large_message_direct_read_returns_unexpected_eof_on_short_payload() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 64;

    let (mut push, mut pull) = ready_push_pull_connections();
    let payload = patterned_payload(MSG_SIZE, 44);
    push.send_message(&Message::single(Bytes::from(payload)))
        .unwrap();
    let wire = Bytes::from(drain_transmit(&mut push));
    let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

    pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
    let short_end = prefix_wire_bytes + 1024;
    let mut reader = ScriptedReader::new(wire.slice(prefix_wire_bytes..short_end), [128]);
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    let err = handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
        .await
        .expect_err("short payload must fail");

    assert!(matches!(
        err,
        Error::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[tokio::test]
async fn large_message_direct_read_preserves_repeated_payloads() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_PAYLOAD_BYTES: usize = 632_558;

    let (mut push, mut pull) = ready_push_pull_connections();
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    for seq in 0..2 {
        let payload = patterned_payload(MSG_SIZE, seq);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + PREFIX_PAYLOAD_BYTES;

        pull.handle_input(wire.slice(..prefix_wire_bytes)).unwrap();
        let mut reader = ScriptedReader::new(
            wire.slice(prefix_wire_bytes..),
            [3, 1, 65_537, 8_191, 262_147],
        );
        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
    }
}

#[tokio::test]
async fn large_message_direct_read_small_payload_smoke() {
    const MSG_SIZE: usize = 8 * 1024;
    const PREFIX_CASES: &[usize] = &[0, 4, 4097, MSG_SIZE];

    let (mut push, mut pull) = ready_push_pull_connections();
    let config = PeerDriverConfig {
        large_message_threshold: 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    for (seq, &prefix_payload_bytes) in PREFIX_CASES.iter().enumerate() {
        let payload = patterned_payload(MSG_SIZE, seq as u64);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + prefix_payload_bytes;

        feed_fragmented_input(&mut pull, &wire, prefix_wire_bytes);
        let mut reader = ScriptedReader::new(wire.slice(prefix_wire_bytes..), [1, 7, 31, 257]);
        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        assert_eq!(reader.pos, reader.data.len());
    }
}

#[tokio::test]
async fn large_message_direct_read_survives_prefix_boundary_matrix() {
    const MSG_SIZE: usize = 1024 * 1024;
    const PREFIX_CASES: &[usize] = &[
        0,
        1,
        4,
        8,
        9,
        17,
        255,
        256,
        4095,
        4096,
        65_535,
        65_536,
        128 * 1024 - 4,
        128 * 1024,
        128 * 1024 + 4,
        632_554,
        632_558,
        632_562,
        MSG_SIZE - 1,
        MSG_SIZE,
    ];

    let (mut push, mut pull) = ready_push_pull_connections();
    let config = PeerDriverConfig {
        large_message_threshold: 128 * 1024,
        ..PeerDriverConfig::default()
    };
    let mut last_input = Instant::now();

    for (seq, &prefix_payload_bytes) in PREFIX_CASES.iter().enumerate() {
        let payload = patterned_payload(MSG_SIZE, seq as u64);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        let wire = Bytes::from(drain_transmit(&mut push));
        let prefix_wire_bytes = 9 + prefix_payload_bytes;

        feed_fragmented_input(&mut pull, &wire, prefix_wire_bytes);
        let mut reader = ScriptedReader::new(
            wire.slice(prefix_wire_bytes..),
            [1 + (seq % 7), 3, 31, 4093, 65_537, 131_071, 262_147],
        );

        handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
            .await
            .unwrap();

        let msg = pull.poll_message().expect("large message decoded");
        assert_eq!(msg.part_bytes(0).unwrap().as_ref(), payload.as_slice());
        assert_eq!(reader.pos, reader.data.len());
    }
}

#[tokio::test]
async fn large_message_direct_read_matches_mixed_chunked_wire_reference() {
    const CASES: u64 = 24;

    for case in 0..CASES {
        let mut seed = 0x5151_F00D_ABCD_1234 ^ case;
        let (mut push, mut pull) = ready_push_pull_connections();
        let expected = queue_random_payloads(&mut push, &mut seed, case);
        let wire = Bytes::from(drain_transmit(&mut push));
        let config = PeerDriverConfig {
            large_message_threshold: 128 * 1024,
            ..PeerDriverConfig::default()
        };
        let mut last_input = Instant::now();
        let mut cursor = 0usize;
        let mut got = Vec::new();

        // Alternate codec input chunks with direct large-body reads.
        while cursor < wire.len() {
            let end = cursor
                .saturating_add(random_chunk_len(&mut seed))
                .min(wire.len());
            pull.handle_input(wire.slice(cursor..end)).unwrap();
            cursor = end;

            let mut reader = ScriptedReader::new(wire.slice(cursor..), random_read_caps(&mut seed));
            handle_large_messages_test(&mut pull, &mut reader, &config, &mut last_input)
                .await
                .unwrap();
            cursor += reader.pos;

            while let Some(msg) = pull.poll_message() {
                got.push(msg.part_bytes(0).unwrap().to_vec());
            }
        }

        while let Some(msg) = pull.poll_message() {
            got.push(msg.part_bytes(0).unwrap().to_vec());
        }
        assert_eq!(got, expected, "case={case}");
    }
}

/// Queue 18 patterned messages of random sizes around the large-message
/// threshold. Returns their payloads in order.
fn queue_random_payloads(push: &mut Connection, seed: &mut u64, case: u64) -> Vec<Vec<u8>> {
    const LENGTHS: &[usize] = &[
        16,
        255,
        256,
        4095,
        4096,
        65_536,
        128 * 1024 + 1,
        632_558,
        1024 * 1024,
    ];

    let mut expected = Vec::new();
    for seq in 0..18u64 {
        let len = LENGTHS[next_random(seed) % LENGTHS.len()];
        let payload = patterned_payload(len, (case << 8) | seq);
        push.send_message(&Message::single(Bytes::from(payload.clone())))
            .unwrap();
        expected.push(payload);
    }
    expected
}

/// Codec input chunk sizes that split frame headers and bodies at edges.
fn random_chunk_len(seed: &mut u64) -> usize {
    match next_random(seed) % 10 {
        0 => 1,
        1 => 2,
        2 => 9,
        3 => 17,
        4 => 4096,
        5 => 65_536,
        6 => 128 * 1024,
        7 => 128 * 1024 + 4,
        8 => 9 + 632_558,
        _ => (next_random(seed) % (128 * 1024)) + 1,
    }
}

fn random_read_caps(seed: &mut u64) -> [usize; 6] {
    [
        1 + (next_random(seed) % 7),
        31,
        4093,
        65_537,
        131_071,
        262_147,
    ]
}
