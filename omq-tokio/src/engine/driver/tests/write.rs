use super::*;
use crate::engine::transmit_slot::SlotConfig;

#[tokio::test]
async fn driver_write_preserves_large_payload_after_partial_write() {
    const MSG_SIZE: usize = 1024 * 1024;
    let payload = (0..MSG_SIZE).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>();
    let mut eq = FrameBuffer::one_shot();
    eq.frame(&Message::single(Bytes::from(payload.clone())));
    let mut writer = PartialVectoredWriter::new(9 + 632_554, 65_537);
    write_frames(&mut writer, &mut eq).await;

    assert!(eq.is_empty());
    assert_eq!(writer.out.len(), 9 + MSG_SIZE);
    assert_eq!(writer.out[0], 0x02);
    assert_eq!(
        u64::from_be_bytes(writer.out[1..9].try_into().unwrap()),
        MSG_SIZE as u64
    );
    assert_eq!(&writer.out[9..], &payload);
}

#[tokio::test]
async fn selected_write_cancellation_preserves_external_chunks() {
    const MSG_SIZE: usize = 1024 * 1024;
    let payload = patterned_payload(MSG_SIZE, 7);
    let mut expected = Vec::with_capacity(MSG_SIZE + 9);
    push_expected_single_frame(&mut expected, &payload);

    let mut eq = FrameBuffer::one_shot();
    eq.frame(&Message::single(Bytes::from(payload)));
    let mut pending = PendingWrite::default();
    let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let _ = drain_transmit(&mut connection);
    let mut heartbeat = HeartbeatProbe::default();
    let (stream, mut peer) = tokio::io::duplex(64);
    let (reader_half, mut writer) = tokio::io::split(stream);
    drop(reader_half);

    tokio::select! {
        biased;
        result = write_driver_progress(
            &mut writer,
            &mut eq,
            &mut pending,
            &mut connection,
            &mut heartbeat,
        ) => panic!("stalled write unexpectedly completed: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }

    assert!(
        !pending.is_empty(),
        "cancelled write dropped pending chunks"
    );

    let reader = tokio::spawn(async move {
        let mut actual = Vec::new();
        peer.read_to_end(&mut actual).await.unwrap();
        actual
    });

    while !pending.is_empty() || !eq.is_empty() {
        write_driver_progress(
            &mut writer,
            &mut eq,
            &mut pending,
            &mut connection,
            &mut heartbeat,
        )
        .await
        .unwrap();
    }

    drop(writer);
    assert_eq!(reader.await.unwrap(), expected);
}

#[tokio::test]
async fn driver_write_preserves_large_payloads_after_partial_write_matrix() {
    const MSG_SIZE: usize = 1024 * 1024;
    const CAPS: &[(usize, usize)] = &[
        (1, 16_384),
        (8, 16_384),
        (9, 16_384),
        (9 + 632_554, 65_537),
        (9 + 632_558, 65_537),
        (9 + 632_562, 65_537),
        (9 + MSG_SIZE - 4, 4_097),
        (9 + MSG_SIZE, 4_097),
    ];

    let payloads = (0..3)
        .map(|seq| patterned_payload(MSG_SIZE, seq))
        .collect::<Vec<_>>();
    let mut expected = Vec::new();

    for payload in &payloads {
        expected.push(0x02);
        expected.extend_from_slice(&(MSG_SIZE as u64).to_be_bytes());
        expected.extend_from_slice(payload);
    }

    for &(first_cap, next_cap) in CAPS {
        let mut eq = FrameBuffer::one_shot();
        for payload in &payloads {
            eq.frame(&Message::single(Bytes::copy_from_slice(payload)));
        }

        let mut writer = PartialVectoredWriter::new(first_cap, next_cap);
        write_frames(&mut writer, &mut eq).await;

        assert!(eq.is_empty());
        assert_eq!(
            writer.out, expected,
            "first_cap={first_cap}, next_cap={next_cap}"
        );
    }
}

#[tokio::test]
async fn driver_write_preserves_large_payloads_after_scripted_partial_writes() {
    const MSG_SIZE: usize = 1024 * 1024;
    let payloads = (0..4)
        .map(|seq| patterned_payload(MSG_SIZE, seq))
        .collect::<Vec<_>>();
    let mut expected = Vec::new();

    for payload in &payloads {
        expected.push(0x02);
        expected.extend_from_slice(&(MSG_SIZE as u64).to_be_bytes());
        expected.extend_from_slice(payload);
    }

    let mut eq = FrameBuffer::one_shot();
    for payload in &payloads {
        eq.frame(&Message::single(Bytes::copy_from_slice(payload)));
    }

    let caps = [
        1,
        8,
        9,
        31,
        4_095,
        4_097,
        65_535,
        65_537,
        9 + 632_554,
        9 + 632_558,
        9 + 632_562,
        131_071,
        262_147,
    ];

    let mut writer = ScriptedVectoredWriter::new(caps, 17_003);
    write_frames(&mut writer, &mut eq).await;

    assert!(eq.is_empty());
    assert_eq!(writer.out, expected);
}

#[tokio::test]
async fn driver_write_matches_reference_under_random_partial_writes() {
    const LENGTHS: &[usize] = &[
        16,
        62,
        63,
        254,
        255,
        256,
        4095,
        4096,
        8191,
        65_535,
        65_536,
        131_073,
        632_558,
        1024 * 1024,
    ];

    for case in 0..48u64 {
        let mut seed = 0xA5A5_5A5A_D3C1_BEEF ^ case;
        let mut eq = FrameBuffer::one_shot();
        let mut expected = Vec::new();

        for seq in 0..12u64 {
            let len = LENGTHS[next_random(&mut seed) % LENGTHS.len()];
            let payload = patterned_payload(len, (case << 8) | seq);
            push_expected_single_frame(&mut expected, &payload);
            eq.frame(&Message::single(Bytes::from(payload)));
        }

        let caps = (0..192)
            .map(|_| match next_random(&mut seed) % 12 {
                0 => 1,
                1 => 2,
                2 => 8,
                3 => 9,
                4 => 17,
                5 => 4_095,
                6 => 4_097,
                7 => 65_535,
                8 => 65_537,
                9 => 9 + 632_558,
                10 => 9 + 1024 * 1024 - 4,
                _ => (next_random(&mut seed) % 262_147) + 1,
            })
            .collect::<Vec<_>>();

        let mut writer = ScriptedVectoredWriter::new(caps, 37_111);
        write_frames(&mut writer, &mut eq).await;

        assert!(eq.is_empty(), "case={case}");
        assert_eq!(writer.out, expected, "case={case}");
    }
}

#[tokio::test]
async fn reused_lazy_frame_buffer_matches_reference_under_partial_writes() {
    const LENGTHS: &[usize] = &[4_095, 4_096, 65_537, 632_558, 1024 * 1024];
    let caps = [1, 8, 9, 4_097, 65_537, 9 + 632_554, 9 + 632_558, 262_147];
    let mut eq = FrameBuffer::with_config_lazy(
        omq_proto::frame_buffer::ARENA_THRESHOLD,
        omq_proto::frame_buffer::ARENA_INITIAL_CAP,
    );
    let mut writer = ScriptedVectoredWriter::new(caps, 23_011);
    let mut expected = Vec::new();

    for seq in 0..64u64 {
        let len = LENGTHS[seq as usize % LENGTHS.len()];
        let payload = patterned_payload(len, seq);
        push_expected_single_frame(&mut expected, &payload);
        eq.frame(&Message::single(Bytes::from(payload)));
        write_frames(&mut writer, &mut eq).await;
        assert!(eq.is_empty(), "seq={seq}");
    }

    assert_eq!(writer.out, expected);
}

#[tokio::test]
async fn owned_chunk_writer_receives_payloads_without_copies() {
    let mut eq = FrameBuffer::new();
    let (expected, large_starts) = mixed_frames(&mut eq);
    let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let _ = drain_transmit(&mut connection);
    let mut pending = PendingWrite::default();
    let mut writer = OwnedChunkWriter::new([], usize::MAX);

    write_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

    assert_eq!(writer.out, expected);
    for start in large_starts {
        assert!(
            writer.chunk_starts.contains(&start),
            "large payload reached the writer as a copy"
        );
    }
}

#[test]
fn coalesce_small_chunks_merges_runs_and_keeps_large_chunks() {
    let large = Bytes::from(patterned_payload(OWNED_COALESCE_BELOW, 1));
    let small: Vec<Bytes> = (0..40)
        .map(|seq| Bytes::from(patterned_payload(4_096, seq)))
        .collect();
    let lone = Bytes::from_static(b"lone");
    let mut chunks = vec![Bytes::from_static(b"hdr"), large.clone(), lone.clone()];
    chunks.push(large.clone());
    chunks.extend(small.iter().cloned());
    let expected: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();

    coalesce_small_chunks(&mut chunks);

    let out: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    assert_eq!(out, expected);
    assert_eq!(chunks[1].as_ptr(), large.as_ptr(), "large chunk was copied");
    assert_eq!(
        chunks[2].as_ptr(),
        lone.as_ptr(),
        "single small chunk was copied"
    );
    assert_eq!(chunks[3].as_ptr(), large.as_ptr(), "large chunk was copied");
    // 40 x 4 KiB merge into runs of at most 64 KiB.
    assert_eq!(chunks.len(), 4 + 3);
    assert!(chunks[4..].iter().all(|c| c.len() <= OWNED_COALESCE_TARGET));
}

#[tokio::test]
async fn owned_chunk_writer_receives_few_chunks_for_mid_size_frames() {
    let mut eq = FrameBuffer::new();
    let mut expected = Vec::new();

    for seq in 0..64 {
        let payload = Bytes::from(patterned_payload(4_096, seq));
        push_expected_single_frame(&mut expected, &payload);
        eq.frame(&Message::single(payload));
    }

    let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let _ = drain_transmit(&mut connection);
    let mut pending = PendingWrite::default();
    let mut writer = OwnedChunkWriter::new([], usize::MAX);

    write_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

    assert_eq!(writer.out, expected);
    // 64 headers and 64 payloads, about 257 KiB, in runs of 64 KiB.
    assert!(
        writer.chunk_starts.len() <= 8,
        "{} chunks reached the writer",
        writer.chunk_starts.len()
    );
}

#[test]
fn owned_slot_staging_merges_shared_fan_out_chunks() {
    let slot = PeerTransmitSlot::with_config(1, SlotConfig::default());
    slot.mark_handshake_done();
    // Shared fan-out payloads stay separate chunks in the slot.
    let payload = Bytes::from(patterned_payload(256, 7));
    let mut expected = Vec::new();

    for _ in 0..200 {
        let header = Bytes::from_static(&[0, 0x80]);
        expected.extend_from_slice(&header);
        expected.extend_from_slice(&payload);
        assert_eq!(
            slot.try_push_encoded(&[header, payload.clone()]),
            crate::engine::transmit_slot::TryFrameResult::Ok
        );
    }

    let mut pending = PendingWrite::default();
    pending.stage_slot(&slot, true);

    let staged: Vec<u8> = pending
        .chunks
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    assert_eq!(staged, expected);
    assert_eq!(pending.remaining, expected.len());
    assert!(
        pending.chunks.len() <= 2,
        "{} chunks staged",
        pending.chunks.len()
    );
}

#[tokio::test]
async fn owned_chunk_writer_preserves_bytes_under_partial_writes() {
    for cap in [1, 7, 9, 64, 4_095, 65_537] {
        let mut eq = FrameBuffer::new();
        let (expected, _) = mixed_frames(&mut eq);
        let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
        let _ = drain_transmit(&mut connection);
        let mut pending = PendingWrite::default();
        let mut writer = OwnedChunkWriter::new([], cap);

        write_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;

        assert_eq!(writer.out, expected, "cap {cap}");
    }
}

#[tokio::test]
async fn owned_chunk_write_cancellation_keeps_staged_chunks() {
    let mut eq = FrameBuffer::new();
    let (expected, _) = mixed_frames(&mut eq);
    let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let _ = drain_transmit(&mut connection);
    let mut pending = PendingWrite::default();
    let mut heartbeat = HeartbeatProbe::default();
    let mut writer = OwnedChunkWriter::new([Some(1_234), None], 50_000);

    // The batch time limit may end a call before the scripted stall.
    loop {
        tokio::select! {
            biased;
            result = write_driver_progress(
                &mut writer,
                &mut eq,
                &mut pending,
                &mut connection,
                &mut heartbeat,
            ) => result.unwrap(),
            () = tokio::time::sleep(Duration::from_millis(10)) => break,
        }
    }

    assert!(!pending.is_empty(), "cancelled write dropped staged chunks");
    assert_eq!(writer.out.len(), 1_234);

    writer.caps.pop_front();

    write_until_idle(&mut writer, &mut eq, &mut pending, &mut connection).await;
    assert_eq!(writer.out, expected);
}

#[tokio::test]
async fn busy_sender_yields_to_tasks_on_its_thread() {
    use futures::FutureExt;

    const MESSAGES: usize = 40_000;
    const WIRE_BYTES: usize = MESSAGES * (2 + 100);

    let (push, _pull) = ready_push_pull_connections();
    // Room for all output, so a full transport never parks the driver.
    let (stream, mut remote) = tokio::io::duplex(2 * WIRE_BYTES);
    let (commands, inbox) = mpsc::channel(4);
    let (events, _event_inbox) = mpsc::channel(4);
    let (mut producer, consumer) = crate::engine::send_pipe(MESSAGES);

    for _ in 0..MESSAGES {
        producer.try_send(Message::single(vec![7; 100])).unwrap();
    }

    let driver = ConnectionDriver::new(stream, push, inbox, events, 1, CancellationToken::new())
        .with_send_pipe(consumer);
    commands
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    let task = tokio::spawn(driver.run());
    let mut buffer = vec![0; 2 * WIRE_BYTES];
    let mut received = 0;
    let mut largest_burst = 0;

    // This task shares the driver's thread. It runs only between driver
    // polls, so each read sees what one driver poll wrote.
    tokio::time::timeout(Duration::from_secs(5), async {
        while received < WIRE_BYTES {
            if let Some(read) = remote.read(&mut buffer).now_or_never() {
                let read = read.unwrap();
                received += read;
                largest_burst = largest_burst.max(read);
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("driver output stalled");

    assert!(
        largest_burst < WIRE_BYTES / 4,
        "one driver poll wrote {largest_burst} of {WIRE_BYTES} bytes"
    );
    task.abort();
}
