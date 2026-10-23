use super::*;
use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
use tokio::time::{Duration, timeout};

fn test_slot() -> Arc<PeerTransmitSlot> {
    let slot = PeerTransmitSlot::with_config(1, SlotConfig::default());
    slot.handshake_done.store(true, Ordering::Release);
    slot
}

fn fanout_bytes(msg: &Message) -> Bytes {
    let mut eq = FrameBuffer::one_shot();
    let mut chunks = Vec::new();
    let frame = build_fan_out_frame(&mut eq, msg, &mut chunks, 1, 8 * 1024);
    let wire: Vec<u8> = fanout_frame_chunks(&frame).concat();
    clear_fan_out_frame(&mut eq, &mut chunks);
    Bytes::from(wire)
}

#[test]
fn transmit_slot_caps_queued_messages_independent_of_bytes() {
    let slot = test_slot();
    let msg = Message::from("x");

    for _ in 0..TRANSMIT_SLOT_MSG_CAP_DEFAULT {
        assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
    }
    assert_eq!(slot.try_encode(&msg), TryFrameResult::Full);

    let mut chunks = Vec::new();
    slot.drain(&mut chunks, 1024);
    assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
}

/// Push messages `start..` one at a time until the slot refuses one.
/// Returns the index of the first message not admitted.
fn admit_singles(slot: &PeerTransmitSlot, data: &[u8], ends: &[usize], start: usize) -> usize {
    let mut next = start;
    let mut offset = if start == 0 { 0 } else { ends[start - 1] };
    while next < ends.len() {
        let end = ends[next];
        if slot.try_push_pre_framed_no_signal(&data[offset..end]) != TryFrameResult::Ok {
            break;
        }
        next += 1;
        offset = end;
    }
    next
}

#[test]
fn pre_framed_batch_matches_single_admission_and_retries_suffix() {
    let messages = [
        Message::from("one"),
        Message::from("two"),
        Message::from("three"),
    ];
    let mut wire = FrameBuffer::new();
    let mut ends = Vec::new();
    for message in &messages {
        wire.frame_inline(message);
        ends.push(wire.total_bytes());
    }
    let data = wire.uncommitted_arena();
    for byte_cap in [1, 5, 8, 100] {
        for msg_cap in [1, 2, 8] {
            let make_slot = || {
                PeerTransmitSlot::with_config(
                    1,
                    SlotConfig {
                        arena_threshold: 4096,
                        arena_cap: 16 * 1024,
                        cap: byte_cap,
                        msg_cap,
                        ..SlotConfig::default()
                    },
                )
            };
            let batched = make_slot();
            let singles = make_slot();
            let mut start = 0;
            let mut received = Vec::new();
            while start < ends.len() {
                let expected = admit_singles(&singles, data, &ends, start);
                let (next, result) =
                    batched.try_push_pre_framed_batch_no_signal(data, &ends, start);
                assert_eq!(next, expected);
                assert!(next > start, "an empty slot must accept one whole message");
                assert_eq!(batched.queued_msgs.load(Ordering::Relaxed), next - start);
                assert_eq!(result == TryFrameResult::Ok, next == ends.len());
                assert!(batched.data_signal.is_idle(), "caller owns the batch wake");
                batched.signal_encoded();
                assert!(!batched.data_signal.is_idle());
                let mut actual = Vec::new();
                let mut reference = Vec::new();
                batched.drain(&mut actual, 1024);
                singles.drain(&mut reference, 1024);
                assert_eq!(actual, reference);
                assert_eq!(batched.queued_msgs.load(Ordering::Relaxed), 0);
                received.extend(actual.into_iter().flat_map(|chunk| chunk.to_vec()));
                start = next;
            }
            assert_eq!(received, data);
        }
    }
}

#[test]
fn fanout_drop_oldest_slot_keeps_newest_messages() {
    let slot = PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            msg_cap: 2,
            ..SlotConfig::default()
        },
    );
    slot.handshake_done.store(true, Ordering::Release);

    let first = Message::single("first");
    let second = Message::single("second");
    let third = Message::single("third");

    let mut eq = FrameBuffer::one_shot();
    let mut chunks = Vec::new();
    for msg in [&first, &second, &third] {
        let frame = build_fan_out_frame(&mut eq, msg, &mut chunks, 1, 8 * 1024);
        assert_eq!(slot.try_push_fanout_drop_oldest(&frame), TryFrameResult::Ok);
        clear_fan_out_frame(&mut eq, &mut chunks);
    }

    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    assert_eq!(actual, vec![fanout_bytes(&second), fanout_bytes(&third)]);
}

#[test]
fn fanout_drop_oldest_slot_keeps_protected_entry() {
    let slot = PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            msg_cap: 1,
            ..SlotConfig::default()
        },
    );
    slot.handshake_done.store(true, Ordering::Release);

    let dict = Message::single("dict");
    let payload = Message::single("payload");
    let mut eq = FrameBuffer::one_shot();
    let mut chunks = Vec::new();
    let dict_frame = build_fan_out_frame(&mut eq, &dict, &mut chunks, 1, 8 * 1024);
    assert_eq!(
        slot.try_push_protected_fanout_drop_oldest(&dict_frame),
        TryFrameResult::Ok
    );
    clear_fan_out_frame(&mut eq, &mut chunks);

    let payload_frame = build_fan_out_frame(&mut eq, &payload, &mut chunks, 1, 8 * 1024);
    assert_eq!(
        slot.try_push_fanout_drop_oldest(&payload_frame),
        TryFrameResult::Full
    );
    clear_fan_out_frame(&mut eq, &mut chunks);

    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    assert_eq!(actual, vec![fanout_bytes(&dict)]);
    assert!(slot.fanout_dict_shipped());
}

#[test]
fn fanout_drop_oldest_shares_large_bodies_and_evicts_whole_messages() {
    let slot = PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            msg_cap: 2,
            ..SlotConfig::default()
        },
    );
    slot.handshake_done.store(true, Ordering::Release);
    let bodies: Vec<Bytes> = (0..3u8)
        .map(|tag| Bytes::from(vec![tag; 64 * 1024]))
        .collect();

    let mut eq = FrameBuffer::one_shot();
    let mut chunks = Vec::new();
    for body in &bodies {
        let msg = Message::single(body.clone());
        let frame = build_fan_out_frame(&mut eq, &msg, &mut chunks, 1, 8 * 1024);
        assert_eq!(slot.try_push_fanout_drop_oldest(&frame), TryFrameResult::Ok);
        clear_fan_out_frame(&mut eq, &mut chunks);
    }
    let mut actual = Vec::new();
    slot.drain(&mut actual, 1024);
    // The first message was evicted whole: header and body.
    let shared: Vec<_> = actual.iter().filter(|chunk| chunk.len() > 1024).collect();
    assert_eq!(shared.len(), 2);
    assert_eq!(shared[0].as_ptr(), bodies[1].as_ptr());
    assert_eq!(shared[1].as_ptr(), bodies[2].as_ptr());
    assert_eq!(actual.len(), 4, "one header chunk per remaining message");
    assert_eq!(
        slot.try_push_fanout_drop_oldest(&FanOutFrame::Arena(b"\x00\x01x")),
        TryFrameResult::Ok
    );
}

#[tokio::test]
async fn transmit_slot_clear_rearms_when_nonempty() {
    let slot = test_slot();
    let msg = Message::from("x");

    assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
    timeout(Duration::from_secs(1), slot.data_signal.ready())
        .await
        .expect("initial encode should notify");

    slot.clear_data_signal_and_rearm();
    timeout(Duration::from_secs(1), slot.data_signal.ready())
        .await
        .expect("nonempty slot should rearm after clear");
}
