use std::cell::RefCell;

use bytes::Bytes;

use crate::engine::PeerDriverCommand;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::error::Result;
use omq_proto::fan_out_frame::{
    FanOutFrame, clear_fan_out_frame, encode_fan_out_message, finish_fan_out_frame,
};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;

use super::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};

pub(super) fn dispatch_to_targets(
    targets: &[PeerOutbound],
    msg: &Message,
    mute_policy: FanOutMutePolicy,
    deactivate: &mut impl FnMut(&PeerOutbound),
) -> Result<()> {
    match targets.len() {
        0 => Ok(()),
        1 if mute_policy != FanOutMutePolicy::DropOldest => match targets[0].try_encode(msg) {
            TryFrameResult::Full => {
                if mute_policy == FanOutMutePolicy::DropNewest {
                    deactivate(&targets[0]);
                }
                Ok(())
            }
            _ => Ok(()),
        },
        _ => {
            if targets.iter().any(PeerOutbound::requires_per_peer_encoding) {
                for t in targets {
                    if t.try_encode(msg) == TryFrameResult::Full
                        && mute_policy == FanOutMutePolicy::DropNewest
                    {
                        deactivate(t);
                    }
                }
                return Ok(());
            }

            #[cfg(feature = "ws")]
            if targets.iter().any(PeerOutbound::is_ws) {
                for t in targets {
                    if t.try_encode(msg) == TryFrameResult::Full
                        && mute_policy == FanOutMutePolicy::DropNewest
                    {
                        deactivate(t);
                    }
                }
                return Ok(());
            }

            thread_local! {
                static ARENA: RefCell<FrameBuffer> = RefCell::new(
                    FrameBuffer::one_shot(),
                );
                static CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
            }
            ARENA.with(|cell| {
                let eq = &mut *cell.borrow_mut();
                encode_fan_out_message(eq, msg, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET);
                CHUNKS.with(|drain| {
                    dispatch_encoded(
                        eq,
                        targets,
                        msg,
                        &mut drain.borrow_mut(),
                        mute_policy,
                        deactivate,
                    );
                    Ok(())
                })
            })
        }
    }
}

fn push_to_peers(
    targets: &[PeerOutbound],
    msg: &Message,
    mute_policy: FanOutMutePolicy,
    deactivate: &mut impl FnMut(&PeerOutbound),
    push_wire: impl Fn(&PeerTransmitSlot, FanOutMutePolicy) -> TryFrameResult,
) {
    for t in targets {
        match t {
            PeerOutbound::Wire { slot, .. } => {
                if mute_policy == FanOutMutePolicy::DropNewest && !slot.fanout_active() {
                    continue;
                }
                if push_wire(slot, mute_policy) == TryFrameResult::Full
                    && mute_policy == FanOutMutePolicy::DropNewest
                {
                    deactivate(t);
                }
            }
            PeerOutbound::Inbox(tx) => {
                let _ = tx.try_send(PeerDriverCommand::SendMessage(msg.clone()));
            }
        }
    }
}

fn dispatch_encoded(
    eq: &mut FrameBuffer,
    targets: &[PeerOutbound],
    msg: &Message,
    chunks: &mut Vec<Bytes>,
    mute_policy: FanOutMutePolicy,
    deactivate: &mut impl FnMut(&PeerOutbound),
) {
    match finish_fan_out_frame(eq, chunks, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET) {
        FanOutFrame::Arena(raw) => {
            let frame = FanOutFrame::Arena(raw);
            push_to_peers(
                targets,
                msg,
                mute_policy,
                deactivate,
                |slot, policy| match policy {
                    FanOutMutePolicy::DropOldest => slot.try_push_fanout_drop_oldest(&frame),
                    FanOutMutePolicy::DropNewest | FanOutMutePolicy::Block => {
                        slot.try_push_pre_framed_no_signal(raw)
                    }
                },
            );
            for t in targets {
                if let PeerOutbound::Wire { slot, .. } = t {
                    slot.signal_encoded();
                }
            }
        }
        FanOutFrame::Chunks(encoded) => {
            let frame = FanOutFrame::Chunks(encoded);
            push_to_peers(
                targets,
                msg,
                mute_policy,
                deactivate,
                |slot, policy| match policy {
                    FanOutMutePolicy::DropOldest => slot.try_push_fanout_drop_oldest(&frame),
                    FanOutMutePolicy::DropNewest | FanOutMutePolicy::Block => {
                        slot.try_push_encoded(encoded)
                    }
                },
            );
        }
    }
    clear_fan_out_frame(eq, chunks);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};

    use super::*;

    #[test]
    fn drop_oldest_single_target_fallback_keeps_newest_frames() {
        let slot = test_slot_with_msg_cap(2);
        let (inbox, _rx) = tokio::sync::mpsc::channel(1);
        let target = PeerOutbound::Wire {
            slot: slot.clone(),
            inbox,
            direct: None,
        };
        let mut deactivated = false;

        for body in ["first", "second", "third"] {
            dispatch_to_targets(
                std::slice::from_ref(&target),
                &Message::single(body),
                FanOutMutePolicy::DropOldest,
                &mut |_| {
                    deactivated = true;
                },
            )
            .unwrap();
        }

        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(
            actual,
            vec![encoded_message("second"), encoded_message("third")]
        );
        assert!(!deactivated);
    }

    #[test]
    fn drop_newest_multi_target_fallback_keeps_oldest_frames() {
        let slot1 = test_slot_with_msg_cap(2);
        let slot2 = test_slot_with_msg_cap(2);
        let outbound_a = test_wire_target(&slot1);
        let outbound_b = test_wire_target(&slot2);
        let outbounds = [outbound_a, outbound_b];
        let mut deactivated = Vec::new();

        for body in ["first", "second", "third"] {
            dispatch_to_targets(
                &outbounds,
                &Message::single(body),
                FanOutMutePolicy::DropNewest,
                &mut |target| {
                    if let PeerOutbound::Wire { slot, .. } = target {
                        deactivated.push(slot.peer_id);
                    }
                },
            )
            .unwrap();
        }

        for slot in [&slot1, &slot2] {
            let mut actual = Vec::new();
            slot.drain(&mut actual, 1024);
            assert_eq!(
                actual,
                vec![encoded_messages_for_targets(
                    &["first", "second"],
                    outbounds.len()
                )]
            );
        }
        assert_eq!(deactivated, vec![1, 1]);
    }

    #[test]
    fn transformed_multi_target_fallback_uses_per_peer_encoding() {
        let slot1 = test_transformed_slot(11);
        let slot2 = test_transformed_slot(12);
        let (inbox1, mut rx1) = tokio::sync::mpsc::channel(1);
        let (inbox2, mut rx2) = tokio::sync::mpsc::channel(1);
        let msg = Message::single("payload");
        let targets = [
            PeerOutbound::Wire {
                slot: slot1.clone(),
                inbox: inbox1,
                direct: None,
            },
            PeerOutbound::Wire {
                slot: slot2.clone(),
                inbox: inbox2,
                direct: None,
            },
        ];

        dispatch_to_targets(&targets, &msg, FanOutMutePolicy::DropNewest, &mut |_| {}).unwrap();

        assert!(slot1.is_empty());
        assert!(slot2.is_empty());
        assert_eq!(recv_inbox_message(&mut rx1), msg);
        assert_eq!(recv_inbox_message(&mut rx2), msg);
    }

    fn test_wire_target(
        slot: &Arc<crate::engine::transmit_slot::PeerTransmitSlot>,
    ) -> PeerOutbound {
        let (inbox, _rx) = tokio::sync::mpsc::channel(1);
        PeerOutbound::Wire {
            slot: slot.clone(),
            inbox,
            direct: None,
        }
    }

    fn test_slot_with_msg_cap(
        msg_cap: usize,
    ) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
        let slot = crate::engine::transmit_slot::PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            crate::engine::transmit_slot::TRANSMIT_SLOT_CAP_DEFAULT,
            msg_cap,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        slot.handshake_done.store(true, Ordering::Release);
        slot
    }

    fn test_transformed_slot(peer_id: u64) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
        let slot = crate::engine::transmit_slot::PeerTransmitSlot::new(
            peer_id,
            true,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            crate::engine::transmit_slot::TRANSMIT_SLOT_CAP_DEFAULT,
            8,
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        slot.handshake_done.store(true, Ordering::Release);
        slot
    }

    fn recv_inbox_message(rx: &mut tokio::sync::mpsc::Receiver<PeerDriverCommand>) -> Message {
        match rx.try_recv().expect("inbox message") {
            PeerDriverCommand::SendMessage(msg) => msg,
            other => panic!("unexpected command {other:?}"),
        }
    }

    fn encoded_message(body: &str) -> Bytes {
        encoded_message_for_targets(body, 1)
    }

    fn encoded_message_for_targets(body: &str, target_count: usize) -> Bytes {
        let msg = Message::single(body.to_owned());
        let mut eq = FrameBuffer::one_shot();
        let mut chunks = Vec::new();
        let frame = build_fan_out_frame(&mut eq, &msg, &mut chunks, target_count, 8 * 1024);
        let bytes = match &frame {
            FanOutFrame::Arena(raw) => Bytes::copy_from_slice(raw),
            FanOutFrame::Chunks(chunks) if chunks.len() == 1 => chunks[0].clone(),
            FanOutFrame::Chunks(chunks) => {
                let len = chunks.iter().map(Bytes::len).sum();
                let mut buf = bytes::BytesMut::with_capacity(len);
                for chunk in *chunks {
                    buf.extend_from_slice(chunk);
                }
                buf.freeze()
            }
        };
        clear_fan_out_frame(&mut eq, &mut chunks);
        bytes
    }

    fn encoded_messages_for_targets(bodies: &[&str], target_count: usize) -> Bytes {
        let mut bytes = bytes::BytesMut::new();
        for body in bodies {
            let encoded = encoded_message_for_targets(body, target_count);
            bytes.extend_from_slice(&encoded);
        }
        bytes.freeze()
    }
}
