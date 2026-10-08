use std::cell::RefCell;
use std::sync::Mutex;

use bytes::Bytes;

use crate::engine::send_pipe::SendPreparation;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::engine::{PeerDriverData, SendPipeError};
use crate::routing::peer_outbound::PeerOutbound;
use crate::transport::inproc::{Admission, InprocSender};
use omq_proto::error::Result;
use omq_proto::fan_out_frame::{
    FanOutFrame, clear_fan_out_frame, encode_fan_out_message, finish_fan_out_frame,
};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;

use super::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};

fn data_inbox(target: &PeerOutbound) -> Option<&crate::engine::data_inbox::Sender> {
    match target {
        PeerOutbound::Wire { inbox, .. } | PeerOutbound::Inbox(inbox) => Some(inbox),
        PeerOutbound::Inproc(_) => None,
    }
}

/// Space held for one fallback peer until the publication commits.
pub(super) enum Reserved<'a> {
    Inbox(crate::engine::data_inbox::Permit<'a>),
    /// Inproc ring with free space. The publish lock keeps other senders
    /// of this socket out until the message is pushed.
    Inproc(&'a InprocSender),
}

impl Reserved<'_> {
    pub(super) fn send(self, msg: Message) {
        match self {
            Self::Inbox(permit) => permit.send(PeerDriverData::SendMessage(msg)),
            Self::Inproc(sender) => {
                let _ = sender.try_send_prepared(msg, SendPreparation::Plain);
            }
        }
    }
}

/// Reserve the entire fallback publication before changing any peer queue.
/// Closed peers are ignored; full live peers must cause try-send to retry.
/// The caller holds the publish lock until every reservation is sent.
pub(super) fn try_reserve_targets(
    targets: &[PeerOutbound],
) -> Option<smallvec::SmallVec<[Reserved<'_>; 8]>> {
    let mut reserved = smallvec::SmallVec::new();
    for target in targets {
        match target {
            PeerOutbound::Inproc(sender) => match sender.admission() {
                Admission::Ready => reserved.push(Reserved::Inproc(sender)),
                Admission::Full => return None,
                Admission::Closed => {}
            },
            PeerOutbound::Wire { inbox, .. } | PeerOutbound::Inbox(inbox) => {
                match inbox.try_reserve() {
                    Ok(permit) => reserved.push(Reserved::Inbox(permit)),
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => return None,
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {}
                }
            }
        }
    }
    Some(reserved)
}

/// Wait until one fallback peer has taken the message or closed.
async fn deliver_blocking(target: &PeerOutbound, mut msg: Message, publish: &Mutex<()>) {
    let PeerOutbound::Inproc(sender) = target else {
        if let Some(inbox) = data_inbox(target) {
            let _ = inbox.send(PeerDriverData::SendMessage(msg)).await;
        }
        return;
    };
    loop {
        let sent = {
            let _publishing = publish.lock().expect("fanout publish poisoned");
            sender.try_send_prepared(msg, SendPreparation::Plain)
        };
        match sent {
            #[cfg(feature = "dart")]
            Err(SendPipeError::Invalid(_)) => unreachable!("inproc has no DART validator"),
            Ok(()) | Err(SendPipeError::Closed(_)) => return,
            Err(SendPipeError::Full(returned)) => msg = returned,
        }
        sender.wait_space().await;
    }
}

/// Each peer waits independently. A stalled peer cannot delay publication
/// to another peer whose inbox has space, including a native wire lane.
pub(super) async fn dispatch_blocking(
    targets: &[PeerOutbound],
    msg: &Message,
    lanes: &super::lane::FanOutLanes,
    publish: &Mutex<()>,
) {
    use futures::{StreamExt, stream::FuturesUnordered};
    let mut pending = FuturesUnordered::new();
    for target in targets {
        pending.push(deliver_blocking(target, msg.clone(), publish));
    }
    let mut budget = omq_proto::flow::DrainBudget::WORKER;
    while !pending.is_empty() {
        tokio::select! {
            biased;
            () = lanes.admission_stopped() => return,
            _ = pending.next() => {}
        }
        if !budget.account(msg.byte_len()) {
            tokio::task::yield_now().await;
            budget.reset();
        }
    }
}

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
            // Inproc rings and inboxes take the message itself, so a set
            // without a wire peer has nothing to encode.
            if targets.iter().any(PeerOutbound::requires_per_peer_encoding)
                || !targets.iter().any(PeerOutbound::is_wire)
            {
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
                let _ = tx.try_send(PeerDriverData::SendMessage(msg.clone()));
            }
            PeerOutbound::Inproc(sender) => {
                let _ = sender.try_send_prepared(msg.clone(), SendPreparation::Plain);
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

    #[tokio::test]
    async fn fanring_publication_reserves_all_clone_lanes_before_committing() {
        use crate::engine::data_inbox::{SenderLanes, channel};
        let scope = SenderLanes::default();
        let (first, mut first_rx) = channel(1);
        let (second, mut second_rx) = channel(1);
        let first = scope.bind(&first);
        let second = scope.bind(&second);
        second
            .try_send(PeerDriverData::SendMessage(Message::single("old")))
            .unwrap();
        let targets = [PeerOutbound::Inbox(first), PeerOutbound::Inbox(second)];
        assert!(try_reserve_targets(&targets).is_none());
        assert!(
            first_rx.is_empty(),
            "failed publication changed an earlier peer"
        );
        assert!(second_rx.recv().await.is_some());
        let reserved = try_reserve_targets(&targets).unwrap();
        for permit in reserved {
            permit.send(Message::single("new"));
        }
        for receiver in [&mut first_rx, &mut second_rx] {
            let PeerDriverData::SendMessage(message) = receiver.recv().await.unwrap() else {
                panic!("message")
            };
            assert_eq!(message.part_slice(0), Some(b"new".as_slice()));
        }
    }

    fn blocking_sender() -> super::super::FanOutSend {
        let options = omq_proto::Options {
            xpub_nodrop: true,
            ..omq_proto::Options::default()
        };
        super::super::FanOutSend::new(
            omq_proto::proto::SocketType::Pub,
            &options,
            super::super::FanOutMode::SubscriptionPrefix,
            &crate::context::IoPoolHandle::none(),
        )
    }

    fn add_inbox_peer(
        sender: &mut super::super::FanOutSend,
        id: u64,
    ) -> tokio::sync::mpsc::Receiver<PeerDriverData> {
        let (inbox, _commands) = tokio::sync::mpsc::channel(1);
        let (data_inbox, data) = tokio::sync::mpsc::channel(1);
        sender.connection_added(
            id,
            crate::engine::ActorPeerDriverHandle {
                inbox: inbox.into(),
                data_inbox: data_inbox.into(),
                cancel: tokio_util::sync::CancellationToken::new(),
                transmit_slot: None,
                direct_tcp_writer: None,
                send_pipe: None,
                inproc: None,
            },
            0,
        );
        assert!(sender.peer_subscribe(id, Bytes::new()).is_none());
        data
    }

    #[tokio::test]
    async fn nodrop_fallback_waits_without_starving_ready_peers_and_close_wakes_it() {
        let mut sender = blocking_sender();
        let mut slow = add_inbox_peer(&mut sender, 0);
        let mut fast = add_inbox_peer(&mut sender, 1);
        sender
            .submitter()
            .send(Message::single("first"))
            .await
            .unwrap();
        let submitter = sender.submitter();
        let blocked = tokio::spawn(async move { submitter.send(Message::single("second")).await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "full peers must backpressure send");
        assert_eq!(recv_inbox_message(&mut fast), Message::single("first"));
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), fast.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(delivered, PeerDriverData::SendMessage(message) if message == Message::single("second"))
        );
        assert!(
            !blocked.is_finished(),
            "slow peer still holds its first message"
        );
        assert_eq!(recv_inbox_message(&mut slow), Message::single("first"));
        blocked.await.unwrap().unwrap();
        assert_eq!(recv_inbox_message(&mut slow), Message::single("second"));

        sender
            .submitter()
            .send(Message::single("third"))
            .await
            .unwrap();
        let submitter = sender.submitter();
        let blocked = tokio::spawn(async move { submitter.send(Message::single("fourth")).await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());
        sender.stop_admission();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(omq_proto::Error::Closed)));
        sender.shutdown();
    }

    #[tokio::test]
    async fn nodrop_try_send_reserves_all_fallbacks_before_publication() {
        let mut sender = blocking_sender();
        let mut first = add_inbox_peer(&mut sender, 0);
        let mut second = add_inbox_peer(&mut sender, 1);
        let submitter = sender.submitter();
        submitter.try_send(Message::single("first")).unwrap();
        assert_eq!(recv_inbox_message(&mut first), Message::single("first"));
        assert!(matches!(
            submitter.try_send(Message::single("second")),
            Err(omq_proto::TrySendError::Full(message)) if message == Message::single("second")
        ));
        assert!(
            first.try_recv().is_err(),
            "no partial publication before retry"
        );
        assert_eq!(recv_inbox_message(&mut second), Message::single("first"));
        submitter.try_send(Message::single("second")).unwrap();
        for receive in [&mut first, &mut second] {
            assert_eq!(recv_inbox_message(receive), Message::single("second"));
            assert!(receive.try_recv().is_err());
        }
        drop(first);
        drop(second);
        submitter.send(Message::single("gone")).await.unwrap();
        sender.shutdown();
    }

    #[test]
    fn drop_oldest_single_target_fallback_keeps_newest_frames() {
        let slot = test_slot_with_msg_cap(2);
        let (inbox, _rx) = tokio::sync::mpsc::channel(1);
        let target = PeerOutbound::Wire {
            slot: slot.clone(),
            inbox: inbox.into(),
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
                inbox: inbox1.into(),
                direct: None,
            },
            PeerOutbound::Wire {
                slot: slot2.clone(),
                inbox: inbox2.into(),
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
            inbox: inbox.into(),
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
            crate::engine::framing::WireFraming::Zmtp,
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
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);
        slot
    }

    fn recv_inbox_message(rx: &mut tokio::sync::mpsc::Receiver<PeerDriverData>) -> Message {
        match rx.try_recv().expect("inbox message") {
            PeerDriverData::SendMessage(msg) => msg,
            other @ PeerDriverData::SendEncoded(_) => panic!("unexpected command {other:?}"),
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
