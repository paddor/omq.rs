use std::sync::Arc;

use bytes::Bytes;

use super::*;
use crate::engine::transmit_slot::SlotConfig;

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
            routes: crate::engine::driver::DirectRoutes::default(),
        },
        0,
    );

    assert!(sender.peer_subscribe(id, Bytes::new()).is_none());
    data
}

#[tokio::test]
async fn nodrop_readiness_wakes_on_fallback_removal_and_shutdown() {
    use futures::FutureExt;

    for shutdown in [false, true] {
        let mut sender = blocking_sender();
        let _data = add_inbox_peer(&mut sender, 1);
        let submitter = sender.submitter();

        submitter.try_send(Message::single("queued")).unwrap();
        assert!(matches!(
            submitter.try_send(Message::single("full")),
            Err(omq_proto::TrySendError::Full(_))
        ));
        assert!(!submitter.send_ready());

        let mut wait = Box::pin(submitter.wait_send_ready());
        assert!(wait.as_mut().now_or_never().is_none());

        if shutdown {
            sender.stop_admission();
        } else {
            sender.connection_removed(1);
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("topology or shutdown did not wake readiness");
        assert!(submitter.send_ready());
        sender.shutdown();
    }
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
        FallbackPublication {
            targets: std::slice::from_ref(&target),
            msg: &Message::single(body),
            policy: FanOutMutePolicy::DropOldest,
        }
        .dispatch(&mut |_| {
            deactivated = true;
        });
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
        FallbackPublication {
            targets: &outbounds,
            msg: &Message::single(body),
            policy: FanOutMutePolicy::DropNewest,
        }
        .dispatch(&mut |target| {
            if let PeerOutbound::Wire { slot, .. } = target {
                deactivated.push(slot.peer_id);
            }
        });
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

    FallbackPublication {
        targets: &targets,
        msg: &msg,
        policy: FanOutMutePolicy::DropNewest,
    }
    .dispatch(&mut |_| {});

    assert!(slot1.is_empty());
    assert!(slot2.is_empty());
    assert_eq!(recv_inbox_message(&mut rx1), msg);
    assert_eq!(recv_inbox_message(&mut rx2), msg);
}

fn test_wire_target(slot: &Arc<crate::engine::transmit_slot::PeerTransmitSlot>) -> PeerOutbound {
    let (inbox, _rx) = tokio::sync::mpsc::channel(1);
    PeerOutbound::Wire {
        slot: slot.clone(),
        inbox: inbox.into(),
        direct: None,
    }
}

fn test_slot_with_msg_cap(msg_cap: usize) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
    let slot = crate::engine::transmit_slot::PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            msg_cap,
            ..SlotConfig::default()
        },
    );
    slot.mark_handshake_done();

    slot
}

fn test_transformed_slot(peer_id: u64) -> Arc<crate::engine::transmit_slot::PeerTransmitSlot> {
    let slot = crate::engine::transmit_slot::PeerTransmitSlot::with_config(
        peer_id,
        SlotConfig {
            has_transform: true,
            msg_cap: 8,
            ..SlotConfig::default()
        },
    );

    slot.mark_handshake_done();
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
    let mut encoder = FanOutEncoder::new();
    let cost = omq_proto::fan_out_frame::FanOutCost {
        targets: target_count,
        copy_budget: 8 * 1024,
    };
    let frame = encoder.build(&msg, cost);

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

    encoder.clear();
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
