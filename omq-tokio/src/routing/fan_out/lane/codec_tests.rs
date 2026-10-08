use super::*;
use crate::engine::codec::CodecProfile;
use crate::engine::framing::WireFraming;
use omq_proto::proto::transform::{CompressionKind, MessageDecoder, MessageEncoder};

fn kinds() -> Vec<Option<CompressionKind>> {
    vec![
        None,
        #[cfg(feature = "lz4")]
        Some(CompressionKind::Lz4),
        #[cfg(feature = "zstd")]
        Some(CompressionKind::Zstd),
    ]
}

fn worker(
    mode: FanOutMode,
    mute_policy: FanOutMutePolicy,
) -> (LaneWorker, yring::Producer<LaneControl>) {
    let (_data_tx, data_rx) = yring::spsc(4);
    let (ctrl_tx, ctrl_rx) = yring::spsc(LANE_CTRL_RING_CAP);
    (
        LaneWorker {
            data_rx,
            ctrl_rx,
            data_signal: Arc::new(DataSignal::new()),
            data_space: Arc::new(StateSignal::new()),
            ctrl_notify: Arc::new(DataSignal::new()),
            mode,
            mute_policy,
            peers: FxHashMap::default(),
            subscribe_all_count: 0,
            eq: FrameBuffer::one_shot(),
            chunks: Vec::new(),
            codec_groups: std::array::from_fn(|_| None),
            distribution_targets: Vec::new(),
            active_flags: None,
            exited: Arc::new(AtomicBool::new(false)),
        },
        ctrl_tx,
    )
}

fn add_peer(
    worker: &mut LaneWorker,
    id: u64,
    group: usize,
    kind: Option<CompressionKind>,
    options: &Options,
    cap: usize,
) -> Arc<PeerTransmitSlot> {
    let slot = PeerTransmitSlot::new(
        id,
        kind.is_some(),
        kind.map(|kind| CodecProfile::new(kind, options)),
        None,
        4096,
        16384,
        512 * 1024,
        cap,
        WireFraming::Zmtp,
    );
    worker.handle_control(LaneControl::AddPeer {
        add: LanePeerAdd {
            peer_id: id,
            slot: slot.clone(),
            any_groups: false,
        },
        codec_group: group,
    });
    worker.handle_control(LaneControl::Subscribe {
        peer_id: id,
        prefix: Bytes::from_static(b"topic"),
        ack: None,
    });
    worker.handle_control(LaneControl::Join {
        peer_id: id,
        group: Bytes::from_static(b"topic"),
    });
    slot
}

fn dispatch(body: &[u8]) -> LaneDispatch {
    LaneDispatch {
        msg: Message::multipart([Bytes::from_static(b"topic"), Bytes::copy_from_slice(body)]),
        topic: Bytes::from_static(b"topic"),
    }
}

fn decoder(kind: Option<CompressionKind>, options: &Options) -> Option<MessageDecoder> {
    kind.map(|kind| {
        MessageEncoder::for_compression_kind(kind, options)
            .unwrap()
            .unwrap()
            .1
    })
}

/// Decode using the production ZMTP frame decoder and production transforms.
fn receive(slot: &PeerTransmitSlot, decoder: &mut Option<MessageDecoder>) -> (Vec<Message>, usize) {
    let mut chunks = Vec::new();
    slot.drain(&mut chunks, usize::MAX);
    let mut input = Bytes::from(
        chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>(),
    );
    let mut messages = Vec::new();
    let mut parts = Vec::new();
    let mut dictionaries = 0;
    while !input.is_empty() {
        let (frame, remaining) =
            omq_proto::proto::frame::decode_frame_from_bytes(input.clone()).unwrap();
        let frame = frame.expect("complete fan-out frame");
        assert!(!frame.flags.command);
        input = input.slice(input.len() - remaining..);
        parts.push(frame.payload.as_bytes());
        if !frame.flags.more {
            let wire = Message::multipart(parts.drain(..));
            let mut transformed = smallvec::smallvec![wire.clone()];
            if MessageEncoder::take_leading_dict_shipment(&mut transformed).is_some() {
                dictionaries += 1;
            }
            let plain = if let Some(decoder) = decoder {
                decoder.decode(wire).unwrap()
            } else {
                Some(wire)
            };
            if let Some(plain) = plain {
                messages.push(plain);
            }
        }
    }
    assert!(parts.is_empty(), "multipart publication was truncated");
    (messages, dictionaries)
}

#[tokio::test]
async fn encodes_once_per_matched_group_per_lane_in_both_registration_orders() {
    for mode in [FanOutMode::SubscriptionPrefix, FanOutMode::Group] {
        for policy in [
            FanOutMutePolicy::Block,
            FanOutMutePolicy::DropNewest,
            FanOutMutePolicy::DropOldest,
        ] {
            for reverse in [false, true] {
                for lane_count in [1, 2] {
                    let options = Options::default();
                    let kinds = kinds();
                    for _ in 0..lane_count {
                        let (mut worker, _ctrl) = worker(mode, policy);
                        let mut order: Vec<_> = (0..kinds.len() * 9).collect();
                        if reverse {
                            order.reverse();
                        }
                        let mut peers = Vec::new();
                        for index in order {
                            let group = index % kinds.len();
                            let slot = add_peer(
                                &mut worker,
                                index as u64,
                                group,
                                kinds[group],
                                &options,
                                16,
                            );
                            peers.push((slot, decoder(kinds[group], &options)));
                        }
                        let publication = dispatch(&[0x5a; 4096]);
                        assert!(!worker.dispatch(&publication, &mut SmallVec::new()).await);
                        for (slot, decoder) in &mut peers {
                            assert_eq!(receive(slot, decoder), (vec![publication.msg.clone()], 0));
                        }
                        let unmatched = LaneDispatch {
                            topic: Bytes::from_static(b"other"),
                            ..publication
                        };
                        assert!(!worker.dispatch(&unmatched, &mut SmallVec::new()).await);
                        for group in worker.codec_groups.iter().flatten() {
                            assert_eq!(group.encode_count, 1);
                        }
                        assert!(peers.iter().all(|(slot, _)| slot.is_empty()));
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn trained_dictionary_precedes_payload_once_and_is_sent_to_late_joins() {
    for kind in kinds().into_iter().flatten() {
        for policy in [
            FanOutMutePolicy::Block,
            FanOutMutePolicy::DropNewest,
            FanOutMutePolicy::DropOldest,
        ] {
            let options = Options::default().compression_auto_train(true);
            let (mut worker, _ctrl) = worker(FanOutMode::SubscriptionPrefix, policy);
            let mut peers: Vec<_> = (0..2)
                .map(|id| {
                    let slot = add_peer(&mut worker, id, 0, Some(kind), &options, 16);
                    (slot, decoder(Some(kind), &options))
                })
                .collect();
            for index in 0..101 {
                let body = format!(
                    "{{\"topic\":\"orders\",\"id\":{index},\"price\":1234,\"quantity\":17}}"
                )
                .repeat(4);
                let publication = dispatch(body.as_bytes());
                assert!(!worker.dispatch(&publication, &mut SmallVec::new()).await);
                for (slot, decoder) in &mut peers {
                    assert_eq!(
                        receive(slot, decoder),
                        (vec![publication.msg.clone()], usize::from(index == 100))
                    );
                }
            }
            let slot = add_peer(&mut worker, 2, 0, Some(kind), &options, 16);
            peers.push((slot, decoder(Some(kind), &options)));
            let publication = dispatch(b"orders orders orders orders orders orders orders orders");
            assert!(!worker.dispatch(&publication, &mut SmallVec::new()).await);
            for (index, (slot, decoder)) in peers.iter_mut().enumerate() {
                assert_eq!(
                    receive(slot, decoder),
                    (vec![publication.msg.clone()], usize::from(index == 2))
                );
            }
            assert_eq!(worker.codec_groups[0].as_ref().unwrap().encode_count, 102);
        }
    }
}

#[tokio::test]
async fn nodrop_services_ready_groups_and_removal_discards_only_old_connection_output() {
    use futures::FutureExt;
    let options = Options::default();
    let kind = kinds().into_iter().flatten().next().unwrap();
    let (mut worker, mut ctrl) = worker(FanOutMode::SubscriptionPrefix, FanOutMutePolicy::Block);
    let slow = add_peer(&mut worker, 0, 0, None, &options, 1);
    let fast = add_peer(&mut worker, 1, 0, None, &options, 16);
    let other = add_peer(&mut worker, 2, 1, Some(kind), &options, 16);
    assert_eq!(
        slow.try_push_pre_framed_no_signal(b"\x00\x03old"),
        TryFrameResult::Ok
    );
    let publication = dispatch(&[0x5a; 4096]);
    let notify = worker.ctrl_notify.clone();
    let mut touched = SmallVec::new();
    let send = worker.dispatch(&publication, &mut touched);
    tokio::pin!(send);
    assert!(send.as_mut().now_or_never().is_none());
    assert_eq!(
        receive(&fast, &mut None),
        (vec![publication.msg.clone()], 0)
    );
    assert_eq!(
        receive(&other, &mut decoder(Some(kind), &options)),
        (vec![publication.msg.clone()], 0)
    );
    let replacement = PeerTransmitSlot::new(
        0,
        false,
        None,
        None,
        4096,
        16384,
        512 * 1024,
        16,
        WireFraming::Zmtp,
    );
    ctrl.push(LaneControl::RemovePeer { peer_id: 0 }).unwrap();
    ctrl.push(LaneControl::AddPeer {
        add: LanePeerAdd {
            peer_id: 0,
            slot: replacement.clone(),
            any_groups: false,
        },
        codec_group: 0,
    })
    .unwrap();
    ctrl.flush();
    notify.mark();
    assert!(
        !tokio::time::timeout(std::time::Duration::from_secs(1), send)
            .await
            .unwrap()
    );
    assert_eq!(receive(&slow, &mut None), (vec![Message::single("old")], 0));
    assert!(
        replacement.is_empty(),
        "replacement connection inherited an old blocked publication"
    );
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn shutdown_remains_reachable_while_a_dictionary_cannot_be_admitted() {
    use futures::FutureExt;
    let options = Options::default().compression_dict(Bytes::from_static(b"shared-dictionary"));
    let (mut worker, mut ctrl) = worker(FanOutMode::SubscriptionPrefix, FanOutMutePolicy::Block);
    let slow = add_peer(&mut worker, 0, 0, Some(CompressionKind::Lz4), &options, 1);
    let fast = add_peer(&mut worker, 1, 1, None, &Options::default(), 16);
    slow.try_push_pre_framed_no_signal(b"\x00\x03old");
    let notify = worker.ctrl_notify.clone();
    let publication = dispatch(&[0x5a; 4096]);
    let mut touched = SmallVec::new();
    let send = worker.dispatch(&publication, &mut touched);
    tokio::pin!(send);
    assert!(send.as_mut().now_or_never().is_none());
    assert_eq!(
        receive(&fast, &mut None),
        (vec![publication.msg.clone()], 0)
    );
    assert!(!slow.fanout_dict_shipped());
    ctrl.push(LaneControl::Shutdown).unwrap();
    ctrl.flush();
    notify.mark();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), send)
            .await
            .unwrap()
    );
    assert!(!slow.fanout_dict_shipped());
}

#[tokio::test]
async fn grouped_large_multipart_remains_atomic_with_all_mute_policies() {
    for policy in [
        FanOutMutePolicy::Block,
        FanOutMutePolicy::DropNewest,
        FanOutMutePolicy::DropOldest,
    ] {
        for parts in [600, 1025] {
            let options = Options::default();
            let (mut worker, _ctrl) = worker(FanOutMode::SubscriptionPrefix, policy);
            let mut peers = Vec::new();
            for (group, kind) in kinds().into_iter().enumerate() {
                for copy in 0..2 {
                    let slot = add_peer(
                        &mut worker,
                        (group * 2 + copy) as u64,
                        group,
                        kind,
                        &options,
                        16,
                    );
                    peers.push((slot, decoder(kind, &options)));
                }
            }
            let publication = LaneDispatch {
                msg: Message::multipart(
                    std::iter::once(Bytes::from_static(b"topic"))
                        .chain((0..parts).map(|_| Bytes::from_static(&[0x5a; 1024]))),
                ),
                topic: Bytes::from_static(b"topic"),
            };
            assert!(!worker.dispatch(&publication, &mut SmallVec::new()).await);
            for (slot, decoder) in &mut peers {
                assert_eq!(receive(slot, decoder), (vec![publication.msg.clone()], 0));
            }
        }
    }
}
