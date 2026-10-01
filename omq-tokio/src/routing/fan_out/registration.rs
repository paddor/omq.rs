//! Peer admission, lane compatibility, and capacity reactivation.

use super::lane::LanePeerAdd;
use super::{FanOutPeer, FanOutSend, PeerOutbound, SubscriptionSet};
use crate::engine::PeerDriverHandle;
use rustc_hash::FxHashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl FanOutSend {
    pub(crate) fn connection_added(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, false, io_thread);
    }

    pub(crate) fn connection_added_any_groups(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        io_thread: usize,
    ) {
        self.add_peer(peer_id, handle, true, io_thread);
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_peer(
        &mut self,
        peer_id: u64,
        handle: PeerDriverHandle,
        any_groups: bool,
        io_thread: usize,
    ) {
        let target = PeerOutbound::from_handle(&handle);
        let lane = self.register_lane_peer(peer_id, &target, any_groups, io_thread);
        if lane.is_none() {
            self.fallback_peer_count.fetch_add(1, Ordering::Release);
        } else {
            self.lane_peer_count.fetch_add(1, Ordering::Release);
        }
        self.observe_peer_reactivation(&target);
        let mut inner = self.inner.lock().expect("fanout inner poisoned");
        inner.peers.insert(
            peer_id,
            FanOutPeer {
                subscriptions: SubscriptionSet::new(),
                groups: FxHashSet::default(),
                any_groups,
                target,
                lane,
                fanout_active: true,
            },
        );
        self.bump_generation();
    }

    fn register_lane_peer(
        &self,
        peer_id: u64,
        target: &PeerOutbound,
        any_groups: bool,
        io_thread: usize,
    ) -> Option<usize> {
        let PeerOutbound::Wire { slot, .. } = target else {
            return None;
        };
        #[cfg(feature = "ws")]
        if slot.is_ws() {
            return None;
        }
        self.lanes.add_lane_peer(
            io_thread,
            LanePeerAdd {
                peer_id,
                slot: slot.clone(),
                any_groups,
            },
        )
    }

    fn observe_peer_reactivation(&self, target: &PeerOutbound) {
        let PeerOutbound::Wire { slot, .. } = target else {
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        let generation = self.generation.clone();
        slot.set_fanout_reactivation(Arc::new(move |peer_id| {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let mut inner = inner.lock().expect("fanout inner poisoned");
            if inner.reactivate_fanout_peer(peer_id) {
                drop(inner);
                generation.fetch_add(1, Ordering::Release);
            }
        }));
    }
}

#[cfg(all(test, any(feature = "ws", feature = "lz4", feature = "zstd")))]
mod tests {
    use super::super::FanOutMode;
    use super::*;
    use crate::engine::codec::CodecProfile;
    use crate::engine::framing::WireFraming;
    use crate::engine::transmit_slot::PeerTransmitSlot;
    #[cfg(feature = "ws")]
    use bytes::Bytes;
    #[cfg(any(feature = "ws", feature = "lz4", feature = "zstd"))]
    use omq_proto::Message;
    use omq_proto::proto::transform::CompressionKind;
    use omq_proto::{Options, SocketType};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn peer(
        peer_id: u64,
        framing: WireFraming,
        compression: Option<CompressionKind>,
    ) -> (
        Arc<PeerTransmitSlot>,
        PeerDriverHandle,
        mpsc::Receiver<crate::engine::PeerDriverData>,
    ) {
        peer_with_options(peer_id, framing, compression, &Options::default())
    }

    fn peer_with_options(
        peer_id: u64,
        framing: WireFraming,
        compression: Option<CompressionKind>,
        options: &Options,
    ) -> (
        Arc<PeerTransmitSlot>,
        PeerDriverHandle,
        mpsc::Receiver<crate::engine::PeerDriverData>,
    ) {
        let slot = PeerTransmitSlot::new(
            peer_id,
            compression.is_some(),
            compression.map(|kind| CodecProfile::new(kind, options)),
            None,
            4096,
            16384,
            512 * 1024,
            16,
            framing,
        );
        slot.handshake_done.store(true, Ordering::Release);
        let (inbox, _commands) = mpsc::channel(1);
        let (data_inbox, data) = mpsc::channel(1);
        let handle = PeerDriverHandle {
            inbox,
            data_inbox,
            cancel: CancellationToken::new(),
            transmit_slot: Some(slot.clone()),
            direct_tcp_writer: None,
            send_pipe: None,
        };
        (slot, handle, data)
    }

    fn sender() -> FanOutSend {
        FanOutSend::new(
            SocketType::Pub,
            &Options::default(),
            FanOutMode::SubscriptionPrefix,
            &crate::context::IoPoolHandle::none(),
        )
    }

    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn mixed_wire_registration_preserves_each_format_in_either_order() {
        use omq_proto::proto::connection::WsRole;
        for reverse in [false, true] {
            let mut sender = sender();
            let mut peers: Vec<_> = [
                WireFraming::Zmtp,
                WireFraming::Zws(WsRole::Server),
                WireFraming::Zws(WsRole::Client),
            ]
            .into_iter()
            .enumerate()
            .map(|(index, framing)| {
                let id = u64::try_from(index).unwrap();
                let (slot, handle, data) = peer(id, framing, None);
                (id, framing, slot, handle, data)
            })
            .collect();
            if reverse {
                peers.reverse();
            }
            for (id, _, _, handle, _) in &peers {
                sender.connection_added(*id, handle.clone(), 0);
                if let Some(ack) = sender.peer_subscribe(*id, Bytes::new()) {
                    tokio::time::timeout(std::time::Duration::from_secs(1), ack)
                        .await
                        .unwrap()
                        .unwrap();
                }
            }
            assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 1);
            assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 2);
            sender
                .submitter()
                .try_send(Message::multipart(["topic", "payload"]))
                .unwrap();
            // WS fallback enqueues raw messages for its connection driver.
            // Service that boundary here; the TCP lane frames independently.
            for (_, framing, slot, _, data) in &mut peers {
                if framing.is_ws() {
                    service_fallback_driver(slot, data);
                }
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while peers.iter().any(|(_, _, slot, _, _)| slot.is_empty()) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            for (id, framing, slot, _, _) in peers {
                let mut chunks = Vec::new();
                slot.drain(&mut chunks, 16);
                let wire: Vec<_> = chunks
                    .iter()
                    .flat_map(|chunk| chunk.iter().copied())
                    .collect();
                match framing {
                    WireFraming::Zmtp => assert_eq!(wire, b"\x01\x05topic\x00\x07payload"),
                    WireFraming::Zws(_) => assert_zws_wire(&wire, framing.is_masked()),
                }
                sender.connection_removed(id);
            }
            assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 0);
            assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 0);
            sender.shutdown();
        }
    }

    #[cfg(feature = "ws")]
    fn service_fallback_driver(
        slot: &PeerTransmitSlot,
        data: &mut mpsc::Receiver<crate::engine::PeerDriverData>,
    ) {
        assert!(slot.is_empty());
        let crate::engine::PeerDriverData::SendMessage(message) = data.try_recv().unwrap() else {
            panic!("WS fallback received bytes already framed for another peer");
        };
        assert_eq!(message, Message::multipart(["topic", "payload"]));
        assert_eq!(
            slot.try_encode(&message),
            crate::engine::transmit_slot::TryFrameResult::Ok
        );
    }

    #[cfg(feature = "ws")]
    fn assert_zws_wire(wire: &[u8], masked: bool) {
        let mut offset = 0;
        for payload in [b"\x01topic".as_slice(), b"\x00payload".as_slice()] {
            assert_eq!(wire[offset], 0x82);
            assert_eq!(
                wire[offset + 1],
                u8::try_from(payload.len()).unwrap() | if masked { 0x80 } else { 0 }
            );
            let header = if masked { 6 } else { 2 };
            let mut actual = wire[offset + header..offset + header + payload.len()].to_vec();
            if masked {
                for (index, byte) in actual.iter_mut().enumerate() {
                    *byte ^= wire[offset + 2 + index % 4];
                }
            }
            assert_eq!(actual, payload);
            offset += header + payload.len();
        }
        assert_eq!(offset, wire.len());
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn codec_kind() -> CompressionKind {
        #[cfg(feature = "lz4")]
        {
            CompressionKind::Lz4
        }
        #[cfg(all(not(feature = "lz4"), feature = "zstd"))]
        {
            CompressionKind::Zstd
        }
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    async fn wait_for_wire(slot: &PeerTransmitSlot) -> Vec<u8> {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while slot.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut chunks = Vec::new();
        slot.drain(&mut chunks, 1024);
        chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect()
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[tokio::test]
    async fn materialized_codec_settings_control_each_groups_wire_bytes() {
        let kind = codec_kind();
        let mut sender = sender();
        let mut peers = Vec::new();
        for (id, threshold) in [64, 8192].into_iter().enumerate() {
            let options = Options::default().compression_threshold(threshold);
            let (slot, handle, data) =
                peer_with_options(id as u64, WireFraming::Zmtp, Some(kind), &options);
            sender.connection_added(id as u64, handle, 0);
            sender
                .peer_subscribe(id as u64, bytes::Bytes::new())
                .unwrap()
                .await
                .unwrap();
            peers.push((slot, data, options));
        }
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 2);
        assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 0);
        let message = Message::single(bytes::Bytes::from(vec![0x5a; 256]));
        sender.submitter().try_send(message.clone()).unwrap();
        for (slot, mut data, options) in peers {
            let actual = wait_for_wire(&slot).await;
            assert!(
                data.try_recv().is_err(),
                "grouped peers must bypass per-peer encoding"
            );
            let (mut encoder, _) =
                omq_proto::proto::transform::MessageEncoder::for_compression_kind(kind, &options)
                    .unwrap()
                    .unwrap();
            let mut expected = omq_proto::frame_buffer::FrameBuffer::one_shot();
            for wire_message in encoder.encode(&message).unwrap() {
                expected.frame(&wire_message);
            }
            let mut chunks = Vec::new();
            expected.drain(&mut chunks, 1024);
            let expected: Vec<u8> = chunks
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect();
            assert_eq!(actual, expected, "use materialized connection options");
        }
        sender.shutdown();
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[tokio::test]
    async fn equivalent_configurations_do_not_exhaust_codec_group_capacity() {
        let kind = codec_kind();
        let mut sender = sender();
        let mut peers = Vec::new();
        let peer_count = super::super::codec_group::MAX_CODEC_GROUPS + 3;
        for id in 0..peer_count {
            let mut options = Options::default().compression_dict_capacity(100 + id);
            options.max_recv_dict_size = Some(id + 1);
            options.compression_threshold = (id % 2 == 0).then_some(512);
            options.compression_level = match id % 3 {
                0 => None,
                1 => Some(0),
                _ => Some(1),
            };
            let (slot, handle, data) =
                peer_with_options(id as u64, WireFraming::Zmtp, Some(kind), &options);
            sender.connection_added(id as u64, handle, 0);
            sender
                .peer_subscribe(id as u64, bytes::Bytes::new())
                .expect("equivalent profiles must remain eligible after eight peers")
                .await
                .unwrap();
            peers.push((slot, data));
        }
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), peer_count);
        assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 0);
        let message = Message::single(bytes::Bytes::from(vec![0x5a; 1024]));
        sender.submitter().try_send(message).unwrap();
        let expected = wait_for_wire(&peers[0].0).await;
        for (id, (slot, mut data)) in peers.into_iter().enumerate() {
            if id != 0 {
                assert_eq!(wait_for_wire(&slot).await, expected);
            }
            assert!(data.try_recv().is_err(), "must retain grouped encoding");
        }
        sender.shutdown();
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[tokio::test]
    async fn retired_codec_group_can_be_reused_by_a_plain_peer() {
        let mut sender = sender();
        let (_, handle, _old_data) = peer(0, WireFraming::Zmtp, Some(codec_kind()));
        sender.connection_added(0, handle, 0);
        sender
            .peer_subscribe(0, bytes::Bytes::new())
            .unwrap()
            .await
            .unwrap();
        sender.connection_removed(0);
        let (slot, handle, mut data) = peer(1, WireFraming::Zmtp, None);
        sender.connection_added(1, handle, 0);
        sender
            .peer_subscribe(1, bytes::Bytes::new())
            .unwrap()
            .await
            .unwrap();
        sender
            .submitter()
            .try_send(Message::single("plain"))
            .unwrap();
        assert_eq!(wait_for_wire(&slot).await, b"\x00\x05plain");
        assert!(data.try_recv().is_err());
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 1);
        assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 0);
        sender.shutdown();
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    #[tokio::test]
    async fn codec_group_limit_keeps_reliable_fallback_and_reuses_retired_slots() {
        let mut sender = sender();
        let mut peers = Vec::new();
        for id in 0..=super::super::codec_group::MAX_CODEC_GROUPS {
            let options = Options::default().compression_threshold(id + 1);
            let (slot, handle, data) =
                peer_with_options(id as u64, WireFraming::Zmtp, Some(codec_kind()), &options);
            sender.connection_added(id as u64, handle, 0);
            let ack = sender.peer_subscribe(id as u64, bytes::Bytes::new());
            assert_eq!(
                ack.is_some(),
                id < super::super::codec_group::MAX_CODEC_GROUPS
            );
            if let Some(ack) = ack {
                ack.await.unwrap();
            }
            peers.push((slot, data));
        }
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 8);
        assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 1);
        let message = Message::single("payload");
        sender.submitter().try_send(message.clone()).unwrap();
        let crate::engine::PeerDriverData::SendMessage(raw) = peers[8].1.try_recv().unwrap() else {
            panic!("overflow profile received another group's wire bytes");
        };
        assert_eq!(raw, message);
        sender.connection_removed(0);
        let options = Options::default().compression_threshold(9);
        let (_, handle, _data) =
            peer_with_options(9, WireFraming::Zmtp, Some(codec_kind()), &options);
        sender.connection_added(9, handle, 0);
        sender
            .peer_subscribe(9, bytes::Bytes::new())
            .unwrap()
            .await
            .unwrap();
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 8);
        for id in 1..=9 {
            sender.connection_removed(id);
        }
        assert_eq!(sender.lane_peer_count.load(Ordering::Acquire), 0);
        assert_eq!(sender.fallback_peer_count.load(Ordering::Acquire), 0);
        sender.shutdown();
    }
}
