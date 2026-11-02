//! Publication dispatch under a lossy mute policy.

use super::{GroupPeers, LanePeer, LaneWorker, frame_publication};
use crate::engine::transmit_slot::TryFrameResult;
use crate::routing::fan_out::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};
use omq_proto::fan_out_frame::{FanOutFrame, build_fan_out_frame, clear_fan_out_frame};
use omq_proto::message::Message;
use smallvec::SmallVec;

impl LaneWorker {
    #[cfg(test)]
    pub(super) fn dispatch_lossy(&mut self, msg: &Message, touched: &mut SmallVec<[u64; 32]>) {
        for (group, peers) in self.matching_peer_groups(msg).iter().enumerate() {
            self.dispatch_group_lossy(GroupPeers { group, peers }, msg, touched);
        }
    }

    /// Frame one publication for a codec group under a lossy mute policy.
    /// Full peers drop it instead of waiting.
    pub(super) fn dispatch_group_lossy(
        &mut self,
        target: GroupPeers<'_>,
        msg: &Message,
        touched: &mut SmallVec<[u64; 32]>,
    ) {
        if target.peers.is_empty() {
            return;
        }
        if self.is_shm_group(target.group) {
            self.dispatch_shm_group(target.peers, msg, touched, None);
            return;
        }

        let group = self.codec_groups[target.group]
            .as_mut()
            .expect("matched codec group");
        let Ok(wire_messages) = group.encode(msg) else {
            return;
        };
        let dict_msg = group.dictionary().cloned();

        // A payload is only useful to peers whose dictionary already went out.
        let payload_peer_ids = match dict_msg.as_ref() {
            Some(dict) => self.ship_dictionary_lossy(dict, target.peers, touched),
            None => SmallVec::<[u64; 32]>::from_slice(target.peers),
        };
        if payload_peer_ids.is_empty() {
            return;
        }

        let encoded = frame_publication(
            &mut self.eq,
            &mut self.chunks,
            &wire_messages,
            payload_peer_ids.len(),
        );
        let lossy = LossyPush {
            policy: self.mute_policy,
            protected: false,
        };
        for peer_id in payload_peer_ids {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                lossy.push(peer_id, peer, &encoded, touched);
            }
        }
        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    /// Queue the group dictionary where it is not queued or shipped yet.
    /// Returns the peers that now have it ahead of the payload.
    fn ship_dictionary_lossy(
        &mut self,
        dict: &Message,
        peer_ids: &[u64],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> SmallVec<[u64; 32]> {
        let mut payload_peer_ids = SmallVec::new();
        let encoded = build_fan_out_frame(
            &mut self.eq,
            dict,
            &mut self.chunks,
            peer_ids.len(),
            FAN_OUT_TOTAL_COPY_BUDGET,
        );

        let lossy = LossyPush {
            policy: self.mute_policy,
            protected: true,
        };
        for &peer_id in peer_ids {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            let dict_ready = peer.slot.fanout_dict_queued_or_shipped()
                || lossy.push(peer_id, peer, &encoded, touched);
            if dict_ready {
                payload_peer_ids.push(peer_id);
            }
        }
        clear_fan_out_frame(&mut self.eq, &mut self.chunks);

        payload_peer_ids
    }
}

/// How a lossy push treats a full peer. A protected frame is a dictionary
/// that `DropOldest` eviction must keep.
#[derive(Clone, Copy)]
pub(super) struct LossyPush {
    policy: FanOutMutePolicy,
    protected: bool,
}

impl LossyPush {
    /// Returns whether the peer took the frame.
    fn push(
        self,
        peer_id: u64,
        peer: &mut LanePeer,
        frame: &FanOutFrame<'_>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let result = match self.policy {
            FanOutMutePolicy::DropOldest if self.protected => {
                peer.slot.try_push_protected_fanout_drop_oldest(frame)
            }
            FanOutMutePolicy::DropOldest => peer.slot.try_push_fanout_drop_oldest(frame),
            FanOutMutePolicy::DropNewest | FanOutMutePolicy::Block => {
                LaneWorker::try_push_frame(&peer.slot, frame)
            }
        };

        match result {
            TryFrameResult::Ok => {
                // Under DropOldest the slot marks a protected dictionary shipped
                // once it drains it. Here it is only queued.
                if self.protected && self.policy != FanOutMutePolicy::DropOldest {
                    peer.mark_dict_shipped();
                }
                LaneWorker::touch_peer(peer_id, peer, touched);
                true
            }
            TryFrameResult::Dead | TryFrameResult::Ineligible => false,
            TryFrameResult::Full => {
                if self.policy == FanOutMutePolicy::DropNewest {
                    peer.slot.deactivate_fanout();
                }
                false
            }
        }
    }
}
