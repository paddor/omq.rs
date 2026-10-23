//! Publication dispatch under a blocking mute policy.

use super::{GroupPeers, LaneDispatch, LaneWorker, PendingPeer, PreparedFrame, frame_publication};
use crate::engine::transmit_slot::TryFrameResult;
use crate::routing::fan_out::FAN_OUT_TOTAL_COPY_BUDGET;
use futures::{StreamExt, stream::FuturesUnordered};
use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
use omq_proto::message::Message;
use smallvec::SmallVec;
use std::sync::Arc;

impl LaneWorker {
    /// Frame one publication for a codec group under a blocking mute policy.
    /// Peers without room join `pending` with a shared owned copy.
    pub(super) fn dispatch_group_ready(
        &mut self,
        target: GroupPeers<'_>,
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
        pending: &mut SmallVec<[PendingPeer; 8]>,
    ) {
        if target.peers.is_empty() {
            return;
        }
        if self.is_shm_group(target.group) {
            self.dispatch_shm_group(target.peers, dispatch, touched, Some(pending));
            return;
        }

        let group = self.codec_groups[target.group]
            .as_mut()
            .expect("matched codec group");
        let Ok(wire_messages) = group.encode(&dispatch.msg) else {
            return;
        };
        let dictionary = group.dictionary().cloned();
        let owned_dict = dictionary
            .as_ref()
            .and_then(|dict| self.ship_dictionary_ready(dict, target.peers, touched));

        let frame = frame_publication(
            &mut self.eq,
            &mut self.chunks,
            &wire_messages,
            target.peers.len(),
        );
        let mut owned_payload = None;
        for &peer_id in target.peers {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            // A payload must not overtake its peer's unshipped dictionary.
            let needs_dict = dictionary.is_some() && !peer.dict_shipped;
            let result = if needs_dict {
                TryFrameResult::Full
            } else {
                Self::try_push_frame(&peer.slot, &frame)
            };
            match result {
                TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                TryFrameResult::Full => {
                    let payload = owned_payload
                        .get_or_insert_with(|| Arc::new(PreparedFrame::from_frame(&frame)))
                        .clone();
                    pending.push(PendingPeer {
                        peer_id,
                        slot: peer.slot.clone(),
                        payload,
                        dictionary: if needs_dict { owned_dict.clone() } else { None },
                    });
                }
                TryFrameResult::Dead | TryFrameResult::Ineligible => {}
            }
        }
        clear_fan_out_frame(&mut self.eq, &mut self.chunks);
    }

    /// Ship the group dictionary to peers that lack it. Returns an owned copy
    /// for peers that must wait for room, if any.
    fn ship_dictionary_ready(
        &mut self,
        dict: &Message,
        peer_ids: &[u64],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> Option<Arc<PreparedFrame>> {
        let frame = build_fan_out_frame(
            &mut self.eq,
            dict,
            &mut self.chunks,
            peer_ids.len(),
            FAN_OUT_TOTAL_COPY_BUDGET,
        );

        let mut owned_dict = None;
        for &peer_id in peer_ids {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            if peer.dict_shipped {
                continue;
            }

            if Self::try_push_frame(&peer.slot, &frame) == TryFrameResult::Ok {
                peer.dict_shipped = true;
                peer.slot.mark_fanout_dict_shipped();
                Self::touch_peer(peer_id, peer, touched);
            } else {
                owned_dict.get_or_insert_with(|| Arc::new(PreparedFrame::from_frame(&frame)));
            }
        }
        clear_fan_out_frame(&mut self.eq, &mut self.chunks);

        owned_dict
    }

    pub(super) async fn finish_pending(
        &mut self,
        pending: &mut SmallVec<[PendingPeer; 8]>,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        while !pending.is_empty() {
            let mut waits = FuturesUnordered::new();
            pending.retain_mut(|target| {
                let Some(peer) = self
                    .peers
                    .get_mut(&target.peer_id)
                    .filter(|peer| Arc::ptr_eq(&peer.slot, &target.slot))
                else {
                    return false;
                };
                // Snapshot before admission so a concurrent drain cannot lose
                // the only space wake between the failed push and parking.
                let seen = target.slot.space_available.generation();
                if let Some(dict) = &target.dictionary {
                    match Self::try_push_frame(&target.slot, &dict.as_frame()) {
                        TryFrameResult::Ok => {
                            peer.dict_shipped = true;
                            target.slot.mark_fanout_dict_shipped();
                            Self::touch_peer(target.peer_id, peer, touched);
                            target.dictionary = None;
                        }
                        TryFrameResult::Full => {}
                        TryFrameResult::Dead | TryFrameResult::Ineligible => return false,
                    }
                }
                if target.dictionary.is_none() {
                    match target.payload.try_push(&target.slot) {
                        TryFrameResult::Ok => {
                            Self::touch_peer(target.peer_id, peer, touched);
                            return false;
                        }
                        TryFrameResult::Full => {}
                        TryFrameResult::Dead | TryFrameResult::Ineligible => return false,
                    }
                }
                target.slot.signal_encoded();
                let space = target.slot.space_available.clone();
                waits.push(async move { space.changed_after(seen).await });
                true
            });
            self.flush_touched(touched);
            touched.clear();
            if pending.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = self.ctrl.ready() => {
                    if self.drain_control() { return true; }
                }
                _ = waits.next() => {}
            }
        }
        false
    }
}
