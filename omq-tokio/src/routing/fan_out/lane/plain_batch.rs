//! Plain frame batches: encode several small messages per peer write.

use super::{LaneData, LaneWorker, PLAIN_BATCH_BYTES, PLAIN_BATCH_MESSAGES, PendingBatchPeer};
use crate::engine::transmit_slot::TryFrameResult;
use crate::routing::fan_out::filter;
use crate::routing::fan_out::{FAN_OUT_TOTAL_COPY_BUDGET, FanOutMutePolicy};
use futures::{StreamExt, stream::FuturesUnordered};
use omq_proto::flow::DrainBudget;
use omq_proto::frame_buffer::FrameBuffer;
use smallvec::SmallVec;
use std::sync::Arc;

impl LaneWorker {
    /// Reuse the contiguous small-message path when every peer wants the
    /// same plaintext publications. Other filters, codecs and oldest eviction
    /// retain their existing per-publication dispatch.
    pub(super) fn encode_plain_batch(
        &self,
        batch: &[LaneData],
        wire: &mut FrameBuffer,
        ends: &mut SmallVec<[usize; PLAIN_BATCH_MESSAGES]>,
    ) -> usize {
        if batch.len() < 2 || self.mute_policy == FanOutMutePolicy::DropOldest {
            return 0;
        }
        let LaneData::Dispatch(first) = &batch[0];
        let first_max_bytes = first
            .msg
            .byte_len()
            .saturating_add(first.msg.len().saturating_mul(9));
        if first_max_bytes.saturating_mul(self.peers.len()) > FAN_OUT_TOTAL_COPY_BUDGET
            || first_max_bytes >= omq_proto::frame_buffer::ARENA_THRESHOLD
            || !filter::all_peers_subscribe_all(
                self.mode,
                self.subscribe_all_count,
                self.peers.len(),
            )
            || self
                .peers
                .values()
                .any(|peer| peer.slot.has_transform || peer.slot.codec_profile().is_some())
        {
            return 0;
        }
        wire.clear_arena();
        ends.clear();
        let mut budget = DrainBudget::new(PLAIN_BATCH_MESSAGES, PLAIN_BATCH_BYTES);
        for data in batch.iter().take(PLAIN_BATCH_MESSAGES) {
            let LaneData::Dispatch(dispatch) = data;
            let max_bytes = dispatch
                .msg
                .byte_len()
                .saturating_add(dispatch.msg.len().saturating_mul(9));
            if max_bytes.saturating_mul(self.peers.len()) > FAN_OUT_TOTAL_COPY_BUDGET
                || max_bytes >= omq_proto::frame_buffer::ARENA_THRESHOLD
                || wire.total_bytes().saturating_add(max_bytes) > PLAIN_BATCH_BYTES
            {
                break;
            }
            let start = wire.total_bytes();
            wire.frame_inline(&dispatch.msg);
            ends.push(wire.total_bytes());
            if !budget.account(wire.total_bytes() - start) {
                break;
            }
        }
        ends.len()
    }

    /// Push a pre-framed plain batch to every active peer. A full peer keeps
    /// its own resume offset; under `DropNewest` it is muted instead.
    /// Returns `true` on shutdown.
    pub(super) async fn dispatch_plain_batch(
        &mut self,
        wire: &[u8],
        ends: &[usize],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let mut pending = SmallVec::<[PendingBatchPeer; 8]>::new();
        for (&peer_id, peer) in &mut self.peers {
            if !peer.slot.fanout_active() {
                continue;
            }

            let (next, result) = peer.slot.try_push_pre_framed_batch_no_signal(wire, ends, 0);
            if next > 0 {
                Self::touch_peer(peer_id, peer, touched);
            }
            if result == TryFrameResult::Full {
                if self.mute_policy == FanOutMutePolicy::DropNewest {
                    peer.slot.deactivate_fanout();
                    continue;
                }
                pending.push(PendingBatchPeer {
                    peer_id,
                    slot: peer.slot.clone(),
                    next,
                });
            }
        }

        self.finish_plain_batch(&mut pending, wire, ends, touched)
            .await
    }

    /// Retry blocked peers from their resume offsets until all finish, while
    /// still serving control. Returns `true` on shutdown.
    async fn finish_plain_batch(
        &mut self,
        pending: &mut SmallVec<[PendingBatchPeer; 8]>,
        wire: &[u8],
        ends: &[usize],
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
                    // Removed or replaced peers drop their unsent suffix.
                    return false;
                };

                // Snapshot before admission so a concurrent drain cannot lose
                // the only space wake between the failed push and parking.
                let seen = target.slot.space_available.generation();
                let (next, result) =
                    target
                        .slot
                        .try_push_pre_framed_batch_no_signal(wire, ends, target.next);
                if next > target.next {
                    Self::touch_peer(target.peer_id, peer, touched);
                    target.next = next;
                }
                if result != TryFrameResult::Full {
                    return false;
                }

                target.slot.signal_encoded();
                let space = target.slot.space_available.clone();
                waits.push(async move { space.changed_after(seen).await });
                true
            });
            self.flush_touched(touched);
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
