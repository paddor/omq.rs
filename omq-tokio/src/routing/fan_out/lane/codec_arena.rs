//! Shared codec arenas: one encode per codec group, gather-written to each peer.

use super::{GroupPeers, LaneWorker};
use crate::engine::transmit_slot::TryFrameResult;
use crate::routing::fan_out::codec_group::{CodecGroup, MAX_CODEC_GROUPS};
use crate::routing::fan_out::{FanOutMutePolicy, fan_out_cost};
use omq_proto::message::Message;
use smallvec::SmallVec;

const CODEC_ARENA_FRAME_OVERHEAD: usize = 16 + 9;

impl LaneWorker {
    /// Share an arena only when the same recipients can admit every message.
    /// Partial credit, changed filters and oldest eviction use scalar dispatch.
    pub(super) async fn dispatch_codec_arena(
        &mut self,
        batch: &[Message],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> (usize, bool) {
        if batch.len() < 2 || self.mute_policy == FanOutMutePolicy::DropOldest {
            return (0, false);
        }
        let first = &batch[0];
        let targets = self.matching_peer_groups(first);
        if !self.codec_arena_targets_ready(&targets) {
            return (0, false);
        }

        let messages = self.codec_arena_members(batch, &targets);
        if messages.len() < 2 {
            return (0, false);
        }
        let count = messages.len();
        let fallback = self.push_codec_arena(&targets, &messages, touched);

        // A slot can close during encoding. If admission otherwise changed,
        // retain normal per-message mute behavior for just that recipient.
        if fallback.is_empty() {
            return (count, false);
        }
        let shutdown = self
            .dispatch_arena_fallback(&batch[..count], &fallback, touched)
            .await;
        (count, shutdown)
    }

    /// Leading publications that share `targets` and fit one arena within
    /// every target's message and byte credit.
    fn codec_arena_members<'b>(
        &self,
        batch: &'b [Message],
        targets: &[SmallVec<[u64; 32]>; MAX_CODEC_GROUPS],
    ) -> SmallVec<[&'b Message; 32]> {
        use omq_proto::proto::transform::arena::{
            MAX_ARENA_MESSAGES, MAX_ARENA_PARTS, arena_message_size,
        };

        let capacity = targets
            .iter()
            .enumerate()
            .filter(|(_, peers)| !peers.is_empty())
            .map(|(group, _)| self.codec_groups[group].as_ref().unwrap().arena_capacity())
            .min()
            .unwrap();
        let (max_messages, max_bytes) = targets.iter().flatten().fold(
            (MAX_ARENA_MESSAGES, capacity + CODEC_ARENA_FRAME_OVERHEAD),
            |(messages, bytes), id| {
                let (credit, space) = self.peers[id].slot.arena_credit();
                (messages.min(credit), bytes.min(space))
            },
        );

        let mut messages = SmallVec::new();
        let mut bytes = 0;
        let mut parts = 0;
        for msg in batch.iter().take(max_messages) {
            let Some(size) = arena_message_size(msg) else {
                break;
            };
            if bytes + size > capacity
                || parts + msg.len() > MAX_ARENA_PARTS
                || self.matching_peer_groups(msg) != *targets
                || bytes + size + CODEC_ARENA_FRAME_OVERHEAD > max_bytes
            {
                break;
            }
            bytes += size;
            parts += msg.len();
            messages.push(msg);
        }

        messages
    }

    /// Encode one arena per codec group and push it to that group's targets.
    /// Returns the `(group, peer)` pairs that could not take the arena.
    fn push_codec_arena(
        &mut self,
        targets: &[SmallVec<[u64; 32]>; MAX_CODEC_GROUPS],
        messages: &[&Message],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> SmallVec<[(usize, u64); 8]> {
        let mut fallback = SmallVec::new();
        for (group, peers) in targets
            .iter()
            .enumerate()
            .filter(|(_, peers)| !peers.is_empty())
        {
            let Ok(wire) = self.codec_groups[group]
                .as_mut()
                .unwrap()
                .encode_arena(messages)
            else {
                fallback.extend(peers.iter().map(|id| (group, *id)));
                continue;
            };

            let frame = self.encoder.build(&wire, fan_out_cost(peers.len()));
            for id in peers {
                let peer = self.peers.get_mut(id).unwrap();
                match peer.slot.try_push_arena_no_signal(&frame, messages.len()) {
                    TryFrameResult::Ok => Self::touch_peer(*id, peer, touched),
                    TryFrameResult::Full => fallback.push((group, *id)),
                    TryFrameResult::Dead | TryFrameResult::Ineligible => {}
                }
            }
            self.encoder.clear();
        }

        fallback
    }

    /// Dispatch each arena member to the recipients that missed the arena,
    /// one publication at a time. Returns `true` on shutdown.
    async fn dispatch_arena_fallback(
        &mut self,
        members: &[Message],
        fallback: &[(usize, u64)],
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        for msg in members {
            let mut pending = SmallVec::new();
            for &(group, id) in fallback {
                let target = GroupPeers {
                    group,
                    peers: &[id],
                };
                if self.mute_policy.is_lossy() {
                    self.dispatch_group_lossy(target, msg, touched);
                } else {
                    self.dispatch_group_ready(target, msg, touched, &mut pending);
                }
            }
            if self.finish_pending(&mut pending, touched).await {
                return true;
            }
        }

        false
    }

    fn codec_arena_targets_ready(&self, targets: &[SmallVec<[u64; 32]>; MAX_CODEC_GROUPS]) -> bool {
        targets.iter().any(|peers| !peers.is_empty())
            && targets.iter().enumerate().all(|(group, peers)| {
                peers.is_empty()
                    || (self.codec_groups[group]
                        .as_ref()
                        .is_some_and(CodecGroup::can_encode_arena)
                        && peers.iter().all(|id| {
                            let peer = &self.peers[id];
                            #[cfg(feature = "ws")]
                            if peer.slot.is_ws() {
                                return false;
                            }
                            self.codec_groups[group]
                                .as_ref()
                                .unwrap()
                                .dictionary()
                                .is_none()
                                || peer.dict_shipped
                        }))
            })
    }
}
