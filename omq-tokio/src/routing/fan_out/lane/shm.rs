//! SHM batch and group dispatch for fan-out lanes.

#[cfg(target_os = "linux")]
use super::LaneData;
use super::{LaneDispatch, LaneWorker, PendingPeer, PreparedFrame};
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::routing::fan_out::FanOutMutePolicy;
#[cfg(target_os = "linux")]
use crate::routing::fan_out::filter::FanOutMode;
use omq_proto::fan_out_frame::FanOutFrame;
#[cfg(target_os = "linux")]
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;
use smallvec::SmallVec;
use std::sync::Arc;

impl LaneWorker {
    /// Pack a run of same-envelope group publications into one SHM batch
    /// record. Returns the record, how many publications it covers, and
    /// whether bodies are pool references rather than copies.
    #[cfg(target_os = "linux")]
    pub(super) fn prepare_shm_batch(&self, batch: &[LaneData]) -> Option<(Message, usize, bool)> {
        if batch.len() < SHM_BATCH_MIN_MESSAGES
            || !matches!(self.mode, FanOutMode::Group)
            || self.peers.is_empty()
            || self.peers.values().any(|peer| !peer.slot.is_shm())
        {
            return None;
        }

        // Every recipient must fit the whole batch as messages.
        let limit = self
            .peers
            .values()
            .map(|peer| peer.slot.message_capacity())
            .min()?
            .min(omq_shm::BATCH_MAX_MESSAGES);
        let LaneData::Dispatch(first) = &batch[0];
        let shape = ShmBatchShape::new(&first.msg)?;
        let mut messages = shape.collect(batch, limit)?;

        if shape.referenced {
            messages.truncate(omq_shm::pooled_prefix(&messages));
        }
        if messages.len() < SHM_BATCH_MIN_MESSAGES {
            return None;
        }

        let message = self.shm_pool.prepare_batch(&messages, shape.kind).ok()??;
        Some((message, messages.len(), shape.referenced))
    }

    #[cfg(target_os = "linux")]
    pub(super) async fn dispatch_shm_batch(
        &mut self,
        message: Message,
        count: usize,
        referenced: bool,
        touched: &mut SmallVec<[u64; 32]>,
    ) -> bool {
        let dispatch = LaneDispatch { msg: message };
        let targets = self.matching_peer_groups(&dispatch);
        let mut pending = SmallVec::<[PendingPeer; 8]>::new();
        let mut payload = None;
        for peers in targets {
            for peer_id in peers {
                let Some(peer) = self.peers.get_mut(&peer_id) else {
                    continue;
                };
                match peer
                    .slot
                    .try_encode_shm_batch_no_signal(&dispatch.msg, count, referenced)
                {
                    TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                    TryFrameResult::Full if self.mute_policy == FanOutMutePolicy::Block => {
                        let payload = payload
                            .get_or_insert_with(|| {
                                Arc::new(PreparedFrame::ShmBatch(
                                    dispatch.msg.clone(),
                                    count,
                                    referenced,
                                ))
                            })
                            .clone();
                        pending.push(PendingPeer {
                            peer_id,
                            slot: peer.slot.clone(),
                            payload,
                            dictionary: None,
                        });
                    }
                    TryFrameResult::Full => {
                        peer.slot.deactivate_fanout();
                    }
                    TryFrameResult::Dead | TryFrameResult::Ineligible => {}
                }
            }
        }
        self.finish_pending(&mut pending, touched).await
    }

    pub(super) fn dispatch_shm_group(
        &mut self,
        peer_ids: &[u64],
        dispatch: &LaneDispatch,
        touched: &mut SmallVec<[u64; 32]>,
        mut pending: Option<&mut SmallVec<[PendingPeer; 8]>>,
    ) {
        debug_assert!(pending.is_some() || self.mute_policy == FanOutMutePolicy::DropNewest);
        let multipart = self.prepare_shm_multipart(dispatch);
        let prepared = multipart
            .is_none()
            .then(|| self.prepare_shm_publication(dispatch))
            .flatten();
        let dispatch = prepared.as_ref().unwrap_or(dispatch);
        let copied = multipart.is_none() && Self::shm_copied_body(&dispatch.msg);
        #[cfg(target_os = "linux")]
        if copied {
            // Copied records are identical for every recipient. Reference
            // records below instead reserve a distinct lease per connection.
            omq_shm::frame_copied(&dispatch.msg, &mut self.eq);
        }
        let mut payload = None;
        for &peer_id in peer_ids {
            let Some(peer) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            let result = if copied {
                peer.slot
                    .try_push_pre_framed_no_signal(self.eq.arena_bytes())
            } else if let Some(record) = &multipart {
                Self::try_encode_shm_multipart(&peer.slot, record)
            } else {
                peer.slot.try_encode_shm_no_signal(&dispatch.msg)
            };
            match result {
                TryFrameResult::Ok => Self::touch_peer(peer_id, peer, touched),
                TryFrameResult::Full => {
                    let Some(pending) = pending.as_mut() else {
                        peer.slot.deactivate_fanout();
                        continue;
                    };
                    let payload = payload
                        .get_or_insert_with(|| {
                            Arc::new(if copied {
                                PreparedFrame::from_frame(&FanOutFrame::Arena(
                                    self.eq.arena_bytes(),
                                ))
                            } else if let Some(record) = &multipart {
                                PreparedFrame::ShmMultipart(record.clone())
                            } else {
                                PreparedFrame::Shm(dispatch.msg.clone())
                            })
                        })
                        .clone();
                    pending.push(PendingPeer {
                        peer_id,
                        slot: peer.slot.clone(),
                        payload,
                        dictionary: None,
                    });
                }
                TryFrameResult::Dead | TryFrameResult::Ineligible => {}
            }
        }
        self.eq.clear_arena();
    }

    fn shm_copied_body(message: &Message) -> bool {
        #[cfg(target_os = "linux")]
        {
            message
                .len()
                .checked_sub(1)
                .and_then(|index| message.part_slice(index))
                .is_some_and(|body| body.len() <= omq_shm::COPY_THRESHOLD)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = message;
            false
        }
    }

    /// Copy a large-envelope publication once into one shared multipart record.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn prepare_shm_multipart(&self, dispatch: &LaneDispatch) -> Option<Message> {
        #[cfg(target_os = "linux")]
        {
            omq_shm::uses_multipart_record(&dispatch.msg)
                .then(|| {
                    self.shm_pool
                        .prepare_multipart(&dispatch.msg)
                        .ok()
                        .flatten()
                })
                .flatten()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = dispatch;
            None
        }
    }

    pub(super) fn try_encode_shm_multipart(
        slot: &PeerTransmitSlot,
        record: &Message,
    ) -> TryFrameResult {
        #[cfg(target_os = "linux")]
        {
            slot.try_encode_shm_multipart_no_signal(record)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (slot, record);
            TryFrameResult::Ineligible
        }
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn prepare_shm_publication(&self, dispatch: &LaneDispatch) -> Option<LaneDispatch> {
        #[cfg(target_os = "linux")]
        {
            self.shm_pool
                .prepare(&dispatch.msg)
                .ok()
                .flatten()
                .map(|msg| LaneDispatch { msg })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = dispatch;
            None
        }
    }
}

/// Shorter runs are cheaper as individual SHM records.
#[cfg(target_os = "linux")]
const SHM_BATCH_MIN_MESSAGES: usize = 4;

/// Layout every member of one fan-out SHM batch shares with the first
/// publication: two parts, the same envelope, and one body storage kind.
#[cfg(target_os = "linux")]
struct ShmBatchShape<'m> {
    first: &'m Message,
    kind: omq_shm::BatchKind,
    referenced: bool,
    copy_limit: usize,
}

#[cfg(target_os = "linux")]
impl<'m> ShmBatchShape<'m> {
    fn new(first: &'m Message) -> Option<Self> {
        // Larger shared envelopes use multipart records instead.
        if first.part_slice(0)?.len() > omq_shm::COPIED_BATCH_MAX_BODY {
            return None;
        }

        let kind =
            omq_shm::BatchKind::for_message(first, omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD);
        let referenced = matches!(kind, omq_shm::BatchKind::Referenced);
        let body = first.part_slice(1)?.len();
        let copy_limit = body.clamp(
            omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD,
            omq_shm::COPIED_BATCH_MAX_BODY,
        );
        if !referenced && body > omq_shm::COPIED_BATCH_MAX_BODY {
            return None;
        }

        Some(Self {
            first,
            kind,
            referenced,
            copy_limit,
        })
    }

    /// Leading publications that share this shape and fit one record.
    /// `None` when a candidate has no body part.
    fn collect<'b>(
        &self,
        batch: &'b [LaneData],
        limit: usize,
    ) -> Option<SmallVec<[&'b Message; omq_shm::BATCH_MAX_MESSAGES]>> {
        // The record starts with a u16 member count.
        let mut packed_bytes = std::mem::size_of::<u16>();
        let mut budget = DrainBudget::new(limit, omq_shm::BATCH_MAX_BYTES);
        let mut messages = SmallVec::new();

        for data in batch.iter().take(limit) {
            let LaneData::Dispatch(dispatch) = data;
            if dispatch.msg.len() != 2
                || dispatch.msg.part_slice(0) != self.first.part_slice(0)
                || (self.referenced && LaneWorker::shm_copied_body(&dispatch.msg))
            {
                break;
            }

            let body_length = dispatch.msg.part_slice(1)?.len();
            let bytes = if self.referenced {
                omq_shm::REFERENCE_ENTRY_BYTES
            } else {
                if body_length > self.copy_limit {
                    break;
                }
                // Copied entries carry a u64 length before the body.
                std::mem::size_of::<u64>() + body_length
            };
            packed_bytes += bytes;
            if packed_bytes > omq_shm::BATCH_MAX_BYTES {
                break;
            }

            messages.push(&dispatch.msg);
            if !budget.account(bytes) {
                break;
            }
        }

        Some(messages)
    }
}
