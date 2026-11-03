//! Frame and peer-group helpers shared by the dispatch paths.

use super::LaneWorker;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::routing::fan_out::fan_out_cost;
use bytes::Bytes;
use omq_proto::fan_out_frame::{FanOutEncoder, FanOutFrame};
use omq_proto::message::Message;
use omq_proto::proto::transform::TransformedOut;
use smallvec::SmallVec;
use std::sync::Arc;

/// Owned only when a ready-path admission blocks. Large chunks stay shared.
#[derive(Debug)]
pub(super) enum PreparedFrame {
    Arena(Bytes),
    Chunks(Vec<Bytes>),
    Shm(Message),
    ShmMultipart(Message),
    #[cfg(target_os = "linux")]
    ShmBatch(Message, usize, bool),
}

impl PreparedFrame {
    pub(super) fn from_frame(frame: &FanOutFrame<'_>) -> Self {
        match frame {
            FanOutFrame::Arena(raw) => {
                omq_proto::copy_stats::record(
                    omq_proto::copy_stats::Site::FanOutPrepared,
                    raw.len(),
                );
                Self::Arena(Bytes::copy_from_slice(raw))
            }
            FanOutFrame::Chunks(chunks) => Self::Chunks(chunks.to_vec()),
        }
    }

    pub(super) fn as_frame(&self) -> FanOutFrame<'_> {
        match self {
            Self::Arena(raw) => FanOutFrame::Arena(raw),
            Self::Chunks(chunks) => FanOutFrame::Chunks(chunks),
            Self::Shm(_) | Self::ShmMultipart(_) => {
                unreachable!("SHM references require per-recipient encoding")
            }
            #[cfg(target_os = "linux")]
            Self::ShmBatch(..) => unreachable!("SHM batches require per-recipient encoding"),
        }
    }

    pub(super) fn try_push(&self, slot: &PeerTransmitSlot) -> TryFrameResult {
        match self {
            Self::Shm(message) => slot.try_encode_shm_no_signal(message),
            Self::ShmMultipart(record) => LaneWorker::try_encode_shm_multipart(slot, record),
            #[cfg(target_os = "linux")]
            Self::ShmBatch(message, count, referenced) => {
                slot.try_encode_shm_batch_no_signal(message, *count, *referenced)
            }
            _ => LaneWorker::try_push_frame(slot, &self.as_frame()),
        }
    }
}

#[derive(Debug)]
pub(super) struct PendingPeer {
    pub(super) peer_id: u64,
    pub(super) slot: Arc<PeerTransmitSlot>,
    pub(super) payload: Arc<PreparedFrame>,
    pub(super) dictionary: Option<Arc<PreparedFrame>>,
}

/// Peers of one publication that wait for room. They share one owned
/// payload, built when the first peer parks.
pub(super) struct Parked<'a> {
    pending: &'a mut SmallVec<[PendingPeer; 8]>,
    payload: Option<Arc<PreparedFrame>>,
}

impl<'a> Parked<'a> {
    pub(super) fn new(pending: &'a mut SmallVec<[PendingPeer; 8]>) -> Self {
        Self {
            pending,
            payload: None,
        }
    }

    pub(super) fn park(
        &mut self,
        peer_id: u64,
        slot: &Arc<PeerTransmitSlot>,
        dictionary: Option<Arc<PreparedFrame>>,
        payload: impl FnOnce() -> PreparedFrame,
    ) {
        let payload = self
            .payload
            .get_or_insert_with(|| Arc::new(payload()))
            .clone();
        self.pending.push(PendingPeer {
            peer_id,
            slot: slot.clone(),
            payload,
            dictionary,
        });
    }
}

/// Matched recipients of one codec group.
#[derive(Clone, Copy)]
pub(super) struct GroupPeers<'a> {
    pub(super) group: usize,
    pub(super) peers: &'a [u64],
}

/// Frame a group's encoded publication once for `recipients` peers.
pub(super) fn frame_publication<'f>(
    encoder: &'f mut FanOutEncoder,
    wire: &TransformedOut,
    recipients: usize,
) -> FanOutFrame<'f> {
    let cost = fan_out_cost(recipients);
    for message in wire {
        encoder.encode(message, cost);
    }
    encoder.finish(cost)
}

impl LaneWorker {
    pub(super) fn try_push_frame(
        slot: &PeerTransmitSlot,
        frame: &FanOutFrame<'_>,
    ) -> TryFrameResult {
        match frame {
            FanOutFrame::Arena(raw) => slot.try_push_pre_framed_no_signal(raw),
            FanOutFrame::Chunks(chunks) => slot.try_push_encoded_no_signal(chunks),
        }
    }
}
