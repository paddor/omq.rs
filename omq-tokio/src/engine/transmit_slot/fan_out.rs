// Fan-out admission: encoded publications, arenas, and drop-oldest pushes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use smallvec::SmallVec;

use super::{PeerTransmitSlot, TryFrameResult};
use omq_proto::copy_stats::{self, Site};
use omq_proto::fan_out_frame::FanOutFrame;

type FanOutReactivation = Arc<dyn Fn(u64) + Send + Sync + 'static>;

/// Per-slot fan-out state: dictionary shipment and mute reactivation.
pub(super) struct FanOutState {
    dict_queued: AtomicBool,
    dict_shipped: AtomicBool,
    active: AtomicBool,
    reactivation: Mutex<Option<FanOutReactivation>>,
}

impl FanOutState {
    pub(super) fn new() -> Self {
        Self {
            dict_queued: AtomicBool::new(false),
            dict_shipped: AtomicBool::new(false),
            active: AtomicBool::new(true),
            reactivation: Mutex::new(None),
        }
    }

    /// Forget dictionary and activity state of a dead slot.
    pub(super) fn retire(&self) {
        self.dict_queued.store(false, Ordering::Relaxed);
        self.dict_shipped.store(false, Ordering::Relaxed);
        self.active.store(false, Ordering::Relaxed);
    }
}

impl PeerTransmitSlot {
    pub(crate) fn try_push_encoded(&self, chunks: &[Bytes]) -> TryFrameResult {
        let result = self.try_push_encoded_no_signal(chunks);
        if result == TryFrameResult::Ok {
            self.signal_encoded();
        }

        result
    }

    /// Admit one encoded publication; the caller signals accepted batches.
    pub(crate) fn try_push_encoded_no_signal(&self, chunks: &[Bytes]) -> TryFrameResult {
        if self.is_dead() {
            return TryFrameResult::Dead;
        }

        let bytes = chunks.iter().map(Bytes::len).sum();
        let Some(mut eq) = self.lock_live() else {
            return TryFrameResult::Dead;
        };

        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        if queued_msgs >= self.msg_cap
            || (queued_msgs > 0 && eq.total_bytes().saturating_add(bytes) >= self.cap)
        {
            return self.full();
        }

        eq.push_shared_chunks(chunks);
        // Count writers are serialized by eq, including drains and disconnects.
        let queued = queued_msgs + 1;
        self.commit(&eq, queued);

        TryFrameResult::Ok
    }

    pub(crate) fn try_push_pre_framed_no_signal(&self, data: &[u8]) -> TryFrameResult {
        if self.is_dead() {
            return TryFrameResult::Dead;
        }

        let Some(mut eq) = self.lock_live() else {
            return TryFrameResult::Dead;
        };

        if self.is_full(&eq) {
            return self.full();
        }

        eq.push_pre_framed(data);
        // Count writers are serialized by eq, including drains and disconnects.
        let queued = self.queued_msgs.load(Ordering::Relaxed) + 1;
        self.commit(&eq, queued);

        TryFrameResult::Ok
    }

    /// Conservative admission before compressing a shared arena. A lane is
    /// the sole data writer of its compressed fan-out slots; drains can only
    /// increase this available space while the lane encodes.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn arena_credit(&self) -> (usize, usize) {
        let Some(eq) = self.lock_live() else {
            return (0, 0);
        };

        (
            self.msg_cap
                .saturating_sub(self.queued_msgs.load(Ordering::Relaxed)),
            self.cap.saturating_sub(eq.total_bytes().saturating_add(1)),
        )
    }

    /// One indivisible carrier still consumes credit for every logical message.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn try_push_arena_no_signal(
        &self,
        frame: &FanOutFrame<'_>,
        messages: usize,
    ) -> TryFrameResult {
        let bytes = match frame {
            FanOutFrame::Arena(data) => data.len(),
            FanOutFrame::Chunks(chunks) => chunks.iter().map(Bytes::len).sum(),
        };

        let Some(mut eq) = self.lock_live() else {
            return TryFrameResult::Dead;
        };

        let queued = self.queued_msgs.load(Ordering::Relaxed);
        if queued.saturating_add(messages) > self.msg_cap
            || eq.total_bytes().saturating_add(bytes) >= self.cap
        {
            return self.full();
        }

        match frame {
            FanOutFrame::Arena(data) => eq.push_pre_framed(data),
            FanOutFrame::Chunks(chunks) => eq.push_shared_chunks(chunks),
        }

        self.commit(&eq, queued + messages);
        TryFrameResult::Ok
    }

    /// Admit a prefix of complete publications under one lock. `ends` holds
    /// their cumulative wire lengths; the caller retries any unaccepted suffix.
    pub(crate) fn try_push_pre_framed_batch_no_signal(
        &self,
        data: &[u8],
        ends: &[usize],
        start: usize,
    ) -> (usize, TryFrameResult) {
        if self.is_dead() {
            return (start, TryFrameResult::Dead);
        }

        let Some(mut eq) = self.lock_live() else {
            return (start, TryFrameResult::Dead);
        };

        let offset = if start == 0 { 0 } else { ends[start - 1] };
        let queued = self.queued_msgs.load(Ordering::Relaxed);
        let bytes = eq.total_bytes();
        let mut next = start;
        let mut end = offset;

        for &message_end in &ends[start..] {
            // Match single-publication admission: the last accepted message
            // may cross the byte cap, but never the message cap.
            if queued + next - start >= self.msg_cap || bytes + end - offset >= self.cap {
                break;
            }

            end = message_end;
            next += 1;
        }

        if next != start {
            eq.push_pre_framed(&data[offset..end]);
            let queued = queued + next - start;
            self.commit(&eq, queued);
        }

        let result = if next == ends.len() {
            TryFrameResult::Ok
        } else {
            self.full()
        };

        (next, result)
    }

    pub(crate) fn try_push_fanout_drop_oldest(&self, frame: &FanOutFrame<'_>) -> TryFrameResult {
        self.try_push_fanout_drop_oldest_with_protection(frame, false)
    }

    pub(crate) fn try_push_protected_fanout_drop_oldest(
        &self,
        frame: &FanOutFrame<'_>,
    ) -> TryFrameResult {
        self.try_push_fanout_drop_oldest_with_protection(frame, true)
    }

    fn try_push_fanout_drop_oldest_with_protection(
        &self,
        frame: &FanOutFrame<'_>,
        protected: bool,
    ) -> TryFrameResult {
        if self.is_dead() {
            return TryFrameResult::Dead;
        }

        // Queue each encoded fan-out message as one entry or chunk group so
        // full-slot eviction removes the oldest whole message.
        let chunks = fanout_frame_chunks(frame);
        let len = chunks.iter().map(Bytes::len).sum::<usize>();

        let Some(mut eq) = self.lock_live() else {
            return TryFrameResult::Dead;
        };

        let mut queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        while queued_msgs > 0
            && (eq.total_bytes().saturating_add(len) >= self.cap || queued_msgs >= self.msg_cap)
        {
            if !eq.pop_oldest_unprotected_entry() {
                return self.full();
            }

            queued_msgs = queued_msgs.saturating_sub(1);
        }

        eq.push_raw_message(&chunks, protected);
        if protected {
            self.fan_out.dict_queued.store(true, Ordering::Release);
        }

        queued_msgs += 1;
        self.commit(&eq, queued_msgs);
        drop(eq);
        self.signal_encoded();

        TryFrameResult::Ok
    }

    #[inline]
    pub(crate) fn fanout_dict_shipped(&self) -> bool {
        self.fan_out.dict_shipped.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn fanout_dict_queued_or_shipped(&self) -> bool {
        self.fan_out.dict_queued.load(Ordering::Acquire) || self.fanout_dict_shipped()
    }

    #[inline]
    pub(crate) fn mark_fanout_dict_shipped(&self) {
        self.fan_out.dict_queued.store(false, Ordering::Release);
        self.fan_out.dict_shipped.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn fanout_active(&self) -> bool {
        self.fan_out.active.load(Ordering::Acquire)
    }

    #[inline]
    /// Mute the peer after a full queue refused a push. Returns whether
    /// this call muted it. The next drain below the LWM unmutes it.
    ///
    /// That drain can run between the refused push and this call. It finds
    /// the peer active and reactivates nothing, and an idle queue never
    /// drains again. So this rechecks the queue once the flag is clear.
    pub(crate) fn deactivate_fanout(&self) -> bool {
        let deactivated = self.clear_fanout_active();
        self.reactivate_fanout_if_drained();
        deactivated
    }

    /// Clear the active flag only. The caller must follow up with
    /// [`Self::reactivate_fanout_if_drained`].
    pub(crate) fn clear_fanout_active(&self) -> bool {
        self.fan_out
            .active
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Reactivate a muted peer whose queue is below the LWM already, so
    /// that no later drain would do it. A peer out of SHM credit waits for
    /// [`Self::shm_credit_returned`] instead.
    pub(crate) fn reactivate_fanout_if_drained(&self) {
        let eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let below_lwm = !self.shm_credit_blocked()
            && self.is_below_lwm(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        let reactivate = self.claim_reactivation(below_lwm);
        drop(eq);
        if reactivate {
            self.notify_reactivation();
        }
    }

    pub(crate) fn set_fanout_reactivation(&self, cb: FanOutReactivation) {
        *self
            .fan_out
            .reactivation
            .lock()
            .expect("transmit_slot fanout_reactivation poisoned") = Some(cb);
    }

    /// Claim the fan-out reactivation once the queue fell below the LWM.
    #[inline]
    pub(super) fn claim_reactivation(&self, below_lwm: bool) -> bool {
        below_lwm
            && self
                .fan_out
                .active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    pub(super) fn notify_reactivation(&self) {
        if let Some(cb) = self
            .fan_out
            .reactivation
            .lock()
            .expect("transmit_slot fanout_reactivation poisoned")
            .clone()
        {
            cb(self.peer_id);
        }
    }
}

pub(super) fn fanout_frame_chunks(frame: &FanOutFrame<'_>) -> SmallVec<[Bytes; 2]> {
    match frame {
        FanOutFrame::Arena(raw) => {
            copy_stats::record(Site::FanOutChunk, raw.len());
            smallvec::smallvec![Bytes::copy_from_slice(raw)]
        }
        FanOutFrame::Chunks(chunks) => chunks.iter().cloned().collect(),
    }
}
