// Plain TCP latency writes share admission and write ownership with the
// connection driver. The slot retains an accepted direct write's tail;
// queued messages cannot pass it while the driver waits for writability.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{Bytes, BytesMut};

use super::codec::CodecProfile;
use super::framing::WireFraming;
use super::signal::{DataSignal, StateSignal};
use omq_proto::fan_out_frame::FanOutFrame;
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::handle_frame::{
    HandleFrameCaps, HandleFrameDecision, HandleFrameState, decide_handle_frame,
};
use omq_proto::message::Message;

pub(crate) const TRANSMIT_SLOT_CAP_DEFAULT: usize = 512 * 1024;
#[cfg(test)]
pub(crate) const TRANSMIT_SLOT_MSG_CAP_DEFAULT: usize = 1000;
const TRANSMIT_SLOT_LWM_DIVISOR: usize = 2;
/// Slices per direct write. The latency path queues few messages.
const DIRECT_WRITE_SLICES: usize = 64;

type FanOutReactivation = Arc<dyn Fn(u64) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryFrameResult {
    Ok,
    Dead,
    Full,
    Ineligible,
}

pub(crate) struct PeerTransmitSlot {
    eq: Mutex<FrameBuffer>,
    direct_writer: OnceLock<Arc<crate::socket::dispatch::DirectTcpWriter>>,
    cap: usize,
    msg_cap: usize,
    pub(crate) data_signal: DataSignal,
    pub(crate) space_available: Arc<StateSignal>,
    pub(crate) handshake_done: AtomicBool,
    pub(crate) has_transform: bool,
    codec_profile: Option<CodecProfile>,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    framing: WireFraming,
    pub(crate) dead: AtomicBool,
    pub(crate) peer_id: u64,
    queued_msgs: AtomicUsize,
    fanout_dict_queued: AtomicBool,
    fanout_dict_shipped: AtomicBool,
    fanout_active: AtomicBool,
    above_lwm: AtomicBool,
    fanout_reactivation: Mutex<Option<FanOutReactivation>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DrainOutcome {
    pub(crate) space_available: bool,
}

impl std::fmt::Debug for PeerTransmitSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransmitSlot")
            .field("peer_id", &self.peer_id)
            .field(
                "handshake_done",
                &self.handshake_done.load(Ordering::Relaxed),
            )
            .field("dead", &self.dead.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl PeerTransmitSlot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        peer_id: u64,
        has_transform: bool,
        codec_profile: Option<CodecProfile>,
        transform_passthrough: Option<(Bytes, usize)>,
        arena_threshold: usize,
        arena_cap: usize,
        cap: usize,
        msg_cap: usize,
        framing: WireFraming,
    ) -> Arc<Self> {
        Arc::new(Self {
            eq: Mutex::new(FrameBuffer::with_config_lazy(arena_threshold, arena_cap)),
            direct_writer: OnceLock::new(),
            cap,
            msg_cap: msg_cap.max(1),
            data_signal: DataSignal::new(),
            space_available: Arc::new(StateSignal::new()),
            handshake_done: AtomicBool::new(false),
            has_transform,
            codec_profile,
            transform_passthrough,
            framing,
            dead: AtomicBool::new(false),
            peer_id,
            queued_msgs: AtomicUsize::new(0),
            fanout_dict_queued: AtomicBool::new(false),
            fanout_dict_shipped: AtomicBool::new(false),
            fanout_active: AtomicBool::new(true),
            above_lwm: AtomicBool::new(false),
            fanout_reactivation: Mutex::new(None),
        })
    }

    #[cfg(feature = "ws")]
    pub(crate) fn is_ws(&self) -> bool {
        self.framing.is_ws()
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn try_encode(&self, msg: &Message) -> TryFrameResult {
        let result = self.try_encode_without_signal(msg);
        if result == TryFrameResult::Ok {
            self.signal_encoded();
        }
        result
    }

    pub(crate) fn set_direct_writer(&self, writer: Arc<crate::socket::dispatch::DirectTcpWriter>) {
        self.direct_writer.get_or_init(|| writer);
    }

    pub(crate) fn direct_writer(&self) -> Option<&Arc<crate::socket::dispatch::DirectTcpWriter>> {
        self.direct_writer.get()
    }

    pub(crate) fn try_encode_without_signal(&self, msg: &Message) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        if !self.handshake_done.load(Ordering::Acquire) {
            return TryFrameResult::Ineligible;
        }

        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let decision = decide_handle_frame(
            HandleFrameState {
                uses_crypto: false,
                handshake_done: true,
                has_transform: self.has_transform,
                transform_passthrough: self.transform_passthrough.as_ref(),
                is_ws: self.framing.is_ws(),
                queued_bytes: eq.total_bytes(),
                queued_messages: self.queued_msgs.load(Ordering::Relaxed),
            },
            HandleFrameCaps {
                byte_cap: self.cap,
                message_cap: self.msg_cap,
            },
            msg,
        );
        match decision {
            HandleFrameDecision::Plain => eq.frame(msg),
            #[cfg(feature = "ws")]
            HandleFrameDecision::WebSocket => eq.frame_ws(msg, self.framing.is_masked()),
            #[cfg(not(feature = "ws"))]
            HandleFrameDecision::WebSocket => unreachable!("ws disabled"),
            HandleFrameDecision::TransformPassthrough { sentinel } => {
                eq.frame_prefixed(sentinel, msg);
            }
            HandleFrameDecision::Full => {
                self.above_lwm.store(true, Ordering::Relaxed);
                return TryFrameResult::Full;
            }
            HandleFrameDecision::Ineligible => return TryFrameResult::Ineligible,
        }
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        drop(eq);
        TryFrameResult::Ok
    }

    pub(crate) fn try_push_encoded(&self, chunks: &[Bytes]) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let bytes = chunks.iter().map(Bytes::len).sum();
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        if queued_msgs >= self.msg_cap
            || (queued_msgs > 0 && eq.total_bytes().saturating_add(bytes) >= self.cap)
        {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        eq.push_shared_chunks(chunks);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        drop(eq);
        self.signal_encoded();
        TryFrameResult::Ok
    }

    pub(crate) fn try_push_pre_framed_no_signal(&self, data: &[u8]) -> TryFrameResult {
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        if self.is_full(&eq) {
            self.above_lwm.store(true, Ordering::Relaxed);
            return TryFrameResult::Full;
        }
        eq.push_pre_framed(data);
        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        TryFrameResult::Ok
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
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        // Store one encoded fan-out message per entry so full-slot eviction
        // can remove the oldest whole message instead of an arbitrary chunk.
        let chunk = fanout_frame_chunk(frame);
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.dead.load(Ordering::Acquire) {
            return TryFrameResult::Dead;
        }
        let mut queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        while queued_msgs > 0
            && (eq.total_bytes().saturating_add(chunk.len()) >= self.cap
                || queued_msgs >= self.msg_cap)
        {
            if !eq.pop_oldest_unprotected_entry() {
                self.above_lwm.store(true, Ordering::Relaxed);
                return TryFrameResult::Full;
            }
            queued_msgs = queued_msgs.saturating_sub(1);
        }
        if protected {
            eq.push_raw_protected(vec![chunk]);
            self.fanout_dict_queued.store(true, Ordering::Release);
        } else {
            eq.push_raw(vec![chunk]);
        }
        queued_msgs += 1;
        self.queued_msgs.store(queued_msgs, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), queued_msgs);
        drop(eq);
        self.signal_encoded();
        TryFrameResult::Ok
    }

    pub(crate) fn signal_encoded(&self) {
        self.data_signal.mark();
    }

    #[inline]
    pub(crate) fn codec_profile(&self) -> Option<&CodecProfile> {
        self.codec_profile.as_ref()
    }

    #[inline]
    pub(crate) fn fanout_dict_shipped(&self) -> bool {
        self.fanout_dict_shipped.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn fanout_dict_queued_or_shipped(&self) -> bool {
        self.fanout_dict_queued.load(Ordering::Acquire) || self.fanout_dict_shipped()
    }

    #[inline]
    pub(crate) fn mark_fanout_dict_shipped(&self) {
        self.fanout_dict_queued.store(false, Ordering::Release);
        self.fanout_dict_shipped.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn fanout_active(&self) -> bool {
        self.fanout_active.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn deactivate_fanout(&self) -> bool {
        self.fanout_active
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn set_fanout_reactivation(&self, cb: FanOutReactivation) {
        *self
            .fanout_reactivation
            .lock()
            .expect("transmit_slot fanout_reactivation poisoned") = Some(cb);
    }

    /// Arena-only fast path: if all queued data is in the
    /// [`FrameBuffer`] arena (no external `Bytes`), copy the arena
    /// bytes into `out` and clear the arena, preserving its capacity.
    /// Returns `None` if the fast path does not apply.
    pub(crate) fn try_drain_arena_only(&self, out: &mut Vec<u8>) -> Option<DrainOutcome> {
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if !eq.has_arena_only() {
            return None;
        }
        out.extend_from_slice(eq.arena_bytes());
        eq.clear_arena();
        self.data_signal.begin_drain();
        self.queued_msgs.store(0, Ordering::Relaxed);
        let below_lwm = self.is_below_lwm(0, 0);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        let reactivate = below_lwm
            && self
                .fanout_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        drop(eq);
        self.clear_data_signal_and_rearm();
        if reactivate
            && let Some(cb) = self
                .fanout_reactivation
                .lock()
                .expect("transmit_slot fanout_reactivation poisoned")
                .clone()
        {
            cb(self.peer_id);
        }
        Some(DrainOutcome { space_available })
    }

    /// One caller-thread write of the queued bytes. Large payloads are
    /// written from their own buffers; the slot keeps any unwritten tail.
    pub(crate) fn try_direct_write(
        &self,
        write: impl FnOnce(&[io::IoSlice<'_>]) -> io::Result<usize>,
    ) -> io::Result<()> {
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let n = if eq.has_arena_only() {
            // Small messages: one contiguous slice, as before gathering.
            write(&[io::IoSlice::new(eq.arena_bytes())])?
        } else {
            let mut slices = [io::IoSlice::new(&[]); DIRECT_WRITE_SLICES];
            let count = eq.io_slices(&mut slices);
            write(&slices[..count])?
        };
        if n > 0 {
            eq.advance(n);
        }
        let eq_empty = eq.is_empty();
        let eq_bytes = eq.total_bytes();
        self.data_signal.begin_drain();
        if eq_empty {
            self.queued_msgs.store(0, Ordering::Relaxed);
        }
        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        let below_lwm = self.is_below_lwm(eq_bytes, queued_msgs);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        let reactivate = below_lwm
            && self
                .fanout_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        drop(eq);
        self.clear_data_signal_and_rearm();
        if space_available {
            self.space_available.notify_changed();
        }
        if reactivate
            && let Some(cb) = self
                .fanout_reactivation
                .lock()
                .expect("transmit_slot fanout_reactivation poisoned")
                .clone()
        {
            cb(self.peer_id);
        }

        Ok(())
    }

    pub(crate) fn drain(&self, buf: &mut Vec<Bytes>, max_chunks: usize) -> DrainOutcome {
        self.drain_with(buf, max_chunks, false)
    }

    /// Like [`Self::drain`], but moves arena bytes into `buf` without a
    /// copy. See [`FrameBuffer::drain_owned`].
    pub(crate) fn drain_owned(&self, buf: &mut Vec<Bytes>, max_chunks: usize) -> DrainOutcome {
        self.drain_with(buf, max_chunks, true)
    }

    fn drain_with(&self, buf: &mut Vec<Bytes>, max_chunks: usize, owned: bool) -> DrainOutcome {
        let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let before_chunks = buf.len();
        let protected_drained = if owned {
            eq.drain_owned(buf, max_chunks)
        } else {
            eq.drain(buf, max_chunks)
        };
        let eq_drained_chunks = buf.len() - before_chunks;
        let eq_empty = eq.is_empty();
        let eq_bytes = eq.total_bytes();
        self.data_signal.begin_drain();

        if protected_drained > 0 {
            self.mark_fanout_dict_shipped();
        }

        if eq_drained_chunks > 0 {
            #[allow(deprecated)]
            self.queued_msgs
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(eq_drained_chunks))
                })
                .ok();
        }

        if eq_empty {
            self.queued_msgs.store(0, Ordering::Relaxed);
        }
        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        let below_lwm = self.is_below_lwm(eq_bytes, queued_msgs);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        let reactivate = below_lwm
            && self
                .fanout_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        drop(eq);
        if eq_empty {
            self.clear_data_signal_and_rearm();
        }
        if reactivate
            && let Some(cb) = self
                .fanout_reactivation
                .lock()
                .expect("transmit_slot fanout_reactivation poisoned")
                .clone()
        {
            cb(self.peer_id);
        }
        DrainOutcome { space_available }
    }

    pub(crate) fn is_empty(&self) -> bool {
        let eq = self.eq.lock().expect("transmit_slot eq poisoned");
        eq.is_empty()
    }

    fn clear_data_signal_and_rearm(&self) {
        self.data_signal.clear_after(self.is_empty());
    }

    pub(crate) fn mark_dead(&self) {
        let mut ownership = self.direct_writer.get().map(|writer| writer.lock());
        if let Some(state) = ownership.as_mut() {
            state.close();
        }
        self.dead.store(true, Ordering::Release);
        let retired = {
            let mut eq = self.eq.lock().expect("transmit_slot eq poisoned");
            self.queued_msgs.store(0, Ordering::Relaxed);
            std::mem::replace(&mut *eq, FrameBuffer::one_shot())
        };
        self.fanout_dict_queued.store(false, Ordering::Relaxed);
        self.fanout_dict_shipped.store(false, Ordering::Relaxed);
        self.fanout_active.store(false, Ordering::Relaxed);
        self.above_lwm.store(false, Ordering::Relaxed);
        drop(ownership);
        self.data_signal.wake_all();
        self.space_available.notify_changed();
        drop(retired);
    }

    fn is_full(&self, eq: &FrameBuffer) -> bool {
        eq.total_bytes() >= self.cap || self.queued_msgs.load(Ordering::Relaxed) >= self.msg_cap
    }

    fn mark_above_lwm_if_needed(&self, queued_bytes: usize, queued_messages: usize) {
        if !self.is_below_lwm(queued_bytes, queued_messages) {
            self.above_lwm.store(true, Ordering::Relaxed);
        }
    }

    fn is_below_lwm(&self, queued_bytes: usize, queued_messages: usize) -> bool {
        queued_bytes <= self.cap / TRANSMIT_SLOT_LWM_DIVISOR
            && queued_messages <= self.msg_cap / TRANSMIT_SLOT_LWM_DIVISOR
    }
}

fn fanout_frame_chunk(frame: &FanOutFrame<'_>) -> Bytes {
    match frame {
        FanOutFrame::Arena(raw) => Bytes::copy_from_slice(raw),
        FanOutFrame::Chunks(chunks) if chunks.len() == 1 => chunks[0].clone(),
        FanOutFrame::Chunks(chunks) => {
            let len = chunks.iter().map(Bytes::len).sum();
            let mut buf = BytesMut::with_capacity(len);
            for chunk in *chunks {
                buf.extend_from_slice(chunk);
            }
            buf.freeze()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omq_proto::fan_out_frame::{build_fan_out_frame, clear_fan_out_frame};
    use tokio::time::{Duration, timeout};

    fn test_slot() -> Arc<PeerTransmitSlot> {
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            TRANSMIT_SLOT_CAP_DEFAULT,
            TRANSMIT_SLOT_MSG_CAP_DEFAULT,
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);
        slot
    }

    fn fanout_bytes(msg: &Message) -> Bytes {
        let mut eq = FrameBuffer::one_shot();
        let mut chunks = Vec::new();
        let frame = build_fan_out_frame(&mut eq, msg, &mut chunks, 1, 8 * 1024);
        let bytes = fanout_frame_chunk(&frame);
        clear_fan_out_frame(&mut eq, &mut chunks);
        bytes
    }

    #[test]
    fn transmit_slot_caps_queued_messages_independent_of_bytes() {
        let slot = test_slot();
        let msg = Message::from("x");

        for _ in 0..TRANSMIT_SLOT_MSG_CAP_DEFAULT {
            assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
        }
        assert_eq!(slot.try_encode(&msg), TryFrameResult::Full);

        let mut chunks = Vec::new();
        slot.drain(&mut chunks, 1024);
        assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
    }

    #[test]
    fn fanout_drop_oldest_slot_keeps_newest_messages() {
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            TRANSMIT_SLOT_CAP_DEFAULT,
            2,
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);

        let first = Message::single("first");
        let second = Message::single("second");
        let third = Message::single("third");

        let mut eq = FrameBuffer::one_shot();
        let mut chunks = Vec::new();
        for msg in [&first, &second, &third] {
            let frame = build_fan_out_frame(&mut eq, msg, &mut chunks, 1, 8 * 1024);
            assert_eq!(slot.try_push_fanout_drop_oldest(&frame), TryFrameResult::Ok);
            clear_fan_out_frame(&mut eq, &mut chunks);
        }

        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(actual, vec![fanout_bytes(&second), fanout_bytes(&third)]);
    }

    #[test]
    fn fanout_drop_oldest_slot_keeps_protected_entry() {
        let slot = PeerTransmitSlot::new(
            1,
            false,
            None,
            None,
            omq_proto::frame_buffer::ARENA_THRESHOLD,
            omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            TRANSMIT_SLOT_CAP_DEFAULT,
            1,
            crate::engine::framing::WireFraming::Zmtp,
        );
        slot.handshake_done.store(true, Ordering::Release);

        let dict = Message::single("dict");
        let payload = Message::single("payload");
        let mut eq = FrameBuffer::one_shot();
        let mut chunks = Vec::new();
        let dict_frame = build_fan_out_frame(&mut eq, &dict, &mut chunks, 1, 8 * 1024);
        assert_eq!(
            slot.try_push_protected_fanout_drop_oldest(&dict_frame),
            TryFrameResult::Ok
        );
        clear_fan_out_frame(&mut eq, &mut chunks);

        let payload_frame = build_fan_out_frame(&mut eq, &payload, &mut chunks, 1, 8 * 1024);
        assert_eq!(
            slot.try_push_fanout_drop_oldest(&payload_frame),
            TryFrameResult::Full
        );
        clear_fan_out_frame(&mut eq, &mut chunks);

        let mut actual = Vec::new();
        slot.drain(&mut actual, 1024);
        assert_eq!(actual, vec![fanout_bytes(&dict)]);
        assert!(slot.fanout_dict_shipped());
    }

    #[tokio::test]
    async fn transmit_slot_clear_rearms_when_nonempty() {
        let slot = test_slot();
        let msg = Message::from("x");

        assert_eq!(slot.try_encode(&msg), TryFrameResult::Ok);
        timeout(Duration::from_secs(1), slot.data_signal.ready())
            .await
            .expect("initial encode should notify");

        slot.clear_data_signal_and_rearm();
        timeout(Duration::from_secs(1), slot.data_signal.ready())
            .await
            .expect("nonempty slot should rearm after clear");
    }
}
