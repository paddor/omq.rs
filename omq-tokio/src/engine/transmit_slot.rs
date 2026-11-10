// Plain TCP latency writes share admission and write ownership with the
// connection driver. The slot retains an accepted direct write's tail;
// queued messages cannot pass it while the driver waits for writability.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use bytes::Bytes;

use super::codec::CodecProfile;
use super::framing::WireFraming;
use super::signal::{DataSignal, StateSignal};
use omq_proto::copy_stats::{self, Site};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::handle_frame::{
    HandleFrameCaps, HandleFrameDecision, HandleFrameState, decide_handle_frame,
};
use omq_proto::message::Message;

use fan_out::FanOutState;
use shm::SlotShm;

mod fan_out;
mod shm;

pub(crate) const TRANSMIT_SLOT_CAP_DEFAULT: usize = 512 * 1024;

#[cfg(test)]
pub(crate) const TRANSMIT_SLOT_MSG_CAP_DEFAULT: usize = 1000;

const TRANSMIT_SLOT_LWM_DIVISOR: usize = 2;

/// Slices per direct write. The latency path queues few messages.
const DIRECT_WRITE_SLICES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryFrameResult {
    Ok,
    Dead,
    Full,
    Ineligible,
}

pub(crate) struct PeerTransmitSlot {
    eq: Mutex<FrameBuffer>,
    direct_writer: OnceLock<Arc<crate::socket::direct_writer::DirectTcpWriter>>,
    cap: usize,
    msg_cap: usize,
    pub(crate) data_signal: DataSignal,
    pub(crate) space_available: Arc<StateSignal>,
    handshake_done: AtomicBool,
    pub(crate) has_transform: bool,
    codec_profile: Option<CodecProfile>,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    framing: WireFraming,
    dead: AtomicBool,
    pub(crate) peer_id: u64,
    queued_msgs: AtomicUsize,
    above_lwm: AtomicBool,
    fan_out: FanOutState,
    shm: SlotShm,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DrainOutcome {
    pub(crate) space_available: bool,
}

/// Wakes owed after a drain, decided while `eq` is held.
#[derive(Debug, Clone, Copy)]
struct DrainSettle {
    space_available: bool,
    reactivate: bool,
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

/// Encoding and capacity settings of one peer's transmit slot.
#[derive(Debug, Clone)]
pub(crate) struct SlotConfig {
    pub(crate) has_transform: bool,
    pub(crate) codec_profile: Option<CodecProfile>,
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    pub(crate) arena_threshold: usize,
    pub(crate) arena_cap: usize,
    /// Queued byte limit.
    pub(crate) cap: usize,
    /// Queued message limit; at least one.
    pub(crate) msg_cap: usize,
    pub(crate) framing: WireFraming,
}

#[cfg(test)]
impl Default for SlotConfig {
    fn default() -> Self {
        Self {
            has_transform: false,
            codec_profile: None,
            transform_passthrough: None,
            arena_threshold: omq_proto::frame_buffer::ARENA_THRESHOLD,
            arena_cap: omq_proto::frame_buffer::ARENA_INITIAL_CAP,
            cap: TRANSMIT_SLOT_CAP_DEFAULT,
            msg_cap: TRANSMIT_SLOT_MSG_CAP_DEFAULT,
            framing: WireFraming::Zmtp,
        }
    }
}

impl PeerTransmitSlot {
    pub(crate) fn with_config(peer_id: u64, config: SlotConfig) -> Arc<Self> {
        let SlotConfig {
            has_transform,
            codec_profile,
            transform_passthrough,
            arena_threshold,
            arena_cap,
            cap,
            msg_cap,
            framing,
        } = config;

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
            above_lwm: AtomicBool::new(false),
            fan_out: FanOutState::new(),
            shm: SlotShm::new(),
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

    #[inline]
    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn is_handshake_done(&self) -> bool {
        self.handshake_done.load(Ordering::Acquire)
    }

    /// Open the slot to caller-thread encoding once the codec is ready.
    pub(crate) fn mark_handshake_done(&self) {
        self.handshake_done.store(true, Ordering::Release);
    }

    pub(crate) fn set_direct_writer(
        &self,
        writer: Arc<crate::socket::direct_writer::DirectTcpWriter>,
    ) {
        self.direct_writer.get_or_init(|| writer);
    }

    pub(crate) fn direct_writer(
        &self,
    ) -> Option<&Arc<crate::socket::direct_writer::DirectTcpWriter>> {
        self.direct_writer.get()
    }

    pub(crate) fn try_encode_without_signal(&self, msg: &Message) -> TryFrameResult {
        if self.is_dead() {
            return TryFrameResult::Dead;
        }

        if !self.is_handshake_done() {
            return TryFrameResult::Ineligible;
        }

        let Some(mut eq) = self.lock_live() else {
            return TryFrameResult::Dead;
        };

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
            HandleFrameDecision::WebSocket => eq.frame_ws(msg, self.framing.ws_send_role()),
            #[cfg(not(feature = "ws"))]
            HandleFrameDecision::WebSocket => unreachable!("ws disabled"),
            HandleFrameDecision::TransformPassthrough { sentinel } => {
                eq.frame_prefixed(sentinel, msg);
            }
            HandleFrameDecision::Full => {
                return self.full();
            }
            HandleFrameDecision::Ineligible => return TryFrameResult::Ineligible,
        }

        self.queued_msgs.fetch_add(1, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        drop(eq);

        TryFrameResult::Ok
    }

    #[inline]
    pub(crate) fn signal_encoded(&self) {
        self.data_signal.mark();
    }

    #[inline]
    pub(crate) fn codec_profile(&self) -> Option<&CodecProfile> {
        self.codec_profile.as_ref()
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

        copy_stats::record(Site::ArenaDrain, eq.arena_bytes().len());
        out.extend_from_slice(eq.arena_bytes());
        eq.clear_arena();

        self.data_signal.begin_drain();
        self.queued_msgs.store(0, Ordering::Relaxed);
        let settled = self.settle_after_drain(0, 0);
        drop(eq);
        self.clear_data_signal_and_rearm();

        if settled.reactivate {
            self.notify_reactivation();
        }

        Some(DrainOutcome {
            space_available: settled.space_available,
        })
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
        let settled = self.settle_after_drain(eq_bytes, queued_msgs);
        drop(eq);
        self.clear_data_signal_and_rearm();

        if settled.space_available {
            self.space_available.notify_changed();
        }

        if settled.reactivate {
            self.notify_reactivation();
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
        let drained = if owned {
            eq.drain_owned(buf, max_chunks)
        } else {
            eq.drain(buf, max_chunks)
        };

        let eq_empty = eq.is_empty();
        let eq_bytes = eq.total_bytes();
        self.data_signal.begin_drain();

        if drained.protected > 0 {
            self.mark_fanout_dict_shipped();
        }

        if drained.entries > 0 {
            #[allow(deprecated)]
            self.queued_msgs
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(drained.entries))
                })
                .ok();
        }

        if eq_empty {
            self.queued_msgs.store(0, Ordering::Relaxed);
        }

        let queued_msgs = self.queued_msgs.load(Ordering::Relaxed);
        let settled = self.settle_after_drain(eq_bytes, queued_msgs);
        drop(eq);

        if eq_empty {
            self.clear_data_signal_and_rearm();
        }

        if settled.reactivate {
            self.notify_reactivation();
        }

        DrainOutcome {
            space_available: settled.space_available,
        }
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

        self.fan_out.retire();
        self.above_lwm.store(false, Ordering::Relaxed);
        drop(ownership);
        self.data_signal.wake_all();
        self.space_available.notify_changed();
        drop(retired);
    }

    /// Lock the queue, or `None` if `mark_dead` won the race for it.
    #[inline]
    fn lock_live(&self) -> Option<MutexGuard<'_, FrameBuffer>> {
        let eq = self.eq.lock().expect("transmit_slot eq poisoned");
        if self.is_dead() { None } else { Some(eq) }
    }

    /// Refuse a message for lack of credit; the next drain below the LWM
    /// reports space.
    #[inline]
    fn full(&self) -> TryFrameResult {
        self.above_lwm.store(true, Ordering::Relaxed);
        TryFrameResult::Full
    }

    /// Record `queued` messages after a push. Count writers are serialized
    /// by `eq`.
    #[inline]
    fn commit(&self, eq: &FrameBuffer, queued: usize) {
        self.queued_msgs.store(queued, Ordering::Relaxed);
        self.mark_above_lwm_if_needed(eq.total_bytes(), queued);
    }

    /// Decide under `eq` whether a drain freed space and reactivates fan-out.
    #[inline]
    fn settle_after_drain(&self, queued_bytes: usize, queued_messages: usize) -> DrainSettle {
        let below_lwm = self.is_below_lwm(queued_bytes, queued_messages);
        let space_available = below_lwm && self.above_lwm.swap(false, Ordering::AcqRel);
        DrainSettle {
            space_available,
            reactivate: self.claim_reactivation(below_lwm),
        }
    }

    #[inline]
    fn is_full(&self, eq: &FrameBuffer) -> bool {
        eq.total_bytes() >= self.cap || self.queued_msgs.load(Ordering::Relaxed) >= self.msg_cap
    }

    #[inline]
    fn mark_above_lwm_if_needed(&self, queued_bytes: usize, queued_messages: usize) {
        if !self.is_below_lwm(queued_bytes, queued_messages) {
            self.above_lwm.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    fn is_below_lwm(&self, queued_bytes: usize, queued_messages: usize) -> bool {
        queued_bytes <= self.cap / TRANSMIT_SLOT_LWM_DIVISOR
            && queued_messages <= self.msg_cap / TRANSMIT_SLOT_LWM_DIVISOR
    }
}

#[cfg(test)]
mod tests;
