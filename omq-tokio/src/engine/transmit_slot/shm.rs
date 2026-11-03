// SHM records encoded per recipient into the slot's FrameBuffer.

#[cfg(target_os = "linux")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_proto::message::Message;

use super::{PeerTransmitSlot, TryFrameResult};

/// SHM record kinds published by per-recipient encoding.
#[derive(Debug, Clone, Copy)]
enum ShmRecord {
    Body,
    #[cfg(target_os = "linux")]
    Batch(omq_shm::BatchKind),
    #[cfg(target_os = "linux")]
    Multipart,
}

/// SHM encoding state, set once after a SHM handshake.
pub(super) struct SlotShm {
    #[cfg(target_os = "linux")]
    state: OnceLock<crate::transport::shm::ShmState>,
    /// The last encode found no free lease; cleared when credit returns.
    credit_blocked: AtomicBool,
}

impl SlotShm {
    pub(super) fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            state: OnceLock::new(),
            credit_blocked: AtomicBool::new(false),
        }
    }
}

impl PeerTransmitSlot {
    #[cfg(target_os = "linux")]
    pub(crate) fn set_shm(&self, state: crate::transport::shm::ShmState) {
        self.shm
            .state
            .set(state)
            .expect("SHM slot initialized once");
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub(crate) fn is_shm(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.shm.state.get().is_some()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn shm_credit_blocked(&self) -> bool {
        self.shm.credit_blocked.load(Ordering::Acquire)
    }

    pub(crate) fn shm_credit_returned(&self) {
        self.shm.credit_blocked.store(false, Ordering::Release);
        self.space_available.notify_changed();
        let eq = self.eq.lock().expect("transmit_slot eq poisoned");
        let below_lwm =
            self.is_below_lwm(eq.total_bytes(), self.queued_msgs.load(Ordering::Relaxed));
        let reactivate = self.claim_reactivation(below_lwm);
        drop(eq);
        if reactivate {
            self.notify_reactivation();
        }
    }

    /// Per-recipient SHM leases are created by the IO lane, after admission.
    pub(crate) fn try_encode_shm_no_signal(&self, message: &Message) -> TryFrameResult {
        self.try_encode_shm_record(message, 1, ShmRecord::Body)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn try_encode_shm_batch_no_signal(
        &self,
        message: &Message,
        count: usize,
        referenced: bool,
    ) -> TryFrameResult {
        let record = if referenced {
            ShmRecord::Batch(omq_shm::BatchKind::Referenced)
        } else {
            ShmRecord::Batch(omq_shm::BatchKind::Copied)
        };
        self.try_encode_shm_record(message, count, record)
    }

    /// Publish a record prepared once by the IO lane for every recipient.
    #[cfg(target_os = "linux")]
    pub(crate) fn try_encode_shm_multipart_no_signal(&self, record: &Message) -> TryFrameResult {
        self.try_encode_shm_record(record, 1, ShmRecord::Multipart)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn message_capacity(&self) -> usize {
        self.msg_cap
    }

    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn try_encode_shm_record(
        &self,
        message: &Message,
        count: usize,
        record: ShmRecord,
    ) -> TryFrameResult {
        #[cfg(target_os = "linux")]
        {
            let Some(state) = self.shm.state.get() else {
                return TryFrameResult::Ineligible;
            };
            if self.is_dead() {
                return TryFrameResult::Dead;
            }
            if !self.is_handshake_done() {
                return TryFrameResult::Ineligible;
            }
            // Wire-space wakes do not return borrowed SHM leases. Keep the
            // failed encode's credit snapshot until the driver wakes us for
            // returned credit instead of rescanning the payload pool.
            if self.shm.credit_blocked.load(Ordering::Acquire) {
                return TryFrameResult::Full;
            }
            let Some(mut eq) = self.lock_live() else {
                return TryFrameResult::Dead;
            };
            if eq.total_bytes() >= self.cap
                || self.queued_msgs.load(Ordering::Relaxed) + count > self.msg_cap
            {
                return self.full();
            }
            let mut state = state.lock().expect("SHM connection");
            let result = match record {
                ShmRecord::Body => state.encode_and_frame(message, &mut eq),
                ShmRecord::Batch(kind) => state.encode_batch_and_frame(message, kind, &mut eq),
                ShmRecord::Multipart => state.encode_multipart_and_frame(message, &mut eq),
            };
            drop(state);
            match result {
                Ok(true) => {
                    let queued = self.queued_msgs.fetch_add(count, Ordering::Relaxed) + count;
                    self.mark_above_lwm_if_needed(eq.total_bytes(), queued);
                    TryFrameResult::Ok
                }
                Ok(false) => {
                    self.shm.credit_blocked.store(true, Ordering::Release);
                    drop(eq);
                    // The driver must arm its futex wait even with no wire bytes.
                    self.signal_encoded();
                    TryFrameResult::Full
                }
                Err(_) => {
                    drop(eq);
                    self.mark_dead();
                    TryFrameResult::Dead
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (message, count, record);
            TryFrameResult::Ineligible
        }
    }
}
