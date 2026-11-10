//! Shared outbound flow-control knobs.
//!
//! The backend drains shared outbound queues in batches and flushes via
//! a single writev. Two caps bound each batch so one busy consumer can
//! never starve its siblings:
//!
//! - a **message cap** ([`fair_share`]): each driver takes at most its
//!   fair share of the queued messages, leaving the rest for the other
//!   peers draining the same queue.
//! - a **byte cap** ([`max_batch_bytes`]): independent of message count,
//!   stop pulling once the batch has buffered this many bytes so a few
//!   large messages don't monopolize the writev or outgrow the kernel
//!   send buffer.
//!
//! [`DrainBudget`] encodes the dual-cap invariant as a type: every
//! drain loop in the codebase composes from it so the budget is
//! structural, not ad-hoc.
//!
//! Per-peer yield intervals and direct-encode admission caps stay in the
//! backend because they are runtime-specific.

use std::sync::OnceLock;

/// Dual message-count / byte-count drain budget.
///
/// Every drain loop that processes data-plane items composes from this
/// type. The invariant (always cap both message count AND byte count)
/// is structural: `new` requires both caps.
///
/// After exhausting either cap the loop must yield and service
/// control-plane work before continuing data.
#[derive(Debug, Clone)]
pub struct DrainBudget {
    msgs: usize,
    bytes: usize,
    max_msgs: usize,
    max_bytes: usize,
}

impl DrainBudget {
    /// 256 msgs / 2 MiB: shard workers, deferred fan-out.
    pub const WORKER: Self = Self::new(256, 2 * 1024 * 1024);

    /// 1024 iterations / 1 MiB: transmit-slot drain path. Each iteration
    /// may yield multiple messages, so the "msgs" dimension counts
    /// drain rounds, not individual messages.
    pub const WIRE_DRAIN: Self = Self::new(1024, 1024 * 1024);

    /// Create a drain budget bounded by message count and payload bytes.
    #[must_use]
    pub const fn new(max_msgs: usize, max_bytes: usize) -> Self {
        Self {
            msgs: 0,
            bytes: 0,
            max_msgs,
            max_bytes,
        }
    }

    /// Account one item. Returns `true` while budget remains.
    #[must_use]
    pub fn account(&mut self, byte_len: usize) -> bool {
        self.account_many(1, byte_len)
    }

    /// Account a batch's logical item count and total work bytes.
    /// Returns `true` while both budgets remain.
    #[must_use]
    pub fn account_many(&mut self, messages: usize, byte_len: usize) -> bool {
        self.msgs = self.msgs.saturating_add(messages);
        self.bytes = self.bytes.saturating_add(byte_len);
        !self.exhausted()
    }

    #[must_use]
    /// Returns whether either cap has been reached.
    pub fn exhausted(&self) -> bool {
        self.msgs >= self.max_msgs || self.bytes >= self.max_bytes
    }

    /// Clear the accounted message and byte counts.
    pub fn reset(&mut self) {
        self.msgs = 0;
        self.bytes = 0;
    }

    #[must_use]
    /// Number of accounted items in the current batch.
    pub fn msgs(&self) -> usize {
        self.msgs
    }

    #[must_use]
    /// Remaining item count before the message cap is exhausted.
    pub fn remaining_msgs(&self) -> usize {
        self.max_msgs.saturating_sub(self.msgs)
    }

    #[must_use]
    /// Number of accounted bytes in the current batch.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Max bytes one shared-queue batch buffers before flushing.
///
/// 128 KiB bounds encode-before-write bursts while retaining enough data
/// for efficient writev calls. Larger caps add latency without reliable
/// throughput gains once the kernel send buffer is the bottleneck.
///
/// Override at runtime via `OMQ_BATCH_BYTES`. Read once and cached.
#[must_use]
pub fn max_batch_bytes() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OMQ_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128 * 1024)
    })
}

/// Fair share of the current queue for one consumer.
///
/// Single peer: full batch (`cap`), no competition. Multiple peers: each
/// consumer takes at most `queue_len / peers` to leave work for the
/// others, but always at least 1 and never more than `cap`.
#[must_use]
pub fn fair_share(queue_len: usize, peers: usize, cap: usize) -> usize {
    if peers <= 1 {
        return cap;
    }

    (queue_len / peers).clamp(1, cap)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod batch_tests;
