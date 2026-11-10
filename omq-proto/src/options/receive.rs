//! Receive path: spinning, batching, pools, rate limits, and size limits.

use std::time::Duration;

use super::Options;

/// Token-bucket message rate limit.
///
/// `messages_per_second` controls refill speed. `burst` is the maximum token
/// capacity. Exceeding either receive limit closes the offending connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageRateLimit {
    /// Sustained complete messages allowed per second.
    pub messages_per_second: u32,
    /// Maximum immediate message burst.
    pub burst: u32,
}

impl MessageRateLimit {
    /// Create a message rate limit.
    #[must_use]
    pub const fn new(messages_per_second: u32, burst: u32) -> Self {
        Self {
            messages_per_second,
            burst,
        }
    }
}

impl Options {
    /// Set the busy-wait budget before parking in native blocking receives.
    /// Zero disables spinning. See [`Self::recv_spin`] for scope and tradeoffs.
    #[must_use]
    pub fn recv_spin(mut self, budget: Duration) -> Self {
        self.recv_spin = budget;
        self
    }

    /// Allow bounded per-connection bursts when receiving batches.
    /// See [`Self::recv_batching`] for supported socket types and semantics.
    #[must_use]
    pub fn recv_batching(mut self, enabled: bool) -> Self {
        self.recv_batching = enabled;
        self
    }

    /// Recycle native byte-stream receive frame tables through this bounded pool.
    #[must_use]
    pub fn recv_message_pool(mut self, pool: crate::message::MessagePool) -> Self {
        self.recv_message_pool = Some(pool);
        self
    }

    /// Set explicit receive payload size classes, shared across connections.
    /// Pools supply destinations for incomplete bodies, fragmentation, and
    /// decoders that support fixed storage. Contiguous buffered bodies keep their
    /// byte views. Inline bodies skip checkout; exhaustion and oversized bodies
    /// use owned allocations. Inproc transfers payload owners unchanged.
    #[must_use]
    pub fn recv_payload_pool(mut self, pool: crate::PayloadPool) -> Self {
        self.recv_payload_pool = Some(pool);
        self
    }

    /// Set the per-connection receive message rate and burst.
    #[must_use]
    pub fn recv_rate_limit(mut self, messages_per_second: u32, burst: u32) -> Self {
        self.recv_rate_limit = Some(MessageRateLimit::new(messages_per_second, burst));
        self
    }

    /// Set the aggregate receive message rate and burst per remote IP.
    #[must_use]
    pub fn recv_ip_rate_limit(mut self, messages_per_second: u32, burst: u32) -> Self {
        self.recv_ip_rate_limit = Some(MessageRateLimit::new(messages_per_second, burst));
        self
    }

    #[must_use]
    /// Set max allowed size for one complete message.
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    /// Set the wire-payload size at which the recv path switches to a
    /// sized one-shot read. See the field-level docs on
    /// [`large_message_threshold`](Self::large_message_threshold) for
    /// the trade-offs. Pass `0` to fall back to the multi-shot path
    /// for every frame; the threshold is treated as `usize::MAX` in
    /// that case.
    #[must_use]
    pub fn large_message_threshold(mut self, n: usize) -> Self {
        self.large_message_threshold = if n == 0 { None } else { Some(n) };
        self
    }

    /// Disable the one-shot recv switch entirely; the multi-shot path
    /// is used for every inbound frame regardless of size.
    #[must_use]
    pub fn disable_large_message_path(mut self) -> Self {
        self.large_message_threshold = None;
        self
    }
}
