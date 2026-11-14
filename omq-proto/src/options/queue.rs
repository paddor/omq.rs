//! Queue capacity: HWM, PEER queue budgets, and mute behavior.

use super::{MAX_QUEUE_CAPACITY, Options};

/// Queue capacity in messages for `hwm`, clamped to `1..=MAX_QUEUE_CAPACITY`.
#[must_use]
pub fn queue_capacity(hwm: u32) -> usize {
    usize::try_from(hwm)
        .unwrap_or(MAX_QUEUE_CAPACITY)
        .clamp(1, MAX_QUEUE_CAPACITY)
}

/// What to do when native send HWM is reached and a new message arrives.
///
/// Native bound no-peer round-robin sockets mute immediately. Connected
/// no-peer round-robin sockets queue into a pre-ready pipe until `send_hwm`
/// is reached, then apply this policy. Native OMQ has no `ZMQ_IMMEDIATE`
/// option; `omq-libzmq` implements `ZMQ_IMMEDIATE` at the C layer.
///
/// `PUB`, `XPUB`, and `RADIO` honor `DropOldest` for per-peer fan-out
/// queues. Other fan-out sockets keep the native drop-newest behavior unless
/// `xpub_nodrop` asks them to wait. A subscriber that is not served from a
/// shared frame queue, such as a WebSocket or CURVE peer, has an inbox of
/// at most 64 messages and drops the newest message when it is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnMute {
    /// Block the sender until room is available.
    ///
    /// Ignored by fan-out sockets (`PUB`, `XPUB`, `RADIO`), which drop on
    /// mute unless `xpub_nodrop` is set.
    #[default]
    Block,
    /// Drop the incoming message silently.
    DropNewest,
    /// Drop the oldest queued message, then enqueue the new one.
    DropOldest,
}

impl Options {
    /// Send queue capacity in messages, from `send_hwm` via [`queue_capacity`].
    #[must_use]
    pub fn send_queue_capacity(&self) -> usize {
        queue_capacity(self.send_hwm)
    }

    /// Receive queue capacity in messages, from `recv_hwm` via [`queue_capacity`].
    #[must_use]
    pub fn recv_queue_capacity(&self) -> usize {
        queue_capacity(self.recv_hwm)
    }

    /// Set send-side HWM as a message count.
    ///
    /// This is not a byte limit. Large messages count the same as small
    /// messages. Native OMQ may hold more than this value across multiple
    /// connect-side pre-ready pipes, per-peer pipes, fan-out lane rings, and
    /// transmit slots.
    #[must_use]
    pub fn send_hwm(mut self, hwm: u32) -> Self {
        self.send_hwm = hwm;
        self
    }

    #[must_use]
    /// Set receive-side HWM as a message count.
    ///
    /// This bounds complete messages queued for application receive. It is
    /// not a byte limit.
    pub fn recv_hwm(mut self, hwm: u32) -> Self {
        self.recv_hwm = hwm;
        self
    }

    /// Set the native PEER queue capacity per sending handle and destination.
    /// See [`Self::peer_send_queue_capacity`] for rounding and shared limits.
    #[must_use]
    pub fn peer_send_queue_capacity(mut self, messages: usize) -> Self {
        self.peer_send_queue_capacity = messages;
        self
    }

    /// Set the native PEER sending-handle limit per destination.
    /// See [`Self::peer_max_senders`] for retired queue accounting.
    #[must_use]
    pub fn peer_max_senders(mut self, senders: usize) -> Self {
        self.peer_max_senders = senders;
        self
    }

    /// Set the native PEER receive-peer limit per socket.
    /// See [`Self::peer_max_peers`] for connection admission behavior.
    #[must_use]
    pub fn peer_max_peers(mut self, peers: usize) -> Self {
        self.peer_max_peers = peers;
        self
    }

    /// Set the native PEER retained-message budget per receiving connection.
    /// See [`Self::peer_recv_max_messages`] for claim and HWM accounting.
    #[must_use]
    pub fn peer_recv_max_messages(mut self, messages: usize) -> Self {
        self.peer_recv_max_messages = messages;
        self
    }

    /// Set the native PEER outbound byte budget for each connection.
    /// See [`Self::peer_send_max_bytes`] for accounting and defaults.
    #[must_use]
    pub fn peer_send_max_bytes(mut self, bytes: usize) -> Self {
        self.peer_send_max_bytes = Some(bytes);
        self
    }

    /// Set the native PEER receive byte budget for each connection.
    /// See [`Self::peer_recv_max_bytes`] for accounting and defaults.
    #[must_use]
    pub fn peer_recv_max_bytes(mut self, bytes: usize) -> Self {
        self.peer_recv_max_bytes = Some(bytes);
        self
    }

    #[must_use]
    /// Set behavior when send HWM mutes the socket or peer.
    pub fn on_mute(mut self, m: OnMute) -> Self {
        self.on_mute = m;
        self
    }

    #[must_use]
    /// Keep only the most recent inbound message.
    pub fn conflate(mut self, c: bool) -> Self {
        self.conflate = c;
        self
    }
}
