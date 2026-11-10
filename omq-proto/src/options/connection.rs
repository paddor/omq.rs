//! Connection lifecycle: identity, reconnect, heartbeats, handshakes, linger.

use std::time::Duration;

use bytes::Bytes;

use super::Options;

/// Reconnection policy applied after a lost connection on `connect()` sockets.
/// Receiving a fatal handshake ERROR stops automatic retries under every policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReconnectPolicy {
    /// No reconnect; the connection is dropped permanently on failure.
    Disabled,
    /// Retry at a constant interval.
    Fixed(Duration),
    /// Exponential backoff, doubling on each retry.
    Exponential {
        /// Initial retry interval.
        min: Duration,
        /// Maximum retry interval.
        max: Duration,
    },
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        // Constant 100ms matches libzmq's `ZMQ_RECONNECT_IVL` default.
        // Users who want exponential backoff opt in via
        // `Options::reconnect(ReconnectPolicy::Exponential { .. })`.
        Self::Fixed(Duration::from_millis(100))
    }
}

impl Options {
    /// Set close linger to a finite duration.
    ///
    /// `Duration::ZERO` means drop queued outbound messages immediately.
    /// Non-zero values keep endpoints alive so queued sends can drain to
    /// existing or late peers before the deadline.
    #[must_use]
    pub fn linger(mut self, d: Duration) -> Self {
        self.linger = Some(d);
        self
    }

    /// Wait forever for queued outbound messages to drain on close/drop.
    ///
    /// This can wait forever if queued messages have no peer and no peer ever
    /// arrives. Use finite linger for services that need bounded shutdown.
    #[must_use]
    pub fn linger_forever(mut self) -> Self {
        self.linger = None;
        self
    }

    #[must_use]
    /// Set the ZMTP identity advertised during handshake.
    pub fn identity(mut self, id: impl Into<Bytes>) -> Self {
        self.identity = id.into();
        self
    }

    #[must_use]
    /// Set reconnect behavior for connect-side peers.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    #[must_use]
    /// Stop reconnecting when the remote side refuses the connection.
    pub fn reconnect_stop_conn_refused(mut self, stop: bool) -> Self {
        self.reconnect_stop_conn_refused = stop;
        self
    }

    #[must_use]
    /// Set interval between heartbeat PING commands.
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = Some(d);
        self
    }

    #[must_use]
    /// Set heartbeat TTL advertised to peers.
    pub fn heartbeat_ttl(mut self, d: Duration) -> Self {
        self.heartbeat_ttl = Some(d);
        self
    }

    #[must_use]
    /// Set max time to wait for peer heartbeat traffic before disconnecting.
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.heartbeat_timeout = Some(d);
        self
    }

    /// Set max time allowed to complete the ZMTP handshake.
    ///
    /// For encrypted mechanisms this also controls how long a stalled peer can
    /// occupy one `max_pending_handshakes` slot.
    #[must_use]
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    /// Set max simultaneous inbound byte-stream handshakes.
    ///
    /// This caps pre-auth TCP/IPC resource use. If full, new accepted peers
    /// are rejected before a peer driver is spawned and monitors receive
    /// `HandshakeFailed`.
    #[must_use]
    pub fn max_pending_handshakes(mut self, n: usize) -> Self {
        self.max_pending_handshakes = n;
        self
    }
}
