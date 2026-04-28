//! Monitor: socket-like consumer of connection-lifecycle events.
//!
//! `Socket::monitor()` returns a [`MonitorStream`], a pull-style handle
//! (really a `broadcast::Receiver`) delivering owned [`MonitorEvent`]s.
//!
//! The underlying broadcast channel has finite capacity (default 64).
//! If a subscriber lags behind, it observes a `Lagged` error and
//! resumes from the current event.
//!
//! The event types themselves live in `omq_proto::monitor` so other
//! backends share the wire-level data model.

use tokio::sync::broadcast;

pub use omq_proto::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorTryRecvError,
    PeerCommandKind, PeerIdent, PeerInfo,
};

/// Capacity of the per-socket monitor broadcast channel.
pub(crate) const MONITOR_CAPACITY: usize = 64;

/// Pull-style monitor stream. Drop to stop receiving events.
#[derive(Debug)]
pub struct MonitorStream {
    rx: broadcast::Receiver<MonitorEvent>,
}

impl MonitorStream {
    pub(crate) fn new(rx: broadcast::Receiver<MonitorEvent>) -> Self {
        Self { rx }
    }

    /// Receive the next event. Returns `Ok(event)` on success, or an error
    /// when the socket has closed or we lagged.
    pub async fn recv(&mut self) -> Result<MonitorEvent, MonitorRecvError> {
        match self.rx.recv().await {
            Ok(e) => Ok(e),
            Err(broadcast::error::RecvError::Closed) => Err(MonitorRecvError::Closed),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(MonitorRecvError::Lagged(n)),
        }
    }

    /// Try to receive without waiting.
    pub fn try_recv(&mut self) -> Result<MonitorEvent, MonitorTryRecvError> {
        match self.rx.try_recv() {
            Ok(e) => Ok(e),
            Err(broadcast::error::TryRecvError::Empty) => Err(MonitorTryRecvError::Empty),
            Err(broadcast::error::TryRecvError::Closed) => Err(MonitorTryRecvError::Closed),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Err(MonitorTryRecvError::Lagged(n))
            }
        }
    }
}

/// Internal publisher used by the socket driver.
#[derive(Debug, Clone)]
pub(crate) struct MonitorPublisher {
    tx: broadcast::Sender<MonitorEvent>,
}

impl MonitorPublisher {
    pub(crate) fn new() -> Self {
        let (tx, _) = broadcast::channel(MONITOR_CAPACITY);
        Self { tx }
    }

    pub(crate) fn publish(&self, event: MonitorEvent) {
        // `broadcast::Sender::send` only errors if there are no
        // subscribers, which is the common case when no one called
        // `Socket::monitor()`. Silently ignore.
        let _ = self.tx.send(event);
    }

    pub(crate) fn subscribe(&self) -> MonitorStream {
        MonitorStream::new(self.tx.subscribe())
    }
}
