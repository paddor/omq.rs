//! Monitor: socket-like consumer of connection-lifecycle events.
//!
//! `Socket::monitor()` returns a [`MonitorStream`], a pull-style handle
//! (really a `broadcast::Receiver`) delivering owned [`MonitorEvent`]s.
//!
//! The underlying broadcast channel has finite capacity
//! ([`omq_proto::Options::monitor_capacity`], default 256).
//! If a subscriber lags behind, it observes a `Lagged` error and
//! resumes from the current event.
//!
//! The event types themselves live in `omq_proto::monitor` so other
//! backends share the wire-level data model.

use tokio::sync::broadcast;

use std::sync::{Arc, Mutex};

pub use omq_proto::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorTryRecvError,
    PeerCommandKind, PeerIdent, PeerInfo,
};

/// Pull-style monitor stream. Drop to stop receiving events.
///
/// Monitor events are diagnostic. Under sustained data-plane load they
/// may lag (see [`omq_proto::Options::monitor_capacity`]). Do not use
/// monitor events for readiness gating in production. Use
/// [`crate::Socket::wait_connected`] or [`crate::Socket::connections`] for
/// routing readiness, and [`crate::Socket::connections_changed`] to track
/// the connection set.
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
            Err(broadcast::error::TryRecvError::Lagged(n)) => Err(MonitorTryRecvError::Lagged(n)),
        }
    }
}

/// Internal publisher used by the socket driver.
#[derive(Debug, Clone)]
pub(crate) struct MonitorPublisher {
    tx: Arc<Mutex<Option<broadcast::Sender<MonitorEvent>>>>,
    capacity: usize,
}

impl MonitorPublisher {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            tx: Arc::new(Mutex::new(None)),
            capacity,
        }
    }

    pub(crate) fn publish(&self, event: MonitorEvent) {
        // `broadcast::Sender::send` only errors if there are no
        // subscribers, which is the common case when no one called
        // `Socket::monitor()`. Silently ignore.
        if let Some(tx) = self.tx.lock().expect("monitor publisher").as_ref() {
            let _ = tx.send(event);
        }
    }

    pub(crate) fn subscribe(&self) -> MonitorStream {
        let mut tx = self.tx.lock().expect("monitor publisher");
        let tx = tx.get_or_insert_with(|| {
            let (tx, _) = broadcast::channel(self.capacity);
            tx
        });
        MonitorStream::new(tx.subscribe())
    }
}
