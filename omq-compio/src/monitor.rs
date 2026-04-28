//! Monitor: socket-like consumer of connection-lifecycle events.
//!
//! `Socket::monitor()` returns a [`MonitorStream`] that delivers
//! [`MonitorEvent`]s. Multiple monitors can be active simultaneously.
//!
//! Implementation: each subscriber gets a bounded `flume` channel plus
//! a lagged counter. Publishing iterates subscribers and `try_send`s;
//! when a subscriber's queue is full the new event is dropped and the
//! lagged counter bumps. The next `recv` returns `Lagged(n)` so the
//! consumer learns it missed `n` events. Disconnected subscribers are
//! pruned lazily on the next publish.
//!
//! Event types come from `omq_proto::monitor` so backends share the
//! same data model.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub use omq_proto::monitor::{
    ConnectionStatus, DisconnectReason, MonitorEvent, MonitorRecvError, MonitorTryRecvError,
    PeerCommandKind, PeerIdent, PeerInfo,
};

/// Per-subscriber bounded queue capacity.
pub(crate) const MONITOR_CAPACITY: usize = 64;

struct MonitorSink {
    tx: flume::Sender<MonitorEvent>,
    lagged: Arc<AtomicU64>,
}

#[derive(Clone)]
pub(crate) struct MonitorPublisher {
    sinks: Arc<Mutex<Vec<MonitorSink>>>,
}

impl std::fmt::Debug for MonitorPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitorPublisher").finish_non_exhaustive()
    }
}

impl MonitorPublisher {
    pub(crate) fn new() -> Self {
        Self {
            sinks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn publish(&self, event: MonitorEvent) {
        let mut sinks = self.sinks.lock().expect("monitor sinks");
        sinks.retain(|sink| !sink.tx.is_disconnected());
        for sink in sinks.iter() {
            match sink.tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(flume::TrySendError::Full(_)) => {
                    sink.lagged.fetch_add(1, Ordering::Relaxed);
                }
                Err(flume::TrySendError::Disconnected(_)) => {}
            }
        }
    }

    pub(crate) fn subscribe(&self) -> MonitorStream {
        let (tx, rx) = flume::bounded(MONITOR_CAPACITY);
        let lagged = Arc::new(AtomicU64::new(0));
        self.sinks
            .lock()
            .expect("monitor sinks")
            .push(MonitorSink {
                tx,
                lagged: lagged.clone(),
            });
        MonitorStream { rx, lagged }
    }
}

/// Pull-style monitor stream. Drop to stop receiving events.
#[derive(Debug)]
pub struct MonitorStream {
    rx: flume::Receiver<MonitorEvent>,
    lagged: Arc<AtomicU64>,
}

impl MonitorStream {
    /// Receive the next event. Returns `Lagged(n)` if the queue
    /// overflowed since the last call; subsequent calls deliver
    /// fresh events.
    pub async fn recv(&mut self) -> Result<MonitorEvent, MonitorRecvError> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            return Err(MonitorRecvError::Lagged(n));
        }
        match self.rx.recv_async().await {
            Ok(e) => Ok(e),
            Err(_) => Err(MonitorRecvError::Closed),
        }
    }

    /// Try to receive without waiting.
    pub fn try_recv(&mut self) -> Result<MonitorEvent, MonitorTryRecvError> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            return Err(MonitorTryRecvError::Lagged(n));
        }
        match self.rx.try_recv() {
            Ok(e) => Ok(e),
            Err(flume::TryRecvError::Empty) => Err(MonitorTryRecvError::Empty),
            Err(flume::TryRecvError::Disconnected) => Err(MonitorTryRecvError::Closed),
        }
    }
}
