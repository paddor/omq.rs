//! Send and receive state owned by the application thread.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use omq_tokio::engine::StateSignal;

use super::RecvConsumers;
use crate::local_cell::LocalCell;

#[derive(Debug)]
pub(crate) struct SendState {
    /// Accumulator for SNDMORE multipart assembly.
    pub accum: LocalCell<Vec<Bytes>>,
    /// `accum` holds frames kept after the final frame would block.
    pub retry: LocalCell<bool>,
    /// Frozen inverse of `ZMQ_IMMEDIATE`, captured at first bind/connect.
    /// Read only from the app-facing socket thread after materialization.
    pub queue_without_ready_peer: LocalCell<bool>,
}

impl SendState {
    pub(super) fn new(queue_without_ready_peer: bool) -> Self {
        Self {
            accum: LocalCell::new(Vec::new()),
            retry: LocalCell::new(false),
            queue_without_ready_peer: LocalCell::new(queue_without_ready_peer),
        }
    }

    pub(super) fn allow_thread_migration(&self) {
        self.accum.allow_thread_migration();
        self.retry.allow_thread_migration();
        self.queue_without_ready_peer.allow_thread_migration();
    }
}

#[derive(Debug)]
pub(crate) struct RecvState {
    /// Leftover frames from a multipart recv (RCVMORE).
    pub drain: Mutex<VecDeque<Bytes>>,
    /// True when `drain` is non-empty. Checked without the lock so the
    /// mutex is skipped entirely on the common single-frame recv path.
    pub drain_nonempty: AtomicBool,
    /// Dual yring consumers. `fast` is filled directly by the first
    /// peer's `ConnectionDriver` (bypasses `async_channel` + recv pump).
    /// `pump` is filled by the recv pump task for second+ peers.
    /// Accessed only from the `zmq_recv` caller thread.
    pub consumers: LocalCell<Option<RecvConsumers>>,
    /// Dedicated receive queue for ZAP-authenticated sockets. Each item keeps
    /// the authenticated peer properties attached to its message.
    pub authenticated:
        LocalCell<Option<tokio::sync::mpsc::Receiver<omq_tokio::engine::AuthenticatedRecvItem>>>,
    /// Remaining frames and properties from a partially received
    /// authenticated multipart message.
    pub authenticated_drain: LocalCell<VecDeque<(Bytes, Arc<omq_tokio::proto::PeerProperties>)>>,
    /// Backpressure: recv pump waits on this when the recv ring is full.
    pub space: OnceLock<Arc<StateSignal>>,
    /// Shared config for recycling the recv fast yring on peer churn.
    pub sink_config: OnceLock<Arc<omq_tokio::engine::RecvSinkConfig>>,
    pub pump: OnceLock<tokio::task::JoinHandle<()>>,
}

impl RecvState {
    pub(super) fn new() -> Self {
        Self {
            drain: Mutex::new(VecDeque::new()),
            drain_nonempty: AtomicBool::new(false),
            consumers: LocalCell::new(None),
            authenticated: LocalCell::new(None),
            authenticated_drain: LocalCell::new(VecDeque::new()),
            space: OnceLock::new(),
            sink_config: OnceLock::new(),
            pump: OnceLock::new(),
        }
    }

    pub(super) fn allow_thread_migration(&self) {
        self.consumers.allow_thread_migration();
        self.authenticated.allow_thread_migration();
        self.authenticated_drain.allow_thread_migration();
    }
}
