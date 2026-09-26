//! Socket-owned PEER fan-in. Each connection owns a bounded producer;
//! ordinary receives share one fair receiver with reconnect fencing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::Wake;

use bytes::Bytes;
use fanring::{mpsc, teardown::Coordinated};
use omq_proto::flow::DrainBudget;
use omq_proto::{Error, Message, Result};
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::engine::signal::{DataSignal, StateSignal};

mod budget;
mod sink;
#[cfg(test)]
mod tests;
use budget::{Budget, QueuedMessage};
pub(crate) use sink::PeerRecvSink;

#[derive(Debug, Clone, Copy)]
struct RecvLimits {
    // Includes disconnected queues awaiting draining.
    peers: usize,
    messages: usize,
    // Payload bytes plus per-frame storage. Identity is stored once per peer.
    bytes: usize,
}

impl Default for RecvLimits {
    fn default() -> Self {
        Self {
            peers: 128,
            messages: 8192,
            bytes: 64 * 1024 * 1024,
        }
    }
}

/// Socket-owned receiver, serialized by the ordinary receive path.
#[derive(Debug)]
pub(crate) struct PeerReceiver {
    shared: Arc<RecvShared>,
    receiver: Option<mpsc::Receiver<RecvItem, Coordinated>>,
    yield_pending: bool,
}

#[derive(Debug)]
struct RecvShared {
    registration: Mutex<Registration>,
    closed: AtomicBool,
    data: Arc<DataSignal>,
    blocking: Arc<super::recv::BlockingRecvWaker>,
    budget: Arc<Budget>,
}

/// Cold registration and shutdown only. Ready-peer selection belongs to fanring.
#[derive(Debug)]
struct Registration {
    registrar: mpsc::Sender<RecvItem, Coordinated>,
    states: Vec<Weak<QueueState>>,
}

impl RecvShared {
    fn mark(&self) {
        self.data.mark();
        self.blocking.wake();
    }

    fn wake_all(&self) {
        self.data.wake_all();
        self.blocking.wake();
    }
}

#[derive(Debug)]
struct QueueState {
    identity: Bytes,
    // Identity handover invalidates the old queue before publishing its replacement.
    current: AtomicBool,
    space: StateSignal,
    cancel: CancellationToken,
}

impl Wake for QueueState {
    fn wake(self: Arc<Self>) {
        self.space.notify_changed();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.space.notify_changed();
    }
}

#[derive(Debug)]
struct RecvItem {
    body: QueuedMessage,
    state: Arc<QueueState>,
}

impl RecvItem {
    fn budget_bytes(&self) -> usize {
        self.body
            .byte_len()
            .saturating_add(self.state.identity.len())
            .saturating_add(std::mem::size_of::<omq_proto::message::Payload>())
    }
}

/// Actor-owned connection registration and identity handover. Registration and
/// shutdown share a cold lock; message drains never take it.
#[derive(Debug)]
pub(crate) struct PeerRecvRoutes {
    shared: Arc<RecvShared>,
    previous: FxHashMap<Bytes, Weak<QueueState>>,
    max_peers: usize,
}

impl PeerRecvRoutes {
    pub(crate) fn new(
        hwm: usize,
        handles: &super::recv::SpscHandles,
        max_message_size: Option<usize>,
    ) -> (Self, PeerReceiver) {
        let mut limits = RecvLimits::default();
        limits.bytes = limits.bytes.max(max_message_size.unwrap_or(0));
        Self::with_limits(hwm, handles, limits)
    }

    fn with_limits(
        hwm: usize,
        handles: &super::recv::SpscHandles,
        limits: RecvLimits,
    ) -> (Self, PeerReceiver) {
        let (registrar, receiver) = mpsc::channel_with_policy(hwm.max(16));
        let shared = Arc::new(RecvShared {
            registration: Mutex::new(Registration {
                registrar,
                states: Vec::new(),
            }),
            closed: AtomicBool::new(false),
            data: handles.recv_signal.clone(),
            blocking: handles.blocking_recv_waker.clone(),
            budget: Arc::new(Budget::new(limits.messages, limits.bytes)),
        });
        let receiver = PeerReceiver {
            shared: shared.clone(),
            receiver: Some(receiver),
            yield_pending: false,
        };
        (
            Self {
                shared,
                previous: FxHashMap::default(),
                max_peers: limits.peers,
            },
            receiver,
        )
    }

    pub(crate) fn register(
        &mut self,
        identity: Bytes,
        cancel: CancellationToken,
    ) -> Result<PeerRecvSink> {
        let mut registration = self
            .shared
            .registration
            .lock()
            .expect("PEER registration poisoned");
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // The idle registrar owns one internal lane. Retired connection rings
        // still count until fanring observes them disconnected and empty.
        let Ok(producer) = registration
            .registrar
            .try_register_bounded(self.max_peers.saturating_add(1))
        else {
            return Err(Error::Protocol(
                "PEER receive closed or peer queue limit reached".into(),
            ));
        };
        // Reclaim weak keys on churn, not on the data path. This also bounds
        // identities of disconnected peers, not merely their message rings.
        self.previous.retain(|_, state| state.strong_count() != 0);
        registration
            .states
            .retain(|state| state.strong_count() != 0);
        let state = Arc::new(QueueState {
            identity: identity.clone(),
            current: AtomicBool::new(true),
            space: StateSignal::new(),
            cancel,
        });
        registration.states.push(Arc::downgrade(&state));
        if let Some(previous) = self
            .previous
            .insert(identity, Arc::downgrade(&state))
            .and_then(|state| state.upgrade())
        {
            previous.current.store(false, Ordering::Release);
            previous.cancel.cancel();
            previous.space.notify_changed();
        }
        drop(registration);
        Ok(PeerRecvSink::new(producer, state, self.shared.clone()))
    }

    pub(crate) fn close_receive(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.wake_all();
        self.shared.budget.space.notify_changed();
        for state in self.previous.values().filter_map(Weak::upgrade) {
            state.space.notify_changed();
        }
    }
}

impl Drop for PeerRecvRoutes {
    fn drop(&mut self) {
        self.close_receive();
        for state in self.previous.values().filter_map(Weak::upgrade) {
            state.cancel.cancel();
            state.space.notify_changed();
        }
    }
}

impl PeerReceiver {
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.budget.is_empty()
    }

    pub(crate) fn shutdown(&mut self) {
        let states = {
            let mut registration = self
                .shared
                .registration
                .lock()
                .expect("PEER registration poisoned");
            self.shared.closed.store(true, Ordering::Release);
            std::mem::take(&mut registration.states)
        };
        for state in states.into_iter().filter_map(|state| state.upgrade()) {
            state.cancel.cancel();
            state.space.notify_changed();
        }
        // Coordinated teardown reclaims unread payloads even while senders
        // remain alive. Never run payload drops under the registration lock.
        self.receiver.take();
        self.shared.budget.space.notify_changed();
        self.shared.wake_all();
    }

    pub(crate) fn take_yield_pending(&mut self) -> bool {
        std::mem::take(&mut self.yield_pending)
    }

    /// Receive one currently queued message without waiting.
    pub(crate) fn try_recv(&mut self) -> Result<Message> {
        self.shared.data.begin_drain();
        let mut budget = DrainBudget::WORKER;
        self.drain_one(&mut budget)
    }

    fn drain_one(&mut self, budget: &mut DrainBudget) -> Result<Message> {
        self.yield_pending = false;
        let receiver = self.receiver.as_mut().ok_or(Error::Closed)?;
        while !budget.exhausted() {
            if let Ok(item) = receiver.try_recv_fair() {
                let _ = budget.account(item.budget_bytes());
                if item.state.current.load(Ordering::Acquire) {
                    return Ok(Message::with_prefix(
                        item.state.identity.clone(),
                        item.body.into_message(),
                    ));
                }
                // Dropping a stale generation returns its aggregate permit.
            } else {
                receiver.release_consumed();
                if self.shared.data.clear_after(true) {
                    self.shared.blocking.wake();
                }
                return if self.shared.closed.load(Ordering::Acquire) {
                    Err(Error::Closed)
                } else {
                    Err(Error::WouldBlock)
                };
            }
        }
        // Reconnect churn cannot bury runtime/control work behind stale data.
        receiver.release_consumed();
        self.yield_pending = true;
        self.shared.data.clear_after(false);
        self.shared.data.reschedule();
        self.shared.blocking.wake();
        Err(Error::WouldBlock)
    }

    /// Fair bulk drain into reusable caller storage. Never waits to fill a batch.
    /// Returns `WouldBlock` only if no message was appended; zero limit succeeds.
    pub(crate) fn try_recv_many_into(
        &mut self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        self.drain_many(Self::bulk_budget(max), out)
    }

    pub(crate) fn try_recv_many_after_first(
        &mut self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        let mut budget = Self::bulk_budget(max);
        let first = out.last().expect("first message already received");
        let _ = budget.account(first.max_message_size_len());
        // Even an exhausted budget must release the first message's credits.
        self.drain_many(budget, out)
    }

    fn bulk_budget(max: usize) -> DrainBudget {
        DrainBudget::new(
            max.min(super::recv::RECV_BATCH_MESSAGES),
            omq_proto::flow::max_batch_bytes(),
        )
    }

    fn drain_many(&mut self, mut budget: DrainBudget, out: &mut Vec<Message>) -> Result<usize> {
        let start = out.len();
        self.shared.data.begin_drain();
        let mut error = None;
        while !budget.exhausted() {
            match self.drain_one(&mut budget) {
                Ok(message) => {
                    out.push(message);
                }
                Err(stopped) => {
                    error = Some(stopped);
                    break;
                }
            }
        }
        if let Some(receiver) = &mut self.receiver {
            receiver.release_consumed();
        }
        if out.len() == start
            && let Some(error) = error
        {
            Err(error)
        } else {
            Ok(out.len() - start)
        }
    }
}

impl Drop for PeerReceiver {
    fn drop(&mut self) {
        self.shutdown();
    }
}
