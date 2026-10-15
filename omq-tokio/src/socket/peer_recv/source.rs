//! Source claims, one held message per source, and targeted receive waits.

use super::budget::{Permit, QueuedMessage};
use super::{
    Arc, Bytes, DrainBudget, Error, Message, Ordering, PausedSource, PeerReceiver, QueueState,
    ReceiveCell, RecvItem, RecvShared, Result, Weak, mpsc,
};

/// Opaque receive lane used for selective backpressure and retries.
/// A replacement peer gets a different lane even with the same logical identity.
/// Obtain a handle through [`ReceiveReceipt::lane`].
#[derive(Clone)]
pub struct ReceiveLane {
    inner: SourceOwner,
}

#[derive(Clone)]
enum SourceOwner {
    Peer(PeerSource),
    Fanin(super::super::fanin::Source),
}

#[derive(Clone)]
struct PeerSource {
    state: Arc<QueueState>,
    shared: Weak<RecvShared>,
}

impl PartialEq for ReceiveLane {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (SourceOwner::Peer(a), SourceOwner::Peer(b)) => Arc::ptr_eq(&a.state, &b.state),
            (SourceOwner::Fanin(a), SourceOwner::Fanin(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ReceiveLane {}

impl std::hash::Hash for ReceiveLane {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.inner {
            SourceOwner::Peer(source) => {
                std::hash::Hash::hash(&0_u8, state);
                std::hash::Hash::hash(&Arc::as_ptr(&source.state), state);
            }
            SourceOwner::Fanin(source) => {
                std::hash::Hash::hash(&1_u8, state);
                std::hash::Hash::hash(source, state);
            }
        }
    }
}

impl std::fmt::Debug for ReceiveLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            SourceOwner::Peer(source) => f
                .debug_struct("ReceiveLane")
                .field("identity", &source.state.identity)
                .finish_non_exhaustive(),
            SourceOwner::Fanin(source) => source.fmt(f),
        }
    }
}

impl ReceiveLane {
    fn peer(&self) -> Result<&PeerSource> {
        match &self.inner {
            SourceOwner::Peer(source) => Ok(source),
            SourceOwner::Fanin(_) => Err(Error::Protocol(
                "receive lane belongs to another socket".into(),
            )),
        }
    }

    pub(in crate::socket) fn fanin(&self) -> Result<&super::super::fanin::Source> {
        match &self.inner {
            SourceOwner::Fanin(source) => Ok(source),
            SourceOwner::Peer(_) => Err(Error::Protocol(
                "receive lane belongs to another socket".into(),
            )),
        }
    }
}

/// Owns a lane's FIFO claim. PEER also retains its receive memory charge.
/// Drop after admitting or discarding the message to resume that lane.
/// While live, all other receive calls skip this lane. PULL and GATHER use
/// their bounded lane storage plus this one claimed message.
#[derive(Debug)]
pub struct ReceiveReceipt {
    lane: Option<ReceiveLane>,
    identity: Option<omq_proto::Identity>,
    permit: Option<Permit>,
    fanin_bytes: usize,
}

impl ReceiveReceipt {
    /// Claimed receive lane, when supported. ROUTER receipts have no lane.
    pub fn lane(&self) -> Option<&ReceiveLane> {
        self.lane.as_ref()
    }

    /// Logical peer identity, when supplied by an identity-routing socket.
    pub fn identity(&self) -> Option<&[u8]> {
        self.identity.as_ref().map(AsRef::as_ref)
    }

    /// Clone the logical peer identity without copying its allocation.
    pub fn identity_bytes(&self) -> Option<Bytes> {
        self.identity.as_ref().map(omq_proto::Identity::to_bytes)
    }

    /// Original sender snapshot, independent of current routing and lane lifetime.
    pub fn peer_identity(&self) -> Option<&omq_proto::Identity> {
        self.identity.as_ref()
    }

    pub(in crate::socket) fn take_identity(&mut self) -> Option<omq_proto::Identity> {
        self.identity.take()
    }

    pub(in crate::socket) fn inert(identity: Option<omq_proto::Identity>) -> Self {
        Self {
            lane: None,
            identity,
            permit: None,
            fanin_bytes: 0,
        }
    }

    pub(in crate::socket) fn from_fanin(source: super::super::fanin::Source, bytes: usize) -> Self {
        Self {
            lane: Some(ReceiveLane {
                inner: SourceOwner::Fanin(source),
            }),
            identity: None,
            permit: None,
            fanin_bytes: bytes,
        }
    }

    pub(in crate::socket) fn fanin_claim(&self) -> Result<(&super::super::fanin::Source, usize)> {
        let source = self.lane.as_ref().ok_or_else(|| {
            Error::Protocol("unshift requires a receipt with a lane claim".into())
        })?;
        Ok((source.fanin()?, self.fanin_bytes))
    }

    pub(in crate::socket) fn forget_claim(&mut self) {
        self.lane = None;
    }
}

impl Drop for ReceiveReceipt {
    fn drop(&mut self) {
        if let Some(source) = &self.lane {
            match &source.inner {
                SourceOwner::Peer(source) => {
                    if self.permit.is_some()
                        && source.state.release_claim()
                        && let Some(shared) = source.shared.upgrade()
                    {
                        shared.source_changed();
                        source.state.data.notify_changed();
                    }
                }
                SourceOwner::Fanin(source) => source.release_claim(),
            }
        }
    }
}

/// Failed return of a message. The caller retains the original message.
#[derive(Debug)]
pub struct UnshiftError {
    /// Reason the receipt could not be returned to this socket/lane.
    pub error: Error,
    /// Message returned to the caller for retry or discard.
    pub message: Message,
}

impl std::fmt::Display for UnshiftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for UnshiftError {}

impl PeerReceiver {
    pub(super) fn process_source_changes(&mut self) {
        if !self.shared.control_pending.swap(false, Ordering::AcqRel) {
            return;
        }
        // At most max_peers entries, one per registered lane. Flags coalesce
        // churn and receipt release without retaining historical generations.
        let mut index = 0;
        while index < self.paused.len() {
            let state = &self.paused[index].state;
            let status = state.status.load(Ordering::Acquire);
            let retired = status & QueueState::LIVE != QueueState::LIVE;
            if !retired && status & QueueState::CLAIMED != 0 {
                index += 1;
                continue;
            }
            if retired {
                state.retire();
                state.release_claim();
            } else {
                debug_assert!(
                    self.paused[index].held.is_none(),
                    "live receipt cannot also be held"
                );
            }
            let entry = self.paused.swap_remove(index);
            // Drop the one held frame. Unread stale entries use the ordinary
            // count/byte drain budgets, independent of idle producer lifetime.
            if let Some(receiver) = &mut self.receiver {
                let _ = receiver.resume(&entry.state.lane);
            }
        }
    }

    fn belongs(&self, source: &PeerSource) -> bool {
        source
            .shared
            .upgrade()
            .is_some_and(|shared| Arc::ptr_eq(&shared, &self.shared))
    }

    /// Acquire one source, preserving its byte permit until receipt release.
    pub(crate) fn try_recv_with_receipt(
        &mut self,
        source: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        let source = source.map(ReceiveLane::peer).transpose()?;
        self.process_source_changes();
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.yield_pending = false;
        if let Some(source) = source {
            if !self.belongs(source) {
                return Err(Error::Protocol(
                    "receive lane belongs to another socket".into(),
                ));
            }
            if !source.state.live() {
                return Err(Error::Closed);
            }
            if let Some(entry) = self
                .paused
                .iter_mut()
                .find(|entry| Arc::ptr_eq(&entry.state, &source.state))
            {
                let held = entry.held.take().ok_or(Error::WouldBlock)?;
                return Ok(Self::receipt(&self.shared, source.state.clone(), held));
            }
            let receiver = self.receiver.as_mut().ok_or(Error::Closed)?;
            let item = match receiver.try_recv_from(&source.state.lane) {
                Ok(item) => item,
                Err(mpsc::TryRecvError::Empty) => {
                    receiver.release_consumed();
                    return Err(Error::WouldBlock);
                }
                Err(mpsc::TryRecvError::Disconnected) => return Err(Error::Closed),
            };
            return self.claim(item);
        }
        self.shared.data.begin_drain();
        let mut budget = DrainBudget::WORKER;
        while !budget.exhausted() {
            let receiver = self.receiver.as_mut().ok_or(Error::Closed)?;
            if let Ok(item) = receiver.try_recv_fair() {
                let _ = budget.account(item.budget_bytes());
                if item.state.current() {
                    return self.claim(item);
                }
            } else {
                receiver.release_consumed();
                self.observed_empty = true;
                if self.shared.data.clear_after(true) {
                    self.shared.blocking.wake();
                }
                return Err(Error::WouldBlock);
            }
        }
        self.receiver
            .as_mut()
            .expect("live receive")
            .release_consumed();
        self.yield_pending = true;
        self.shared.data.clear_after(false);
        self.shared.data.reschedule();
        Err(Error::WouldBlock)
    }

    fn claim(&mut self, item: RecvItem) -> Result<(ReceiveReceipt, Message)> {
        self.receiver
            .as_mut()
            .ok_or(Error::Closed)?
            .pause(&item.state.lane)
            .map_err(|_| Error::Closed)?;
        self.paused.push(PausedSource {
            state: item.state.clone(),
            held: None,
        });
        if !item.state.claim() {
            self.shared.source_changed();
        }
        self.observed_empty = false;
        self.wake_waiter();
        Ok(Self::receipt(&self.shared, item.state, item.body))
    }

    fn receipt(
        shared: &Arc<RecvShared>,
        state: Arc<QueueState>,
        body: QueuedMessage,
    ) -> (ReceiveReceipt, Message) {
        let (message, permit) = body.into_parts();
        (
            ReceiveReceipt {
                identity: Some(state.identity.clone()),
                lane: Some(ReceiveLane {
                    inner: SourceOwner::Peer(PeerSource {
                        state,
                        shared: Arc::downgrade(shared),
                    }),
                }),
                permit: Some(permit),
                fanin_bytes: 0,
            },
            message,
        )
    }

    pub(crate) async fn recv_with_receipt(
        receiver: &ReceiveCell<Self>,
        source: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        let peer_source = source.map(ReceiveLane::peer).transpose()?;
        let shared = receiver.lock().shared.clone();
        loop {
            let seen = peer_source.map(|source| source.state.data.generation());
            let (result, yielded) = {
                let mut receiver = receiver.lock();
                let result = receiver.try_recv_with_receipt(source);
                (result, receiver.take_yield_pending())
            };
            match result {
                Err(Error::WouldBlock) => {}
                result => return result,
            }
            if yielded {
                tokio::task::yield_now().await;
                continue;
            }
            if let Some(source) = peer_source {
                source
                    .state
                    .data
                    .changed_after(seen.expect("source generation"))
                    .await;
            } else {
                let _waiter = Self::wait(receiver);
                shared.data.ready().await;
            }
        }
    }

    pub(crate) fn unshift(
        &mut self,
        mut receipt: ReceiveReceipt,
        mut message: Message,
    ) -> std::result::Result<(), UnshiftError> {
        self.process_source_changes();
        let error = if self.shared.closed.load(Ordering::Acquire) {
            Some(Error::Closed)
        } else if let Some(source) = receipt.lane.as_ref().and_then(|source| source.peer().ok()) {
            if !self.belongs(source) {
                Some(Error::Protocol("receipt belongs to another socket".into()))
            } else if !source.state.live() {
                Some(Error::Closed)
            } else {
                None
            }
        } else {
            Some(Error::Protocol(
                "unshift requires a receipt with a lane claim".into(),
            ))
        };
        if let Some(error) = error {
            return Err(UnshiftError { error, message });
        }
        let source = receipt
            .lane
            .as_ref()
            .expect("validated receipt")
            .peer()
            .expect("validated source");
        let Some(entry) = self
            .paused
            .iter_mut()
            .find(|entry| Arc::ptr_eq(&entry.state, &source.state))
        else {
            return Err(UnshiftError {
                error: Error::Closed,
                message,
            });
        };
        if entry.held.is_some() {
            return Err(UnshiftError {
                error: Error::Protocol("receive source already holds a message".into()),
                message,
            });
        }
        message.bound_storage();
        if !receipt
            .permit
            .as_ref()
            .is_some_and(|permit| permit.fits(&message))
        {
            return Err(UnshiftError {
                error: Error::Protocol("message exceeds its receipt's storage charge".into()),
                message,
            });
        }
        entry.held = Some(QueuedMessage::with_permit(
            message,
            receipt.permit.take().expect("receipt permit"),
        ));
        source.state.data.notify_changed();
        receipt.lane = None;
        Ok(())
    }
}
