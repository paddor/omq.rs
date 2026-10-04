//! Source claims, one held message per source, and targeted receive waits.

use super::budget::{Permit, QueuedMessage};
use super::{
    Arc, Bytes, DrainBudget, Error, Message, Mutex, Ordering, PausedSource, PeerReceiver,
    QueueState, RecvItem, RecvShared, Result, Weak, mpsc,
};

/// One physical receive connection and generation. A replacement connection
/// has a different source even when it has the same logical identity.
#[derive(Clone)]
pub struct ReceiveSource {
    state: Arc<QueueState>,
    shared: Weak<RecvShared>,
}

impl PartialEq for ReceiveSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for ReceiveSource {}

impl std::hash::Hash for ReceiveSource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&Arc::as_ptr(&self.state), state);
    }
}

impl std::fmt::Debug for ReceiveSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveSource")
            .field("identity", &self.state.identity)
            .finish_non_exhaustive()
    }
}

/// Owns a source's FIFO claim and receive memory charge. Drop after admitting
/// or discarding the message to release its charge and resume that source.
/// While live, all other receive calls skip this source.
#[derive(Debug)]
pub struct ReceiveReceipt {
    source: Option<ReceiveSource>,
    identity: Option<Bytes>,
    permit: Option<Permit>,
}

impl ReceiveReceipt {
    /// Physical source, when the socket supports source-aware drainage.
    pub fn source(&self) -> Option<&ReceiveSource> {
        self.source.as_ref()
    }

    /// Logical peer identity, when supplied by an identity-routing socket.
    pub fn identity(&self) -> Option<&[u8]> {
        self.identity.as_deref()
    }

    pub(in crate::socket) fn inert(identity: Option<Bytes>) -> Self {
        Self {
            source: None,
            identity,
            permit: None,
        }
    }
}

impl Drop for ReceiveReceipt {
    fn drop(&mut self) {
        if self.permit.is_some()
            && let Some(source) = &self.source
            && source.state.release_claim()
            && let Some(shared) = source.shared.upgrade()
        {
            shared.source_changed();
            source.state.data.notify_changed();
        }
    }
}

/// Failed return of a message. The caller retains the original message.
#[derive(Debug)]
pub struct UnshiftError {
    /// Reason the receipt could not be returned to this socket/source.
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

    fn belongs(&self, source: &ReceiveSource) -> bool {
        source
            .shared
            .upgrade()
            .is_some_and(|shared| Arc::ptr_eq(&shared, &self.shared))
    }

    /// Acquire one source, preserving its byte permit until receipt release.
    pub(crate) fn try_recv_from(
        &mut self,
        source: Option<&ReceiveSource>,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.process_source_changes();
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.yield_pending = false;
        if let Some(source) = source {
            if !self.belongs(source) {
                return Err(Error::Protocol(
                    "receive source belongs to another socket".into(),
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
                source: Some(ReceiveSource {
                    state,
                    shared: Arc::downgrade(shared),
                }),
                permit: Some(permit),
            },
            message,
        )
    }

    pub(crate) async fn recv_from(
        receiver: &Mutex<Self>,
        source: Option<&ReceiveSource>,
    ) -> Result<(ReceiveReceipt, Message)> {
        let shared = receiver
            .lock()
            .expect("PEER receive poisoned")
            .shared
            .clone();
        loop {
            let seen = source.map(|source| source.state.data.generation());
            let (result, yielded) = {
                let mut receiver = receiver.lock().expect("PEER receive poisoned");
                let result = receiver.try_recv_from(source);
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
            if let Some(source) = source {
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
        } else if let Some(source) = &receipt.source {
            if !self.belongs(source) {
                Some(Error::Protocol("receipt belongs to another socket".into()))
            } else if !source.state.live() {
                Some(Error::Closed)
            } else {
                None
            }
        } else {
            Some(Error::Protocol(
                "unshift requires a source-aware receipt".into(),
            ))
        };
        if let Some(error) = error {
            return Err(UnshiftError { error, message });
        }
        let source = receipt.source.as_ref().expect("validated receipt");
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
        receipt.source = None;
        Ok(())
    }
}
