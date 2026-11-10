//! Optional source claims. Plain queue slots still contain only Message.

use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, fence};

use rustc_hash::FxHashMap;

use super::{Arc, AtomicBool, Fanin, Message, Ordering, StateSignal, mpsc};
use crate::engine::signal::DataSignal;
use crate::socket::{ReceiveLane, ReceiveReceipt, UnshiftError};
use omq_proto::{Error, Result};

// Discover externally signaled inproc sources during a busy scalar drain.
const SOURCE_POLL_INTERVAL: usize = 64;

#[derive(Clone, Debug)]
pub(crate) struct Source {
    owner: Weak<Fanin>,
    state: Arc<SourceState>,
}

#[derive(Debug)]
struct SourceState {
    lane: mpsc::LaneId,
    connected: AtomicBool,
    claimed: AtomicBool,
    waiters: AtomicUsize,
    batch: DataSignal,
    data: StateSignal,
}

impl PartialEq for Source {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for Source {}

impl std::hash::Hash for Source {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&Arc::as_ptr(&self.state), state);
    }
}

impl Source {
    fn belongs(&self, owner: &Arc<Fanin>) -> bool {
        self.owner
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
    }

    fn live(&self) -> bool {
        self.state.connected.load(Ordering::Acquire)
            && self
                .owner
                .upgrade()
                .is_some_and(|owner| !owner.closed.load(Ordering::Acquire))
    }

    pub(crate) fn release_claim(&self) {
        if self.state.claimed.swap(false, Ordering::AcqRel) {
            if let Some(owner) = self.owner.upgrade() {
                owner.source_changed();
            }

            self.state.data.notify_changed();
        }
    }
}

#[derive(Debug)]
pub(super) struct SourceRegistration(Source);

impl SourceRegistration {
    pub(super) fn published(&self) {
        // Producer publication precedes DataSignal's SeqCst fence. A targeted
        // waiter registers with SeqCst before rechecking its lane, so neither
        // side can miss the other. No source wake without a targeted waiter.
        if self.0.state.waiters.load(Ordering::Acquire) != 0 {
            self.0.state.batch.mark();
        }
    }
}

impl Drop for SourceRegistration {
    fn drop(&mut self) {
        self.0.state.connected.store(false, Ordering::Release);
        self.0.state.data.notify_changed();

        if let Some(owner) = self.0.owner.upgrade() {
            owner.source_changed();
        }
    }
}

struct SourceWaiter<'a>(&'a Source);

impl<'a> SourceWaiter<'a> {
    fn new(source: &'a Source) -> Self {
        source.state.waiters.fetch_add(1, Ordering::SeqCst);
        // Order registration before the queue recheck below.
        fence(Ordering::SeqCst);

        Self(source)
    }
}

impl Drop for SourceWaiter<'_> {
    fn drop(&mut self) {
        self.0.state.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct Paused {
    source: Source,
    held: Option<(Message, usize)>,
}

#[derive(Debug)]
pub(super) struct ReceiveState {
    pub(super) receiver: super::queue::Queue,
    sources: FxHashMap<mpsc::LaneId, Source>,
    paused: Vec<Paused>,
    waiters: usize,
    handoff: bool,
    pub(super) observed_empty: bool,
    pub(super) scan_lanes: bool,
    pub(super) until_poll: usize,
}

impl ReceiveState {
    pub(super) fn new(receiver: super::queue::Queue) -> Self {
        Self {
            receiver,
            sources: FxHashMap::default(),
            paused: Vec::new(),
            waiters: 0,
            handoff: false,
            observed_empty: true,
            scan_lanes: false,
            until_poll: 0,
        }
    }

    pub(super) fn register(
        &mut self,
        owner: &Arc<Fanin>,
        lane: mpsc::LaneId,
    ) -> SourceRegistration {
        let source = Source {
            owner: Arc::downgrade(owner),
            state: Arc::new(SourceState {
                lane,
                connected: AtomicBool::new(true),
                claimed: AtomicBool::new(false),
                waiters: AtomicUsize::new(0),
                batch: DataSignal::new(),
                data: StateSignal::new(),
            }),
        };

        self.sources.insert(lane, source.clone());
        SourceRegistration(source)
    }

    pub(super) fn reclaim_sources(&mut self) {
        let receiver = &mut self.receiver;
        self.sources.retain(|lane, source| {
            source.state.connected.load(Ordering::Acquire)
                || source.state.claimed.load(Ordering::Acquire)
                || receiver.resume(lane).is_ok()
        });
    }

    pub(super) fn process_changes(&mut self, owner: &Fanin) {
        if !owner.source_pending.load(Ordering::Acquire) {
            return;
        }

        if !owner.source_pending.swap(false, Ordering::AcqRel) {
            return;
        }

        let mut index = 0;
        while index < self.paused.len() {
            let source = &self.paused[index].source;
            if source.state.connected.load(Ordering::Acquire)
                && source.state.claimed.load(Ordering::Acquire)
            {
                index += 1;
                continue;
            }

            source.state.claimed.store(false, Ordering::Release);
            let entry = self.paused.swap_remove(index);
            let _ = self.receiver.resume(&entry.source.state.lane);
        }
    }

    pub(super) fn close_sources(&self) {
        for source in self.sources.values() {
            source.state.data.notify_changed();
        }
    }

    pub(super) fn poll_sources(&mut self, force: bool) {
        if self.scan_lanes && (force || self.observed_empty || self.until_poll == 0) {
            self.receiver.poll_all_lanes();
            self.until_poll = SOURCE_POLL_INTERVAL;
        }
    }

    pub(super) fn wake_waiter(&mut self, owner: &Fanin) {
        if self.waiters != 0 && !self.handoff {
            self.handoff = true;
            owner.signal.reschedule();
        }
    }

    fn claim(
        &mut self,
        owner: &Fanin,
        source: Source,
        mut message: Message,
    ) -> Result<(ReceiveReceipt, Message)> {
        self.receiver
            .pause(&source.state.lane)
            .map_err(|_| Error::Closed)?;
        source.state.claimed.store(true, Ordering::Release);

        if !source.state.connected.load(Ordering::Acquire) {
            owner.source_changed();
        }

        self.paused.push(Paused {
            source: source.clone(),
            held: None,
        });
        self.observed_empty = false;
        self.until_poll = self.until_poll.saturating_sub(1);
        self.wake_waiter(owner);
        message.bound_storage();
        let bytes = message.retained_size().unwrap_or(usize::MAX);

        Ok((ReceiveReceipt::from_fanin(source, bytes), message))
    }
}

#[derive(Debug)]
pub(crate) struct ReceiveWaiter<'a>(&'a Fanin);

impl Drop for ReceiveWaiter<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.0.receiver.lock().as_mut() {
            state.waiters -= 1;
            state.handoff = false;

            if !state.observed_empty {
                state.wake_waiter(self.0);
            }
        }
    }
}

impl Fanin {
    fn source_changed(&self) {
        self.source_pending.store(true, Ordering::Release);
        self.signal.mark();
        self.blocking.wake();
    }

    pub(crate) fn wait(&self) -> ReceiveWaiter<'_> {
        if let Some(state) = self.receiver.lock().as_mut() {
            state.waiters += 1;
        }

        ReceiveWaiter(self)
    }

    pub(crate) fn try_recv_with_receipt(
        self: &Arc<Self>,
        source: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        let source = source.map(ReceiveLane::fanin).transpose()?;
        if source.is_some_and(|source| !source.belongs(self)) {
            return Err(Error::Protocol(
                "receive lane belongs to another socket".into(),
            ));
        }

        let mut guard = self.receiver.lock();
        let state = guard.as_mut().ok_or(Error::Closed)?;
        state.process_changes(self);

        if let Some(source) = source {
            if !source.live() {
                return Err(Error::Closed);
            }

            if let Some(entry) = state
                .paused
                .iter_mut()
                .find(|entry| entry.source == *source)
            {
                let (message, bytes) = entry.held.take().ok_or(Error::WouldBlock)?;
                return Ok((ReceiveReceipt::from_fanin(source.clone(), bytes), message));
            }

            let message = match state.receiver.try_recv_from(&source.state.lane) {
                Ok(message) => message,
                Err(mpsc::TryRecvError::Empty) => {
                    state.receiver.release_consumed();
                    return Err(Error::WouldBlock);
                }
                Err(mpsc::TryRecvError::Disconnected) => return Err(Error::Closed),
            };

            return state.claim(self, source.clone(), message);
        }

        if !state.scan_lanes {
            self.signal.begin_drain();
        }

        state.poll_sources(false);
        let mut result = state.receiver.try_recv_fair_with_lane();

        if result.is_err() && state.scan_lanes {
            self.signal.begin_drain();
            state.poll_sources(true);
            result = state.receiver.try_recv_fair_with_lane();
        }

        let Ok((lane, message)) = result else {
            state.receiver.release_consumed();
            state.observed_empty = true;

            if self.signal.clear_after(true) {
                self.blocking.wake();
            }

            return Err(Error::WouldBlock);
        };

        let source = state.sources.get(&lane).expect("registered source").clone();
        state.claim(self, source, message)
    }

    pub(crate) async fn recv_with_receipt(
        self: &Arc<Self>,
        source: Option<&ReceiveLane>,
    ) -> Result<(ReceiveReceipt, Message)> {
        let selected = source.map(ReceiveLane::fanin).transpose()?;
        let _source_waiter = selected.map(SourceWaiter::new);

        loop {
            let seen = selected.map(|source| source.state.data.generation());
            if let Some(selected) = selected {
                selected.state.batch.begin_drain();
            }

            let result = self.try_recv_with_receipt(source);
            if let Some(selected) = selected {
                // A successful receive claims the lane. Otherwise no message
                // is currently admissible to this targeted caller. A racing
                // publication remains DIRTY and rearms the coalesced signal.
                selected.state.batch.clear_after(true);
            }

            match result {
                Err(Error::WouldBlock) => {}
                result => return result,
            }

            if let Some(selected) = selected {
                let changed = selected
                    .state
                    .data
                    .changed_after(seen.expect("targeted wait generation"));

                if selected.state.claimed.load(Ordering::Acquire) {
                    // Queued data cannot overtake another live receipt. Wait
                    // for acceptance, return, or retirement instead.
                    changed.await;
                } else {
                    tokio::select! {
                        () = changed => {},
                        () = selected.state.batch.ready() => {},
                    }
                }
            } else {
                let _waiter = self.wait();
                self.signal.ready().await;
            }
        }
    }

    pub(crate) fn unshift(
        self: &Arc<Self>,
        mut receipt: ReceiveReceipt,
        mut message: Message,
    ) -> std::result::Result<(), UnshiftError> {
        let claim = receipt.fanin_claim();
        let (source, bytes) = match claim {
            Ok(claim) => claim,
            Err(error) => return Err(UnshiftError { error, message }),
        };

        let mut guard = self.receiver.lock();
        let Some(state) = guard.as_mut() else {
            return Err(UnshiftError {
                error: Error::Closed,
                message,
            });
        };

        state.process_changes(self);
        let error = if !source.belongs(self) {
            Some(Error::Protocol("receipt belongs to another socket".into()))
        } else if !source.live() {
            Some(Error::Closed)
        } else {
            None
        };

        if let Some(error) = error {
            return Err(UnshiftError { error, message });
        }

        let Some(entry) = state
            .paused
            .iter_mut()
            .find(|entry| entry.source == *source)
        else {
            return Err(UnshiftError {
                error: Error::Closed,
                message,
            });
        };

        message.bound_storage();
        if message.retained_size().unwrap_or(usize::MAX) > bytes {
            return Err(UnshiftError {
                error: Error::Protocol("message exceeds its receipt's storage charge".into()),
                message,
            });
        }

        debug_assert!(entry.held.is_none(), "one claim per source");
        entry.held = Some((message, bytes));
        source.state.data.notify_changed();
        receipt.forget_claim();

        Ok(())
    }
}

#[cfg(test)]
mod tests;
