//! Drain per-peer yrings and the shared pipe under a `DrainBudget`.

use super::{
    RECV_BATCH_BYTES, RECV_BATCH_MESSAGES, SpscAwareRecv, TcpYringConsumer, recv_budget_bytes,
};
use omq_proto::error::Error;
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(super) struct DrainState {
    pub(super) generation: u64,
    pub(super) recv_cursor: usize,
    pub(super) tcp: Vec<Arc<TcpYringConsumer>>,
    pub(super) batch: VecDeque<Message>,
    pub(super) recv_consumer: yring::Consumer<Message>,
    pub(super) recv_batch: RingDrain,
    pub(super) latency: bool,
}

pub(super) enum DrainResult {
    Message(Message),
    Empty,
    Closed,
}

#[derive(Default)]
pub(super) struct SourceDrain {
    pub(super) message: Option<Message>,
    pub(super) disconnected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecvSource {
    Stream(usize),
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DrainLimit {
    One,
    Budget,
}

pub(super) fn recv_source_at(index: usize, stream_len: usize) -> RecvSource {
    if index < stream_len {
        RecvSource::Stream(index)
    } else {
        RecvSource::Shared
    }
}

/// Receive release policy. The cached window stays valid after partial releases.
#[derive(Debug, Default)]
pub(super) struct RingDrain {
    pub(super) remaining: usize,
    pub(super) popped: usize,
}

impl RingDrain {
    pub(super) fn lwm(capacity: usize) -> usize {
        // libzmq keeps at most 1024 occupied slots at the large-ring boundary.
        if capacity > 2048 {
            capacity - 1024
        } else {
            (capacity / 2).max(1)
        }
    }

    /// Release only consumed slots; retain the remainder of the cached window.
    pub(super) fn release(&mut self, consumer: &mut yring::Consumer<Message>) -> bool {
        if self.popped == 0 {
            return false;
        }
        self.popped = 0;
        consumer.release_with_full()
    }
}

pub(super) fn drain_peer_source(
    peer: &TcpYringConsumer,
    latency: bool,
    batch: &mut Vec<Message>,
    budget: &mut DrainBudget,
    limit: DrainLimit,
) -> SourceDrain {
    let Some(mut consumer) = peer.consumer.try_lock() else {
        return SourceDrain::default();
    };
    let mut progress = RingDrain {
        remaining: peer.batch_remaining.load(Ordering::Relaxed),
        popped: peer.batch_popped.load(Ordering::Relaxed),
    };
    let message = if latency {
        let (item, wake) = drain_yring_one(&mut consumer, &mut progress);
        if wake {
            peer.space.notify_changed();
        }
        item
    } else {
        match limit {
            DrainLimit::One => {
                let (_, wake) =
                    drain_yring_one_into_batch(&mut consumer, batch, &mut progress, budget);
                if wake {
                    peer.space.notify_changed();
                }
            }
            DrainLimit::Budget => {
                drain_yring(&mut consumer, batch, &mut progress, budget, || {
                    peer.space.notify_changed();
                });
            }
        }
        None
    };
    peer.batch_remaining
        .store(progress.remaining, Ordering::Relaxed);
    peer.batch_popped.store(progress.popped, Ordering::Relaxed);
    SourceDrain {
        message,
        disconnected: consumer.is_disconnected(),
    }
}

pub(super) fn drain_yring_one_into_batch(
    consumer: &mut yring::Consumer<Message>,
    batch: &mut Vec<Message>,
    progress: &mut RingDrain,
    budget: &mut DrainBudget,
) -> (usize, bool) {
    if budget.exhausted() {
        return (0, false);
    }
    let (item, released) = drain_yring_one(consumer, progress);
    let Some(item) = item else {
        return (0, released);
    };
    let _ = budget.account(recv_budget_bytes(&item));
    batch.push(item);
    (1, released)
}

pub(super) fn drain_yring(
    consumer: &mut yring::Consumer<Message>,
    batch: &mut Vec<Message>,
    progress: &mut RingDrain,
    budget: &mut DrainBudget,
    mut on_release: impl FnMut(),
) -> usize {
    let mut drained = 0;
    let lwm = RingDrain::lwm(consumer.capacity());
    while !budget.exhausted() {
        if progress.remaining == 0 {
            progress.remaining = consumer.prefetch();
            if progress.remaining == 0 {
                break;
            }
        }
        let count = consumer.pop_into_while(
            batch,
            budget.remaining_msgs().min(lwm - progress.popped),
            |message| {
                if budget.exhausted() {
                    return false;
                }
                // Admit the item exhausting the byte budget, even if oversized.
                let _ = budget.account(recv_budget_bytes(message));
                true
            },
        );
        progress.remaining -= count;
        progress.popped += count;
        drained += count;
        if (progress.remaining == 0 || progress.popped >= lwm) && progress.release(consumer) {
            // Publish and wake at LWM while this drain continues on its other
            // half. Deferring the wake until return prevents overlap.
            on_release();
        }
        if count == 0 {
            break;
        }
    }
    // A budget boundary hands control elsewhere. Publish partial credits.
    if progress.release(consumer) {
        on_release();
    }
    drained
}

/// Pop once, publishing consumed credits at LWM or the cached window boundary.
pub(super) fn drain_yring_one(
    consumer: &mut yring::Consumer<Message>,
    progress: &mut RingDrain,
) -> (Option<Message>, bool) {
    loop {
        if progress.remaining == 0 {
            progress.remaining = consumer.prefetch();
            if progress.remaining == 0 {
                return (None, progress.release(consumer));
            }
        }
        if let Some(item) = consumer.pop() {
            progress.remaining -= 1;
            progress.popped += 1;
            let wake = (progress.remaining == 0
                || progress.popped >= RingDrain::lwm(consumer.capacity()))
                && progress.release(consumer);
            return (Some(item), wake);
        }
        let wake = progress.release(consumer);
        progress.remaining = 0;
        if wake {
            return (None, true);
        }
    }
}

impl SpscAwareRecv {
    pub(super) fn try_drain(&self) -> DrainResult {
        if let Some(fanin) = self.active_fanin() {
            if !self.has_ring_sources() {
                return match fanin.try_recv() {
                    Ok(message) => DrainResult::Message(message),
                    Err(Error::Closed) => DrainResult::Closed,
                    Err(_) => DrainResult::Empty,
                };
            }
            // Alternate the first source so neither starves the other.
            // Both are checked before reporting an empty socket.
            let rings_first = self.rings_first.fetch_xor(true, Ordering::Relaxed);
            if rings_first && let DrainResult::Message(message) = self.try_drain_queues() {
                return DrainResult::Message(message);
            }
            match fanin.try_recv() {
                Ok(message) => return DrainResult::Message(message),
                Err(Error::Closed) => return DrainResult::Closed,
                Err(_) => {}
            }
            if !rings_first && let DrainResult::Message(message) = self.try_drain_queues() {
                return DrainResult::Message(message);
            }
            return DrainResult::Empty;
        }
        self.try_drain_queues()
    }

    fn try_drain_queues(&self) -> DrainResult {
        if let Some(peer) = &self.peer_recv {
            return match peer.lock().try_recv() {
                Ok(message) => DrainResult::Message(message),
                Err(Error::Closed) => DrainResult::Closed,
                Err(_) => DrainResult::Empty,
            };
        }
        if let Some(msg) = self.take_conflate_message() {
            return DrainResult::Message(msg);
        }

        let mut guard = self.drain_state.lock();

        if let Some(msg) = guard.batch.pop_front() {
            return DrainResult::Message(msg);
        }

        self.refresh_snapshot(&mut guard);
        let state = &mut *guard;
        if let Some(msg) = Self::try_single_peer_fast_path(state) {
            return DrainResult::Message(msg);
        }

        // Only enter a signaling drain when no single ready item was returned.
        // A failed fast probe is followed by another queue check after these
        // fences, before clearing signals or allowing the caller to park.
        self.recv_signal.begin_drain();
        self.recv_pipe_notify.begin_drain();
        if let Some(msg) = self.take_conflate_message() {
            return DrainResult::Message(msg);
        }
        let mut budget = DrainBudget::new(RECV_BATCH_MESSAGES, RECV_BATCH_BYTES);
        // The staging deque is empty here. Reuse its allocation for the
        // queues' contiguous bulk output, then restore FIFO single receives.
        let mut batch = Vec::from(std::mem::take(&mut state.batch));
        let (latency_result, has_disconnected) =
            self.drain_sources(state, &mut budget, false, &mut batch);
        state.batch = batch.into();
        let result = latency_result.or_else(|| state.batch.pop_front());
        let pipe_disconnected = state.recv_consumer.is_disconnected();
        let has_peers = !state.tcp.is_empty();
        let all_empty = Self::state_is_empty(state) && self.conflate_slot_empty();
        if result.is_none() {
            self.release_partial_batches(state);
        }
        if result.is_none()
            && all_empty
            && (self.recv_signal.clear_after(all_empty)
                || self
                    .recv_pipe_notify
                    .clear_after(state.recv_consumer.is_empty()))
        {
            self.blocking_recv_waker.wake();
        }
        drop(guard);

        if has_disconnected {
            self.cleanup_disconnected();
        }

        match result {
            Some(msg) => DrainResult::Message(msg),
            None if pipe_disconnected && !has_peers => DrainResult::Closed,
            None => DrainResult::Empty,
        }
    }

    pub(super) fn refresh_snapshot(&self, state: &mut DrainState) {
        let current_gen = self.consumer_generation.load(Ordering::Acquire);
        if state.generation == current_gen {
            return;
        }
        state.tcp.clone_from(&self.tcp_consumers.read().unwrap());
        state.generation = current_gen;
    }

    fn try_single_peer_fast_path(state: &mut DrainState) -> Option<Message> {
        // One ready source needs no application staging vector. LWM progress
        // remains in the peer consumer; bulk calls keep their bounded scanner.
        if state.tcp.len() != 1
            || (!state.latency && state.tcp[0].capacity < RECV_BATCH_MESSAGES)
            || !state.recv_consumer.is_empty()
        {
            return None;
        }
        let mut budget = DrainBudget::new(1, RECV_BATCH_BYTES);
        drain_peer_source(
            &state.tcp[0],
            true,
            &mut Vec::new(),
            &mut budget,
            DrainLimit::One,
        )
        .message
    }

    pub(super) fn drain_sources(
        &self,
        state: &mut DrainState,
        budget: &mut DrainBudget,
        bulk: bool,
        batch: &mut Vec<Message>,
    ) -> (Option<Message>, bool) {
        let mut result = None;
        let mut has_disconnected = false;
        let latency = state.latency && !bulk;
        let tcp_len = state.tcp.len();
        let source_count = tcp_len + 1;
        let peer_source_count = tcp_len;
        // Batching bulk calls take one peer's whole window before moving
        // on. Everything else rotates peers after each message.
        let batching = bulk && self.recv_batching;
        let limit = if !latency && peer_source_count > 1 && !batching {
            DrainLimit::One
        } else {
            DrainLimit::Budget
        };
        loop {
            let start = state.recv_cursor % source_count;
            let before = budget.msgs();
            // One logical round-robin space covers all sources. Bulk calls
            // repeat fair rounds with one aggregate budget, never waiting for
            // arrivals. Advance the cursor even when a limit cuts a round short.
            for offset in 0..source_count {
                if result.is_some() || budget.exhausted() {
                    break;
                }
                let source = (start + offset) % source_count;
                state.recv_cursor = (source + 1) % source_count;
                let outcome = match recv_source_at(source, tcp_len) {
                    RecvSource::Stream(index) => {
                        drain_peer_source(&state.tcp[index], latency, batch, budget, limit)
                    }
                    RecvSource::Shared if latency => self.drain_shared_one(state),
                    RecvSource::Shared => self.drain_shared_batch(state, budget, limit, batch),
                };
                result = outcome.message;
                has_disconnected |= outcome.disconnected;
            }
            if !bulk || budget.exhausted() || budget.msgs() == before {
                break;
            }
        }
        (result, has_disconnected)
    }

    /// Latency path: take one message from the shared pipe straight to the
    /// caller, bypassing the batch.
    fn drain_shared_one(&self, state: &mut DrainState) -> SourceDrain {
        let (item, wake) = drain_yring_one(&mut state.recv_consumer, &mut state.recv_batch);
        if wake {
            self.recv_pipe_space.notify_changed();
        }
        SourceDrain {
            message: item,
            disconnected: false,
        }
    }

    /// Throughput path: move shared-pipe messages into `batch`, one or up to
    /// the budget depending on `limit`.
    fn drain_shared_batch(
        &self,
        state: &mut DrainState,
        budget: &mut DrainBudget,
        limit: DrainLimit,
        batch: &mut Vec<Message>,
    ) -> SourceDrain {
        match limit {
            DrainLimit::One => {
                let (_, wake) = drain_yring_one_into_batch(
                    &mut state.recv_consumer,
                    batch,
                    &mut state.recv_batch,
                    budget,
                );
                if wake {
                    self.recv_pipe_space.notify_changed();
                }
            }
            DrainLimit::Budget => {
                drain_yring(
                    &mut state.recv_consumer,
                    batch,
                    &mut state.recv_batch,
                    budget,
                    || self.recv_pipe_space.notify_changed(),
                );
            }
        }
        SourceDrain::default()
    }

    pub(super) fn state_is_empty(state: &DrainState) -> bool {
        state.batch.is_empty()
            && state.recv_consumer.is_empty()
            && state.tcp.iter().all(|tc| {
                tc.consumer
                    .try_lock()
                    .is_some_and(|consumer| consumer.is_empty())
            })
    }

    /// A fair round can leave a prefetched window partly consumed. Publish
    /// those slots before returning a bulk result so blocked producers resume.
    pub(super) fn release_partial_batches(&self, state: &mut DrainState) {
        for peer in &state.tcp {
            if peer.batch_popped.load(Ordering::Relaxed) > 0 {
                let mut consumer = peer.consumer.lock();
                if peer.batch_popped.swap(0, Ordering::Relaxed) > 0 && consumer.release_with_full()
                {
                    peer.space.notify_changed();
                }
            }
        }
        if state.recv_batch.release(&mut state.recv_consumer) {
            self.recv_pipe_space.notify_changed();
        }
    }
}
