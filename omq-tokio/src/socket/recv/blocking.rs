//! Thread-blocking receive, with an optional spin before parking.

use super::{
    BlockingRecvCancel, DrainLimit, DrainResult, RECV_BATCH_BYTES, SpscAwareRecv, drain_peer_source,
};
use crate::engine::signal::BlockingRecvCancelGuard;
use omq_proto::error::{Error, Result};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl SpscAwareRecv {
    pub(crate) fn blocking_recv(&self) -> Result<Message> {
        let mut waiter = None;
        loop {
            match self.try_drain_with_spin(None, || false) {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        continue;
                    }
                    waiter.park();
                }
            }
        }
    }

    pub(crate) fn blocking_recv_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        let thread = std::thread::current();
        cancel.register(&thread);
        let _guard = BlockingRecvCancelGuard { cancel };
        if cancel.is_canceled() {
            return Ok(None);
        }
        self.blocking_recv_registered_cancelable(cancel)
    }

    #[inline]
    pub(crate) fn blocking_recv_registered_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        self.blocking_recv_registered_cancelable_until(cancel, None)
    }

    #[inline]
    pub(crate) fn blocking_recv_registered_cancelable_until(
        &self,
        cancel: &BlockingRecvCancel,
        deadline: Option<Instant>,
    ) -> Result<Option<Message>> {
        let mut waiter = None;
        let mut woke_without_message = false;
        loop {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(Error::Timeout);
            }
            match self.try_drain_with_spin(deadline, || cancel.is_canceled()) {
                DrainResult::Message(msg) => return Ok(Some(msg)),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {
                    if woke_without_message && cancel.is_canceled() {
                        return Ok(None);
                    }
                    woke_without_message = false;
                }
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(Some(msg));
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        if cancel.is_canceled() {
                            return Ok(None);
                        }
                        continue;
                    }
                    if cancel.is_canceled() {
                        return Ok(None);
                    }
                    if let Some(deadline) = deadline {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            return Err(Error::Timeout);
                        }
                        waiter.park_timeout(remaining);
                    } else {
                        waiter.park();
                    }
                    woke_without_message = true;
                }
            }
        }
    }

    pub(crate) fn blocking_recv_timeout(&self, timeout: Duration) -> Result<Message> {
        let now = Instant::now();
        let Some(deadline) = now.checked_add(timeout) else {
            return self.blocking_recv();
        };
        self.blocking_recv_until(deadline)
    }

    /// Wake `waker` whenever a message may have become receivable.
    pub(crate) fn register_recv_waker(
        &self,
        waker: std::task::Waker,
    ) -> crate::engine::signal::WakerRegistration {
        self.blocking_recv_waker.register_waker(waker)
    }

    pub(crate) fn blocking_recv_until(&self, deadline: Instant) -> Result<Message> {
        let mut waiter = None;
        loop {
            match self.try_drain_with_spin(Some(deadline), || false) {
                DrainResult::Message(msg) => return Ok(msg),
                DrainResult::Closed => return Err(Error::Closed),
                DrainResult::Empty => {}
            }
            let waiter = waiter.get_or_insert_with(|| self.blocking_recv_waker.register());
            waiter.prepare_sleep();
            match self.try_drain() {
                DrainResult::Message(msg) => {
                    return Ok(msg);
                }
                DrainResult::Closed => {
                    return Err(Error::Closed);
                }
                DrainResult::Empty => {
                    if !self.buffered_sources_empty() {
                        if Instant::now() >= deadline {
                            return Err(Error::Timeout);
                        }
                        continue;
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(Error::Timeout);
                    }
                    waiter.park_timeout(remaining);
                }
            }
        }
    }

    /// Spin only after an empty drain, before the existing sleep/recheck
    /// protocol. No drain lock is held between attempts, so producers and
    /// socket close can still make progress. The default path reads no clock.
    #[inline]
    fn try_drain_with_spin(
        &self,
        deadline: Option<Instant>,
        interrupted: impl Fn() -> bool,
    ) -> DrainResult {
        let result = self.try_drain();
        if !matches!(result, DrainResult::Empty) || self.recv_spin.is_zero() {
            return result;
        }
        let spin_deadline = (self.recv_spin != Duration::MAX)
            .then(|| crate::socket::deadline_after(self.recv_spin))
            .flatten();
        let deadline = match (deadline, spin_deadline) {
            (Some(timeout), Some(spin)) => Some(timeout.min(spin)),
            (timeout, spin) => timeout.or(spin),
        };
        let single = if self.spin_single {
            let state = self.drain_state.lock();
            (state.tcp.len() == 1 && state.tcp[0].spin_probe).then_some(state.generation)
        } else {
            None
        };
        let fanin_probe = self.active_fanin().filter(|_| !self.has_ring_sources());
        // Cheap idle probes check time once per eight attempts. Other
        // receive sources keep their per-attempt budget check. Cancellation
        // and queue closure remain observable on every attempt.
        let clock_stride = if single.is_some() || fanin_probe.is_some() {
            8
        } else {
            1
        };
        while deadline.is_none_or(|deadline| Instant::now() < deadline) {
            if fanin_probe.is_some() && self.has_ring_sources() {
                // A peer added during continuous spinning must join the
                // full fair drain; the initial source snapshot is stale.
                return self.try_drain();
            }
            for _ in 0..clock_stride {
                if interrupted() {
                    return DrainResult::Empty;
                }
                std::hint::spin_loop();
                let result = if let Some(fanin) = fanin_probe {
                    match fanin.try_recv_spinning() {
                        Ok(message) => DrainResult::Message(message),
                        Err(Error::Closed) => DrainResult::Closed,
                        Err(_) => DrainResult::Empty,
                    }
                } else if let Some(generation) = single {
                    self.try_drain_spinning(generation)
                } else {
                    self.try_drain()
                };
                match result {
                    DrainResult::Empty => {}
                    result => return result,
                }
            }
        }
        DrainResult::Empty
    }

    /// A spinning caller will poll again or perform the full sleep/recheck
    /// protocol. Empty probes need no signal fences or staging vector.
    fn try_drain_spinning(&self, generation: u64) -> DrainResult {
        // The full empty drain established these signals before spinning.
        // Deliveries and pipe closure mark them; source changes bump the
        // generation. An idle probe needs no mutable consumer access.
        if self.recv_signal.is_idle()
            && self.recv_pipe_notify.is_idle()
            && self.consumer_generation.load(Ordering::Acquire) == generation
        {
            return DrainResult::Empty;
        }
        {
            let mut state = self.drain_state.lock();
            self.refresh_snapshot(&mut state);
            if state.batch.is_empty()
                && state.tcp.len() == 1
                && state.tcp[0].spin_probe
                && state.recv_consumer.is_empty()
                && !state.recv_consumer.is_disconnected()
            {
                let mut budget = DrainBudget::new(1, RECV_BATCH_BYTES);
                let outcome = drain_peer_source(
                    &state.tcp[0],
                    true,
                    &mut Vec::new(),
                    &mut budget,
                    DrainLimit::One,
                );
                if let Some(message) = outcome.message {
                    return DrainResult::Message(message);
                }
                if !outcome.disconnected {
                    return DrainResult::Empty;
                }
            }
        }
        self.try_drain()
    }
}
