//! Consumer half: budgeted drains and low-water credit return.

#[cfg(feature = "dart")]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_proto::message::Message;

use super::{ConflateState, SEND_PIPE_LWM_DIVISOR, SendPipeConsumer, SendPipeConsumerInner};
use crate::engine::signal::{DataSignal, StateSignal};

impl SendPipeConsumer {
    #[cfg(feature = "dart")]
    pub(crate) fn dart_forward_to(&self, signal: Arc<DataSignal>) {
        self.data_signal.forward_to(signal);
    }

    #[cfg(feature = "dart")]
    pub(crate) fn dart_acknowledge(&self, count: usize) {
        if let Some(counter) = self.data_signal.dart_admission.get() {
            counter.release(count);
            self.space_available.notify_changed();
        }
    }

    pub(crate) fn needs_drain(&self) -> bool {
        !self.is_empty() || self.is_disconnected() || !self.data_signal.is_idle()
    }

    pub(crate) async fn ready(&self) {
        loop {
            if self.needs_drain() {
                return;
            }

            self.data_signal.ready().await;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.is_empty(),
            SendPipeConsumerInner::Queue(consumer) => consumer.is_empty(),
            SendPipeConsumerInner::Conflate(state) => {
                state.slot.lock().expect("conflate send pipe").is_none()
            }
        }
    }

    /// Whether a drain freed space for a producer that found the queue full
    /// since the last call. Peer and conflate pipes never report one.
    pub(crate) fn take_freed_full(&mut self) -> bool {
        std::mem::take(&mut self.freed_full)
    }

    /// Transfer Queue entries directly into the IO owner's storage. The
    /// callback can stop after an entry consumes the remaining destination
    /// capacity, leaving the following entries in the ring.
    #[inline]
    pub(crate) fn drain_queue_with(
        &mut self,
        max_msgs: usize,
        max_bytes: usize,
        mut accept: impl FnMut(Message) -> bool,
    ) -> Option<(usize, usize)> {
        let SendPipeConsumerInner::Queue(consumer) = &mut self.inner else {
            return None;
        };

        self.data_signal.begin_drain();
        consumer.prefetch();
        let mut count = 0;
        let mut bytes = 0;

        while count < max_msgs && bytes < max_bytes {
            let Some(message) = consumer.pop() else {
                break;
            };

            bytes += message.byte_len();
            count += 1;

            if !accept(message) {
                break;
            }
        }

        if count != 0 {
            return_queue_credit(
                consumer,
                &self.above_lwm,
                &self.space_available,
                &mut self.freed_full,
            );
        }

        self.data_signal.clear_after(consumer.is_empty());
        Some((count, bytes))
    }

    pub(crate) fn drain_into(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        self.drain_into_with_cost(batch, max_msgs, max_bytes, |message, admitted| {
            admitted.unwrap_or_else(|| message.byte_len())
        })
    }

    /// Drain with an IO-work byte charge, preserving admission byte accounting.
    pub(crate) fn drain_into_with_cost(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
        mut cost: impl FnMut(&Message, Option<usize>) -> usize,
    ) -> usize {
        let Self {
            inner,
            data_signal,
            space_available,
            above_lwm,
            freed_full,
        } = self;
        data_signal.begin_drain();

        if let SendPipeConsumerInner::Peer(peer) = inner {
            let count = peer.drain_into_with_cost(batch, max_msgs, max_bytes, |message, bytes| {
                cost(message, Some(bytes))
            });
            data_signal.clear_after(peer.is_empty());

            return count;
        }

        let SendPipeConsumerInner::Queue(consumer) = inner else {
            let SendPipeConsumerInner::Conflate(state) = inner else {
                unreachable!("send pipe consumer inner must be queue or conflate")
            };

            return Self::drain_conflate(data_signal, state, batch, max_msgs, max_bytes);
        };

        consumer.prefetch();
        let mut count = 0usize;
        let mut bytes = 0usize;

        while count < max_msgs && bytes < max_bytes {
            let Some(msg) = consumer.pop() else {
                break;
            };

            bytes += cost(&msg, None);
            batch.push(msg);
            count += 1;
        }

        if count > 0 {
            return_queue_credit(consumer, above_lwm, space_available, freed_full);
        }

        data_signal.clear_after(consumer.is_empty());
        count
    }

    #[cold]
    fn drain_conflate(
        data_signal: &DataSignal,
        state: &ConflateState,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        let count = if max_msgs == 0 || max_bytes == 0 {
            0
        } else if let Some(msg) = state.slot.lock().expect("conflate send pipe").take() {
            batch.push(msg);
            1
        } else {
            0
        };

        let is_empty = state.slot.lock().expect("conflate send pipe").is_none();
        data_signal.clear_after(is_empty);
        count
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        match &self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.is_disconnected(),
            SendPipeConsumerInner::Queue(consumer) => consumer.is_disconnected(),
            SendPipeConsumerInner::Conflate(state) => {
                state.producer_dropped.load(Ordering::Acquire)
                    && state.slot.lock().expect("conflate send pipe").is_none()
            }
        }
    }
}

/// Release drained ring slots; the first drain to the low-water mark after a
/// producer found the queue full wakes it.
#[inline]
fn return_queue_credit(
    consumer: &mut yring::Consumer<Message>,
    above_lwm: &AtomicBool,
    space_available: &StateSignal,
    freed_full: &mut bool,
) {
    consumer.release();
    // An AcqRel swap after the release, paired with the producer's
    // `arm_space_wake`: either this sees the producer's flag, or the
    // producer acquires the released slots.
    if consumer.len() <= consumer.capacity() / SEND_PIPE_LWM_DIVISOR
        && above_lwm.swap(false, Ordering::AcqRel)
    {
        *freed_full = true;
        space_available.notify_changed();
    }
}

impl Drop for SendPipeConsumer {
    fn drop(&mut self) {
        #[cfg(feature = "dart")]
        if let Some(counter) = self.data_signal.dart_admission.get() {
            counter.close();
        }

        match &mut self.inner {
            SendPipeConsumerInner::Peer(peer) => peer.close(),
            SendPipeConsumerInner::Queue(consumer) => consumer.close(),
            SendPipeConsumerInner::Conflate(state) => {
                state.consumer_dropped.store(true, Ordering::Release);
            }
        }

        self.space_available.notify_changed();
    }
}
