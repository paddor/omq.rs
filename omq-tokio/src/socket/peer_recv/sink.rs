//! Connection-owned receive producer. A full lane pauses only reading; the
//! connection driver still services replies, control commands and cancellation.

use super::{Arc, LaneShared, Message, Ordering, QueueState, QueuedMessage};

#[derive(Debug)]
pub(crate) struct PeerRecvSink {
    producer: yring::Producer<QueuedMessage>,
    state: Arc<QueueState>,
    lane: Arc<LaneShared>,
    pending: Option<Message>,
    dirty: bool,
}

impl PeerRecvSink {
    pub(super) fn new(
        producer: yring::Producer<QueuedMessage>,
        state: Arc<QueueState>,
        lane: Arc<LaneShared>,
    ) -> Self {
        Self {
            producer,
            state,
            lane,
            pending: None,
            dirty: false,
        }
    }

    fn alive(&self) -> bool {
        self.state.current.load(Ordering::Acquire) && !self.producer.is_consumer_dropped()
    }

    pub(crate) fn blocked(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) fn push(&mut self, message: Message) -> bool {
        assert!(
            self.pending.is_none(),
            "drain must stop at a full PEER queue"
        );
        if !self.alive() || self.lane.budget.oversize(&message) {
            return false;
        }
        // Socket close stops application admission immediately, but outbound
        // messages may still drain during linger. Receiver drop cancels the
        // peer separately; merely starting socket close must not do that.
        if self.lane.closed.load(Ordering::Acquire) {
            return true;
        }
        if self.producer.is_full() {
            self.pending = Some(message);
            return true;
        }
        let message = match self.lane.budget.reserve(message) {
            Ok(message) => message,
            Err(message) => {
                self.pending = Some(message);
                return true;
            }
        };
        match self.producer.push(message) {
            Ok(()) => self.dirty = true,
            Err(message) => self.pending = Some(message.into_message()),
        }
        true
    }

    pub(crate) fn retry_pending(&mut self) -> bool {
        if !self.alive() {
            return false;
        }
        if let Some(message) = self.pending.take() {
            if !self.push(message) {
                return false;
            }
            self.flush();
        }
        true
    }

    pub(crate) fn flush(&mut self) {
        if self.dirty {
            let flushed = self.producer.flush_and_check();
            self.dirty = false;
            if matches!(
                flushed,
                yring::FlushResult::Flushed {
                    was_empty: true,
                    ..
                }
            ) {
                self.lane.mark();
            }
        }
    }

    pub(crate) async fn ready(&mut self) {
        let seen = self.state.space.generation();
        let budget_seen = self.lane.budget.space.generation();
        if self.lane.closed.load(Ordering::Acquire) {
            return;
        }
        if self.alive() && self.producer.is_full() {
            self.state.space.changed_after(seen).await;
        } else if self.alive()
            && let Some(message) = &self.pending
            && !self.lane.budget.room(message.max_message_size_len())
        {
            self.lane.budget.space.changed_after(budget_seen).await;
        }
    }
}

impl Drop for PeerRecvSink {
    fn drop(&mut self) {
        self.flush();
        self.producer.close();
        // Release the unqueued payload before the final QueueState drop can
        // return this connection's allocation allowance.
        self.pending.take();
        self.state.space.notify_changed();
        self.lane.mark();
    }
}
