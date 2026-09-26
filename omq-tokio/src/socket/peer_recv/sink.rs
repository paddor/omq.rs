//! Connection-owned receive producer. A full queue pauses only reading; the
//! connection driver still services replies, control commands and cancellation.

use std::task::{Context, Waker};

use super::{Arc, Coordinated, Message, Ordering, QueueState, RecvItem, RecvShared, mpsc};

#[derive(Debug)]
pub(crate) struct PeerRecvSink {
    producer: Option<mpsc::Sender<RecvItem, Coordinated>>,
    waker: Waker,
    state: Arc<QueueState>,
    shared: Arc<RecvShared>,
    pending: Option<Message>,
    dirty: bool,
}

impl PeerRecvSink {
    pub(super) fn new(
        producer: mpsc::Sender<RecvItem, Coordinated>,
        state: Arc<QueueState>,
        shared: Arc<RecvShared>,
    ) -> Self {
        Self {
            producer: Some(producer),
            waker: Waker::from(state.clone()),
            state,
            shared,
            pending: None,
            dirty: false,
        }
    }

    fn alive(&self) -> bool {
        self.state.current.load(Ordering::Acquire)
            && self
                .producer
                .as_ref()
                .is_some_and(|producer| !producer.is_disconnected())
    }

    pub(crate) fn blocked(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) fn push(&mut self, message: Message) -> bool {
        assert!(
            self.pending.is_none(),
            "drain must stop at a full PEER queue"
        );
        if !self.alive() || self.shared.budget.oversize(&message) {
            return false;
        }
        // Socket close stops application admission immediately, but outbound
        // messages may still drain during linger. Receiver drop cancels the
        // peer separately; merely starting socket close must not do that.
        if self.shared.closed.load(Ordering::Acquire) {
            return true;
        }
        let producer = self.producer.as_mut().expect("live receive producer");
        if producer.is_full() {
            self.pending = Some(message);
            return true;
        }
        let message = match self.shared.budget.reserve(message) {
            Ok(message) => message,
            Err(message) => {
                self.pending = Some(message);
                return true;
            }
        };
        match producer.try_send(RecvItem {
            body: message,
            state: self.state.clone(),
        }) {
            Ok(()) => self.dirty = true,
            Err(mpsc::TrySendError::Full(item)) => self.pending = Some(item.body.into_message()),
            Err(mpsc::TrySendError::Disconnected(_)) => return false,
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
            self.dirty = false;
            // Coordinated fanring publishes each entry. External notification
            // remains one coalesced mark per driver batch, never per push.
            self.shared.mark();
        }
    }

    pub(crate) async fn ready(&mut self) {
        self.flush();
        let seen = self.state.space.generation();
        let budget_seen = self.shared.budget.space.generation();
        if self.shared.closed.load(Ordering::Acquire) || !self.alive() {
            return;
        }
        if self
            .producer
            .as_mut()
            .expect("live receive producer")
            .poll_ready(&mut Context::from_waker(&self.waker))
            .is_pending()
        {
            self.state.space.changed_after(seen).await;
        } else if let Some(message) = &self.pending
            && !self.shared.budget.room(message.max_message_size_len())
        {
            self.shared.budget.space.changed_after(budget_seen).await;
        }
    }
}

impl Drop for PeerRecvSink {
    fn drop(&mut self) {
        self.flush();
        // Publish disconnect before the external wake, so receive can retire
        // an empty ring without needing another connection to send data.
        self.producer.take();
        // Release the unqueued payload before waking the application.
        self.pending.take();
        self.state.space.notify_changed();
        self.shared.mark();
    }
}
