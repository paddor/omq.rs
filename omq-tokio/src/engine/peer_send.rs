//! Per-connection PEER fan-in. Payload limits cover every producer together;
//! registration limits include retired rings until the consumer reclaims them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

use fanring::{mpsc, teardown::Coordinated};
use omq_proto::Message;

use super::SendPipeError;
use super::signal::{DataSignal, StateSignal};

pub(crate) const MAX_SENDERS: usize = 64;
pub(crate) const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
struct Shared {
    messages: AtomicUsize,
    bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    closed: AtomicBool,
    data: Arc<DataSignal>,
    space: Arc<StateSignal>,
}

#[derive(Debug)]
struct Queued {
    message: Option<Message>,
    bytes: usize,
    shared: Arc<Shared>,
}

impl Queued {
    fn into_message(mut self) -> Message {
        self.message.take().expect("queued message")
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.shared.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.shared.messages.fetch_sub(1, Ordering::AcqRel);
        self.shared.space.notify_changed();
    }
}

#[derive(Debug)]
struct SpaceWake {
    signal: Arc<StateSignal>,
    waiting: AtomicBool,
}

impl Wake for SpaceWake {
    fn wake(self: Arc<Self>) {
        self.waiting.store(false, Ordering::Release);
        self.signal.notify_changed();
    }
}

#[derive(Debug)]
pub(crate) struct Producer {
    sender: Option<mpsc::Sender<Queued, Coordinated>>,
    shared: Arc<Shared>,
    space: Arc<StateSignal>,
    waker: Waker,
    ring_space: Arc<SpaceWake>,
    waiting_bytes: usize,
    waiting_budget: bool,
}

#[derive(Debug)]
pub(crate) struct Consumer {
    receiver: Option<mpsc::Receiver<Queued, Coordinated>>,
    shared: Arc<Shared>,
}

pub(crate) fn channel(
    capacity: usize,
    max_message_size: Option<usize>,
    data: Arc<DataSignal>,
    space: Arc<StateSignal>,
) -> (Producer, Consumer) {
    let (sender, receiver) = mpsc::channel_with_policy(capacity.clamp(1, 64));
    let shared = Arc::new(Shared {
        messages: AtomicUsize::new(0),
        bytes: AtomicUsize::new(0),
        max_messages: capacity.max(1),
        max_bytes: max_message_size
            .unwrap_or(DEFAULT_MAX_BYTES)
            .max(DEFAULT_MAX_BYTES),
        closed: AtomicBool::new(false),
        data,
        space,
    });
    (
        Producer::new(sender, shared.clone()),
        Consumer {
            receiver: Some(receiver),
            shared,
        },
    )
}

impl Producer {
    fn new(sender: mpsc::Sender<Queued, Coordinated>, shared: Arc<Shared>) -> Self {
        let space = Arc::new(StateSignal::new());
        let ring_space = Arc::new(SpaceWake {
            signal: space.clone(),
            waiting: AtomicBool::new(false),
        });
        Self {
            sender: Some(sender),
            shared,
            waker: Waker::from(ring_space.clone()),
            ring_space,
            space,
            waiting_bytes: 0,
            waiting_budget: false,
        }
    }

    pub(crate) fn register(&self) -> Option<Self> {
        self.sender
            .as_ref()?
            .try_register_bounded(MAX_SENDERS + 1)
            .ok()
            .map(|sender| Self::new(sender, self.shared.clone()))
    }

    pub(crate) fn registration_ready(&self) -> bool {
        !self.alive()
            || self
                .sender
                .as_ref()
                .is_some_and(|sender| sender.registered_lanes() < MAX_SENDERS + 1)
    }

    pub(crate) fn max_bytes(&self) -> usize {
        self.shared.max_bytes
    }

    pub(crate) fn alive(&self) -> bool {
        !self.shared.closed.load(Ordering::Acquire)
            && self
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_disconnected())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.shared.messages.load(Ordering::Acquire) == 0
    }

    pub(crate) fn space(&self) -> Arc<StateSignal> {
        if self.waiting_budget {
            self.shared.space.clone()
        } else {
            self.space.clone()
        }
    }

    pub(crate) fn registration_space(&self) -> Arc<StateSignal> {
        self.shared.space.clone()
    }

    pub(crate) fn ready(&self) -> bool {
        !self.alive()
            || if self.waiting_budget {
                self.shared.messages.load(Ordering::Acquire) < self.shared.max_messages
                    && self.shared.bytes.load(Ordering::Acquire)
                        <= self.shared.max_bytes.saturating_sub(self.waiting_bytes)
            } else {
                // A ring wake must release this wait even if another producer
                // filled the shared budget. Retry then selects the budget wake.
                !self.ring_space.waiting.load(Ordering::Acquire)
            }
    }

    pub(crate) fn try_send(&mut self, message: Message) -> Result<(), SendPipeError> {
        if !self.alive() {
            return Err(SendPipeError::Closed(message));
        }
        self.waiting_budget = false;
        let bytes = message.max_message_size_len();
        self.waiting_bytes = bytes;
        self.ring_space.waiting.store(true, Ordering::Release);
        if self
            .sender
            .as_mut()
            .expect("live sender")
            .poll_ready(&mut Context::from_waker(&self.waker))
            .is_pending()
        {
            return Err(SendPipeError::Full(message));
        }
        self.ring_space.waiting.store(false, Ordering::Release);
        self.waiting_budget = true;
        if !self.ready()
            || self
                .shared
                .messages
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    n.checked_add(1).filter(|n| *n <= self.shared.max_messages)
                })
                .is_err()
        {
            return Err(SendPipeError::Full(message));
        }
        if self
            .shared
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= self.shared.max_bytes)
            })
            .is_err()
        {
            self.shared.messages.fetch_sub(1, Ordering::AcqRel);
            self.shared.space.notify_changed();
            return Err(SendPipeError::Full(message));
        }
        self.waiting_budget = false;
        let queued = Queued {
            message: Some(message),
            bytes,
            shared: self.shared.clone(),
        };
        match self.sender.as_mut().expect("live sender").try_send(queued) {
            Ok(()) => {
                self.shared.data.mark();
                Ok(())
            }
            Err(mpsc::TrySendError::Full(queued)) => {
                Err(SendPipeError::Full(queued.into_message()))
            }
            Err(mpsc::TrySendError::Disconnected(queued)) => {
                Err(SendPipeError::Closed(queued.into_message()))
            }
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.space.notify_changed();
        self.sender.take();
        // Publish disconnect before waking the receiver for ring retirement.
        self.shared.data.mark();
        self.shared.space.notify_changed();
    }
}

impl Consumer {
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.messages.load(Ordering::Acquire) == 0
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.receiver
            .as_ref()
            .is_none_or(mpsc::Receiver::is_disconnected)
    }

    pub(crate) fn drain_into(
        &mut self,
        out: &mut Vec<Message>,
        max: usize,
        max_bytes: usize,
    ) -> usize {
        let Some(receiver) = &mut self.receiver else {
            return 0;
        };
        let start = out.len();
        let mut bytes = 0;
        while out.len() - start < max && bytes < max_bytes {
            let Ok(queued) = receiver.try_recv() else {
                break;
            };
            bytes += queued.bytes;
            out.push(queued.into_message());
        }
        receiver.release_consumed();
        // Includes ring retirement even when no payload was received.
        self.shared.space.notify_changed();
        out.len() - start
    }

    pub(crate) fn close(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.receiver.take();
        self.shared.space.notify_changed();
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(capacity: usize) -> (Producer, Consumer) {
        channel(
            capacity,
            None,
            Arc::new(DataSignal::new()),
            Arc::new(StateSignal::new()),
        )
    }

    #[test]
    fn producer_count_and_payload_budget_stay_bounded_through_churn() {
        let (root, mut receiver) = queue(4);
        let mut senders: Vec<_> = (0..MAX_SENDERS).map(|_| root.register().unwrap()).collect();
        assert!(root.register().is_none());
        for sender in &mut senders[..4] {
            sender.try_send(Message::single("x")).unwrap();
        }
        assert!(matches!(
            senders[4].try_send(Message::single("x")),
            Err(SendPipeError::Full(_))
        ));
        assert_eq!(root.shared.messages.load(Ordering::Acquire), 4);
        assert_eq!(
            root.shared.bytes.load(Ordering::Acquire),
            4 * (1 + std::mem::size_of::<omq_proto::message::Payload>())
        );
        drop(senders);
        assert!(root.register().is_none(), "retired rings still count");
        let mut out = Vec::new();
        assert_eq!(receiver.drain_into(&mut out, 8, 1024), 4);
        assert!(root.register().is_some());
        assert_eq!(root.shared.messages.load(Ordering::Acquire), 0);
        assert_eq!(root.shared.bytes.load(Ordering::Acquire), 0);
        assert!(root.sender.as_ref().unwrap().registered_lanes() <= 2);
    }

    #[test]
    fn receiver_drop_reclaims_payloads_while_idle_senders_live() {
        let (root, receiver) = queue(4);
        let mut sender = root.register().unwrap();
        sender.try_send(Message::single("held")).unwrap();
        drop(receiver);
        assert_eq!(root.shared.messages.load(Ordering::Acquire), 0);
        assert_eq!(root.shared.bytes.load(Ordering::Acquire), 0);
        assert!(!sender.alive());
        assert!(matches!(
            sender.try_send(Message::single("returned")),
            Err(SendPipeError::Closed(_))
        ));
    }

    #[test]
    fn busy_sender_cannot_starve_an_already_queued_sender() {
        let (root, mut receiver) = queue(256);
        let mut busy = root.register().unwrap();
        let mut quiet = root.register().unwrap();
        for _ in 0..64 {
            busy.try_send(Message::single("busy")).unwrap();
        }
        quiet.try_send(Message::single("quiet")).unwrap();
        let mut out = Vec::new();
        for _ in 0..=64 {
            receiver.drain_into(&mut out, 1, 1024);
            if out
                .last()
                .is_some_and(|message| message.part_slice(0) == Some(b"quiet"))
            {
                return;
            }
            busy.try_send(Message::single("busy")).unwrap();
        }
        panic!("quiet sender starved beyond one fanring burst");
    }
}
