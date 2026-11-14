//! Per-connection PEER fan-in. Payload limits cover every producer together;
//! registration limits include retired rings until the consumer reclaims them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Wake, Waker};

use fanring::{mpsc, teardown::Coordinated};
use omq_proto::{Message, Options, options::DEFAULT_PEER_MAX_BYTES};

use super::SendPipeError;
use super::send_pipe::SendPreparation;
use super::signal::{DataSignal, StateSignal};

#[derive(Debug)]
struct Shared {
    messages: AtomicUsize,
    bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    max_senders: usize,
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
    options: &Options,
    data: Arc<DataSignal>,
    space: Arc<StateSignal>,
) -> (Producer, Consumer) {
    let (sender, receiver) =
        mpsc::channel_with_policy(capacity.max(1).min(options.peer_send_queue_capacity));

    let shared = Arc::new(Shared {
        messages: AtomicUsize::new(0),
        bytes: AtomicUsize::new(0),
        max_messages: capacity.max(1),
        max_bytes: options
            .peer_send_max_bytes
            .unwrap_or_else(|| DEFAULT_PEER_MAX_BYTES.max(options.max_message_size.unwrap_or(0))),
        max_senders: options.peer_max_senders,
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
            .try_register_bounded(self.shared.max_senders + 1)
            .ok()
            .map(|sender| Self::new(sender, self.shared.clone()))
    }

    pub(crate) fn registration_ready(&self) -> bool {
        !self.alive()
            || self
                .sender
                .as_ref()
                .is_some_and(|sender| sender.registered_lanes() < self.shared.max_senders + 1)
    }

    #[cfg(feature = "dart")]
    pub(crate) fn max_messages(&self) -> usize {
        self.shared.max_messages
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

    /// Ring space and shared budget signals: either can admit a send.
    pub(crate) fn space_signals(&self) -> [Arc<StateSignal>; 2] {
        [self.space.clone(), self.shared.space.clone()]
    }

    pub(crate) fn registration_space(&self) -> Arc<StateSignal> {
        self.shared.space.clone()
    }

    /// Whether a send may be admitted now, judged without polling the ring:
    /// the last ring poll found space and the shared budget has room.
    pub(crate) fn has_space(&self) -> bool {
        !self.ring_space.waiting.load(Ordering::Acquire) && self.budget_has_room()
    }

    /// Shared budget room for the message a budget rejection left waiting,
    /// or for any message otherwise. Readiness and admission must agree, or
    /// a poller wakes for a send that fails again.
    fn budget_has_room(&self) -> bool {
        let bytes = self.shared.bytes.load(Ordering::Acquire);
        self.shared.messages.load(Ordering::Acquire) < self.shared.max_messages
            && if self.waiting_budget {
                bytes <= self.shared.max_bytes.saturating_sub(self.waiting_bytes)
            } else {
                bytes < self.shared.max_bytes
            }
    }

    pub(crate) fn ready(&self) -> bool {
        !self.alive()
            || if self.waiting_budget {
                self.budget_has_room()
            } else {
                // A ring wake must release this wait even if another producer
                // filled the shared budget. Retry then selects the budget wake.
                !self.ring_space.waiting.load(Ordering::Acquire)
            }
    }

    #[cfg(test)]
    pub(crate) fn try_send(&mut self, message: Message) -> Result<(), SendPipeError> {
        self.try_send_prepared(message, SendPreparation::Plain, |_| false)
    }

    // Atomic::try_update is unstable on MSRV 1.93.
    #[allow(deprecated)]
    pub(crate) fn try_send_prepared(
        &mut self,
        message: Message,
        preparation: SendPreparation,
        direct: impl FnOnce(&Message) -> bool,
    ) -> Result<(), SendPipeError> {
        if !self.alive() {
            return Err(SendPipeError::Closed(message));
        }

        self.waiting_budget = false;
        let bytes = preparation.bytes(&message);
        self.waiting_bytes = bytes;

        match super::producer_ready::ready(
            self.sender.as_mut().expect("live sender"),
            &self.ring_space.waiting,
            &self.waker,
            || self.space.notify_changed(),
        ) {
            Ok(false) => return Err(SendPipeError::Full(message)),
            Err(()) => return Err(SendPipeError::Closed(message)),
            Ok(true) => {}
        }

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
            message: Some(preparation.prepare(message)),
            bytes,
            shared: self.shared.clone(),
        };

        if direct(queued.message.as_ref().expect("admitted message")) {
            return Ok(());
        }

        match self.sender.as_mut().expect("live sender").try_send(queued) {
            Ok(()) => {
                self.shared.data.mark();
                Ok(())
            }
            Err(mpsc::TrySendError::Full(_)) => {
                unreachable!("admitted producer capacity cannot be stolen")
            }
            // The receiver can disappear after admission, just as it can
            // immediately after publication. Never retry an accepted message.
            Err(mpsc::TrySendError::Disconnected(_)) => Ok(()),
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

    #[cfg(test)]
    pub(crate) fn drain_into(
        &mut self,
        out: &mut Vec<Message>,
        max: usize,
        max_bytes: usize,
    ) -> usize {
        self.drain_into_with_cost(out, max, max_bytes, |_, bytes| bytes)
    }

    pub(crate) fn drain_into_with_cost(
        &mut self,
        out: &mut Vec<Message>,
        max: usize,
        max_bytes: usize,
        mut cost: impl FnMut(&Message, usize) -> usize,
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

            bytes += cost(
                queued.message.as_ref().expect("queued message"),
                queued.bytes,
            );
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
mod tests;
