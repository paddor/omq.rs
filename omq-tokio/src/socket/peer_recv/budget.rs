//! Per-source receive memory bounds. Ownership returns capacity on cancellation
//! or queue destruction. Socket-wide message counts are observation only.

use super::{Arc, Message, Ordering, StateSignal};
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub(super) struct Budget {
    messages: AtomicUsize,
    bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    socket_messages: Arc<AtomicUsize>,
    pub(super) space: StateSignal,
}

#[derive(Debug)]
pub(super) struct Permit {
    budget: Arc<Budget>,
    bytes: usize,
}

#[derive(Debug)]
pub(super) struct QueuedMessage {
    message: Message,
    permit: Permit,
}

impl QueuedMessage {
    pub(super) fn byte_len(&self) -> usize {
        self.message.max_message_size_len()
    }

    pub(super) fn into_message(self) -> Message {
        self.message
    }

    pub(super) fn into_parts(self) -> (Message, Permit) {
        (self.message, self.permit)
    }

    pub(super) fn with_permit(message: Message, permit: Permit) -> Self {
        Self { message, permit }
    }
}

impl Permit {
    pub(super) fn fits(&self, message: &Message) -> bool {
        Budget::charge(message) <= self.bytes
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.messages.fetch_sub(1, Ordering::AcqRel);
        self.budget.socket_messages.fetch_sub(1, Ordering::AcqRel);
        self.budget.space.notify_changed();
    }
}

impl Budget {
    pub(super) fn charge(message: &Message) -> usize {
        message.retained_size().unwrap_or(usize::MAX)
    }

    pub(super) fn new(
        max_messages: usize,
        max_bytes: usize,
        socket_messages: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            messages: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            max_messages,
            max_bytes,
            socket_messages,
            space: StateSignal::new(),
        }
    }

    pub(super) fn oversize(&self, message: &Message) -> bool {
        Self::charge(message) > self.max_bytes
    }

    pub(super) fn room(&self, bytes: usize) -> bool {
        bytes <= self.max_bytes
            && self.messages.load(Ordering::Acquire) < self.max_messages
            && self.bytes.load(Ordering::Acquire) <= self.max_bytes.saturating_sub(bytes)
    }

    // Atomic::try_update is unstable on MSRV 1.93.
    #[allow(deprecated)]
    pub(super) fn reserve(self: &Arc<Self>, message: Message) -> Result<QueuedMessage, Message> {
        let bytes = Self::charge(&message);
        // Avoid provisional reservations/rollback notifications while no room
        // exists: blocked producers must not continually wake each other.
        if !self.room(bytes) {
            return Err(message);
        }
        if self
            .messages
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(1)
                    .filter(|next| *next <= self.max_messages)
            })
            .is_err()
        {
            return Err(message);
        }
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.max_bytes)
            })
            .is_err()
        {
            self.messages.fetch_sub(1, Ordering::AcqRel);
            self.space.notify_changed();
            return Err(message);
        }
        self.socket_messages.fetch_add(1, Ordering::AcqRel);
        Ok(QueuedMessage {
            message,
            permit: Permit {
                budget: self.clone(),
                bytes,
            },
        })
    }
}
