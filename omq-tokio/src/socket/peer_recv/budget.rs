//! Shared lane admission limits. Ownership returns capacity even on cancellation
//! or queue destruction; no per-message allocation, just an Arc count.

use super::{Arc, AtomicUsize, Message, Ordering, StateSignal};

#[derive(Debug)]
pub(super) struct Budget {
    messages: AtomicUsize,
    bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    pub(super) space: StateSignal,
}

#[derive(Debug)]
struct Permit {
    budget: Arc<Budget>,
    bytes: usize,
}

#[derive(Debug)]
pub(super) struct QueuedMessage {
    message: Message,
    _permit: Permit,
}

impl QueuedMessage {
    pub(super) fn into_message(self) -> Message {
        self.message
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.messages.fetch_sub(1, Ordering::AcqRel);
        self.budget.space.notify_changed();
    }
}

impl Budget {
    pub(super) fn is_empty(&self) -> bool {
        self.messages.load(Ordering::Acquire) == 0
    }

    pub(super) fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            messages: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            max_messages,
            max_bytes,
            space: StateSignal::new(),
        }
    }

    pub(super) fn oversize(&self, message: &Message) -> bool {
        message.max_message_size_len() > self.max_bytes
    }

    pub(super) fn room(&self, bytes: usize) -> bool {
        bytes <= self.max_bytes
            && self.messages.load(Ordering::Acquire) < self.max_messages
            && self.bytes.load(Ordering::Acquire) <= self.max_bytes.saturating_sub(bytes)
    }

    pub(super) fn reserve(self: &Arc<Self>, message: Message) -> Result<QueuedMessage, Message> {
        let bytes = message.max_message_size_len();
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
        Ok(QueuedMessage {
            message,
            _permit: Permit {
                budget: self.clone(),
                bytes,
            },
        })
    }
}
