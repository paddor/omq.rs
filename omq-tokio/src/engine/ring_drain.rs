//! Receive-ring release policy shared by every consumer of a recv yring.

use omq_proto::message::Message;

/// Consumer-side progress through a recv yring's prefetched window.
///
/// Consumed slots go back to the producer in batches: at the low-water mark
/// or when the window runs out. A producer parked on a full ring then wakes
/// once per batch instead of once per message. The cached window stays valid
/// after partial releases.
#[derive(Debug, Default)]
pub struct RingDrain {
    pub(crate) remaining: usize,
    pub(crate) popped: usize,
}

impl RingDrain {
    pub(crate) fn lwm(capacity: usize) -> usize {
        // libzmq keeps at most 1024 occupied slots at the large-ring boundary.
        if capacity > 2048 {
            capacity - 1024
        } else {
            (capacity / 2).max(1)
        }
    }

    /// Release only consumed slots; retain the remainder of the cached window.
    pub(crate) fn release(&mut self, consumer: &mut yring::Consumer<Message>) -> bool {
        if self.popped == 0 {
            return false;
        }

        self.popped = 0;
        consumer.release_with_full()
    }

    /// Pop one message, releasing consumed slots at the low-water mark or the
    /// window boundary. An empty ring releases everything consumed so far.
    ///
    /// The flag reports that a producer waiting for space must be woken.
    pub fn pop(&mut self, consumer: &mut yring::Consumer<Message>) -> (Option<Message>, bool) {
        loop {
            if self.remaining == 0 {
                self.remaining = consumer.prefetch();
                if self.remaining == 0 {
                    return (None, self.release(consumer));
                }
            }

            if let Some(item) = consumer.pop() {
                self.remaining -= 1;
                self.popped += 1;
                let wake = (self.remaining == 0 || self.popped >= Self::lwm(consumer.capacity()))
                    && self.release(consumer);

                return (Some(item), wake);
            }

            let wake = self.release(consumer);
            self.remaining = 0;

            if wake {
                return (None, true);
            }
        }
    }
}

#[cfg(test)]
mod tests;
