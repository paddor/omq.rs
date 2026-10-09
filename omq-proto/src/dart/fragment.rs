//! Bounded fragment retention and incremental atomic receive assembly.
use super::{DATA_HEADER, Duration, Ecn, Message, Sent, Session};
use crate::dart::MAX_FRAGMENT_BODY;
use crate::message::Payload;

#[derive(Debug)]
pub(super) struct FragmentSend {
    message: Message,
    offset: usize,
    chunk: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FragmentPiece {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) length: Option<u64>,
    pub(super) last: bool,
}

/// Exclusive, fully reserved storage for incremental fragment assembly.
/// Appending must not allocate or exceed the reserved capacity.
pub trait FragmentBuffer: std::fmt::Debug + AsRef<[u8]> {
    /// Reserved byte capacity, including the already appended prefix.
    fn capacity(&self) -> usize;
    /// Append every byte and extend the prefix exposed by `AsRef<[u8]>`.
    /// The caller supplies no more than the remaining reserved capacity.
    fn append(&mut self, bytes: &[u8]);
}

impl FragmentBuffer for Vec<u8> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn append(&mut self, bytes: &[u8]) {
        assert!(bytes.len() <= self.capacity() - self.len());
        self.extend_from_slice(bytes);
    }
}

#[derive(Debug)]
pub(super) enum FragmentReceive<B> {
    First(Assembly<B>),
    Continuation,
}

#[derive(Debug)]
pub(super) struct Assembly<B> {
    body: B,
    length: usize,
    group: Option<bytes::Bytes>,
}

impl<B: FragmentBuffer> Session<B> {
    pub(super) fn submit_fragmented(&mut self, message: Message) -> Result<u64, Message> {
        let grouped = message.len() == 2;
        let length = message
            .part_slice(usize::from(grouped))
            .expect("validated body")
            .len();
        let metadata = if grouped {
            1 + message.part_slice(0).expect("group").len()
        } else {
            0
        };
        // Include FIRST's metadata when balancing wire lengths. Prefer an
        // exact split when other messages are queued, so the final fragment
        // does not end each GSO/GRO batch. Isolated messages minimize packets.
        // Permit at most one extra datagram to bound per-fragment overhead.
        let payload = length + 8 + metadata;
        let minimum = payload.div_ceil(MAX_FRAGMENT_BODY).max(2);
        let count = if self.send_cursor < self.send_next {
            (minimum..=minimum + 1)
                .find(|count| payload.is_multiple_of(*count))
                .unwrap_or(minimum)
        } else {
            minimum
        };
        let chunk = payload.div_ceil(count);
        let Some(end) = self
            .send_next
            .checked_add(count as u64)
            .filter(|end| *end <= u64::MAX - self.config.window as u64)
        else {
            return Err(message);
        };
        self.sending = Some(FragmentSend {
            message,
            offset: 0,
            chunk,
        });
        self.fill_fragments();
        Ok(end - 1)
    }

    pub(super) fn fill_fragments(&mut self) -> bool {
        let mut budget = crate::flow::DrainBudget::new(64, 64_000);
        while self.outstanding() < self.config.window && !budget.exhausted() {
            let Some(pending) = &mut self.sending else {
                break;
            };
            let grouped = pending.message.len() == 2;
            let body_len = pending
                .message
                .part_slice(usize::from(grouped))
                .expect("body")
                .len();
            let first = pending.offset == 0;
            let metadata = if first && grouped {
                1 + pending.message.part_slice(0).expect("group").len()
            } else {
                0
            };
            let extra = usize::from(first) * 8 + metadata;
            let header = DATA_HEADER + extra;
            let chunk = (pending.chunk - extra).min(body_len - pending.offset);
            let part = FragmentPiece {
                start: pending.offset,
                end: pending.offset + chunk,
                length: first.then_some(body_len as u64),
                last: pending.offset + chunk == body_len,
            };
            pending.offset += chunk;
            let sent = Sent {
                message: pending.message.clone(),
                fragment: Some(part),
                first: None,
                last: Duration::ZERO,
                attempts: 0,
                repair: false,
                in_flight: false,
                bytes: header + chunk,
            };
            let index = self.index(self.send_next);
            self.tx[index] = Some(sent);
            self.send_next += 1;
            let _ = budget.account(chunk);
            if part.last {
                self.sending = None;
            }
        }
        budget.msgs() != 0
    }

    /// Reserve the entire advertised body before committing FIRST. No partial
    /// message is visible to the application, even across multiple windows.
    /// `reserve` runs only for FIRST and must return empty storage with at
    /// least the advertised byte capacity. `false` leaves receipt uncommitted.
    /// The sequence must have been accepted by [`Self::classify`].
    pub fn commit_fragment_with(
        &mut self,
        sequence: u64,
        length: Option<u64>,
        message: Message,
        ecn: Ecn,
        now: Duration,
        reserve: impl FnOnce(usize) -> Option<B>,
    ) -> bool {
        let fragment = if let Some(length) = length {
            let Ok(length) = usize::try_from(length) else {
                return false;
            };
            let grouped = message.len() == 2;
            let Some(chunk) = message.part_slice(usize::from(grouped)) else {
                return false;
            };
            if length <= chunk.len() || chunk.is_empty() {
                return false;
            }
            let Some(body) = reserve(length) else {
                return false;
            };
            if !body.as_ref().is_empty() || body.capacity() < length {
                return false;
            }
            FragmentReceive::First(Assembly {
                body,
                length,
                group: grouped.then(|| message.part_bytes(0).expect("group")),
            })
        } else {
            if message.len() != 1 || message.byte_len() == 0 {
                return false;
            }
            FragmentReceive::Continuation
        };
        let index = self.index(sequence);
        self.fragments[index] = Some(fragment);
        self.commit_receive(sequence, message, ecn, now);
        true
    }

    /// Whether fragment assembly encountered an invalid sequence or body length.
    pub const fn receive_failed(&self) -> bool {
        self.receive_failed
    }

    pub(super) fn take_fragmented(&mut self, finish: impl FnOnce(B) -> Payload) -> Option<Message> {
        let mut budget = crate::flow::DrainBudget::new(64, 64_000);
        while self.deliver_next < self.receive_next && !budget.exhausted() {
            let index = self.index(self.deliver_next);
            let fragment = self.fragments[index].take();
            match fragment {
                Some(FragmentReceive::First(assembly)) if self.assembly.is_none() => {
                    self.assembly = Some(assembly);
                }
                Some(FragmentReceive::Continuation) if self.assembly.is_some() => {}
                _ => {
                    self.receive_failed = true;
                    return None;
                }
            }
            let message = self.rx[index].take().expect("retained fragment");
            let chunk = message
                .part_slice(usize::from(message.len() == 2))
                .expect("chunk");
            let assembly = self.assembly.as_mut().expect("FIRST assembly");
            if chunk.len() > assembly.length - assembly.body.as_ref().len() {
                self.receive_failed = true;
                return None;
            }
            assembly.body.append(chunk);
            let _ = budget.account(chunk.len());
            self.deliver_next += 1;
            if assembly.body.as_ref().len() == assembly.length {
                let assembly = self.assembly.take().expect("complete body");
                let mut message = Message::from(finish(assembly.body));
                if let Some(group) = assembly.group {
                    message = Message::with_prefix(group, message);
                }
                return Some(message);
            }
            self.release_receive(1);
        }
        None
    }
}
