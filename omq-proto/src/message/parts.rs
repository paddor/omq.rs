//! Bounded owning multipart tables. Payload storage is independent of recycling.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use concurrent_queue::ConcurrentQueue;

use super::{Message, Payload};

/// Preallocated multipart frame tables shared by message construction and drop.
///
/// This pool only bounds cached tables, not outstanding messages or payload
/// bytes. Exhaustion uses an ordinary allocation; sending never waits for a
/// table. Oversized tables are discarded on return. Messages can outlive the
/// pool handle and may be cloned or dropped on other threads.
/// Cached table checkout and return use a bounded atomic MPMC queue.
#[derive(Debug, Clone)]
pub struct MessagePool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    free: Option<ConcurrentQueue<Vec<Payload>>>,
    parts: usize,
}

impl MessagePool {
    /// Allocate `tables` empty tables, each with room for `parts` frames.
    /// Zero tables disables caching; zero parts only caches empty tables.
    #[must_use]
    pub fn new(tables: usize, parts: usize) -> Self {
        let free = (tables != 0).then(|| {
            let free = ConcurrentQueue::bounded(tables);
            for _ in 0..tables {
                free.push(Vec::with_capacity(parts))
                    .expect("preallocated table cache capacity");
            }

            free
        });

        Self(Arc::new(Pool { free, parts }))
    }

    /// Construct a message with the same frame and ownership rules as
    /// [`Message::multipart`], reusing a table when available.
    pub fn multipart<I, P>(&self, parts: I) -> Message
    where
        I: IntoIterator<Item = P>,
        P: Into<Bytes>,
    {
        self.multipart_payloads(
            parts
                .into_iter()
                .map(|part| Payload::from_compact_bytes(part.into())),
        )
    }

    /// Reuse a frame table without converting owning payloads to `Bytes`.
    pub fn multipart_payloads(&self, parts: impl IntoIterator<Item = Payload>) -> Message {
        let mut values = self.take();
        values.extend(parts);
        Message::from_parts(values)
    }

    pub(crate) fn take(&self) -> Parts {
        let free = self.0.free.as_ref().and_then(|free| free.pop().ok());
        Parts {
            values: free.unwrap_or_else(|| Vec::with_capacity(self.0.parts)),
            pool: Some(self.clone()),
            bytes: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Parts {
    values: Vec<Payload>,
    pool: Option<MessagePool>,
    bytes: AtomicUsize,
}

impl Parts {
    /// Keep the length while frame bytes are hot during assembly. Arbitrary
    /// mutable borrows still invalidate it through `DerefMut` below.
    pub(crate) fn push(&mut self, part: Payload) {
        let length = (*self.bytes.get_mut() != usize::MAX).then(|| part.len());
        self.values.push(part);

        if let Some(length) = length {
            let bytes = self.bytes.get_mut();
            *bytes = bytes.saturating_add(length);
        }
    }

    pub(crate) fn extend(&mut self, parts: impl IntoIterator<Item = Payload>) {
        let parts = parts.into_iter();
        self.values.reserve(parts.size_hint().0);

        for part in parts {
            self.push(part);
        }
    }

    pub(crate) fn insert(&mut self, index: usize, part: Payload) {
        let length = (*self.bytes.get_mut() != usize::MAX).then(|| part.len());
        self.values.insert(index, part);

        if let Some(length) = length {
            let bytes = self.bytes.get_mut();
            *bytes = bytes.saturating_add(length);
        }
    }

    pub(crate) fn remove(&mut self, index: usize) -> Payload {
        let length = (*self.bytes.get_mut() != usize::MAX).then(|| self.values[index].len());
        let part = self.values.remove(index);

        if let Some(length) = length {
            *self.bytes.get_mut() -= length;
        }

        part
    }

    pub(crate) fn take(&mut self) -> Self {
        let next = self
            .pool
            .as_ref()
            .map_or_else(|| Vec::new().into(), MessagePool::take);

        std::mem::replace(self, next)
    }

    pub(super) fn byte_len(&self) -> usize {
        let cached = self.bytes.load(Ordering::Relaxed);
        if cached != usize::MAX {
            return cached;
        }

        let bytes = self
            .values
            .iter()
            .fold(0usize, |total, part| total.saturating_add(part.len()));
        // Shared readers can compute the same immutable sum concurrently.
        self.bytes.store(bytes, Ordering::Relaxed);

        bytes
    }
}

impl From<Vec<Payload>> for Parts {
    fn from(values: Vec<Payload>) -> Self {
        Self {
            values,
            pool: None,
            bytes: AtomicUsize::new(usize::MAX),
        }
    }
}

impl Deref for Parts {
    type Target = Vec<Payload>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl DerefMut for Parts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Every mutable table access invalidates the sum, including routing,
        // decompression, delimiter normalization, and replacement of a frame.
        *self.bytes.get_mut() = usize::MAX;

        &mut self.values
    }
}

impl Clone for Parts {
    fn clone(&self) -> Self {
        let mut copy = self.pool.as_ref().map_or_else(
            || Self::from(Vec::with_capacity(self.len())),
            MessagePool::take,
        );
        copy.extend(self.iter().cloned());

        copy
    }
}

impl Drop for Parts {
    fn drop(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };

        let Some(free) = &pool.0.free else {
            return;
        };

        if self.values.capacity() > pool.0.parts {
            return;
        }

        // A payload owner may run arbitrary drop logic, including constructing
        // another message. Publish only after releasing all payload owners.
        self.values.clear();
        // Concurrent fallback returns can fill the cache; discard excess tables.
        let _ = free.push(std::mem::take(&mut self.values));
    }
}

#[cfg(test)]
mod tests;
