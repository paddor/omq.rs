//! Bounded owning multipart tables. Payload storage is independent of recycling.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{MAX_INLINE_PAYLOAD, Message, Payload};

/// Preallocated multipart frame tables shared by message construction and drop.
///
/// This pool only bounds cached tables, not outstanding messages or payload
/// bytes. Exhaustion uses an ordinary allocation; sending never waits for a
/// table. Oversized tables are discarded on return. Messages can outlive the
/// pool handle and may be cloned or dropped on other threads.
#[derive(Debug, Clone)]
pub struct MessagePool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    free: Mutex<Vec<Vec<Payload>>>,
    tables: usize,
    parts: usize,
}

impl MessagePool {
    /// Allocate `tables` empty tables, each with room for `parts` frames.
    /// Zero tables disables caching; zero parts only caches empty tables.
    #[must_use]
    pub fn new(tables: usize, parts: usize) -> Self {
        Self(Arc::new(Pool {
            free: Mutex::new((0..tables).map(|_| Vec::with_capacity(parts)).collect()),
            tables,
            parts,
        }))
    }

    /// Construct a message with the same frame and ownership rules as
    /// [`Message::multipart`], reusing a table when available.
    pub fn multipart<I, P>(&self, parts: I) -> Message
    where
        I: IntoIterator<Item = P>,
        P: Into<Bytes>,
    {
        self.multipart_payloads(parts.into_iter().map(|part| {
            let bytes = part.into();
            if bytes.len() <= MAX_INLINE_PAYLOAD {
                Payload::from_slice(&bytes)
            } else {
                Payload::from_bytes(bytes)
            }
        }))
    }

    /// Reuse a frame table without converting owning payloads to `Bytes`.
    pub fn multipart_payloads(&self, parts: impl IntoIterator<Item = Payload>) -> Message {
        let mut values = self.take();
        values.extend(parts);
        Message::from_parts(values)
    }

    pub(crate) fn take(&self) -> Parts {
        // The guard must be released before allocation or payload destruction.
        let free = self.0.free.lock().expect("message pool poisoned").pop();
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
        if self.values.capacity() > pool.0.parts {
            return;
        }
        // A payload owner may run arbitrary drop logic, including constructing
        // another message. Never hold the cache lock while releasing payloads.
        self.values.clear();
        let mut free = pool.0.free.lock().expect("message pool poisoned");
        if free.len() < pool.0.tables {
            free.push(std::mem::take(&mut self.values));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageInner;

    fn pointer(message: &Message) -> *const Payload {
        let (MessageInner::Multi(parts) | MessageInner::RoutedMulti { parts, .. }) = &message.inner
        else {
            panic!("expected multipart table");
        };
        parts.as_ptr()
    }

    #[test]
    fn byte_length_cache_invalidates_on_every_mutable_table_borrow() {
        let mut parts = Parts::from(vec![Payload::from_static(b"abc"), Payload::new()]);
        assert_eq!(parts.byte_len(), 3);
        assert_eq!(parts.bytes.load(Ordering::Relaxed), 3);
        parts[1] = Payload::from_static(b"12345");
        assert_eq!(parts.byte_len(), 8);
        parts.remove(0);
        assert_eq!(parts.byte_len(), 5);
        parts.clear();
        assert_eq!(parts.byte_len(), 0);
        parts.extend([Payload::from_static(b"abc"), Payload::from_static(b"defg")]);
        let message = Message::from_parts(parts).with_routing_id(7);
        assert_eq!(message.byte_len(), 7);
        let mut other = message.clone();
        other.pop_front_payload();
        assert_eq!(other.byte_len(), 4);
        assert_eq!(message.byte_len(), 7);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let message = &message;
                scope.spawn(move || assert_eq!(message.byte_len(), 7));
            }
        });
    }

    #[test]
    fn assembly_and_routing_update_a_known_length_without_rescanning() {
        let pool = MessagePool::new(1, 4);
        let mut parts = pool.take();
        assert_eq!(parts.bytes.load(Ordering::Relaxed), 0);
        parts.extend([Payload::from_static(b"abc"), Payload::new()]);
        assert_eq!(parts.bytes.load(Ordering::Relaxed), 3);
        parts.insert(0, Payload::from_static(b"route"));
        assert_eq!(parts.bytes.load(Ordering::Relaxed), 8);
        assert_eq!(parts.remove(0).as_slice(), b"route");
        assert_eq!(parts.bytes.load(Ordering::Relaxed), 3);
        parts[0] = Payload::from_static(b"12345");
        assert_eq!(parts.bytes.load(Ordering::Relaxed), usize::MAX);
        parts.push(Payload::from_static(b"x"));
        assert_eq!(parts.bytes.load(Ordering::Relaxed), usize::MAX);
        parts.insert(1, Payload::from_static(b"yz"));
        assert_eq!(parts.bytes.load(Ordering::Relaxed), usize::MAX);
        parts.remove(1);
        assert_eq!(parts.bytes.load(Ordering::Relaxed), usize::MAX);
        assert_eq!(parts.byte_len(), 6);
    }

    #[test]
    fn panicking_owner_does_not_partially_remove_a_counted_frame() {
        use std::sync::atomic::AtomicBool;

        struct Owner(AtomicBool);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                assert!(!self.0.load(Ordering::Relaxed), "owner rejected read");
                b"body"
            }
        }
        impl super::super::PayloadOwner for Owner {}

        let owner = Arc::new(Owner(AtomicBool::new(false)));
        let mut parts = MessagePool::new(1, 4).take();
        parts.extend([
            Payload::from_static(b"route"),
            Payload::from_shared_owner(owner.clone()),
        ]);
        assert_eq!(parts.byte_len(), 9);
        owner.0.store(true, Ordering::Relaxed);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parts.remove(1)));
        assert!(result.is_err());
        owner.0.store(false, Ordering::Relaxed);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.byte_len(), 9);
        assert_eq!(parts.remove(1).as_slice(), b"body");
        assert_eq!(parts.byte_len(), 5);
    }

    #[test]
    fn routing_round_trip_recycles_the_same_table() {
        let pool = MessagePool::new(1, 4);
        let payload = Bytes::from(vec![7; 128]);
        let mut message = pool.multipart([
            Bytes::from_static(b"route"),
            Bytes::new(),
            Bytes::from_static(b"metadata"),
            payload.clone(),
        ]);
        let storage = pointer(&message);
        for _ in 0..10 {
            message = message.with_routing_id(42);
            let route = message.pop_front_payload().unwrap();
            message = Message::with_prefix(route.as_bytes(), message);
            assert_eq!(pointer(&message), storage);
            assert_eq!(message.part_slice(3).unwrap().as_ptr(), payload.as_ptr());
        }
        drop(message);
        let reused = pool.multipart(["a", "b", "c", "d"]);
        assert_eq!(pointer(&reused), storage);
    }

    #[test]
    fn exhausted_and_oversized_tables_do_not_grow_the_cache() {
        let pool = MessagePool::new(1, 2);
        let first = pool.multipart(["a", "b"]);
        let clone = first.clone();
        assert_ne!(pointer(&first), pointer(&clone));
        drop((first, clone));
        assert_eq!(pool.0.free.lock().unwrap().len(), 1);
        let large = pool.multipart(["a", "b", "c"]);
        drop(large);
        assert!(pool.0.free.lock().unwrap().is_empty());
        drop(pool.multipart(["a", "b"]));
        let free = pool.0.free.lock().unwrap();
        assert_eq!(free.len(), 1);
        assert!(free[0].capacity() <= 2);
    }

    #[test]
    fn frames_survive_pool_drop_and_cross_thread_cloning() {
        let pool = MessagePool::new(2, 4);
        let payload = Bytes::from(vec![9; 128]);
        let message = pool.multipart([payload.clone(), Bytes::new(), payload.clone()]);
        let other = message.clone();
        drop(pool);
        std::thread::spawn(move || {
            let clone = other.clone();
            drop(other);
            assert_eq!(clone.part_slice(0), Some([9; 128].as_slice()));
            assert_eq!(clone.part_slice(1), Some([].as_slice()));
        })
        .join()
        .unwrap();
        assert_eq!(message.part_slice(2).unwrap().as_ptr(), payload.as_ptr());
    }

    #[test]
    fn normalized_empty_and_single_messages_return_their_tables() {
        let pool = MessagePool::new(1, 4);
        assert!(pool.multipart::<_, Bytes>([]).is_empty());
        let message = pool.multipart([Bytes::from(vec![3; 128])]);
        assert_eq!(message.len(), 1);
        assert_eq!(pool.0.free.lock().unwrap().len(), 1);
    }
}
