use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use omq_proto::dart::MAX_BODY;
use omq_proto::message::{Message, Payload, PayloadOwner};

#[derive(Debug)]
struct LargeBody {
    bytes: Vec<u8>,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
}

impl AsRef<[u8]> for LargeBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PayloadOwner for LargeBody {
    fn retained_size(&self) -> Option<usize> {
        Some(self.bytes.capacity() + std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
    }
}

impl Drop for LargeBody {
    fn drop(&mut self) {
        // Publish after releasing the reserved body, including final clones.
        drop(std::mem::take(&mut self.bytes));
        self.credits.publish(1);
        self.signal.mark();
    }
}

pub(super) fn large_payload(
    bytes: Vec<u8>,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
) -> Payload {
    Payload::from_shared_owner(Arc::new(LargeBody {
        bytes,
        credits,
        signal,
    }))
}

/// Fixed-size reusable body buffers. Clones share the same bounded pool.
///
/// Initialization allocates every buffer. Exhaustion never grows the pool.
/// Messages may be cloned, sent across threads, and outlive this handle.
#[derive(Clone, Debug)]
pub struct DartPool(Arc<Pool>);

#[derive(Debug)]
struct Pool {
    free: FreeList,
    releases: Box<[Mutex<()>]>,
    capacity: usize,
}

// Scalar returns keep the ordinary concurrent-queue path. Batch returns
// publish one group, including short groups. A group owns at least one slot,
// so at most `capacity` groups can exist. Both queues allocate only at setup.
#[derive(Debug)]
struct FreeList {
    singles: concurrent_queue::ConcurrentQueue<Arc<Storage>>,
    batches: concurrent_queue::ConcurrentQueue<FreeBatch>,
    available: AtomicUsize,
    returns: std::sync::OnceLock<(
        Arc<omq_proto::dart::CreditCounter>,
        Arc<crate::engine::signal::DataSignal>,
    )>,
}

#[derive(Debug)]
struct FreeBatch {
    owners: [Option<Arc<Storage>>; TRANSFER],
    count: usize,
}

impl FreeList {
    fn new(capacity: usize) -> Self {
        Self {
            singles: concurrent_queue::ConcurrentQueue::bounded(capacity.max(1)),
            batches: concurrent_queue::ConcurrentQueue::bounded(capacity.max(1)),
            available: AtomicUsize::new(0),
            returns: std::sync::OnceLock::new(),
        }
    }

    fn push(&self, owner: Arc<Storage>) {
        // Count before publication: a consumer may pop immediately afterward.
        self.available.fetch_add(1, Ordering::Relaxed);
        self.singles
            .push(owner)
            .expect("one return per DART buffer");
    }

    fn push_batch(&self, owners: &mut [Option<Arc<Storage>>]) {
        if owners.is_empty() {
            return;
        }
        let batch = FreeBatch {
            owners: std::array::from_fn(|index| {
                if index < owners.len() {
                    owners[index].take()
                } else {
                    None
                }
            }),
            count: owners.len(),
        };
        self.available.fetch_add(batch.count, Ordering::Relaxed);
        self.batches
            .push(batch)
            .expect("one return per DART buffer");
    }

    fn publish_return(&self, count: usize) {
        if let Some((credits, signal)) = self.returns.get() {
            credits.publish(count);
            signal.mark();
        }
    }

    fn pop(&self) -> Option<Arc<Storage>> {
        if let Ok(owner) = self.singles.pop() {
            self.available.fetch_sub(1, Ordering::Relaxed);
            return Some(owner);
        }
        let mut batch = self.batches.pop().ok()?;
        self.available.fetch_sub(batch.count, Ordering::Relaxed);
        let owner = batch.owners[0].take();
        self.push_batch(&mut batch.owners[1..batch.count]);
        owner
    }

    fn pop_many(&self, limit: usize, output: &mut Vec<DartBuffer>) -> usize {
        let limit = limit.min(TRANSFER).min(output.capacity() - output.len());
        if limit == 0 {
            return 0;
        }
        // Sparse traffic takes the same scalar path as ordinary acquisition.
        if limit == 1 {
            if let Some(owner) = self.pop() {
                output.push(DartBuffer {
                    storage: Some(owner),
                    length: 0,
                });
                return 1;
            }
            return 0;
        }
        let mut taken = 0;
        while taken < limit {
            if let Ok(mut batch) = self.batches.pop() {
                self.available.fetch_sub(batch.count, Ordering::Relaxed);
                let count = (limit - taken).min(batch.count);
                for owner in &mut batch.owners[..count] {
                    output.push(DartBuffer {
                        storage: Some(owner.take().expect("returned owner")),
                        length: 0,
                    });
                }
                self.push_batch(&mut batch.owners[count..batch.count]);
                taken += count;
            } else {
                let mut count = 0;
                while taken + count < limit {
                    let Ok(owner) = self.singles.pop() else {
                        break;
                    };
                    output.push(DartBuffer {
                        storage: Some(owner),
                        length: 0,
                    });
                    count += 1;
                }
                if count != 0 {
                    self.available.fetch_sub(count, Ordering::Relaxed);
                }
                taken += count;
                break;
            }
        }
        taken
    }

    fn len(&self) -> usize {
        // Includes returns in flight. This snapshot is not a reservation.
        self.available.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct Storage {
    bytes: [u8; MAX_BODY],
    pool: Weak<Pool>,
    slot: usize,
}

impl AsRef<[u8]> for Storage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PayloadOwner for Storage {
    fn retained_size(&self) -> Option<usize> {
        Some(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
    }

    fn release(self: Arc<Self>) {
        let mut storage = Some(self);
        let _ = RECYCLE.try_with(|recycle| {
            let Ok(mut recycle) = recycle.try_borrow_mut() else {
                return;
            };
            if recycle.pool.is_none() {
                return;
            }
            let owner = storage.as_ref().expect("unreleased storage");
            if owner.pool.as_ptr() != Arc::as_ptr(recycle.pool.as_ref().expect("active pool")) {
                let Some(pool) = owner.pool.upgrade() else {
                    return;
                };
                recycle.flush();
                recycle.pool = Some(pool);
            }
            let owner = storage.take().expect("unreleased storage");
            if Arc::strong_count(&owner) == 1 {
                recycle.push(owner);
            } else {
                let pool = recycle.pool.as_ref().expect("active pool").clone();
                let _release = pool.releases[owner.slot]
                    .lock()
                    .expect("DART buffer release poisoned");
                if Arc::strong_count(&owner) == 1 {
                    recycle.push(owner);
                } else {
                    drop(owner);
                }
            }
        });
        let Some(storage) = storage else {
            return;
        };
        storage.release_one();
    }
}

impl Storage {
    fn release_one(self: Arc<Self>) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        // Storage is private and has no Weak references. A unique strong
        // owner cannot race a clone, so it can return without locking.
        if Arc::strong_count(&self) == 1 {
            pool.free.push(self);
            pool.free.publish_return(1);
            return;
        }
        // Serialize uniqueness checks with other releases. Each non-final
        // reference must be dropped while still holding this lock; otherwise
        // two simultaneous releases can both miss the final owner.
        let _release = pool.releases[self.slot]
            .lock()
            .expect("DART buffer release poisoned");
        if Arc::strong_count(&self) == 1 {
            pool.free.push(self);
            pool.free.publish_return(1);
        } else {
            drop(self);
        }
    }
}

const TRANSFER: usize = 64;

// Returned owners are staged only during a synchronous drain. No free buffer
// remains hidden in TLS after the call, including during panic unwinding.
struct Recycling {
    pool: Option<Arc<Pool>>,
    owners: [Option<Arc<Storage>>; TRANSFER],
    count: usize,
}

impl Recycling {
    fn flush(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };
        pool.free.push_batch(&mut self.owners[..self.count]);
        pool.free.publish_return(self.count);
        self.count = 0;
    }

    fn push(&mut self, owner: Arc<Storage>) {
        self.owners[self.count] = Some(owner);
        self.count += 1;
        if self.count == TRANSFER {
            self.flush();
        }
    }
}

thread_local! {
    static RECYCLE: RefCell<Recycling> = const { RefCell::new(Recycling {
        pool: None, owners: [const { None }; TRANSFER], count: 0,
    }) };
}

struct RecycleGuard(bool);

impl Drop for RecycleGuard {
    fn drop(&mut self) {
        let _ = RECYCLE.try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            recycle.flush();
            if self.0 {
                recycle.pool = None;
            }
        });
    }
}

impl DartPool {
    pub(crate) fn receiver(
        capacity: usize,
        credits: Arc<omq_proto::dart::CreditCounter>,
        signal: Arc<crate::engine::signal::DataSignal>,
    ) -> Self {
        let pool = Self::new(capacity);
        pool.0
            .free
            .returns
            .set((credits, signal))
            .expect("new receiver pool");
        pool
    }

    /// Preallocate `capacity` buffers. Zero creates an always-exhausted pool.
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new_cyclic(|pool| {
            let free = FreeList::new(capacity);
            for slot in 0..capacity {
                free.push(Arc::new(Storage {
                    bytes: [0; MAX_BODY],
                    pool: pool.clone(),
                    slot,
                }));
            }
            Pool {
                free,
                releases: (0..capacity)
                    .map(|_| {
                        let release = Mutex::new(());
                        // Some platforms allocate mutex storage on first lock.
                        // Initialize it here, before any pooled owner is released.
                        drop(release.lock().expect("new DART buffer release lock"));
                        release
                    })
                    .collect(),
                capacity,
            }
        }))
    }

    /// Acquire exclusive writable storage, or `None` at capacity.
    pub fn try_take(&self) -> Option<DartBuffer> {
        let storage = self.0.free.pop()?;
        Some(DartBuffer {
            storage: Some(storage),
            length: 0,
        })
    }

    /// Acquire up to 64 buffers, consuming returned groups together. Appends
    /// without allocating; the caller's remaining vector capacity bounds the count.
    #[inline]
    pub fn try_take_many_into(&self, limit: usize, output: &mut Vec<DartBuffer>) -> usize {
        // Keep exhaustion probes in the caller. The nonempty drain has a
        // larger stack frame and touches both concurrent free queues.
        if self.0.free.available.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        self.0.free.pop_many(limit, output)
    }

    /// Drop up to 64 messages and publish their final pooled owners together.
    /// Clones and byte views still keep their buffers checked out. Foreign
    /// pools return normally; all returns are visible before this call ends.
    pub fn recycle_many(&self, messages: &mut Vec<Message>, limit: usize) -> usize {
        let count = messages.len().min(limit).min(TRANSFER);
        self.with_recycling_batch(|| {
            drop(messages.drain(..count));
        });
        count
    }

    pub(crate) fn with_recycling_batch<R>(&self, operation: impl FnOnce() -> R) -> R {
        let installed = RECYCLE
            .try_with(|recycle| {
                let mut recycle = recycle.borrow_mut();
                if recycle.pool.is_some() {
                    false
                } else {
                    recycle.pool = Some(self.0.clone());
                    true
                }
            })
            .unwrap_or(false);
        let _guard = RecycleGuard(installed);
        operation()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity
    }

    /// Snapshot of currently returned buffers.
    pub fn available(&self) -> usize {
        self.0.free.len()
    }
}

impl Default for DartPool {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// Exclusive writable buffer. Sending its message freezes the storage.
///
/// The body remains allocated from its original pool until the final message
/// or byte view is dropped. There is no implicit growth on pool exhaustion.
#[derive(Debug)]
pub struct DartBuffer {
    storage: Option<Arc<Storage>>,
    // The frozen payload captures this prefix length. Storage always exposes
    // its fixed capacity, so freezing requires no second Arc uniqueness check.
    length: usize,
}

/// Requested body length exceeds the fixed buffer capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferLengthError {
    pub requested: usize,
}

impl fmt::Display for BufferLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DART body length {} exceeds {MAX_BODY}", self.requested)
    }
}

impl std::error::Error for BufferLengthError {}

impl DartBuffer {
    pub const fn capacity(&self) -> usize {
        MAX_BODY
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entire writable capacity. Only the declared length is sent.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut Arc::get_mut(self.storage.as_mut().expect("live DART buffer"))
            .expect("writable DART storage must be unique")
            .bytes
    }

    /// Set the body length without growing storage.
    pub fn set_len(&mut self, length: usize) -> Result<(), BufferLengthError> {
        if length > MAX_BODY {
            return Err(BufferLengthError { requested: length });
        }
        self.length = length;
        Ok(())
    }

    /// Transfer storage into an ordinary single-body message without allocation.
    pub fn into_message(mut self) -> Message {
        Message::from(Payload::from_shared_owner_prefix(
            self.storage.take().expect("live DART buffer"),
            self.length,
        ))
    }
}

impl AsRef<[u8]> for DartBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.storage.as_ref().expect("live DART buffer").bytes[..self.length]
    }
}

impl Drop for DartBuffer {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.take() {
            storage.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(pool: &DartPool, length: usize, value: u8) -> Message {
        let mut buffer = pool.try_take().unwrap();
        buffer.writable()[..length].fill(value);
        buffer.set_len(length).unwrap();
        buffer.into_message()
    }

    #[test]
    fn capacity_is_fixed_and_returns_after_drop() {
        let pool = DartPool::new(2);
        let first = pool.try_take().unwrap();
        let second = pool.try_take().unwrap();
        assert!(pool.try_take().is_none());
        assert_eq!(pool.available(), 0);
        drop(first);
        assert_eq!(pool.available(), 1);
        drop(second);
        assert_eq!(pool.available(), 2);
        assert!(DartPool::new(0).try_take().is_none());
    }

    #[test]
    fn reused_storage_has_empty_length_and_valid_capacity() {
        let pool = DartPool::new(1);
        drop(body(&pool, MAX_BODY, 7));
        let mut buffer = pool.try_take().unwrap();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.writable().len(), MAX_BODY);
        assert_eq!(
            buffer.set_len(MAX_BODY + 1),
            Err(BufferLengthError {
                requested: MAX_BODY + 1
            })
        );
        assert_eq!(buffer.len(), 0);
        buffer.set_len(MAX_BODY).unwrap();
        assert_eq!(buffer.as_ref(), &[7; MAX_BODY]);
    }

    #[test]
    fn clones_and_bytes_views_prevent_early_reuse() {
        for length in [0, 1, 128, MAX_BODY] {
            let pool = DartPool::new(1);
            let message = body(&pool, length, 7);
            let clone = message.clone();
            let bytes = message.part_bytes(0).unwrap();
            drop(message);
            drop(clone);
            assert_eq!(pool.available(), 0);
            assert_eq!(bytes.as_ref(), vec![7; length]);
            drop(bytes);
            assert_eq!(pool.available(), 1);
        }
    }

    #[test]
    fn concurrent_final_releases_return_exactly_one_buffer() {
        let pool = DartPool::new(1);
        for _ in 0..64 {
            let first = body(&pool, MAX_BODY, 7);
            let second = first.clone();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let other = barrier.clone();
            let thread = std::thread::spawn(move || {
                other.wait();
                drop(second);
            });
            barrier.wait();
            drop(first);
            thread.join().unwrap();
            assert_eq!(pool.available(), 1);
            assert_eq!(pool.try_take().unwrap().len(), 0);
        }
    }

    #[test]
    fn buffers_and_messages_outlive_pool_without_a_cycle() {
        let pool = DartPool::new(2);
        let core = Arc::downgrade(&pool.0);
        let buffer = pool.try_take().unwrap();
        let message = body(&pool, MAX_BODY, 7);
        drop(pool);
        assert!(core.upgrade().is_none());
        assert_eq!(message.part_slice(0), Some([7; MAX_BODY].as_slice()));
        drop(buffer);
        drop(message);
    }

    #[test]
    fn concurrent_acquisition_and_return_preserve_every_slot() {
        let pool = DartPool::new(4);
        std::thread::scope(|scope| {
            for value in 1..=4 {
                let pool = &pool;
                scope.spawn(move || {
                    for _ in 0..256 {
                        let mut buffer = loop {
                            if let Some(buffer) = pool.try_take() {
                                break buffer;
                            }
                            std::thread::yield_now();
                        };
                        buffer.writable().fill(value);
                        buffer.set_len(MAX_BODY).unwrap();
                        let original = buffer.into_message();
                        let clone = original.clone();
                        drop(original);
                        assert!(
                            clone
                                .part_slice(0)
                                .unwrap()
                                .iter()
                                .all(|byte| *byte == value)
                        );
                        drop(clone);
                    }
                });
            }
        });
        assert_eq!(pool.available(), pool.capacity());
    }
    #[test]
    fn bulk_acquisition_and_return_respect_capacity_and_live_views() {
        let pool = DartPool::new(70);
        let mut buffers = Vec::with_capacity(100);
        assert_eq!(pool.try_take_many_into(100, &mut buffers), 64);
        assert_eq!(pool.available(), 6);
        let mut messages: Vec<_> = buffers.drain(..).map(DartBuffer::into_message).collect();
        let view = messages[0].part_bytes(0).unwrap();
        assert_eq!(pool.recycle_many(&mut messages, 0), 0);
        assert_eq!(pool.recycle_many(&mut messages, 100), 64);
        assert_eq!(pool.available(), 69);
        drop(view);
        assert_eq!(pool.available(), 70);
        let mut small = Vec::with_capacity(2);
        assert_eq!(pool.try_take_many_into(64, &mut small), 2);
        assert_eq!(pool.try_take_many_into(64, &mut small), 0);
        assert_eq!(small.capacity(), 2);
        drop(small);
        assert_eq!(pool.available(), 70);
    }

    #[test]
    fn nested_foreign_and_unwinding_returns_leave_no_hidden_buffers() {
        let first = DartPool::new(4);
        let second = DartPool::new(4);
        let outcome = std::panic::catch_unwind(|| {
            first.with_recycling_batch(|| {
                drop(body(&first, 16, 1));
                second.with_recycling_batch(|| {
                    drop(body(&first, 16, 1));
                    drop(body(&second, 16, 2));
                });
                assert_eq!(first.available(), 4);
                assert_eq!(second.available(), 4);
                drop(body(&first, 16, 1));
                panic!("unwind recycling scope");
            });
        });
        assert!(outcome.is_err());
        assert_eq!(first.available(), 4);
        assert_eq!(second.available(), 4);
        drop(body(&first, 16, 1));
        assert_eq!(first.available(), 4);
    }

    #[test]
    fn concurrent_bulk_and_scalar_returns_preserve_unique_slots() {
        let pool = DartPool::new(8);
        std::thread::scope(|scope| {
            for value in 1..=4 {
                let pool = &pool;
                scope.spawn(move || {
                    let mut buffers = Vec::with_capacity(2);
                    let mut messages = Vec::with_capacity(2);
                    for _ in 0..256 {
                        while pool.try_take_many_into(2, &mut buffers) == 0 {
                            std::thread::yield_now();
                        }
                        for mut buffer in buffers.drain(..) {
                            buffer.writable().fill(value);
                            buffer.set_len(MAX_BODY).unwrap();
                            let message = buffer.into_message();
                            let clone = message.clone();
                            messages.push(message);
                            assert!(
                                clone
                                    .part_slice(0)
                                    .unwrap()
                                    .iter()
                                    .all(|byte| *byte == value)
                            );
                            drop(clone);
                        }
                        pool.recycle_many(&mut messages, 2);
                    }
                });
            }
        });
        assert_eq!(pool.available(), 8);
        let mut buffers = Vec::with_capacity(8);
        assert_eq!(pool.try_take_many_into(8, &mut buffers), 8);
        let mut slots: Vec<_> = buffers
            .iter()
            .map(|buffer| buffer.storage.as_ref().unwrap().slot)
            .collect();
        slots.sort_unstable();
        assert_eq!(slots, (0..8).collect::<Vec<_>>());
    }
    #[test]
    fn full_and_partial_groups_never_hide_capacity() {
        for capacity in [0, 1, 2, 63, 64, 65, 127, 128, 129] {
            let pool = DartPool::new(capacity);
            let mut buffers = Vec::with_capacity(capacity);
            for limit in [1, 2, 17, 63, 64] {
                while pool.try_take_many_into(limit, &mut buffers) != 0 {}
                assert_eq!(buffers.len(), capacity);
                assert_eq!(pool.available(), 0);
                // Partial final transfers are all published by scope exit.
                pool.with_recycling_batch(|| buffers.clear());
                assert_eq!(pool.available(), capacity);
                while let Some(buffer) = pool.try_take() {
                    buffers.push(buffer);
                }
                assert_eq!(buffers.len(), capacity);
                drop(buffers.drain(..));
                assert_eq!(pool.available(), capacity);
            }
        }
    }
    #[test]
    fn recycling_during_late_tls_destruction_falls_back_to_global_return() {
        struct LateDrop(RefCell<Option<(DartPool, Message)>>);
        impl Drop for LateDrop {
            fn drop(&mut self) {
                if let Some((pool, message)) = self.0.get_mut().take() {
                    let mut messages = vec![message];
                    pool.recycle_many(&mut messages, 1);
                }
            }
        }
        thread_local! {
            static LATE: LateDrop = const { LateDrop(RefCell::new(None)) };
        }
        let pool = DartPool::new(1);
        let other = pool.clone();
        std::thread::spawn(move || {
            // The recycler is initialized later, so it is destroyed first.
            LATE.with(|late| *late.0.borrow_mut() = Some((other.clone(), body(&other, 16, 7))));
            other.with_recycling_batch(|| {});
        })
        .join()
        .unwrap();
        assert_eq!(pool.available(), 1);
    }
}
