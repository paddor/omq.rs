//! Explicit, transport-independent message preparation and reusable storage.
#![deny(missing_docs)]

use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use omq_proto::message::{Message, Payload, PayloadOwner};

pub(crate) trait BufferReturn: fmt::Debug + Send + Sync + std::panic::RefUnwindSafe {
    fn publish(&self, count: usize);
    fn wake(&self);
    #[cfg(feature = "dart")]
    fn own(&self, body: Vec<u8>) -> Payload;
}

fn same_returns(
    first: Option<&Arc<dyn BufferReturn>>,
    second: Option<&Arc<dyn BufferReturn>>,
) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(first), Some(second)) => Arc::ptr_eq(first, second),
        _ => false,
    }
}

/// Fixed-size reusable body buffers. Clones share the same bounded pool.
///
/// Initialization allocates every buffer. Exhaustion never grows the pool.
/// Messages may be cloned, sent across threads, and outlive this handle.
/// No socket or transport feature is required. Use [`Self::try_message`] for
/// inline, pooled, or independently allocated bodies.
///
/// ```
/// use omq_tokio::BufferPool;
///
/// let pool = BufferPool::new(2048, 128);
/// let message = pool.try_message(16, |body| body.fill(7))?.unwrap();
/// assert_eq!(message.part_slice(0), Some([7; 16].as_slice()));
/// # Ok::<(), omq_tokio::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct BufferPool(Arc<Pool>, Option<Arc<dyn BufferReturn>>);

#[derive(Debug)]
struct Pool {
    free: FreeList,
    queued: Box<[AtomicBool]>,
    pending: concurrent_queue::ConcurrentQueue<Arc<Storage>>,
    pending_count: AtomicUsize,
    capacity: usize,
    buffer_size: usize,
}

// Scalar returns keep the ordinary concurrent-queue path. Batch returns
// publish one group, including short groups. A group owns at least one slot,
// so at most `capacity` groups can exist. Both queues allocate only at setup.
#[derive(Debug)]
struct FreeList {
    singles: concurrent_queue::ConcurrentQueue<Arc<Storage>>,
    batches: concurrent_queue::ConcurrentQueue<FreeBatch>,
    available: AtomicUsize,
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
        }
    }

    fn push(&self, owner: Arc<Storage>) {
        // Count before publication: a consumer may pop immediately afterward.
        self.available.fetch_add(1, Ordering::Relaxed);
        self.singles.push(owner).expect("one return per buffer");
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
        self.batches.push(batch).expect("one return per buffer");
    }

    fn pop(&self) -> Option<Arc<Storage>> {
        if let Ok(owner) = self.singles.pop() {
            self.available.fetch_sub(1, Ordering::Relaxed);
            return Some(owner);
        }
        let mut batch = self.batches.pop().ok()?;
        self.available.fetch_sub(1, Ordering::Relaxed);
        let owner = batch.owners[0].take();
        // Expand a returned group once. Scalar acquisitions must not rebuild
        // and copy the shrinking remainder for every subsequent buffer.
        for owner in &mut batch.owners[1..batch.count] {
            self.singles
                .push(owner.take().expect("returned owner"))
                .expect("one return per buffer");
        }
        owner
    }

    fn pop_many(&self, limit: usize, output: &mut Vec<MessageBuffer>) -> usize {
        let limit = limit.min(TRANSFER).min(output.capacity() - output.len());
        if limit == 0 {
            return 0;
        }
        // Sparse traffic takes the same scalar path as ordinary acquisition.
        if limit == 1 {
            if let Some(owner) = self.pop() {
                output.push(MessageBuffer {
                    storage: Some(owner),
                    length: 0,
                    returns: None,
                    prepared: false,
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
                    output.push(MessageBuffer {
                        storage: Some(owner.take().expect("returned owner")),
                        length: 0,
                        returns: None,
                        prepared: false,
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
                    output.push(MessageBuffer {
                        storage: Some(owner),
                        length: 0,
                        returns: None,
                        prepared: false,
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
    bytes: Box<[u8]>,
    pool: Weak<Pool>,
    slot: usize,
    returns: Option<Arc<dyn BufferReturn>>,
}

impl AsRef<[u8]> for Storage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PayloadOwner for Storage {
    fn retained_size(&self) -> Option<usize> {
        Some(self.bytes.len() + std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
    }

    fn release(self: Arc<Self>) {
        self.release_owner(true);
    }
}

impl Storage {
    fn release_owner(self: Arc<Self>, credited: bool) {
        let mut storage = Some(self);
        let _ = RECYCLE.try_with(|recycle| {
            let Ok(mut recycle) = recycle.try_borrow_mut() else {
                return;
            };
            if !recycle.active {
                return;
            }
            let owner = storage.as_ref().expect("unreleased storage");
            if recycle
                .pool
                .as_ref()
                .is_none_or(|pool| owner.pool.as_ptr() != Arc::as_ptr(pool))
            {
                let Some(pool) = owner.pool.upgrade() else {
                    return;
                };
                recycle.flush();
                recycle.pool = Some(pool);
            }
            let owner = storage.take().expect("unreleased storage");
            if Arc::strong_count(&owner) == 1 {
                recycle.push(owner, credited);
            } else {
                let pool = recycle.pool.as_ref().expect("active pool").clone();
                owner.defer(&pool);
            }
        });
        let Some(storage) = storage else {
            return;
        };
        storage.release_one(credited);
    }

    fn return_one(self: Arc<Self>, pool: &Pool, credited: bool) {
        let returns = credited.then(|| self.returns.clone()).flatten();
        pool.free.push(self);
        if let Some(returns) = returns {
            returns.publish(1);
        }
    }

    fn release_one(self: Arc<Self>, credited: bool) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        // Storage is private and has no Weak references. A unique strong
        // owner cannot race a clone, so it can return without locking.
        if Arc::strong_count(&self) == 1 {
            self.return_one(&pool, credited);
            return;
        }
        self.defer(&pool);
    }

    fn defer(self: Arc<Self>, pool: &Pool) {
        // Keep exactly one queued owner until every concurrent release has
        // dropped its Arc. Reclamation never spins waiting for another thread.
        let returns = self.returns.clone();
        if pool.queued[self.slot]
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            pool.pending_count.fetch_add(1, Ordering::Relaxed);
            pool.pending.push(self).expect("one pending owner per slot");
        } else {
            drop(self);
        }
        if let Some(returns) = returns {
            returns.wake();
        }
    }
}

impl Pool {
    fn reclaim(&self) {
        let limit = self.pending_count.load(Ordering::Relaxed).min(TRANSFER);
        let mut budget = omq_proto::flow::DrainBudget::new(limit, TRANSFER_BYTES);
        while !budget.exhausted() {
            let Ok(owner) = self.pending.pop() else {
                break;
            };
            let _ = budget.account(owner.bytes.len());
            if Arc::strong_count(&owner) == 1 {
                self.queued[owner.slot].store(false, Ordering::Relaxed);
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                owner.return_one(self, true);
            } else {
                self.pending
                    .push(owner)
                    .expect("one pending owner per slot");
            }
        }
    }
}

const TRANSFER: usize = 64;
const TRANSFER_BYTES: usize = 64 * 1024;

// Returned owners are staged only during a synchronous drain. No free buffer
// remains hidden in TLS after the call, including during panic unwinding.
struct Recycling {
    active: bool,
    pool: Option<Arc<Pool>>,
    returns: Option<Arc<dyn BufferReturn>>,
    owners: [Option<Arc<Storage>>; TRANSFER],
    count: usize,
}

impl Recycling {
    fn flush(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };
        pool.free.push_batch(&mut self.owners[..self.count]);
        if let Some(returns) = self.returns.take() {
            returns.publish(self.count);
        }
        self.count = 0;
    }

    fn push(&mut self, owner: Arc<Storage>, credited: bool) {
        let returns = credited.then_some(owner.returns.as_ref()).flatten();
        if self.count != 0 && !same_returns(self.returns.as_ref(), returns) {
            self.flush();
        }
        if self.count == 0 {
            self.returns = returns.cloned();
        }
        self.owners[self.count] = Some(owner);
        self.count += 1;
        if self.count == TRANSFER {
            self.flush();
        }
    }
}

thread_local! {
    static RECYCLE: RefCell<Recycling> = const { RefCell::new(Recycling {
        active: false, pool: None, returns: None, owners: [const { None }; TRANSFER], count: 0,
    }) };
}

struct RecycleGuard(bool);

impl Drop for RecycleGuard {
    fn drop(&mut self) {
        let _ = RECYCLE.try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            recycle.flush();
            if self.0 {
                recycle.active = false;
                recycle.pool = None;
            }
        });
    }
}

impl BufferPool {
    #[cfg(feature = "dart")]
    pub(super) fn copy_received(&self, bytes: &[u8]) -> Option<Payload> {
        let returns = self.1.as_ref().expect("receiver credit");
        let mut body = Vec::new();
        body.try_reserve_exact(bytes.len()).ok()?;
        body.extend_from_slice(bytes);
        Some(returns.own(body))
    }

    #[cfg(feature = "dart")]
    pub(crate) fn with_returns(&self, returns: Arc<dyn BufferReturn>) -> Self {
        Self(self.0.clone(), Some(returns))
    }

    #[cfg(feature = "dart")]
    pub(crate) fn owned_payload(&self, body: Vec<u8>) -> Payload {
        self.1.as_ref().expect("receive return hook").own(body)
    }

    /// Preallocate `capacity` buffers, each holding `buffer_size` bytes.
    /// A zero capacity disables pooled checkout; inline and owned messages
    /// can still be prepared. Buffers initially contain zeroes.
    ///
    /// # Panics
    /// Panics if `buffer_size` is zero or an allocation size overflows.
    pub fn new(buffer_size: usize, capacity: usize) -> Self {
        assert!(buffer_size > 0, "buffer size must be nonzero");
        Self(
            Arc::new_cyclic(|pool| {
                let free = FreeList::new(capacity);
                for slot in 0..capacity {
                    free.push(Arc::new(Storage {
                        bytes: vec![0; buffer_size].into_boxed_slice(),
                        pool: pool.clone(),
                        slot,
                        returns: None,
                    }));
                }
                Pool {
                    free,
                    queued: (0..capacity).map(|_| AtomicBool::new(false)).collect(),
                    pending: concurrent_queue::ConcurrentQueue::bounded(capacity.max(1)),
                    pending_count: AtomicUsize::new(0),
                    capacity,
                    buffer_size,
                }
            }),
            None,
        )
    }

    /// Acquire exclusive writable storage, or `None` when no buffer is available.
    /// The declared length starts at zero. Reused bytes are not cleared.
    pub fn try_take(&self) -> Option<MessageBuffer> {
        self.0.reclaim();
        let storage = self.0.free.pop()?;
        Some(MessageBuffer {
            storage: Some(storage),
            length: 0,
            returns: self.1.clone(),
            prepared: false,
        })
    }

    /// Prepare an ordinary message with exactly `size` body bytes.
    /// Bodies up to 55 bytes stay inline without checking out a buffer.
    /// Larger bodies use this pool when they fit, or an owned allocation
    /// when they exceed [`Self::buffer_size`]. `None` means a fitting pool
    /// is exhausted; exhaustion does not fall back to allocation.
    ///
    /// `fill` runs exactly once on success, and never on exhaustion or error.
    /// Fill the entire slice: reused pooled bytes retain their previous contents.
    ///
    /// # Errors
    /// Returns [`omq_proto::Error::Config`] if owned storage cannot be reserved.
    pub fn try_message(
        &self,
        size: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> omq_proto::Result<Option<Message>> {
        const INLINE: usize = omq_proto::message::MAX_INLINE_MESSAGE;
        if size <= INLINE {
            let mut body = [0; INLINE];
            fill(&mut body[..size]);
            return Ok(Some(Message::from_slice(&body[..size])));
        }
        if size > self.buffer_size() {
            let mut body = Vec::new();
            body.try_reserve_exact(size)
                .map_err(|_| omq_proto::Error::Config("message allocation failed".into()))?;
            body.resize(size, 0);
            fill(&mut body);
            return Ok(Some(Message::single(body)));
        }
        let Some(mut buffer) = self.try_take() else {
            return Ok(None);
        };
        fill(&mut buffer.writable()[..size]);
        buffer.set_len(size).expect("body fits pool buffer");
        Ok(Some(buffer.into_message()))
    }

    /// Append available buffers without growing `output`.
    /// Checkout stops at `limit`, 64 buffers, the vector's remaining capacity,
    /// or 64 KiB of buffer capacity. At least one oversized buffer may be taken.
    /// Returns the number appended. Each starts empty with uncleared storage.
    #[inline]
    pub fn try_take_many_into(&self, limit: usize, output: &mut Vec<MessageBuffer>) -> usize {
        self.0.reclaim();
        // Keep exhaustion probes in the caller. The nonempty drain has a
        // larger stack frame and touches both concurrent free queues.
        if self.0.free.available.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let start = output.len();
        let limit = limit.min((TRANSFER_BYTES / self.buffer_size()).max(1));
        let count = self.0.free.pop_many(limit, output);
        if let Some(returns) = &self.1 {
            for buffer in &mut output[start..] {
                buffer.returns = Some(returns.clone());
            }
        }
        count
    }

    /// Drop up to 64 messages within a 64 KiB body budget and publish their
    /// final pooled owners together, across any pools.
    /// Returns the number removed from the front of `messages`. The final
    /// message may cross the byte budget. Clones and byte views still prevent
    /// reuse; deferred owners need a later bounded reclamation turn.
    pub fn recycle_many(messages: &mut Vec<Message>, limit: usize) -> usize {
        let mut budget = omq_proto::flow::DrainBudget::new(limit.min(TRANSFER), TRANSFER_BYTES);
        let mut count = 0;
        for message in messages.iter().take(limit.min(TRANSFER)) {
            if budget.exhausted() {
                break;
            }
            let _ = budget.account(message.byte_len());
            count += 1;
        }
        recycling(None, || {
            drop(messages.drain(..count));
        });
        count
    }

    #[cfg(any(test, feature = "dart"))]
    pub(crate) fn with_recycling_batch<R>(&self, operation: impl FnOnce() -> R) -> R {
        recycling(Some(self.0.clone()), operation)
    }

    /// Fixed byte capacity of each pooled buffer.
    pub fn buffer_size(&self) -> usize {
        self.0.buffer_size
    }

    /// Total number of pooled buffers, including checked-out buffers.
    pub fn capacity(&self) -> usize {
        self.0.capacity
    }

    /// Snapshot of currently returned buffers, after one bounded reclamation turn.
    /// Concurrent returns may be in flight; this count is not a reservation.
    pub fn available(&self) -> usize {
        self.0.reclaim();
        self.0.free.len()
    }

    #[cfg(feature = "dart")]
    pub(crate) fn reclaim(&self) {
        self.0.reclaim();
    }
}

#[cfg(feature = "dart")]
pub(crate) fn with_recycling_batch<R>(operation: impl FnOnce() -> R) -> R {
    recycling(None, operation)
}

fn recycling<R>(pool: Option<Arc<Pool>>, operation: impl FnOnce() -> R) -> R {
    let installed = RECYCLE
        .try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            if recycle.active {
                false
            } else {
                recycle.active = true;
                recycle.pool = pool;
                true
            }
        })
        .unwrap_or(false);
    let _guard = RecycleGuard(installed);
    operation()
}

/// Exclusive fixed-size writable storage. [`Self::into_message`] freezes it.
///
/// A new checkout starts empty but may retain previously written bytes.
/// Dropping an unused buffer returns it to its pool. After freezing, the body
/// remains checked out until its final message or byte view is dropped.
/// Buffers never grow and can outlive their pool or any socket.
#[derive(Debug)]
pub struct MessageBuffer {
    storage: Option<Arc<Storage>>,
    // The frozen payload captures this prefix length. Storage always exposes
    // its fixed capacity, so freezing requires no second Arc uniqueness check.
    length: usize,
    // Unused reservations return storage without reopening receive credit.
    returns: Option<Arc<dyn BufferReturn>>,
    prepared: bool,
}

/// Requested body length exceeds the fixed buffer capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferLengthError {
    /// Requested body length in bytes.
    pub requested: usize,
    /// Fixed buffer capacity in bytes.
    pub capacity: usize,
}

impl fmt::Display for BufferLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "body length {} exceeds buffer capacity {}",
            self.requested, self.capacity,
        )
    }
}

impl std::error::Error for BufferLengthError {}

impl MessageBuffer {
    /// Fixed byte capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.storage
            .as_ref()
            .expect("live message buffer")
            .bytes
            .len()
    }

    /// Declared body length in bytes, initially zero.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the declared body length is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entire writable capacity. Only the declared length is sent.
    /// Writing does not change that length; call [`Self::set_len`] afterward.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.prepare().bytes
    }

    fn prepare(&mut self) -> &mut Storage {
        let storage = Arc::get_mut(self.storage.as_mut().expect("live buffer"))
            .expect("writable buffer storage must be unique");
        if !self.prepared {
            storage.returns = self.returns.take();
            self.prepared = true;
        }
        storage
    }

    #[cfg(feature = "dart")]
    pub(crate) fn writable_received(&mut self, pool: &BufferPool) -> &mut [u8] {
        let storage = Arc::get_mut(self.storage.as_mut().expect("live buffer"))
            .expect("writable buffer storage must be unique");
        if !same_returns(storage.returns.as_ref(), pool.1.as_ref()) {
            storage.returns.clone_from(&pool.1);
        }
        self.returns = None;
        self.prepared = true;
        &mut storage.bytes
    }

    /// Set the body length without growing storage.
    /// The bytes must already be filled; this method does not clear them.
    ///
    /// # Errors
    /// Returns [`BufferLengthError`] when `length` exceeds capacity.
    /// The previous length is preserved on error.
    pub fn set_len(&mut self, length: usize) -> Result<(), BufferLengthError> {
        if length > self.capacity() {
            return Err(BufferLengthError {
                requested: length,
                capacity: self.capacity(),
            });
        }
        self.length = length;
        Ok(())
    }

    /// Freeze the declared prefix into an ordinary single-body message.
    /// This consumes exclusive access without copying or allocating. Messages
    /// and byte views retain the buffer until their final owner is dropped.
    #[inline]
    pub fn into_message(self) -> Message {
        Message::from(self.into_payload())
    }

    #[inline]
    pub(crate) fn into_payload(mut self) -> Payload {
        if !self.prepared {
            self.prepare();
        }
        Payload::from_shared_owner_prefix(self.storage.take().expect("live buffer"), self.length)
    }
}

impl AsRef<[u8]> for MessageBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.storage.as_ref().expect("live buffer").bytes[..self.length]
    }
}

impl Drop for MessageBuffer {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.take() {
            storage.release_owner(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const BUFFER_CAPACITY: usize = 2048;

    #[test]
    fn message_builder_selects_inline_pooled_and_owned_storage() {
        let pool = BufferPool::new(128, 1);
        for size in [0, 16, 55, 56, 62, 63, 128, 129, 256] {
            let mut calls = 0;
            let message = pool
                .try_message(size, |body| {
                    calls += 1;
                    assert_eq!(body.len(), size);
                    body.fill(7);
                })
                .unwrap()
                .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(message.part_slice(0), Some(vec![7; size].as_slice()));
            let pooled = (56..=128).contains(&size);
            assert_eq!(pool.available(), usize::from(!pooled));
            if size <= 55 {
                assert_eq!(
                    message.retained_size(),
                    Some(std::mem::size_of::<Message>())
                );
            }
            let clone = message.clone();
            let view = message.part_bytes(0).unwrap();
            drop(message);
            drop(clone);
            assert_eq!(pool.available(), usize::from(!pooled));
            assert_eq!(view.as_ref(), vec![7; size]);
            drop(view);
            assert_eq!(pool.available(), 1);
        }
    }

    #[test]
    fn exhausted_pool_skips_fill_without_blocking_other_storage_forms() {
        for capacity in [0, 1] {
            let pool = BufferPool::new(128, capacity);
            let held = pool.try_take();
            assert!(
                pool.try_message(56, |_| panic!("exhausted"))
                    .unwrap()
                    .is_none()
            );
            for size in [16, 129] {
                let message = pool
                    .try_message(size, |body| body.fill(3))
                    .unwrap()
                    .unwrap();
                assert_eq!(message.part_slice(0), Some(vec![3; size].as_slice()));
                assert_eq!(pool.available(), 0);
            }
            drop(held);
            assert_eq!(pool.available(), capacity);
        }
    }

    #[test]
    fn failed_owned_allocation_skips_fill_and_preserves_pool_capacity() {
        let pool = BufferPool::new(128, 1);
        assert!(matches!(
            pool.try_message(usize::MAX, |_| panic!("allocation failed")),
            Err(omq_proto::Error::Config(_))
        ));
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn panicking_fill_returns_its_checked_out_buffer() {
        let pool = BufferPool::new(128, 1);
        let result = std::panic::catch_unwind(|| {
            let _ = pool.try_message(56, |_| panic!("fill failed"));
        });
        assert!(result.is_err());
        assert_eq!(pool.available(), 1);
    }

    #[test]
    #[should_panic(expected = "buffer size must be nonzero")]
    fn zero_buffer_size_is_rejected() {
        let _ = BufferPool::new(0, 1);
    }

    fn body(pool: &BufferPool, length: usize, value: u8) -> Message {
        let mut buffer = pool.try_take().unwrap();
        buffer.writable()[..length].fill(value);
        buffer.set_len(length).unwrap();
        buffer.into_message()
    }

    #[test]
    fn capacity_is_fixed_and_returns_after_drop() {
        let pool = BufferPool::new(2048, 2);
        let first = pool.try_take().unwrap();
        let second = pool.try_take().unwrap();
        assert!(pool.try_take().is_none());
        assert_eq!(pool.available(), 0);
        drop(first);
        assert_eq!(pool.available(), 1);
        drop(second);
        assert_eq!(pool.available(), 2);
        assert!(BufferPool::new(2048, 0).try_take().is_none());
    }

    #[test]
    fn reused_storage_has_empty_length_and_valid_capacity() {
        let pool = BufferPool::new(2048, 1);
        drop(body(&pool, BUFFER_CAPACITY, 7));
        let mut buffer = pool.try_take().unwrap();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.writable().len(), BUFFER_CAPACITY);
        assert_eq!(
            buffer.set_len(BUFFER_CAPACITY + 1),
            Err(BufferLengthError {
                requested: BUFFER_CAPACITY + 1,
                capacity: BUFFER_CAPACITY,
            })
        );
        assert_eq!(buffer.len(), 0);
        buffer.set_len(BUFFER_CAPACITY).unwrap();
        assert_eq!(buffer.as_ref(), &[7; BUFFER_CAPACITY]);
    }

    #[test]
    fn mixed_scalar_and_bulk_acquisition_preserve_capacity_with_unused_slots() {
        for grouped in [false, true] {
            let pool = BufferPool::new(2048, 8);
            let mut buffers = Vec::with_capacity(8);
            assert_eq!(pool.try_take_many_into(3, &mut buffers), 3);
            if grouped {
                pool.with_recycling_batch(|| buffers.clear());
            } else {
                buffers.clear();
            }
            assert_eq!(pool.available(), 8);
            assert_eq!(pool.try_take_many_into(2, &mut buffers), 2);
            buffers.push(pool.try_take().unwrap());
            assert_eq!(pool.available(), 5);
            assert_eq!(pool.try_take_many_into(8, &mut buffers), 5);
            assert!(pool.try_take().is_none());
            let mut slots: Vec<_> = buffers
                .iter()
                .map(|buffer| buffer.storage.as_ref().unwrap().slot)
                .collect();
            slots.sort_unstable();
            assert_eq!(slots, (0..8).collect::<Vec<_>>());
            drop(buffers);
            assert_eq!(pool.available(), 8);
        }
    }

    #[test]
    fn clones_and_bytes_views_prevent_early_reuse() {
        for length in [0, 1, 128, BUFFER_CAPACITY] {
            let pool = BufferPool::new(2048, 1);
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
        let pool = BufferPool::new(2048, 1);
        for _ in 0..64 {
            let first = body(&pool, BUFFER_CAPACITY, 7);
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
        let pool = BufferPool::new(2048, 2);
        let core = Arc::downgrade(&pool.0);
        let buffer = pool.try_take().unwrap();
        let message = body(&pool, BUFFER_CAPACITY, 7);
        drop(pool);
        assert!(core.upgrade().is_none());
        assert_eq!(message.part_slice(0), Some([7; BUFFER_CAPACITY].as_slice()));
        drop(buffer);
        drop(message);
    }

    #[test]
    fn concurrent_acquisition_and_return_preserve_every_slot() {
        let pool = BufferPool::new(2048, 4);
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
                        buffer.set_len(BUFFER_CAPACITY).unwrap();
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
        let pool = BufferPool::new(2048, 70);
        let mut buffers = Vec::with_capacity(100);
        assert_eq!(pool.try_take_many_into(100, &mut buffers), 32);
        assert_eq!(pool.available(), 38);
        let mut messages: Vec<_> = buffers.drain(..).map(MessageBuffer::into_message).collect();
        let view = messages[0].part_bytes(0).unwrap();
        assert_eq!(BufferPool::recycle_many(&mut messages, 0), 0);
        assert_eq!(BufferPool::recycle_many(&mut messages, 100), 32);
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
        let first = BufferPool::new(2048, 4);
        let second = BufferPool::new(2048, 4);
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
        let pool = BufferPool::new(2048, 8);
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
                            buffer.set_len(BUFFER_CAPACITY).unwrap();
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
                        BufferPool::recycle_many(&mut messages, 2);
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
            let pool = BufferPool::new(2048, capacity);
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
        struct LateDrop(RefCell<Option<(BufferPool, Message)>>);
        impl Drop for LateDrop {
            fn drop(&mut self) {
                if let Some((_pool, message)) = self.0.get_mut().take() {
                    let mut messages = vec![message];
                    BufferPool::recycle_many(&mut messages, 1);
                }
            }
        }
        thread_local! {
            static LATE: LateDrop = const { LateDrop(RefCell::new(None)) };
        }
        let pool = BufferPool::new(2048, 1);
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
