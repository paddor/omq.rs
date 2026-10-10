//! Explicit, transport-independent message preparation and reusable storage.
#![deny(missing_docs)]

use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use crate::message::{Message, Payload, PayloadOwner};

/// Receive lifecycle callbacks implemented by I/O backends.
/// Buffer returns publish credit only after the final payload owner is released.
#[doc(hidden)]
pub trait PayloadRelease: fmt::Debug + Send + Sync + std::panic::RefUnwindSafe {
    /// Publish the number of reusable receive positions.
    fn publish(&self, count: usize);
    /// Schedule a bounded deferred-reclamation turn.
    fn wake(&self);
}

fn same_returns(
    first: Option<&Arc<dyn PayloadRelease>>,
    second: Option<&Arc<dyn PayloadRelease>>,
) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(first), Some(second)) => Arc::ptr_eq(first, second),
        _ => false,
    }
}

/// Preallocated payload storage in ascending size classes.
///
/// Clones share storage. Construction allocates every slot; checkout and
/// return never grow the pool. Message parts select the smallest fitting
/// available class. Inline bodies skip checkout. Final message owners and
/// byte views return their storage, including across threads.
///
/// `message` and `payload` fall back to owned storage. Their `try_` forms
/// return `None` when no pooled storage fits or is available.
///
/// ```
/// use omq_proto::PayloadPool;
/// let pool = PayloadPool::new([(1024, 128), (4096, 32)])?;
/// let message = pool.message(16, |body| body.fill(7))?;
/// assert_eq!(message.part_slice(0), Some([7; 16].as_slice()));
/// # Ok::<(), omq_proto::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct PayloadPool {
    classes: Arc<[Arc<Pool>]>,
    returns: Option<Arc<dyn PayloadRelease>>,
}

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

    fn pop_many(&self, limit: usize, output: &mut Vec<PayloadBuffer>) -> usize {
        let limit = limit.min(TRANSFER).min(output.capacity() - output.len());
        if limit == 0 {
            return 0;
        }
        // Sparse traffic takes the same scalar path as ordinary acquisition.
        if limit == 1 {
            if let Some(owner) = self.pop() {
                output.push(PayloadBuffer {
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
                    output.push(PayloadBuffer {
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
                    output.push(PayloadBuffer {
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
    returns: Option<Arc<dyn PayloadRelease>>,
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
        let mut budget = crate::flow::DrainBudget::new(limit, TRANSFER_BYTES);
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
    returns: Option<Arc<dyn PayloadRelease>>,
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

impl PayloadPool {
    /// Preallocate each `(bytes_per_slot, slots)` size class.
    /// Zero slots and an empty class list are allowed. Reused bytes are not
    /// cleared. Class definitions remain immutable after construction.
    ///
    /// # Errors
    /// Returns a configuration error for zero slot size, size overflow, or
    /// failure to reserve payload storage.
    pub fn new(classes: impl IntoIterator<Item = (usize, usize)>) -> crate::Result<Self> {
        let mut pools = Vec::new();
        for (buffer_size, capacity) in classes {
            if buffer_size == 0
                || buffer_size > isize::MAX as usize
                || buffer_size
                    .checked_mul(capacity)
                    .is_none_or(|bytes| bytes > isize::MAX as usize)
            {
                return Err(crate::Error::Config("invalid payload pool size".into()));
            }
            let mut buffers = Vec::new();
            buffers
                .try_reserve_exact(capacity)
                .map_err(|_| crate::Error::Config("payload pool allocation failed".into()))?;
            for _ in 0..capacity {
                buffers.push(owned_body(buffer_size)?.into_boxed_slice());
            }
            pools.push(Arc::new_cyclic(move |pool| {
                let free = FreeList::new(capacity);
                for (slot, bytes) in buffers.into_iter().enumerate() {
                    free.push(Arc::new(Storage {
                        bytes,
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
            }));
        }
        pools.sort_by_key(|pool| pool.buffer_size);
        Ok(Self {
            classes: pools.into(),
            returns: None,
        })
    }

    /// Combine existing handles without allocating payload storage.
    /// Repeated references to the same size class are included only once.
    #[must_use]
    pub fn combine(pools: impl IntoIterator<Item = Self>) -> Self {
        let mut classes: Vec<Arc<Pool>> = Vec::new();
        for pool in pools {
            for class in pool.classes.iter() {
                if !classes.iter().any(|existing| Arc::ptr_eq(existing, class)) {
                    classes.push(class.clone());
                }
            }
        }
        classes.sort_by_key(|pool| pool.buffer_size);
        Self {
            classes: classes.into(),
            returns: None,
        }
    }

    /// Bind receive lifecycle callbacks while sharing the same storage.
    #[doc(hidden)]
    #[must_use]
    pub fn with_release(&self, returns: Arc<dyn PayloadRelease>) -> Self {
        Self {
            classes: self.classes.clone(),
            returns: Some(returns),
        }
    }

    /// Acquire the smallest fitting available slot, or `None`.
    /// The declared length starts at zero. Storage never grows.
    pub fn try_buffer(&self, size: usize) -> Option<PayloadBuffer> {
        for pool in self.classes.iter().filter(|pool| pool.buffer_size >= size) {
            pool.reclaim();
            if let Some(storage) = pool.free.pop() {
                return Some(PayloadBuffer {
                    storage: Some(storage),
                    length: 0,
                    returns: self.returns.clone(),
                    prepared: false,
                });
            }
        }
        None
    }

    /// Construct a single-part message, using inline, pooled, or owned storage.
    /// `fill` receives exactly `size` writable bytes and runs once on success.
    /// Fill the whole slice: pooled bytes retain their previous contents.
    ///
    /// # Errors
    /// Returns a configuration error if owned storage cannot be reserved.
    pub fn message(&self, size: usize, fill: impl FnOnce(&mut [u8])) -> crate::Result<Message> {
        if size <= crate::message::MAX_INLINE_MESSAGE {
            let mut body = [0; crate::message::MAX_INLINE_MESSAGE];
            fill(&mut body[..size]);
            return Ok(Message::from_slice(&body[..size]));
        }
        if let Some(mut buffer) = self.try_buffer(size) {
            fill(&mut buffer.writable()[..size]);
            buffer.set_len(size).expect("selected slot fits body");
            return Ok(buffer.into_message());
        }
        let mut body = owned_body(size)?;
        fill(&mut body);
        let retained = body.capacity();
        Ok(Message::from(Payload::from_bytes_with_retained_size(
            bytes::Bytes::from(body),
            retained,
        )))
    }

    /// Construct a single-part message without an owned-storage fallback.
    /// Bodies up to 55 bytes stay inline. `None` skips `fill` entirely.
    ///
    /// # Errors
    /// Returns a configuration error if `size` exceeds the addressable limit.
    pub fn try_message(
        &self,
        size: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> crate::Result<Option<Message>> {
        if size <= crate::message::MAX_INLINE_MESSAGE {
            return self.message(size, fill).map(Some);
        }
        validate_size(size)?;
        let Some(mut buffer) = self.try_buffer(size) else {
            return Ok(None);
        };
        fill(&mut buffer.writable()[..size]);
        buffer.set_len(size).expect("selected slot fits body");
        Ok(Some(buffer.into_message()))
    }

    /// Construct one multipart payload, using inline, pooled, or owned storage.
    /// Payloads up to 62 bytes stay inline. `fill` runs once on success.
    ///
    /// # Errors
    /// Returns a configuration error if owned storage cannot be reserved.
    pub fn payload(&self, size: usize, fill: impl FnOnce(&mut [u8])) -> crate::Result<Payload> {
        let mut fill = Some(fill);
        if let Some(payload) =
            self.try_payload(size, |body| fill.take().expect("fill once")(body))?
        {
            return Ok(payload);
        }
        let mut body = owned_body(size)?;
        fill.take().expect("fill once")(&mut body);
        let retained = body.capacity();
        Ok(Payload::from_bytes_with_retained_size(
            bytes::Bytes::from(body),
            retained,
        ))
    }

    /// Construct one payload without an owned-storage fallback.
    /// Payloads up to 62 bytes stay inline. `None` skips `fill` entirely.
    ///
    /// # Errors
    /// Returns a configuration error if `size` exceeds the addressable limit.
    pub fn try_payload(
        &self,
        size: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> crate::Result<Option<Payload>> {
        if size <= crate::message::MAX_INLINE_PAYLOAD {
            let mut body = [0; crate::message::MAX_INLINE_PAYLOAD];
            fill(&mut body[..size]);
            return Ok(Some(Payload::from_slice(&body[..size])));
        }
        validate_size(size)?;
        let Some(mut buffer) = self.try_buffer(size) else {
            return Ok(None);
        };
        fill(&mut buffer.writable()[..size]);
        buffer.set_len(size).expect("selected slot fits body");
        Ok(Some(buffer.into_payload()))
    }

    /// Append fitting slots without growing `output`.
    /// Checkout is bounded by `limit`, 64 slots, remaining output capacity,
    /// and 64 KiB of slot capacity. Each batch uses one available size class;
    /// one oversized slot may cross the byte cap.
    pub fn try_buffers_into(
        &self,
        size: usize,
        limit: usize,
        output: &mut Vec<PayloadBuffer>,
    ) -> usize {
        let start = output.len();
        let mut budget = crate::flow::DrainBudget::new(
            limit.min(TRANSFER).min(output.capacity() - start),
            TRANSFER_BYTES,
        );
        for pool in self.classes.iter().filter(|pool| pool.buffer_size >= size) {
            if budget.exhausted() {
                break;
            }
            pool.reclaim();
            if pool.free.available.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let count = pool.free.pop_many(
                budget
                    .remaining_msgs()
                    .min((TRANSFER_BYTES.saturating_sub(budget.bytes()) / pool.buffer_size).max(1)),
                output,
            );
            let batch_start = output.len() - count;
            for buffer in &mut output[batch_start..] {
                buffer.returns.clone_from(&self.returns);
                let _ = budget.account(buffer.capacity());
            }
            if count != 0 {
                break;
            }
        }
        output.len() - start
    }

    /// Drop up to 64 messages within a 64 KiB body budget and publish their
    /// final pooled owners together, across any pools.
    /// Clones and byte views retain storage until their final owner is released.
    pub fn recycle_many(messages: &mut Vec<Message>, limit: usize) -> usize {
        let mut budget = crate::flow::DrainBudget::new(limit.min(TRANSFER), TRANSFER_BYTES);
        let mut count = 0;
        for message in messages.iter().take(limit.min(TRANSFER)) {
            if budget.exhausted() {
                break;
            }
            let _ = budget.account(message.byte_len());
            count += 1;
        }
        recycling(|| drop(messages.drain(..count)));
        count
    }

    /// Batch synchronous final-owner returns, including during unwinding.
    #[doc(hidden)]
    pub fn with_recycling_batch<R>(&self, operation: impl FnOnce() -> R) -> R {
        recycling(operation)
    }

    /// Return the smallest configured slot size that fits `size`.
    /// Availability is checked separately during checkout.
    pub fn class_size(&self, size: usize) -> Option<usize> {
        self.classes
            .iter()
            .find(|pool| pool.buffer_size >= size)
            .map(|pool| pool.buffer_size)
    }

    /// Ascending `(bytes_per_slot, total_slots)` class definitions.
    pub fn classes(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.classes
            .iter()
            .map(|pool| (pool.buffer_size, pool.capacity))
    }

    /// Largest configured slot size, or zero for an empty pool.
    pub fn max_size(&self) -> usize {
        self.classes.last().map_or(0, |pool| pool.buffer_size)
    }

    /// Total slots across all classes, including checked-out slots.
    pub fn capacity(&self) -> usize {
        self.classes.iter().map(|pool| pool.capacity).sum()
    }

    /// Available slots after bounded deferred reclamation in each class.
    /// Concurrent returns may be in flight; this snapshot is not a reservation.
    pub fn available(&self) -> usize {
        self.classes
            .iter()
            .map(|pool| {
                pool.reclaim();
                pool.free.len()
            })
            .sum()
    }

    /// Perform a bounded deferred-reclamation turn in each class.
    #[doc(hidden)]
    pub fn reclaim(&self) {
        for pool in self.classes.iter() {
            pool.reclaim();
        }
    }
}

/// Batch synchronous returns across independently configured pools.
#[doc(hidden)]
pub fn with_recycling_batch<R>(operation: impl FnOnce() -> R) -> R {
    recycling(operation)
}

fn validate_size(size: usize) -> crate::Result<()> {
    if size > isize::MAX as usize {
        return Err(crate::Error::Config(
            "payload size exceeds addressable limit".into(),
        ));
    }
    Ok(())
}

fn owned_body(size: usize) -> crate::Result<Vec<u8>> {
    validate_size(size)?;
    let mut body = Vec::new();
    body.try_reserve_exact(size)
        .map_err(|_| crate::Error::Config("payload allocation failed".into()))?;
    body.resize(size, 0);
    Ok(body)
}

fn recycling<R>(operation: impl FnOnce() -> R) -> R {
    let installed = RECYCLE
        .try_with(|recycle| {
            let mut recycle = recycle.borrow_mut();
            if recycle.active {
                false
            } else {
                recycle.active = true;
                recycle.pool = None;
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
pub struct PayloadBuffer {
    storage: Option<Arc<Storage>>,
    // The frozen payload captures this prefix length. Storage always exposes
    // its fixed capacity, so freezing requires no second Arc uniqueness check.
    length: usize,
    // Unused reservations return storage without reopening receive credit.
    returns: Option<Arc<dyn PayloadRelease>>,
    prepared: bool,
}

/// Requested body length exceeds the fixed buffer capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadLengthError {
    /// Requested body length in bytes.
    pub requested: usize,
    /// Fixed buffer capacity in bytes.
    pub capacity: usize,
}

impl fmt::Display for PayloadLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "payload length {} exceeds slot capacity {}",
            self.requested, self.capacity,
        )
    }
}

impl std::error::Error for PayloadLengthError {}

impl PayloadBuffer {
    /// Fixed byte capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.storage
            .as_ref()
            .expect("live payload buffer")
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

    /// Fill receive storage with connection-specific lifecycle callbacks.
    #[doc(hidden)]
    pub fn writable_received(&mut self, pool: &PayloadPool) -> &mut [u8] {
        let storage = Arc::get_mut(self.storage.as_mut().expect("live buffer"))
            .expect("writable buffer storage must be unique");
        if !same_returns(storage.returns.as_ref(), pool.returns.as_ref()) {
            storage.returns.clone_from(&pool.returns);
        }
        self.returns = None;
        self.prepared = true;
        &mut storage.bytes
    }

    /// Set the body length without growing storage.
    /// The bytes must already be filled; this method does not clear them.
    ///
    /// # Errors
    /// Returns [`PayloadLengthError`] when `length` exceeds capacity.
    /// The previous length is preserved on error.
    pub fn set_len(&mut self, length: usize) -> Result<(), PayloadLengthError> {
        if length > self.capacity() {
            return Err(PayloadLengthError {
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

    /// Freeze the declared prefix into one message part without copying.
    #[inline]
    pub fn into_payload(mut self) -> Payload {
        if !self.prepared {
            self.prepare();
        }
        Payload::from_shared_owner_prefix(self.storage.take().expect("live buffer"), self.length)
    }
}

impl AsRef<[u8]> for PayloadBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.storage.as_ref().expect("live buffer").bytes[..self.length]
    }
}

impl Drop for PayloadBuffer {
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
        let pool = PayloadPool::new([(128, 1), (256, 1)]).unwrap();
        for size in [0, 16, 55, 56, 62, 63, 128, 129, 256, 512] {
            let mut calls = 0;
            let message = pool
                .message(size, |body| {
                    calls += 1;
                    assert_eq!(body.len(), size);
                    body.fill(7);
                })
                .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(message.part_slice(0), Some(vec![7; size].as_slice()));
            let pooled = (56..=256).contains(&size);
            assert_eq!(pool.available(), 2 - usize::from(pooled));
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
            assert_eq!(pool.available(), 2 - usize::from(pooled));
            assert_eq!(view.as_ref(), vec![7; size]);
            drop(view);
            assert_eq!(pool.available(), 2);
        }
    }

    #[test]
    fn exhausted_pool_skips_fill_without_blocking_other_storage_forms() {
        for capacity in [0, 1] {
            let pool = PayloadPool::new([(128, capacity)]).unwrap();
            let held = pool.try_buffer(1);
            assert!(
                pool.try_message(56, |_| panic!("exhausted"))
                    .unwrap()
                    .is_none()
            );
            for size in [16, 129] {
                let message = pool.message(size, |body| body.fill(3)).unwrap();
                assert_eq!(message.part_slice(0), Some(vec![3; size].as_slice()));
                assert_eq!(pool.available(), 0);
            }
            drop(held);
            assert_eq!(pool.available(), capacity);
        }
    }

    #[test]
    fn failed_owned_allocation_skips_fill_and_preserves_pool_capacity() {
        let pool = PayloadPool::new([(128, 1)]).unwrap();
        assert!(matches!(
            pool.message(usize::MAX, |_| panic!("allocation failed")),
            Err(crate::Error::Config(_))
        ));
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn panicking_fill_returns_its_checked_out_buffer() {
        let pool = PayloadPool::new([(128, 1)]).unwrap();
        let result = std::panic::catch_unwind(|| {
            let _ = pool.try_message(56, |_| panic!("fill failed"));
        });
        assert!(result.is_err());
        assert_eq!(pool.available(), 1);
    }

    #[test]
    #[should_panic(expected = "invalid payload pool size")]
    fn zero_buffer_size_is_rejected() {
        let _ = PayloadPool::new([(0, 1)]).unwrap();
    }

    fn body(pool: &PayloadPool, length: usize, value: u8) -> Message {
        let mut buffer = pool.try_buffer(1).unwrap();
        buffer.writable()[..length].fill(value);
        buffer.set_len(length).unwrap();
        buffer.into_message()
    }

    #[test]
    fn capacity_is_fixed_and_returns_after_drop() {
        let pool = PayloadPool::new([(2048, 2)]).unwrap();
        let first = pool.try_buffer(1).unwrap();
        let second = pool.try_buffer(1).unwrap();
        assert!(pool.try_buffer(1).is_none());
        assert_eq!(pool.available(), 0);
        drop(first);
        assert_eq!(pool.available(), 1);
        drop(second);
        assert_eq!(pool.available(), 2);
        assert!(
            PayloadPool::new([(2048, 0)])
                .unwrap()
                .try_buffer(1)
                .is_none()
        );
    }

    #[test]
    fn reused_storage_has_empty_length_and_valid_capacity() {
        let pool = PayloadPool::new([(2048, 1)]).unwrap();
        drop(body(&pool, BUFFER_CAPACITY, 7));
        let mut buffer = pool.try_buffer(1).unwrap();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.writable().len(), BUFFER_CAPACITY);
        assert_eq!(
            buffer.set_len(BUFFER_CAPACITY + 1),
            Err(PayloadLengthError {
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
            let pool = PayloadPool::new([(2048, 8)]).unwrap();
            let mut buffers = Vec::with_capacity(8);
            assert_eq!(pool.try_buffers_into(1, 3, &mut buffers), 3);
            if grouped {
                pool.with_recycling_batch(|| buffers.clear());
            } else {
                buffers.clear();
            }
            assert_eq!(pool.available(), 8);
            assert_eq!(pool.try_buffers_into(1, 2, &mut buffers), 2);
            buffers.push(pool.try_buffer(1).unwrap());
            assert_eq!(pool.available(), 5);
            assert_eq!(pool.try_buffers_into(1, 8, &mut buffers), 5);
            assert!(pool.try_buffer(1).is_none());
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
            let pool = PayloadPool::new([(2048, 1)]).unwrap();
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
        let pool = PayloadPool::new([(2048, 1)]).unwrap();
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
            assert_eq!(pool.try_buffer(1).unwrap().len(), 0);
        }
    }

    #[test]
    fn buffers_and_messages_outlive_pool_without_a_cycle() {
        let pool = PayloadPool::new([(2048, 2)]).unwrap();
        let core = Arc::downgrade(&pool.classes[0]);
        let buffer = pool.try_buffer(1).unwrap();
        let message = body(&pool, BUFFER_CAPACITY, 7);
        drop(pool);
        assert!(core.upgrade().is_none());
        assert_eq!(message.part_slice(0), Some([7; BUFFER_CAPACITY].as_slice()));
        drop(buffer);
        drop(message);
    }

    #[test]
    fn concurrent_acquisition_and_return_preserve_every_slot() {
        let pool = PayloadPool::new([(2048, 4)]).unwrap();
        std::thread::scope(|scope| {
            for value in 1..=4 {
                let pool = &pool;
                scope.spawn(move || {
                    for _ in 0..256 {
                        let mut buffer = loop {
                            if let Some(buffer) = pool.try_buffer(1) {
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
        let pool = PayloadPool::new([(2048, 70)]).unwrap();
        let mut buffers = Vec::with_capacity(100);
        assert_eq!(pool.try_buffers_into(1, 100, &mut buffers), 32);
        assert_eq!(pool.available(), 38);
        let mut messages: Vec<_> = buffers.drain(..).map(PayloadBuffer::into_message).collect();
        let view = messages[0].part_bytes(0).unwrap();
        assert_eq!(PayloadPool::recycle_many(&mut messages, 0), 0);
        assert_eq!(PayloadPool::recycle_many(&mut messages, 100), 32);
        assert_eq!(pool.available(), 69);
        drop(view);
        assert_eq!(pool.available(), 70);
        let mut small = Vec::with_capacity(2);
        assert_eq!(pool.try_buffers_into(1, 64, &mut small), 2);
        assert_eq!(pool.try_buffers_into(1, 64, &mut small), 0);
        assert_eq!(small.capacity(), 2);
        drop(small);
        assert_eq!(pool.available(), 70);
    }

    #[test]
    fn nested_foreign_and_unwinding_returns_leave_no_hidden_buffers() {
        let first = PayloadPool::new([(2048, 4)]).unwrap();
        let second = PayloadPool::new([(2048, 4)]).unwrap();
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
        let pool = PayloadPool::new([(2048, 8)]).unwrap();
        std::thread::scope(|scope| {
            for value in 1..=4 {
                let pool = &pool;
                scope.spawn(move || {
                    let mut buffers = Vec::with_capacity(2);
                    let mut messages = Vec::with_capacity(2);
                    for _ in 0..256 {
                        while pool.try_buffers_into(1, 2, &mut buffers) == 0 {
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
                        PayloadPool::recycle_many(&mut messages, 2);
                    }
                });
            }
        });
        assert_eq!(pool.available(), 8);
        let mut buffers = Vec::with_capacity(8);
        assert_eq!(pool.try_buffers_into(1, 8, &mut buffers), 8);
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
            let pool = PayloadPool::new([(2048, capacity)]).unwrap();
            let mut buffers = Vec::with_capacity(capacity);
            for limit in [1, 2, 17, 63, 64] {
                while pool.try_buffers_into(1, limit, &mut buffers) != 0 {}
                assert_eq!(buffers.len(), capacity);
                assert_eq!(pool.available(), 0);
                // Partial final transfers are all published by scope exit.
                pool.with_recycling_batch(|| buffers.clear());
                assert_eq!(pool.available(), capacity);
                while let Some(buffer) = pool.try_buffer(1) {
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
        struct LateDrop(RefCell<Option<(PayloadPool, Message)>>);
        impl Drop for LateDrop {
            fn drop(&mut self) {
                if let Some((_pool, message)) = self.0.get_mut().take() {
                    let mut messages = vec![message];
                    PayloadPool::recycle_many(&mut messages, 1);
                }
            }
        }
        thread_local! {
            static LATE: LateDrop = const { LateDrop(RefCell::new(None)) };
        }
        let pool = PayloadPool::new([(2048, 1)]).unwrap();
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
