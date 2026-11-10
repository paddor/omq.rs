//! Explicit, transport-independent message preparation and reusable storage.
#![deny(missing_docs)]

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use crate::flow::DrainBudget;
use crate::message::{Message, Payload, PayloadOwner};

mod buffer;
mod free_list;
mod recycle;

pub use buffer::{PayloadBuffer, PayloadLengthError};
use free_list::FreeList;
use recycle::{RECYCLE, recycling};

const TRANSFER: usize = 64;
const TRANSFER_BYTES: usize = 64 * 1024;

/// Backend-provided fixed-size storage. Exclusive access is granted only
/// after all local views and backend leases have returned.
#[doc(hidden)]
pub trait PayloadStorage: AsRef<[u8]> + AsMut<[u8]> + fmt::Debug + Send + Sync {
    /// Whether external readers have released this slot. This check must
    /// acquire their completion writes before allowing another checkout.
    fn reusable(&self) -> bool;
}

#[derive(Debug)]
enum StorageBytes {
    Heap(Box<[u8]>),
    Backend(Box<dyn PayloadStorage>),
}

impl StorageBytes {
    fn reusable(&self) -> bool {
        match self {
            Self::Heap(_) => true,
            Self::Backend(storage) => storage.reusable(),
        }
    }
}

impl std::ops::Deref for StorageBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::Backend(storage) => storage.as_ref().as_ref(),
        }
    }
}

impl std::ops::DerefMut for StorageBytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::Backend(storage) => storage.as_mut().as_mut(),
        }
    }
}

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
    backend: bool,
    reclaim_budget: DrainBudget,
}

/// Whether a returned slot reopens receive credit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Credit {
    /// Final release of a frozen payload: publish the returned position.
    Publish,
    /// Unused checkout: return the storage without publishing credit.
    Reservation,
}

#[derive(Debug)]
struct Storage {
    bytes: StorageBytes,
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
        self.release_owner(Credit::Publish);
    }
}

impl Storage {
    fn release_owner(self: Arc<Self>, credit: Credit) {
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
            if Arc::strong_count(&owner) == 1 && owner.bytes.reusable() {
                recycle.push(owner, credit);
            } else {
                let pool = recycle.pool.as_ref().expect("active pool").clone();
                owner.defer(&pool);
            }
        });

        let Some(storage) = storage else {
            return;
        };

        storage.release_one(credit);
    }

    fn return_one(self: Arc<Self>, pool: &Pool, credit: Credit) {
        let returns = (credit == Credit::Publish)
            .then(|| self.returns.clone())
            .flatten();
        pool.free.push(self);

        if let Some(returns) = returns {
            returns.publish(1);
        }
    }

    fn release_one(self: Arc<Self>, credit: Credit) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };

        // Storage is private and has no Weak references. A unique strong
        // owner cannot race a clone, so it can return without locking.
        if Arc::strong_count(&self) == 1 && self.bytes.reusable() {
            self.return_one(&pool, credit);
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
    fn reclaim(&self, requested: usize) {
        if requested == 0 {
            return;
        }

        let limit = self
            .pending_count
            .load(Ordering::Relaxed)
            .min(self.reclaim_budget.remaining_msgs());
        let mut budget = self.reclaim_budget.clone();
        let mut returned = 0;

        // Inspect each queued owner at most once in this turn, even if it
        // remains held and is pushed back onto the pending queue.
        for _ in 0..limit {
            if budget.exhausted() {
                break;
            }

            let Ok(owner) = self.pending.pop() else {
                break;
            };

            let _ = budget.account(if self.backend {
                std::mem::size_of::<Storage>()
            } else {
                owner.bytes.len()
            });

            if Arc::strong_count(&owner) == 1 && owner.bytes.reusable() {
                self.queued[owner.slot].store(false, Ordering::Relaxed);
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                owner.return_one(self, Credit::Publish);
                returned += 1;

                if returned == requested {
                    break;
                }
            } else {
                self.pending
                    .push(owner)
                    .expect("one pending owner per slot");
            }
        }
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
        Self::build(
            classes,
            &DrainBudget::new(TRANSFER, TRANSFER_BYTES),
            |size, capacity| {
                let mut buffers = Vec::new();
                buffers
                    .try_reserve_exact(capacity)
                    .map_err(|_| crate::Error::Config("payload pool allocation failed".into()))?;

                for _ in 0..capacity {
                    buffers.push(StorageBytes::Heap(owned_body(size)?.into_boxed_slice()));
                }

                Ok(buffers)
            },
        )
    }

    /// Construct the same pool with fixed-size backend storage. The allocator
    /// returns exactly `capacity` slots of `size` bytes per class.
    /// `reclaim_budget` bounds each pending-owner scan by count and metadata
    /// bytes. Backend completion checks do not read application payloads.
    #[doc(hidden)]
    pub fn with_storage(
        classes: impl IntoIterator<Item = (usize, usize)>,
        reclaim_budget: &DrainBudget,
        mut allocate: impl FnMut(usize, usize) -> crate::Result<Vec<Box<dyn PayloadStorage>>>,
    ) -> crate::Result<Self> {
        if reclaim_budget.exhausted() {
            return Err(crate::Error::Config(
                "empty backend reclamation budget".into(),
            ));
        }

        Self::build(classes, reclaim_budget, |size, capacity| {
            let buffers = allocate(size, capacity)?;
            if buffers.len() != capacity
                || buffers
                    .iter()
                    .any(|slot| slot.as_ref().as_ref().len() != size)
            {
                return Err(crate::Error::Config(
                    "invalid backend payload storage".into(),
                ));
            }

            Ok(buffers.into_iter().map(StorageBytes::Backend).collect())
        })
    }

    fn build(
        classes: impl IntoIterator<Item = (usize, usize)>,
        reclaim_budget: &DrainBudget,
        mut allocate: impl FnMut(usize, usize) -> crate::Result<Vec<StorageBytes>>,
    ) -> crate::Result<Self> {
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

            let buffers = allocate(buffer_size, capacity)?;
            let backend = buffers
                .first()
                .is_some_and(|bytes| matches!(bytes, StorageBytes::Backend(_)));
            let reclaim_budget = reclaim_budget.clone();

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
                    backend,
                    reclaim_budget,
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
            let storage = pool.free.pop().or_else(|| {
                // Returned free slots are already reusable. Scan deferred
                // owners only when checkout actually needs their credit.
                pool.reclaim(1);

                pool.free.pop()
            });

            if let Some(storage) = storage {
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

            let requested = budget
                .remaining_msgs()
                .min((TRANSFER_BYTES.saturating_sub(budget.bytes()) / pool.buffer_size).max(1));
            pool.reclaim(requested.saturating_sub(pool.free.len()));

            if pool.free.available.load(Ordering::Relaxed) == 0 {
                continue;
            }

            let count = pool.free.pop_many(requested, output);
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
                pool.reclaim(usize::MAX);
                pool.free.len()
            })
            .sum()
    }

    /// Perform a bounded deferred-reclamation turn in each class.
    #[doc(hidden)]
    pub fn reclaim(&self) {
        for pool in self.classes.iter() {
            pool.reclaim(usize::MAX);
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

#[cfg(test)]
mod tests;
