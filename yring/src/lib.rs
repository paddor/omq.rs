#![deny(unsafe_op_in_unsafe_fn)]
//! Bounded SPSC ring with ypipe-style batched flush/prefetch.
//!
//! Three pointers:
//! - `head`: consumer read position (`AtomicUsize`, consumer-owned)
//! - `cursor`: writer position (plain usize, producer-only, no atomic)
//! - `tail`: last flushed position (`AtomicUsize`, producer writes, consumer reads)
//!
//! `push` writes to the ring with zero atomics. `flush` makes all
//! pending writes visible with one Release store. `pop` reads with
//! zero atomics. `prefetch` loads all flushed items with one Acquire
//! load. Result: 1 atomic per batch on each side.

#[cfg(feature = "async")]
mod r#async;
#[cfg(feature = "async")]
pub use r#async::{AsyncConsumer, AsyncProducer, async_spsc};

#[cfg(not(target_has_atomic = "ptr"))]
compile_error!("yring requires target_has_atomic = \"ptr\"");

mod compat;

use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::OnceLock;

#[cfg(not(all(loom, target_pointer_width = "64")))]
use compat::UnsafeCellExt;
use compat::{Arc, AtomicBool, AtomicUsize, Ordering, UnsafeCell};

type AtomicCursor = AtomicUsize;
type Cursor = usize;

#[repr(align(128))]
pub(crate) struct Padded<T>(pub(crate) T);

pub(crate) struct Ring<T> {
    pub(crate) buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    pub(crate) mask: usize,
    /// Consumer read position. Written by consumer, read by producer.
    pub(crate) head: Padded<AtomicCursor>,
    /// Last flushed position. Written by producer, read by consumer.
    pub(crate) tail: Padded<AtomicCursor>,
    /// Set by `Producer::drop`. Lets the consumer detect that no more
    /// data will ever arrive.
    pub(crate) producer_dropped: AtomicBool,
    /// Set by `Consumer::drop` / `AsyncConsumer::drop`. Lets the producer
    /// detect that the consumer is gone.
    pub(crate) consumer_dropped: AtomicBool,
}

// SAFETY: Ring<T> is Send because all shared mutable state is accessed through
// atomics (head, tail) and UnsafeCell slots follow the SPSC protocol: the
// producer writes cursor..tail, the consumer reads head..tail, non-overlapping.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: Ring<T> is Sync for the same reason: concurrent access is mediated
// by atomics and the SPSC single-producer/single-consumer invariant.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        let cap = capacity
            .checked_next_power_of_two()
            .expect("capacity must fit in the next power of two");
        assert!(
            cap <= usize::MAX / 2,
            "capacity must be <= half the cursor range"
        );
        let buf: Vec<UnsafeCell<MaybeUninit<T>>> = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            head: Padded(AtomicCursor::new(0)),
            tail: Padded(AtomicCursor::new(0)),
            producer_dropped: AtomicBool::new(false),
            consumer_dropped: AtomicBool::new(false),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    fn distance(from: Cursor, to: Cursor) -> Cursor {
        from.wrapping_sub(to)
    }

    #[inline]
    fn count(n: Cursor) -> usize {
        n
    }

    #[inline]
    fn index(&self, cursor: Cursor) -> usize {
        cursor & self.mask
    }

    #[inline]
    pub(crate) fn push(
        &self,
        cursor: &mut Cursor,
        cached_head: &mut Cursor,
        val: T,
    ) -> Result<(), T> {
        if Self::distance(*cursor, *cached_head) >= self.capacity() {
            *cached_head = self.head.0.load(Ordering::Acquire);
            if Self::distance(*cursor, *cached_head) >= self.capacity() {
                return Err(val);
            }
        }
        // SAFETY: capacity check above guarantees this slot is not
        // visible to the consumer (cursor < head + capacity).
        self.buf[self.index(*cursor)].with_mut(|ptr| unsafe {
            (*ptr).write(val);
        });
        *cursor = cursor.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub(crate) fn flush_to(&self, cursor: Cursor, cached_head: &mut Cursor) -> FlushResult {
        let prev_tail = self.tail.0.load(Ordering::Relaxed);
        if cursor == prev_tail {
            return FlushResult::NothingToFlush;
        }
        let count = Self::distance(cursor, prev_tail);
        *cached_head = self.head.0.load(Ordering::Acquire);
        let was_empty = prev_tail == *cached_head;
        self.tail.0.store(cursor, Ordering::Release);
        FlushResult::Flushed {
            count: Self::count(count),
            was_empty,
        }
    }

    #[inline]
    pub(crate) fn is_full(&self, cursor: Cursor, cached_head: &mut Cursor) -> bool {
        if Self::distance(cursor, *cached_head) >= self.capacity() {
            *cached_head = self.head.0.load(Ordering::Acquire);
            Self::distance(cursor, *cached_head) >= self.capacity()
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn producer_len(&self, cursor: Cursor) -> usize {
        Self::count(cursor.wrapping_sub(self.head.0.load(Ordering::Acquire)))
    }

    #[inline]
    pub(crate) fn producer_is_empty(&self, cursor: Cursor) -> bool {
        cursor == self.head.0.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn pop(&self, head: &mut Cursor, cached_tail: Cursor) -> Option<T> {
        if *head == cached_tail {
            return None;
        }
        // SAFETY: head < cached_tail, so this slot was written by the
        // producer and made visible via flush (Release store).
        let val = self.buf[self.index(*head)].with_mut(|ptr| unsafe { (*ptr).assume_init_read() });
        *head = head.wrapping_add(1);
        Some(val)
    }

    #[inline]
    pub(crate) fn release(&self, head: Cursor) {
        self.head.0.store(head, Ordering::Release);
    }

    #[inline]
    pub(crate) fn prefetch(&self, cached_tail: &mut Cursor) -> usize {
        let new_tail = self.tail.0.load(Ordering::Acquire);
        let count = new_tail.wrapping_sub(*cached_tail);
        *cached_tail = new_tail;
        Self::count(count)
    }

    #[inline]
    pub(crate) fn consumer_is_empty(&self, head: Cursor, cached_tail: Cursor) -> bool {
        head == cached_tail && self.tail.0.load(Ordering::Acquire) == head
    }

    #[inline]
    pub(crate) fn consumer_len(&self, head: Cursor) -> usize {
        Self::count(self.tail.0.load(Ordering::Acquire).wrapping_sub(head))
    }

    /// Drop all items between head and tail. Must only be called with
    /// exclusive access (i.e. in a `Drop` impl or when no concurrent
    /// readers/writers exist).
    ///
    /// Idempotent: it advances `head` to `tail` before draining, so a
    /// second call (e.g. `Ring::drop` running after an async wrapper
    /// already drained) sees an empty range and cannot double-drop.
    /// If a `T::drop` panics mid-loop, items past the panicking one are
    /// leaked rather than double-dropped.
    pub(crate) fn drop_remaining(&mut self) {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        // Advance head before the loop so a panicking T::drop cannot
        // cause a second call to re-drop the same items.
        self.head.0.store(tail, Ordering::Relaxed);
        let mut i = head;
        while i != tail {
            // SAFETY: &mut self guarantees exclusive access. Slots
            // [head..tail] were written by the producer and flushed.
            self.buf[self.index(i)].with_mut(|ptr| unsafe {
                (*ptr).assume_init_drop();
            });
            i = i.wrapping_add(1);
        }
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        self.drop_remaining();
    }
}

/// Result of a flush operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    /// Items were flushed. The consumer may have been idle and needs waking.
    Flushed {
        /// Number of items made visible.
        count: usize,
        /// Whether the consumer had no visible items before this flush.
        was_empty: bool,
    },
    /// Nothing to flush (cursor == tail already).
    NothingToFlush,
}

/// Sending half. `Send` but not `Sync`.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    /// Private write position. No atomic; only the producer touches it.
    cursor: Cursor,
    /// Cached copy of the consumer's `head` to avoid Acquire loads.
    cached_head: Cursor,
}

impl<T> Producer<T> {
    /// Address of the underlying ring allocation. Two producers or
    /// consumers sharing the same ring return the same value.
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.close();
    }
}

// SAFETY: Producer<T> is Send because it is single-owner (not Sync) and the
// underlying Ring is Send+Sync. Moving the producer to another thread is safe.
unsafe impl<T: Send> Send for Producer<T> {}

/// Single-thread owner handle for a producer shared through an `Arc`.
///
/// The caller must use this value from exactly one producer thread. This
/// preserves the SPSC producer cursor without a mutex. It is intended for
/// transports that already guarantee one socket thread, such as inproc. The
/// first producer call binds the owner to its current thread; later calls
/// from another thread panic.
pub struct ProducerOwner<T> {
    producer: UnsafeCell<Producer<T>>,
    owner: OnceLock<usize>,
}

// SAFETY: `with_producer` binds access to one thread before touching the
// UnsafeCell. Calls from other threads panic without accessing the cell.
unsafe impl<T: Send> Send for ProducerOwner<T> {}
unsafe impl<T: Send> Sync for ProducerOwner<T> {}

impl<T> ProducerOwner<T> {
    /// Wrap a producer for single-thread shared ownership.
    pub fn new(producer: Producer<T>) -> Self {
        Self {
            producer: UnsafeCell::new(producer),
            owner: OnceLock::new(),
        }
    }

    #[inline]
    fn with_producer<R>(&self, f: impl FnOnce(&mut Producer<T>) -> R) -> R {
        let current = current_thread_token();
        let owner = self.owner.get_or_init(|| current);
        assert_eq!(
            *owner, current,
            "ProducerOwner accessed from multiple producer threads"
        );
        // SAFETY: owner check above ensures one thread accesses producer.
        unsafe { self.producer.with_mut(|ptr| f(&mut *ptr)) }
    }

    /// Push one value without producer locking.
    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        self.with_producer(|producer| producer.push(value))
    }

    /// Push one value and publish pending values with one owner check.
    #[inline]
    pub fn push_flush(&self, value: T) -> Result<(), T> {
        self.with_producer(|producer| {
            let result = producer.push(value);
            if result.is_ok() {
                producer.flush();
            }
            result
        })
    }

    /// Publish pending values.
    #[inline]
    pub fn flush(&self) {
        self.with_producer(Producer::flush);
    }

    /// Test whether the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.with_producer(Producer::is_full)
    }

    /// Test whether the consumer has gone away.
    #[inline]
    pub fn is_consumer_dropped(&self) -> bool {
        self.with_producer(|producer| producer.is_consumer_dropped())
    }
}

static NEXT_THREAD_TOKEN: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    static THREAD_TOKEN: Cell<usize> = const { Cell::new(0) };
}

#[inline]
fn current_thread_token() -> usize {
    THREAD_TOKEN.with(|token| {
        let current = token.get();
        if current != 0 {
            return current;
        }
        let current = NEXT_THREAD_TOKEN.fetch_add(1, Ordering::Relaxed);
        token.set(current);
        current
    })
}

impl<T> std::fmt::Debug for ProducerOwner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerOwner").finish_non_exhaustive()
    }
}

/// Receiving half. `Send` but not `Sync`.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    /// Private read position. Only the consumer touches it.
    head: Cursor,
    /// Cached copy of `tail`. Updated by `prefetch()`.
    cached_tail: Cursor,
}

impl<T> Consumer<T> {
    /// Address of the underlying ring allocation. Two producers or
    /// consumers sharing the same ring return the same value.
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

// SAFETY: Consumer<T> is Send because it is single-owner (not Sync) and the
// underlying Ring is Send+Sync. Moving the consumer to another thread is safe.
unsafe impl<T: Send> Send for Consumer<T> {}

/// Create a bounded SPSC ring with the given capacity (rounded up to
/// next power of two).
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    (
        Producer {
            ring: ring.clone(),
            cursor: 0,
            cached_head: 0,
        },
        Consumer {
            ring,
            head: 0,
            cached_tail: 0,
        },
    )
}

#[cfg(all(loom, target_pointer_width = "64"))]
#[doc(hidden)]
pub fn loom_spsc_with_cursors<T>(capacity: usize, cursor: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    ring.head.0.store(cursor, Ordering::Relaxed);
    ring.tail.0.store(cursor, Ordering::Relaxed);
    (
        Producer {
            ring: ring.clone(),
            cursor,
            cached_head: cursor,
        },
        Consumer {
            ring,
            head: cursor,
            cached_tail: cursor,
        },
    )
}

impl<T> Producer<T> {
    /// Write a value to the ring. Zero atomics. Returns `Err(val)` if full.
    /// The value is NOT visible to the consumer until [`flush`](Self::flush).
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.ring.push(&mut self.cursor, &mut self.cached_head, val)
    }

    /// Make all pushed items visible to the consumer. One Release store.
    /// Does NOT load `head`; `push()` refreshes `cached_head` on demand
    /// when the ring appears full.
    #[inline]
    pub fn flush(&mut self) {
        self.ring.tail.0.store(self.cursor, Ordering::Release);
    }

    /// Flush and report whether the ring was empty (consumer fully caught
    /// up). Loads `head` (one Acquire) in addition to the Release store.
    /// Only needed when the caller uses `was_empty` for wakeup decisions.
    #[inline]
    pub fn flush_and_check(&mut self) -> FlushResult {
        self.ring.flush_to(self.cursor, &mut self.cached_head)
    }

    /// Push + flush in one call (convenience for single-item sends).
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<(), T> {
        self.push(val)?;
        self.flush();
        Ok(())
    }

    #[inline]
    /// Return whether the ring cannot accept another item.
    pub fn is_full(&mut self) -> bool {
        self.ring.is_full(self.cursor, &mut self.cached_head)
    }

    #[inline]
    /// Return the ring capacity after power-of-two rounding.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    /// Return the number of flushed and pending items owned by the producer.
    pub fn len(&self) -> usize {
        self.ring.producer_len(self.cursor)
    }

    #[inline]
    /// Return whether the producer has no pending or flushed items.
    pub fn is_empty(&self) -> bool {
        self.ring.producer_is_empty(self.cursor)
    }

    /// The consumer half has been dropped or explicitly closed.
    ///
    /// `push` does not check this flag so the zero-atomic hot path stays
    /// unchanged. Callers that need disconnect detection can check this
    /// before retrying a full ring or before blocking for space.
    #[inline]
    pub fn is_consumer_dropped(&self) -> bool {
        self.ring.consumer_dropped.load(Ordering::Acquire)
    }

    /// Mark the producer side closed after publishing pending writes.
    ///
    /// This is normally called by `Drop`. Calling it explicitly is useful
    /// for wrappers that need to wake a consumer after marking the producer
    /// as gone. After calling this, the producer should not be used again.
    #[inline]
    pub fn close(&mut self) {
        self.flush();
        self.ring.producer_dropped.store(true, Ordering::Release);
    }
}

impl<T> Consumer<T> {
    /// Pop one item. Zero atomics; reads from the prefetched window.
    /// Returns `None` when the prefetched window is exhausted. Call
    /// [`prefetch`](Self::prefetch) to load newly flushed items and
    /// [`release`](Self::release) to publish consumed slots back to the
    /// producer.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.ring.pop(&mut self.head, self.cached_tail)
    }

    /// Publish consumed position so the producer can reuse slots.
    /// One Release store. Call after draining a batch of pops.
    ///
    /// Only values already removed by [`pop`](Self::pop) are released.
    /// Prefetched-but-unpopped values stay reserved and remain readable.
    #[inline]
    pub fn release(&mut self) {
        self.ring.release(self.head);
    }

    /// Load all items flushed since the last prefetch. One Acquire load.
    /// Returns the count of newly available items.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        self.ring.prefetch(&mut self.cached_tail)
    }

    /// Convenience: prefetch + pop + release. For callers that don't
    /// need batching.
    #[inline]
    pub fn prefetch_and_pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            self.prefetch();
        }
        let val = self.pop();
        if val.is_some() {
            self.release();
        }
        val
    }

    #[inline]
    /// Return whether the consumer has no visible items.
    pub fn is_empty(&self) -> bool {
        self.ring.consumer_is_empty(self.head, self.cached_tail)
    }

    /// The producer has been dropped and all flushed items have been
    /// consumed. No more data will ever arrive.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.ring.producer_dropped.load(Ordering::Acquire)
            && self.head == self.ring.tail.0.load(Ordering::Acquire)
    }

    #[inline]
    /// Return the ring capacity after power-of-two rounding.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    /// Return the number of items visible to the consumer.
    pub fn len(&self) -> usize {
        self.ring.consumer_len(self.head)
    }

    /// Mark the consumer side closed and release consumed slots.
    ///
    /// This is normally called by `Drop`. Calling it explicitly is useful
    /// for wrappers that need to wake a producer after marking the consumer
    /// as gone. After calling this, the consumer should not be used again.
    #[inline]
    pub fn close(&mut self) {
        self.release();
        self.ring.consumer_dropped.store(true, Ordering::Release);
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // CI runs Miri with many seeds. Keep cross-thread coverage without turning
    // per-seed validation into a throughput test.
    fn miri_sized(n: u64) -> u64 {
        if cfg!(miri) { 2_048 } else { n }
    }

    #[test]
    fn push_pop_basic() {
        let (mut p, mut c) = spsc::<u32>(4);
        assert!(c.prefetch_and_pop().is_none());
        p.push(1).unwrap();
        p.push(2).unwrap();
        // Not visible yet.
        assert!(c.prefetch_and_pop().is_none());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(1));
        assert_eq!(c.prefetch_and_pop(), Some(2));
        assert!(c.prefetch_and_pop().is_none());
    }

    #[test]
    fn push_and_flush() {
        let (mut p, mut c) = spsc::<u32>(4);
        p.push_and_flush(42).unwrap();
        assert_eq!(c.prefetch_and_pop(), Some(42));
    }

    #[test]
    fn producer_observes_consumer_close() {
        let (p, mut c) = spsc::<u32>(4);
        assert!(!p.is_consumer_dropped());
        c.close();
        assert!(p.is_consumer_dropped());
    }

    #[test]
    fn producer_observes_consumer_drop() {
        let (p, c) = spsc::<u32>(4);
        assert!(!p.is_consumer_dropped());
        drop(c);
        assert!(p.is_consumer_dropped());
    }

    #[test]
    fn producer_owner_rejects_concurrent_producer_thread() {
        use std::sync::{Arc, Barrier};

        let (producer, _consumer) = spsc::<u32>(4);
        let owner = Arc::new(ProducerOwner::new(producer));
        let barrier = Arc::new(Barrier::new(2));
        let first = owner.clone();
        let second = owner.clone();
        let first_barrier = barrier.clone();
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first.push(1)
        });
        let second_barrier = barrier;
        let second = std::thread::spawn(move || {
            second_barrier.wait();
            second.push(2)
        });

        let first = first.join();
        let second = second.join();
        assert_ne!(first.is_ok(), second.is_ok());
    }

    #[test]
    fn batch_prefetch() {
        let (mut p, mut c) = spsc::<u32>(8);
        for i in 0..5 {
            p.push(i).unwrap();
        }
        assert_eq!(c.prefetch(), 0); // not flushed yet
        p.flush();
        assert_eq!(c.prefetch(), 5);
        for i in 0..5 {
            assert_eq!(c.pop(), Some(i));
        }
        assert!(c.pop().is_none());
    }

    #[test]
    fn release_after_prefetch_releases_only_popped_items() {
        let (mut p, mut c) = spsc::<u32>(2);

        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop(), Some(10));
        c.release();

        p.push(30).unwrap();
        assert_eq!(p.push(40), Err(40));

        assert_eq!(c.pop(), Some(20));
        c.release();

        p.push(40).unwrap();
        p.flush();

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop(), Some(30));
        assert_eq!(c.pop(), Some(40));
        assert_eq!(c.pop(), None);
        c.release();
    }

    #[test]
    fn flush_and_check_reports_was_empty() {
        let (mut p, mut c) = spsc::<u32>(4);
        p.push(1).unwrap();
        let r = p.flush_and_check();
        assert_eq!(
            r,
            FlushResult::Flushed {
                count: 1,
                was_empty: true
            }
        );

        p.push(2).unwrap();
        let r = p.flush_and_check();
        // Consumer hasn't read yet, so was_empty depends on cached_head
        assert!(matches!(r, FlushResult::Flushed { count: 1, .. }));

        c.prefetch_and_pop();
        c.prefetch_and_pop();
        p.push(3).unwrap();
        // Force head cache refresh
        p.push(4).unwrap();
        let _ = p.push(5);
        let _ = p.push(6);
        // After consumer consumed, the producer might see stale cached_head
        let r = p.flush_and_check();
        assert!(matches!(r, FlushResult::Flushed { .. }));
    }

    #[test]
    fn full_ring() {
        let (mut p, mut c) = spsc::<u32>(4);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(0));
        p.push(99).unwrap();
        p.flush();
        for i in 1..=4 {
            let expected = if i < 4 { i } else { 99 };
            assert_eq!(c.prefetch_and_pop(), Some(expected));
        }
    }

    #[test]
    fn wraps_around() {
        let (mut p, mut c) = spsc::<u32>(2);
        for round in 0..100 {
            p.push(round * 2).unwrap();
            p.push(round * 2 + 1).unwrap();
            p.flush();
            assert_eq!(c.prefetch_and_pop(), Some(round * 2));
            assert_eq!(c.prefetch_and_pop(), Some(round * 2 + 1));
        }
    }

    #[test]
    fn capacity_rounds_up() {
        let (p, _c) = spsc::<u8>(3);
        assert_eq!(p.capacity(), 4);
        let (p, _c) = spsc::<u8>(5);
        assert_eq!(p.capacity(), 8);
        let (p, _c) = spsc::<u8>(1);
        assert_eq!(p.capacity(), 1);
    }

    #[test]
    #[should_panic(expected = "capacity must fit in the next power of two")]
    fn capacity_overflow_panics_with_message() {
        let _ = spsc::<u8>(usize::MAX);
    }

    #[test]
    #[should_panic(expected = "capacity must be <= half the cursor range")]
    fn capacity_half_range_panics_with_message() {
        let _ = spsc::<u8>((usize::MAX / 2) + 1);
    }

    #[test]
    fn counters_wrap_without_panicking() {
        let (mut p, mut c) = spsc::<u32>(4);
        let base: Cursor = usize::MAX - 1;
        p.cursor = base;
        p.cached_head = base;
        c.head = base;
        c.cached_tail = base;
        p.ring.head.0.store(base, Ordering::Relaxed);
        p.ring.tail.0.store(base, Ordering::Relaxed);

        p.push(1).unwrap();
        p.push(2).unwrap();
        p.push(3).unwrap();
        assert_eq!(
            p.flush_and_check(),
            FlushResult::Flushed {
                count: 3,
                was_empty: true
            }
        );

        assert_eq!(c.prefetch(), 3);
        assert_eq!(c.pop(), Some(1));
        assert_eq!(c.pop(), Some(2));
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), None);
        c.release();
        assert!(p.is_empty());
    }

    #[test]
    fn counters_cross_u32_boundary_without_aliasing() {
        let (mut p, mut c) = spsc::<u32>(4);
        let base: Cursor = u32::MAX as usize - 1;
        p.cursor = base;
        p.cached_head = base;
        c.head = base;
        c.cached_tail = base;
        p.ring.head.0.store(base, Ordering::Relaxed);
        p.ring.tail.0.store(base, Ordering::Relaxed);

        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert_eq!(p.push(99), Err(99));
        assert_eq!(
            p.flush_and_check(),
            FlushResult::Flushed {
                count: 4,
                was_empty: true
            }
        );

        assert_eq!(c.prefetch(), 4);
        for i in 0..4 {
            assert_eq!(c.pop(), Some(i));
        }
        assert_eq!(c.pop(), None);
        c.release();
        assert!(p.is_empty());
    }

    #[test]
    fn drop_remaining() {
        use std::sync::atomic::AtomicUsize;
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        DROPS.store(0, Ordering::Relaxed);
        let (mut p, c) = spsc::<Counted>(4);
        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.flush();
        drop(p);
        drop(c);
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drop_remaining_handles_wrapped_counters() {
        use std::sync::atomic::AtomicUsize;
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        DROPS.store(0, Ordering::Relaxed);
        let (mut p, mut c) = spsc::<Counted>(4);
        let base: Cursor = usize::MAX - 1;
        p.cursor = base;
        p.cached_head = base;
        c.head = base;
        c.cached_tail = base;
        p.ring.head.0.store(base, Ordering::Relaxed);
        p.ring.tail.0.store(base, Ordering::Relaxed);

        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.push(Counted).unwrap();
        p.flush();
        drop(p);
        drop(c);
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drop_remaining_panicking_drop() {
        use std::sync::atomic::AtomicUsize;
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct PanicOnDrop(bool);
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
                if self.0 {
                    self.0 = false;
                    panic!("intentional panic in drop");
                }
            }
        }

        DROPS.store(0, Ordering::Relaxed);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (mut p, _c) = spsc::<PanicOnDrop>(4);
            p.push(PanicOnDrop(false)).unwrap();
            p.push(PanicOnDrop(true)).unwrap(); // this one panics
            p.push(PanicOnDrop(false)).unwrap();
            p.flush();
            // Ring::drop runs, calls drop_remaining. Item 1 panics.
            // Items 0 and 1 are dropped. Item 2 is leaked (not double-dropped).
        }));
        assert!(result.is_err());
        // Item 0 dropped normally, item 1's Drop panicked (but incremented
        // before panicking), item 2 leaked. Exactly 2 drops.
        assert_eq!(DROPS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cross_thread() {
        let (mut p, mut c) = spsc::<u64>(1024);
        let n = miri_sized(100_000);
        let sender = std::thread::spawn(move || {
            for i in 0..n {
                while p.push(i).is_err() {
                    p.flush();
                    std::thread::yield_now();
                }
                p.flush();
            }
        });
        let mut received = 0u64;
        while received < n {
            if c.prefetch() > 0 {
                while let Some(v) = c.pop() {
                    assert_eq!(v, received);
                    received += 1;
                }
                c.release();
            } else {
                std::thread::yield_now();
            }
        }
        sender.join().unwrap();
    }

    #[test]
    fn flush_and_check_was_empty_after_consumer_drains() {
        let (mut p, mut c) = spsc::<u32>(4);
        for i in 0..5 {
            p.push_and_flush(i).unwrap();
            let val = c.prefetch_and_pop().unwrap();
            assert_eq!(val, i);
        }
        p.push(99).unwrap();
        let r = p.flush_and_check();
        assert_eq!(
            r,
            FlushResult::Flushed {
                count: 1,
                was_empty: true,
            }
        );
    }
}
