//! Async wrapper over the core SPSC ring.
//!
//! `AsyncProducer::flush()` wakes the consumer when items become visible.
//! `AsyncConsumer::release()` wakes the producer when slots become available.
//! `AsyncConsumer` implements `futures_core::Stream`.
//! No runtime dependency; works with any executor.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures_core::Stream;

use crate::{Cursor, Padded, Ring};

struct AsyncRing<T> {
    ring: Ring<T>,
    consumer_waker: Padded<AtomicWaker>,
    producer_waker: Padded<AtomicWaker>,
}

// SAFETY: AsyncRing<T> is Send because the inner Ring<T> is Send and
// AtomicWaker is Send+Sync.
unsafe impl<T: Send> Send for AsyncRing<T> {}
// SAFETY: AsyncRing<T> is Sync for the same reasons as Ring<T> (atomics +
// SPSC protocol for slot access) plus AtomicWaker which is Sync.
unsafe impl<T: Send> Sync for AsyncRing<T> {}

impl<T> Drop for AsyncRing<T> {
    fn drop(&mut self) {
        // Drain leftover items. `drop_remaining` advances `head` to `tail`,
        // so the subsequent automatic `Ring::drop` is a no-op and the `buf`
        // allocation is freed normally (no double-drop, no leak).
        self.ring.drop_remaining();
    }
}

/// Async sending half. Wakes the consumer on flush when the ring was empty.
pub struct AsyncProducer<T> {
    ring: Arc<AsyncRing<T>>,
    cursor: Cursor,
    cached_head: Cursor,
}

// SAFETY: AsyncProducer<T> is Send because it is single-owner (not Sync) and
// the underlying AsyncRing is Send+Sync.
unsafe impl<T: Send> Send for AsyncProducer<T> {}

/// Async receiving half. Implements [`Stream`].
pub struct AsyncConsumer<T> {
    ring: Arc<AsyncRing<T>>,
    head: Cursor,
    cached_tail: Cursor,
}

// SAFETY: AsyncConsumer<T> is Send because it is single-owner (not Sync) and
// the underlying AsyncRing is Send+Sync.
unsafe impl<T: Send> Send for AsyncConsumer<T> {}

/// Create an async bounded SPSC ring with the given capacity (rounded up to
/// next power of two).
pub fn async_spsc<T>(capacity: usize) -> (AsyncProducer<T>, AsyncConsumer<T>) {
    let ring = Arc::new(AsyncRing {
        ring: Ring::new(capacity),
        consumer_waker: Padded(AtomicWaker::new()),
        producer_waker: Padded(AtomicWaker::new()),
    });
    (
        AsyncProducer {
            ring: ring.clone(),
            cursor: 0,
            cached_head: 0,
        },
        AsyncConsumer {
            ring,
            head: 0,
            cached_tail: 0,
        },
    )
}

impl<T> AsyncProducer<T> {
    /// Write a value to the ring. Zero atomics. Returns `Err(val)` if full.
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.ring
            .ring
            .push(&mut self.cursor, &mut self.cached_head, val)
    }

    /// Make all pushed items visible and wake the consumer unconditionally.
    ///
    /// The `was_empty` optimization (only wake when the ring transitions
    /// from empty to non-empty) has a race: the consumer can drain and
    /// re-register its waker between two producer flushes, making
    /// `was_empty` false even though the consumer is parked. Waking
    /// unconditionally is one extra `AtomicWaker::wake()` per flush
    /// (a no-op when no waker is registered) but is race-free.
    #[inline]
    pub fn flush(&mut self) {
        self.ring.ring.tail.0.store(self.cursor, Ordering::Release);
        self.ring.consumer_waker.0.wake();
    }

    /// Push + flush in one call.
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<(), T> {
        self.push(val)?;
        self.flush();
        Ok(())
    }

    /// Push a value, waiting asynchronously if the ring is full.
    ///
    /// Returns `Err(val)` only if the consumer has been dropped (the ring
    /// will never drain). On success the value is buffered but not yet
    /// visible to the consumer; call [`flush`](Self::flush) to publish.
    #[inline]
    pub fn push_async(&mut self, val: T) -> PushFuture<'_, T> {
        PushFuture {
            producer: self,
            val: Some(val),
        }
    }

    #[inline]
    pub fn is_full(&mut self) -> bool {
        self.ring.ring.is_full(self.cursor, &mut self.cached_head)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.ring.producer_len(self.cursor)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.ring.producer_is_empty(self.cursor)
    }

    /// The consumer half has been dropped.
    #[inline]
    pub fn is_consumer_dropped(&self) -> bool {
        self.ring.ring.consumer_dropped.load(Ordering::Acquire)
    }
}

/// Future returned by [`AsyncProducer::push_async`].
pub struct PushFuture<'a, T> {
    producer: &'a mut AsyncProducer<T>,
    val: Option<T>,
}

impl<T> std::fmt::Debug for PushFuture<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushFuture")
            .field("has_value", &self.val.is_some())
            .finish_non_exhaustive()
    }
}

// SAFETY: PushFuture has no self-referential structure. The mutable
// reference and the Option<T> are independent fields.
impl<T> Unpin for PushFuture<'_, T> {}

impl<T> Future for PushFuture<'_, T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let val = this.val.take().expect("PushFuture polled after completion");

        match this.producer.push(val) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(returned) => {
                if this
                    .producer
                    .ring
                    .ring
                    .consumer_dropped
                    .load(Ordering::Acquire)
                {
                    return Poll::Ready(Err(returned));
                }
                this.producer.ring.producer_waker.0.register(cx.waker());
                match this.producer.push(returned) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(returned) => {
                        if this
                            .producer
                            .ring
                            .ring
                            .consumer_dropped
                            .load(Ordering::Acquire)
                        {
                            return Poll::Ready(Err(returned));
                        }
                        this.val = Some(returned);
                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl<T> AsyncConsumer<T> {
    /// Pop one item from the prefetched window. Zero atomics.
    /// Call [`release`](Self::release) after draining a batch.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.ring.ring.pop(&mut self.head, self.cached_tail)
    }

    /// Publish consumed position so the producer can reuse slots,
    /// and wake the producer if it is waiting for space.
    #[inline]
    pub fn release(&mut self) {
        self.ring.ring.release(self.head);
        self.ring.producer_waker.0.wake();
    }

    /// Load all items flushed since the last prefetch. One Acquire load.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        self.ring.ring.prefetch(&mut self.cached_tail)
    }

    /// Prefetch + pop + release in one call.
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
    pub fn is_empty(&self) -> bool {
        self.ring
            .ring
            .consumer_is_empty(self.head, self.cached_tail)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.ring.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.ring.consumer_len(self.head)
    }
}

impl<T> Stream for AsyncConsumer<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.head == this.cached_tail {
            this.prefetch();
        }
        if let Some(val) = this.pop() {
            return Poll::Ready(Some(val));
        }

        // Release BEFORE registering the waker. Reversing this order
        // deadlocks: the producer could fill the freed slots and call
        // wake() between register and release, and we'd park with data
        // available and no pending wake.
        this.release();

        this.ring.consumer_waker.0.register(cx.waker());

        // Re-check after registering to avoid lost wakes.
        if this.head == this.cached_tail {
            this.prefetch();
        }
        if let Some(val) = this.pop() {
            Poll::Ready(Some(val))
        } else if this.ring.ring.producer_dropped.load(Ordering::Acquire) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for AsyncConsumer<T> {
    fn drop(&mut self) {
        self.release();
        self.ring
            .ring
            .consumer_dropped
            .store(true, Ordering::Release);
        self.ring.producer_waker.0.wake();
    }
}

impl<T> Drop for AsyncProducer<T> {
    fn drop(&mut self) {
        self.flush();
        self.ring
            .ring
            .producer_dropped
            .store(true, Ordering::Release);
        self.ring.consumer_waker.0.wake();
    }
}

impl<T> std::fmt::Debug for AsyncProducer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncProducer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for AsyncConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConsumer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::StreamExt;

    use super::*;

    // CI runs Miri with many seeds. Keep cross-thread coverage without turning
    // per-seed validation into a throughput test.
    fn miri_sized(n: u64) -> u64 {
        if cfg!(miri) { 2_048 } else { n }
    }

    #[test]
    fn async_push_pop() {
        let (mut p, mut c) = async_spsc::<u32>(4);
        p.push(1).unwrap();
        p.push(2).unwrap();
        assert!(c.prefetch_and_pop().is_none());
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(1));
        assert_eq!(c.prefetch_and_pop(), Some(2));
    }

    #[test]
    fn async_release_after_prefetch_releases_only_popped_items() {
        let (mut p, mut c) = async_spsc::<u32>(2);

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
    fn stream_impl() {
        futures_lite::future::block_on(async {
            let (mut p, mut c) = async_spsc::<u32>(8);
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.push(30).unwrap();
            p.flush();

            assert_eq!(c.next().await, Some(10));
            assert_eq!(c.next().await, Some(20));
            assert_eq!(c.next().await, Some(30));
        });
    }

    #[test]
    fn stream_wakes_on_flush() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (mut p, mut c) = async_spsc::<u32>(8);
        let done = Arc::new(AtomicBool::new(false));
        let done2 = done.clone();

        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                let val = c.next().await;
                done2.store(true, Ordering::Release);
                val
            })
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(!done.load(Ordering::Acquire));

        p.push(42).unwrap();
        p.flush();

        let val = handle.join().unwrap();
        assert_eq!(val, Some(42));
    }

    #[test]
    fn cross_thread_stream() {
        let (mut p, c) = async_spsc::<u64>(1024);
        let n = miri_sized(50_000);

        let receiver = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                futures_lite::pin!(c);
                let mut received = 0u64;
                while let Some(v) = c.next().await {
                    assert_eq!(v, received);
                    received += 1;
                    if received == n {
                        break;
                    }
                }
                received
            })
        });

        for i in 0..n {
            while p.push(i).is_err() {
                p.flush();
                std::thread::yield_now();
            }
            if i % 64 == 63 {
                p.flush();
            }
        }
        p.flush();

        let count = receiver.join().unwrap();
        assert_eq!(count, n);
    }

    #[test]
    fn alternating_push_pop_wakes() {
        use std::sync::mpsc;

        let (mut p, c) = async_spsc::<u32>(8);
        let (tx, rx) = mpsc::sync_channel::<u32>(0);

        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                futures_lite::pin!(c);
                for _ in 0..5 {
                    let val = c.next().await.unwrap();
                    tx.send(val).unwrap();
                }
            });
        });

        for i in 0..5 {
            p.push_and_flush(i).unwrap();
            let val = rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
            assert_eq!(val, i);
        }

        handle.join().unwrap();
    }

    #[test]
    fn push_async_blocks_when_full() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (mut p, mut c) = async_spsc::<u32>(4);
        // Fill the ring (capacity rounds to 4).
        for i in 0..4 {
            p.push(i).unwrap();
        }
        p.flush();
        assert!(p.push(99).is_err());

        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = pushed.clone();

        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                p.push_async(99).await.unwrap();
                p.flush();
                pushed2.store(true, Ordering::Release);
            });
        });

        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!pushed.load(Ordering::Acquire), "should be blocked");

        // Drain one slot.
        c.prefetch();
        c.pop();
        c.release();

        handle.join().unwrap();
        assert!(pushed.load(Ordering::Acquire));

        // Verify the value arrived.
        c.prefetch();
        // Skip remaining 1,2,3 that were in the ring.
        c.pop(); // 1
        c.pop(); // 2
        c.pop(); // 3
        let val = c.pop(); // 99
        assert_eq!(val, Some(99));
    }

    #[test]
    fn push_async_cross_thread() {
        let (mut p, c) = async_spsc::<u64>(64);
        let n = miri_sized(100_000);

        let receiver = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                futures_lite::pin!(c);
                let mut received = 0u64;
                while let Some(v) = c.next().await {
                    assert_eq!(v, received);
                    received += 1;
                    if received == n {
                        break;
                    }
                }
                received
            })
        });

        futures_lite::future::block_on(async {
            for i in 0..n {
                p.push_async(i).await.unwrap();
                if i % 64 == 63 {
                    p.flush();
                }
            }
            p.flush();
        });

        let count = receiver.join().unwrap();
        assert_eq!(count, n);
    }

    #[test]
    fn push_async_returns_err_on_consumer_drop() {
        let (mut p, c) = async_spsc::<u32>(4);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        p.flush();
        drop(c);

        let result = futures_lite::future::block_on(async { p.push_async(99).await });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), 99);
    }

    #[test]
    fn push_async_detects_drop_during_waker_registration() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::{Arc, Mutex};
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        struct DropConsumerOnClone {
            consumer: Mutex<Option<AsyncConsumer<u32>>>,
        }

        fn clone(data: *const ()) -> RawWaker {
            let hook = unsafe { Arc::from_raw(data.cast::<DropConsumerOnClone>()) };
            hook.consumer.lock().unwrap().take();
            let cloned = hook.clone();
            std::mem::forget(hook);
            RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
        }

        fn drop_waker(data: *const ()) {
            unsafe { std::mem::drop(Arc::from_raw(data.cast::<DropConsumerOnClone>())) };
        }

        fn noop(_: *const ()) {}

        // `wake` consumes the waker, so it must drop the Arc; only
        // `wake_by_ref` is a no-op. Using noop for both leaks the Arc,
        // which Miri's leak checker reports.
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, drop_waker, noop, drop_waker);

        let (mut p, c) = async_spsc::<u32>(1);
        p.push(1).unwrap();
        p.flush();
        let hook = Arc::new(DropConsumerOnClone {
            consumer: Mutex::new(Some(c)),
        });
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(hook.clone()).cast(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = p.push_async(2);

        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(Err(2)));
    }
}
