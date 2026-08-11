#![cfg(all(loom, target_pointer_width = "64"))]

use loom::thread;

#[test]
fn push_flush_prefetch_pop() {
    loom::model(|| {
        let (mut producer, mut consumer) = yring::spsc::<u32>(2);

        let handle = thread::spawn(move || {
            producer.push(1).unwrap();
            producer.push(2).unwrap();
            producer.flush();
        });

        loop {
            if consumer.prefetch() > 0 {
                let first = consumer.pop().unwrap();
                let second = consumer.pop().unwrap();
                assert_eq!(first, 1);
                assert_eq!(second, 2);
                consumer.release();
                break;
            }
            thread::yield_now();
        }

        handle.join().unwrap();
    });
}

#[test]
fn push_full_release_retry() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            c.prefetch();
            let v = c.pop().unwrap();
            c.release();
            v
        });

        while p.push(20).is_err() {
            p.flush();
            thread::yield_now();
        }
        p.flush();

        let v = h.join().unwrap();
        assert_eq!(v, 10);
    });
}

#[test]
fn prefetch_and_pop_with_full_reports_full_transition() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        let first = loop {
            if let Some(item) = c.prefetch_and_pop_with_full() {
                break item;
            }
            thread::yield_now();
        };
        assert_eq!(first, (10, true));
        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
        assert_eq!(c.prefetch_and_pop_with_full(), None);

        h.join().unwrap();
    });
}

#[test]
fn prefetch_and_pop_with_full_observes_refilled_full_ring() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);
        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        let released_one = Arc::new(AtomicBool::new(false));
        let attempted_second_push = Arc::new(AtomicBool::new(false));
        let released_one_for_producer = released_one.clone();
        let attempted_second_push_for_producer = attempted_second_push.clone();

        let h = thread::spawn(move || {
            while !released_one_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }

            while p.push(30).is_err() {
                thread::yield_now();
            }
            p.flush();

            let mut value = match p.push(40) {
                Ok(()) => panic!("released second slot before second pop"),
                Err(value) => value,
            };
            attempted_second_push_for_producer.store(true, Ordering::Release);

            while let Err(returned) = p.push(value) {
                value = returned;
                thread::yield_now();
            }
            p.flush();
        });

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));
        released_one.store(true, Ordering::Release);

        while !attempted_second_push.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, true)));

        let mut tail = Vec::new();
        while tail.len() < 2 {
            if let Some((value, _was_full)) = c.prefetch_and_pop_with_full() {
                tail.push(value);
            } else {
                thread::yield_now();
            }
        }
        assert_eq!(tail, [30, 40]);

        h.join().unwrap();
    });
}

#[test]
fn prefetch_and_pop_with_full_handles_cursor_wrap() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        let first = loop {
            if let Some(item) = c.prefetch_and_pop_with_full() {
                break item;
            }
            thread::yield_now();
        };
        assert_eq!(first, (10, true));
        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
        assert_eq!(c.prefetch_and_pop_with_full(), None);

        h.join().unwrap();
    });
}

#[test]
fn release_after_prefetch_releases_only_popped_items() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);
        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        let released_one = Arc::new(AtomicBool::new(false));
        let attempted_second_push = Arc::new(AtomicBool::new(false));
        let released_one_for_producer = released_one.clone();
        let attempted_second_push_for_producer = attempted_second_push.clone();

        let h = thread::spawn(move || {
            while !released_one_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }

            while p.push(30).is_err() {
                thread::yield_now();
            }
            let mut value = match p.push(40) {
                Ok(()) => panic!("released prefetched-but-unpopped slot"),
                Err(value) => value,
            };
            attempted_second_push_for_producer.store(true, Ordering::Release);

            while let Err(returned) = p.push(value) {
                value = returned;
                thread::yield_now();
            }
            p.flush();
        });

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop(), Some(10));
        c.release();
        released_one.store(true, Ordering::Release);

        while !attempted_second_push.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert_eq!(c.pop(), Some(20));
        c.release();

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(30));
                assert_eq!(c.pop(), Some(40));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn producer_drop_flushes() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(42).unwrap();
            drop(p);
        });

        h.join().unwrap();

        assert_eq!(c.prefetch(), 1);
        assert_eq!(c.pop(), Some(42));
    });
}

#[test]
fn wraparound_correctness() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            for round in 0..2u32 {
                p.push(round * 2).unwrap();
                p.push(round * 2 + 1).unwrap();
                p.flush();
                while p.is_full() {
                    thread::yield_now();
                }
            }
        });

        let mut received = Vec::new();
        while received.len() < 4 {
            if c.prefetch() > 0 {
                while let Some(v) = c.pop() {
                    received.push(v);
                }
                c.release();
            } else {
                thread::yield_now();
            }
        }

        h.join().unwrap();
        assert_eq!(received, [0, 1, 2, 3]);
    });
}

#[test]
fn popped_value_survives_slot_reuse() {
    use loom::sync::Arc;

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<Arc<usize>>(1);

        p.push(Arc::new(10)).unwrap();
        p.flush();

        let first = loop {
            if c.prefetch() > 0 {
                let value = c.pop().unwrap();
                c.release();
                break value;
            }
            thread::yield_now();
        };

        let h = thread::spawn(move || {
            while p.push(Arc::new(20)).is_err() {
                thread::yield_now();
            }
            p.flush();
        });

        h.join().unwrap();
        assert_eq!(*first, 10);

        assert_eq!(c.prefetch(), 1);
        let second = c.pop().unwrap();
        c.release();
        assert_eq!(*first, 10);
        assert_eq!(*second, 20);
    });
}

#[test]
fn pointer_width_cursor_wrap_boundary() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(10));
                assert_eq!(c.pop(), Some(20));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn is_disconnected_after_producer_drop() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            drop(p);
        });

        h.join().unwrap();

        c.prefetch();
        c.pop();
        c.release();
        assert!(c.is_disconnected());
    });
}

/// Verify `push_async` doesn't lose wakeups.
///
/// The critical race: consumer releases between the producer's
/// "ring is full" check and waker registration. The retry after
/// registration (step 4 in `PushFuture::poll`) must catch this.
#[test]
fn push_async_no_lost_wakeup() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    fn noop_waker() -> Waker {
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = pushed.clone();

        // Producer polls push_async on spawned thread.
        let h = thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let mut fut = p.push_async(20);
            let mut pinned = Pin::new(&mut fut);

            loop {
                match pinned.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(())) => {
                        pushed2.store(true, Ordering::Relaxed);
                        break;
                    }
                    Poll::Ready(Err(_)) => panic!("push_async returned Err"),
                    Poll::Pending => thread::yield_now(),
                }
            }
        });

        // Consumer drains on main thread (stays alive until join).
        c.prefetch();
        let v = c.pop().unwrap();
        c.release();
        assert_eq!(v, 10);

        h.join().unwrap();
        assert!(pushed.load(Ordering::Relaxed));
    });
}

/// Verify `push_async` detects consumer drop.
#[test]
fn push_async_consumer_dropped() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    loom::model(|| {
        let (mut p, c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            drop(c);
        });

        h.join().unwrap();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = p.push_async(20);
        let pinned = Pin::new(&mut fut);

        match pinned.poll(&mut cx) {
            Poll::Ready(Err(20)) => {}
            other => panic!("expected Err(20), got {other:?}"),
        }
    });
}

/// Verify `push_async` detects consumer drop during waker registration.
#[test]
fn push_async_consumer_drop_during_registration() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use std::sync::{Arc, Mutex};

    struct DropConsumerOnClone {
        consumer: Mutex<Option<yring::AsyncConsumer<u32>>>,
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

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, drop_waker);

    loom::model(|| {
        let (mut producer, consumer) = yring::async_spsc::<u32>(1);
        producer.push(1).unwrap();
        producer.flush();

        let hook = Arc::new(DropConsumerOnClone {
            consumer: Mutex::new(Some(consumer)),
        });
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(hook.clone()).cast(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = producer.push_async(2);

        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(Err(2)));
    });
}

#[test]
fn upper_layer_lwm_reactivation_rechecks_stale_flag() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        const CAPACITY: usize = 4;
        const LOW_WATER_MARK: usize = CAPACITY / 2;

        let len = Arc::new(AtomicUsize::new(CAPACITY));
        let above_lwm = Arc::new(AtomicBool::new(false));

        let sender_len = len.clone();
        let sender_above_lwm = above_lwm.clone();
        let sender = thread::spawn(move || {
            if sender_len.load(Ordering::Acquire) >= CAPACITY {
                thread::yield_now();
                sender_above_lwm.store(true, Ordering::Release);
            }
        });

        let consumer_len = len.clone();
        let consumer_above_lwm = above_lwm.clone();
        let consumer = thread::spawn(move || {
            consumer_len.store(LOW_WATER_MARK, Ordering::Release);
            let _ = consumer_above_lwm.swap(false, Ordering::AcqRel);
        });

        sender.join().unwrap();
        consumer.join().unwrap();

        let len = len.load(Ordering::Acquire);
        let above_lwm = above_lwm.load(Ordering::Acquire);
        let can_reactivate = !above_lwm || len <= LOW_WATER_MARK;
        assert!(
            can_reactivate,
            "stale high-water flag must not hide a pipe below low-water mark"
        );
    });
}

#[test]
fn upper_layer_ready_survives_cancelled_waiter() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    struct Signal {
        pending: AtomicBool,
        permit: AtomicBool,
    }

    impl Signal {
        fn mark(&self) {
            if !self.pending.swap(true, Ordering::Release) {
                self.permit.store(true, Ordering::Release);
            }
        }

        fn cancel_waiter_after_poll(&self) {
            let _ = self.permit.swap(false, Ordering::AcqRel);
        }

        fn ready_after_enable(&self) -> bool {
            self.pending.load(Ordering::Acquire) || self.permit.swap(false, Ordering::AcqRel)
        }
    }

    loom::model(|| {
        let signal = Arc::new(Signal {
            pending: AtomicBool::new(false),
            permit: AtomicBool::new(false),
        });

        let producer_signal = signal.clone();
        let producer = thread::spawn(move || producer_signal.mark());

        let cancelled_signal = signal.clone();
        let cancelled = thread::spawn(move || cancelled_signal.cancel_waiter_after_poll());

        producer.join().unwrap();
        cancelled.join().unwrap();

        assert!(
            signal.ready_after_enable(),
            "pending flag must preserve readiness after a permit is consumed"
        );
    });
}

#[test]
fn upper_layer_blocking_wait_uses_stateful_generation() {
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::sync::{Arc, Condvar, Mutex};

    struct BlockingSpace {
        generation: Mutex<u64>,
        changed: Condvar,
    }

    impl BlockingSpace {
        fn notify(&self) {
            let mut generation = self.generation.lock().unwrap();
            *generation = generation.wrapping_add(1);
            drop(generation);
            self.changed.notify_all();
        }

        fn wait_until_not_full(&self, full: &AtomicBool) {
            while full.load(Ordering::Acquire) {
                let mut generation = self.generation.lock().unwrap();
                if !full.load(Ordering::Acquire) {
                    return;
                }
                let seen = *generation;
                while seen == *generation && full.load(Ordering::Acquire) {
                    generation = self.changed.wait(generation).unwrap();
                }
            }
        }
    }

    loom::model(|| {
        let space = Arc::new(BlockingSpace {
            generation: Mutex::new(0),
            changed: Condvar::new(),
        });
        let full = Arc::new(AtomicBool::new(true));
        let finished = Arc::new(AtomicBool::new(false));

        let waiter_space = space.clone();
        let waiter_full = full.clone();
        let waiter_finished = finished.clone();
        let waiter = thread::spawn(move || {
            waiter_space.wait_until_not_full(&waiter_full);
            waiter_finished.store(true, Ordering::Release);
        });

        let notifier_space = space.clone();
        let notifier_full = full.clone();
        let notifier = thread::spawn(move || {
            thread::yield_now();
            notifier_full.store(false, Ordering::Release);
            notifier_space.notify();
        });

        waiter.join().unwrap();
        notifier.join().unwrap();

        assert!(finished.load(Ordering::Acquire));
    });
}
