use super::*;
const BUFFER_CAPACITY: usize = 2048;

#[test]
fn variable_slab_slices_keep_the_buffer_until_every_view_is_released() {
    let pool = PayloadPool::new([(2048, 1)]).unwrap();
    let sizes = [64, 256, 1024, 128, 512];
    let mut buffer = pool.try_buffer(2048).unwrap();
    let mut offset = 0;
    for (index, size) in sizes.into_iter().enumerate() {
        buffer.writable()[offset..offset + size].fill(index as u8);
        offset += size;
    }
    buffer.set_len(offset).unwrap();
    let mut bytes: bytes::Bytes = buffer.into_payload().into();
    let messages: Vec<_> = sizes
        .into_iter()
        .map(|size| Message::from(bytes.split_to(size)))
        .collect();
    drop(bytes);
    for (index, message) in messages.iter().enumerate() {
        assert_eq!(
            message.part_slice(0).unwrap(),
            vec![index as u8; sizes[index]]
        );
    }
    let held = messages[2].part_bytes(0).unwrap().slice(1..17);
    std::thread::spawn(move || drop(messages)).join().unwrap();
    assert_eq!(pool.available(), 0);
    assert_eq!(held.as_ref(), &[2; 16]);
    drop(held);
    assert_eq!(pool.available(), 1);
}

#[derive(Debug)]
struct PendingStorage {
    bytes: Box<[u8]>,
    ready: Arc<AtomicBool>,
    checks: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for PendingStorage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsMut<[u8]> for PendingStorage {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl PayloadStorage for PendingStorage {
    fn reusable(&self) -> bool {
        self.checks.fetch_add(1, Ordering::Relaxed);
        self.ready.load(Ordering::Acquire)
    }
}

#[test]
fn checkout_reclaims_only_the_requested_slots() {
    let ready = Arc::new(AtomicBool::new(false));
    let checks = Arc::new(AtomicUsize::new(0));
    let pool = PayloadPool::with_storage(
        [(128, TRANSFER)],
        &DrainBudget::new(TRANSFER, usize::MAX),
        |size, capacity| {
            Ok((0..capacity)
                .map(|_| {
                    Box::new(PendingStorage {
                        bytes: vec![0; size].into_boxed_slice(),
                        ready: ready.clone(),
                        checks: checks.clone(),
                    }) as Box<dyn PayloadStorage>
                })
                .collect())
        },
    )
    .unwrap();
    let buffers: Vec<_> = (0..TRANSFER)
        .map(|_| pool.try_buffer(128).unwrap())
        .collect();
    drop(buffers);
    ready.store(true, Ordering::Release);
    checks.store(0, Ordering::Relaxed);
    let held = pool.try_buffer(128).unwrap();
    assert_eq!(checks.load(Ordering::Relaxed), 1);
    let mut batch = Vec::with_capacity(4);
    assert_eq!(pool.try_buffers_into(128, 4, &mut batch), 4);
    assert_eq!(checks.load(Ordering::Relaxed), 5);
    assert_eq!(
        pool.classes[0].pending_count.load(Ordering::Relaxed),
        TRANSFER - 5
    );
    drop(held);
    drop(batch);
    assert_eq!(pool.available(), TRANSFER);
}

#[test]
fn checkout_finds_ready_storage_behind_unreleased_backend_slots() {
    let ready: Vec<_> = (0..TRANSFER)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect();
    let checks = Arc::new(AtomicUsize::new(0));
    let pool = PayloadPool::with_storage(
        [(128, TRANSFER)],
        &DrainBudget::new(TRANSFER, usize::MAX),
        |size, capacity| {
            Ok((0..capacity)
                .map(|index| {
                    Box::new(PendingStorage {
                        bytes: vec![index as u8; size].into_boxed_slice(),
                        ready: ready[index].clone(),
                        checks: checks.clone(),
                    }) as Box<dyn PayloadStorage>
                })
                .collect())
        },
    )
    .unwrap();
    let buffers: Vec<_> = (0..TRANSFER)
        .map(|_| pool.try_buffer(128).unwrap())
        .collect();
    drop(buffers);
    ready[TRANSFER - 1].store(true, Ordering::Release);
    checks.store(0, Ordering::Relaxed);
    let mut buffer = pool.try_buffer(128).unwrap();
    assert_eq!(checks.load(Ordering::Relaxed), TRANSFER);
    assert_eq!(buffer.writable(), &[(TRANSFER - 1) as u8; 128]);
    assert!(pool.try_buffer(128).is_none());
    for slot in &ready {
        slot.store(true, Ordering::Release);
    }
    drop(buffer);
    assert_eq!(pool.available(), TRANSFER);
}

#[test]
fn backend_reclamation_honors_count_and_metadata_byte_budgets() {
    for (budget, per_turn) in [
        (DrainBudget::new(2, usize::MAX), 2),
        (DrainBudget::new(4, std::mem::size_of::<Storage>()), 1),
    ] {
        let ready = Arc::new(AtomicBool::new(true));
        let checks = Arc::new(AtomicUsize::new(0));
        let pool = PayloadPool::with_storage([(128, 4)], &budget, |size, capacity| {
            Ok((0..capacity)
                .map(|_| {
                    Box::new(PendingStorage {
                        bytes: vec![0; size].into_boxed_slice(),
                        ready: ready.clone(),
                        checks: checks.clone(),
                    }) as Box<dyn PayloadStorage>
                })
                .collect())
        })
        .unwrap();
        let buffers: Vec<_> = (0..4).map(|_| pool.try_buffer(128).unwrap()).collect();
        ready.store(false, Ordering::Release);
        drop(buffers);
        checks.store(0, Ordering::Relaxed);
        pool.reclaim();
        assert_eq!(checks.load(Ordering::Relaxed), per_turn);
        ready.store(true, Ordering::Release);
        for turn in 1..=4 / per_turn {
            pool.reclaim();
            assert_eq!(checks.load(Ordering::Relaxed), per_turn * (turn + 1));
        }
        assert_eq!(pool.available(), 4);
    }
}

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
