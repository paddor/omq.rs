use super::*;

#[test]
fn shared_batch_records_do_not_depend_on_the_first_batch_count() {
    const HWM: usize = 8;
    const MAXIMUM: usize = 1024 * 1024;

    let pool = SendPool::new(HWM, Some(MAXIMUM));
    let messages: Vec<_> = (0..HWM).map(|_| Message::from_slice(&[1; 64])).collect();
    let references: Vec<_> = messages.iter().collect();

    // Eight peers can each retain one HWM-sized batch in this shared class.
    let retained: Vec<_> = (0..HWM)
        .map(|_| {
            pool.prepare_batch(&references, BatchKind::Copied)
                .unwrap()
                .unwrap()
        })
        .collect();

    assert!(pool.memory_used() <= MAXIMUM);
    assert!(
        pool.prepare_batch(&references, BatchKind::Copied)
            .unwrap()
            .is_none()
    );

    drop(retained);
    assert!(
        pool.prepare_batch(&references, BatchKind::Copied)
            .unwrap()
            .is_some()
    );
}

#[test]
fn a_full_record_class_doubles_into_the_free_budget() {
    const SIZE: usize = 4096;

    // A quarter of this cap holds two batch records; the rest stays free.
    let maximum = 4 * storage_length(SIZE, 2).unwrap();
    let pool = SendPool::new(8, Some(maximum));
    let messages: Vec<_> = (0..2).map(|_| Message::from_slice(&[1; 1024])).collect();
    let references: Vec<_> = messages.iter().collect();

    let retained: Vec<_> = (0..4)
        .map(|_| {
            pool.prepare_batch(&references, BatchKind::Copied)
                .unwrap()
                .unwrap()
        })
        .collect();

    assert_eq!(retained.len(), 4);
    assert_eq!(
        pool.memory_used(),
        storage_length(SIZE, 2).unwrap() + storage_length(SIZE, 4).unwrap()
    );
}

#[test]
fn growth_retires_classes_left_unused_for_a_check_interval() {
    const HWM: usize = 8;
    const LARGE: usize = 16 * 1024;
    const SMALL: usize = 4096;

    // The large class takes its whole window and leaves the small class two slots.
    let maximum = storage_length(LARGE, HWM).unwrap() + storage_length(SMALL, 2).unwrap();
    let pool = SendPool::new(HWM, Some(maximum));

    drop(
        pool.prepare(&Message::from_slice(&[1; LARGE]))
            .unwrap()
            .unwrap(),
    );

    let small = Message::from_slice(&[2; SMALL]);
    let retained: Vec<_> = (0..2)
        .map(|_| pool.prepare(&small).unwrap().unwrap())
        .collect();

    // The large class was used before this first growth check, so it stays.
    assert!(pool.prepare(&small).unwrap().is_none());

    // Unused for a whole interval, it returns its budget and the full
    // small class doubles instead of sending bodies through the carrier.
    std::thread::sleep(std::time::Duration::from_millis(
        GROWTH_CHECK_INTERVAL_MS + 10,
    ));
    let grown = pool.prepare(&small).unwrap().unwrap();
    assert!(find_slot(crate::body(&grown).unwrap()).is_some());
    assert_eq!(
        pool.memory_used(),
        storage_length(SMALL, 2).unwrap() + storage_length(SMALL, 4).unwrap()
    );

    for message in &retained {
        assert!(crate::body(message).unwrap().iter().all(|&byte| byte == 2));
    }
}

#[test]
fn cached_class_reads_do_not_wait_for_an_allocation() {
    let pool = SendPool::new(8, Some(1024 * 1024));
    let first = pool.class(16 * 1024, Sizing::Window).unwrap().unwrap();

    let allocation = pool.inner.allocation.lock().unwrap();
    let reader = pool.clone();
    let (done, result) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        done.send(reader.class(16 * 1024, Sizing::Window).unwrap().unwrap())
            .unwrap();
    });

    let reused = result
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();

    drop(allocation);
    thread.join().unwrap();
    assert!(Arc::ptr_eq(&first, &reused));
}

#[test]
fn concurrent_class_misses_share_one_reserved_mapping() {
    const SIZE: usize = 16 * 1024;
    const PEERS: usize = 8;
    let maximum = storage_length(SIZE, PEERS).unwrap();
    let pool = SendPool::new(PEERS, Some(maximum));
    let barrier = Arc::new(std::sync::Barrier::new(PEERS));

    let classes = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..PEERS)
            .map(|_| {
                let pool = &pool;
                let barrier = &barrier;

                scope.spawn(move || {
                    barrier.wait();
                    pool.class(SIZE, Sizing::Window).unwrap().unwrap()
                })
            })
            .collect();

        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .collect::<Vec<_>>()
    });

    assert!(classes.iter().all(|class| Arc::ptr_eq(class, &classes[0])));
    let capacity = classes[0].pool.capacity();
    assert_eq!(pool.memory_used(), storage_length(SIZE, capacity).unwrap());
}
