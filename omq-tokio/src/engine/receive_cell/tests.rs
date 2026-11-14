use super::ReceiveCell;
use std::sync::Arc;

#[test]
fn only_multiple_handles_acquire_the_mutex() {
    let handle = Arc::new(());
    let cell = ReceiveCell::new(0, Arc::downgrade(&handle));
    let mut owned = cell.lock();
    assert!(owned.serialization.is_none());
    *owned += 1;

    let clone = handle.clone();
    assert!(cell.try_lock().is_none());
    drop(owned);

    let mut shared = cell.lock();
    assert!(shared.serialization.is_some());
    assert_eq!(*shared, 1);
    *shared += 1;
    drop(shared);

    drop(clone);
    let owned = cell.lock();
    assert!(owned.serialization.is_none());
    assert_eq!(*owned, 2);
}

#[test]
fn overlapping_calls_on_one_handle_transfer_exclusive_ownership() {
    let handle = Arc::new(());
    let cell = ReceiveCell::new(0, Arc::downgrade(&handle));

    std::thread::scope(|scope| {
        for _ in 0..2 {
            let cell = &cell;
            scope.spawn(move || {
                for _ in 0..10_000 {
                    let mut owned = cell.lock();
                    assert!(owned.serialization.is_none());
                    *owned += 1;
                }
            });
        }
    });

    assert_eq!(*cell.lock(), 20_000);
}

#[test]
fn unwind_returns_single_handle_ownership() {
    let handle = Arc::new(());
    let cell = ReceiveCell::new(0, Arc::downgrade(&handle));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        *cell.lock() = 7;
        let _owned = cell.lock();
        panic!("receive interrupted");
    }));
    assert!(result.is_err());
    assert_eq!(*cell.lock(), 7);
}

#[test]
fn user_wakers_run_after_the_receive_guard_releases() {
    use crate::engine::signal::BlockingSignal;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Reenter {
        cell: Arc<ReceiveCell<u32>>,
        ran: AtomicBool,
    }

    impl std::task::Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // Reentering receive must not find the guard still held.
            assert!(self.cell.try_lock().is_some());
            self.ran.store(true, Ordering::Release);
        }
    }

    let handles = Arc::new(());
    let cell = Arc::new(ReceiveCell::new(7, Arc::downgrade(&handles)));
    let waker = Arc::new(Reenter {
        cell: cell.clone(),
        ran: AtomicBool::new(false),
    });
    let signal = BlockingSignal::new();
    let _registration = signal.register_waker(std::task::Waker::from(waker.clone()));

    let guard = cell.lock();
    signal.wake();
    assert!(!waker.ran.load(Ordering::Acquire));
    drop(guard);
    assert!(waker.ran.load(Ordering::Acquire));
}
