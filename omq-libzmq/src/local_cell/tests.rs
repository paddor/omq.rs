use std::sync::Arc;

use super::LocalCell;

#[test]
fn permits_repeated_access_from_owner_thread() {
    let cell = LocalCell::new(1usize);

    // SAFETY: test accesses the cell from one thread.
    *unsafe { cell.get() } += 1;
    // SAFETY: same owner thread as above.
    assert_eq!(*unsafe { cell.get() }, 2);
}

#[test]
#[cfg(debug_assertions)]
fn rejects_access_from_second_thread() {
    let cell = Arc::new(LocalCell::new(1usize));

    // SAFETY: this binds the owner to the current test thread.
    assert_eq!(*unsafe { cell.get() }, 1);

    let other = Arc::clone(&cell);
    let result = std::thread::spawn(move || {
        // SAFETY: this intentionally violates the owner-thread contract
        // to verify the guard panics before returning a mutable reference.
        let _ = unsafe { other.get() };
    })
    .join();

    assert!(result.is_err());
}

#[test]
#[cfg(debug_assertions)]
fn permits_serialized_thread_migration_when_enabled() {
    let cell = Arc::new(LocalCell::new(1usize));
    cell.allow_thread_migration();
    *unsafe { cell.get() } += 1;

    let other = Arc::clone(&cell);
    std::thread::spawn(move || {
        *unsafe { other.get() } += 1;
    })
    .join()
    .unwrap();

    assert_eq!(*unsafe { cell.get() }, 3);
}
