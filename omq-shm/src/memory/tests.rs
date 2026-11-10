use super::*;

#[test]
fn simultaneous_reservations_share_one_limit() {
    let budget = Budget::new(Some(1024));
    std::thread::scope(|scope| {
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let budget = budget.clone();
                scope.spawn(move || Budget::reserve(&budget, 128))
            })
            .collect();
        let reservations: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            reservations.iter().filter(|value| value.is_some()).count(),
            8
        );
        assert_eq!(budget.used(), 1024);
        drop(reservations);
    });

    assert_eq!(budget.used(), 0);
}

#[test]
fn overflow_and_zero_limits_do_not_reserve_storage() {
    let budget = Budget::new(None);
    let reservation = Budget::reserve(&budget, usize::MAX).unwrap();
    assert!(Budget::reserve(&budget, 1).is_none());
    drop(reservation);
    assert_eq!(budget.used(), 0);
    assert!(Budget::reserve(&Budget::new(Some(0)), 1).is_none());
}
