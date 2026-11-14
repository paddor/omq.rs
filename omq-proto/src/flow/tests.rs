use super::*;

#[test]
fn single_peer_gets_full_cap() {
    assert_eq!(fair_share(0, 1, 256), 256);
    assert_eq!(fair_share(1000, 0, 256), 256);
    assert_eq!(fair_share(5, 1, 64), 64);
}

#[test]
fn multi_peer_splits_with_floor_and_cap() {
    assert_eq!(fair_share(100, 4, 256), 25);
    assert_eq!(fair_share(3, 8, 256), 1); // floor of 1
    assert_eq!(fair_share(10_000, 2, 256), 256); // capped
}

#[test]
fn drain_budget_msg_cap() {
    let mut b = DrainBudget::new(3, usize::MAX);
    assert!(!b.exhausted());

    assert!(b.account(10));
    assert!(b.account(10));
    assert!(!b.account(10)); // 3rd call exhausts

    assert!(b.exhausted());
    assert_eq!(b.msgs(), 3);
    assert_eq!(b.bytes(), 30);
}

#[test]
fn drain_budget_byte_cap() {
    let mut b = DrainBudget::new(usize::MAX, 100);
    assert!(b.account(40));
    assert!(b.account(40));
    assert!(!b.account(40)); // 120 >= 100
    assert!(b.exhausted());
}

#[test]
fn drain_budget_reset() {
    let mut b = DrainBudget::new(2, 100);
    assert!(b.account(50));
    assert!(!b.account(50));

    b.reset();
    assert!(!b.exhausted());
    assert_eq!(b.msgs(), 0);
    assert_eq!(b.bytes(), 0);
    assert!(b.account(50));
}

#[test]
fn drain_budget_both_caps() {
    let mut b = DrainBudget::new(10, 50);
    assert!(b.account(30));
    assert!(!b.account(30)); // bytes hit first: 60 >= 50
    assert!(b.exhausted());
    assert_eq!(b.msgs(), 2);
}
