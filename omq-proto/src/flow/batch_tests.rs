use super::DrainBudget;

#[test]
fn logical_batches_exhaust_both_caps_and_saturate() {
    let mut budget = DrainBudget::new(8, 1024);
    assert!(budget.account_many(3, 512));
    assert!(!budget.account_many(5, 512));
    assert_eq!(budget.msgs(), 8);
    assert_eq!(budget.bytes(), 1024);

    assert!(!budget.account_many(usize::MAX, usize::MAX));
    assert_eq!(budget.msgs(), usize::MAX);
    assert_eq!(budget.bytes(), usize::MAX);
}
