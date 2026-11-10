use super::*;

#[test]
fn preserves_cpu_order_for_explicit_sibling_selection() {
    assert_eq!(parse_cpus("0, 2,4,6,8,10"), [0, 2, 4, 6, 8, 10]);
}

#[test]
#[should_panic(expected = "duplicate CPU")]
fn rejects_duplicate_cpus() {
    parse_cpus("0,2,0");
}
