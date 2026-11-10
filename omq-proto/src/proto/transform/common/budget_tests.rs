use super::*;

#[test]
fn decoded_budget_is_finite_without_a_message_limit() {
    let slot = size_of::<Payload>();
    assert_eq!(
        body_budget(2, None).unwrap(),
        Some(DEFAULT_DECODED_MESSAGE_LIMIT - 2 * slot)
    );
    assert_eq!(body_budget(1, Some(1000)).unwrap(), Some(1000 - slot));
    assert!(check_expansion(255, 1, 255, "test").is_ok());
    assert!(check_expansion(256, 1, 255, "test").is_err());
}
