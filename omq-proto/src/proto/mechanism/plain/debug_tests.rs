use super::PlainClient;

#[test]
fn plain_client_debug_redacts_password() {
    let client = PlainClient::new("alice".into(), "password-sentinel".into());
    let debug = format!("{client:?}");
    assert!(debug.contains("password: \"<redacted>\""));
    assert!(!debug.contains("password-sentinel"));
}
