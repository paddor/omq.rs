use super::*;
use std::io;

#[test]
fn windows_dns_error_is_name_resolution_error() {
    let error = io::Error::from_raw_os_error(11001);
    assert!(is_name_resolution_error(&error));
}

#[test]
fn windows_dns_error_message_is_name_resolution_error() {
    let error = io::Error::other("No such host is known. (os error 11001)");
    assert!(is_name_resolution_error(&error));
}

#[test]
fn cstring_lossy_removes_nul_and_fits_java_read_window() {
    let message = format!("a\0{}", "b".repeat(MAX_ERROR_MESSAGE_BYTES + 10));
    let cstring = cstring_lossy(&message);
    let bytes = cstring.to_bytes();

    assert_eq!(MAX_ERROR_MESSAGE_BYTES, bytes.len());
    assert!(!bytes.contains(&0));
}

#[test]
fn cstring_lossy_truncates_at_utf8_boundary() {
    let message = "\u{e9}".repeat(MAX_ERROR_MESSAGE_BYTES);
    let cstring = cstring_lossy(&message);
    let bytes = cstring.to_bytes();

    assert_eq!(MAX_ERROR_MESSAGE_BYTES - 1, bytes.len());
    assert!(std::str::from_utf8(bytes).is_ok());
}
