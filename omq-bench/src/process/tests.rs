use super::*;

#[test]
fn stat_cpu_ticks_handles_spaces_and_parens_in_comm() {
    let rest = "S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16";
    assert_eq!(stat_cpu_ticks(&format!("42 (peer) {rest}")), 23.0);
    assert_eq!(stat_cpu_ticks(&format!("42 (my peer) {rest}")), 23.0);
    assert_eq!(stat_cpu_ticks(&format!("42 (a) (b c)) {rest}")), 23.0);
    assert_eq!(stat_cpu_ticks("42 (peer) S 1"), 0.0);
}

#[test]
fn uses_reported_executable_without_assuming_target_directory() {
    let message = serde_json::json!({
        "reason": "compiler-artifact",
        "target": { "name": "peer" },
        "executable": "/mnt/bench/tmp/custom/x86_64-unknown-linux-gnu/release/peer"
    });
    assert_eq!(
        executable_artifact(&message, "peer"),
        Some(PathBuf::from(
            "/mnt/bench/tmp/custom/x86_64-unknown-linux-gnu/release/peer"
        ))
    );
    assert_eq!(executable_artifact(&message, "other"), None);
    assert_eq!(
        executable_artifact(&serde_json::json!({"reason": "build-finished"}), "peer"),
        None
    );
}

#[cfg(unix)]
#[test]
fn capture_rejects_unsuccessful_exit_even_with_plausible_output() {
    assert!(
        capture_with_cpu(
            &["sh", "-c", "printf 123; exit 7"],
            &[],
            None,
            Duration::from_secs(1)
        )
        .is_none()
    );

    assert_eq!(
        capture(
            &["sh", "-c", "printf 123; exit 7"],
            &[],
            None,
            Duration::from_secs(1)
        ),
        None
    );

    assert_eq!(
        capture(
            &["sh", "-c", "printf 123"],
            &[],
            None,
            Duration::from_secs(1)
        ),
        Some("123".into())
    );
}
