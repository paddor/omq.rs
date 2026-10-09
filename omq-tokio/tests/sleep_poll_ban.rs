//! Rejects timer polling in the backend, the libzmq API, and binding native
//! code.
//!
//! A wait must park on a wakeup (socket signal, cancel handle, condvar) and
//! use a deadline only to bound it. Sleeping a fixed interval between
//! readiness checks caps a socket at ~1000 operations per second per
//! millisecond of sleep. Every remaining sleep needs an allowlist entry.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &["thread::sleep(", "time::sleep("];

/// (path relative to repo root, line substring, reason)
const ALLOWED: &[(&str, &str, &str)] = &[
    (
        "omq-libzmq/src/util.rs",
        "Duration::from_secs(seconds as u64)",
        "zmq_sleep is a sleep",
    ),
    (
        "omq-libzmq/src/poll.rs",
        "from_millis(timeout_ms as u64)",
        "zmq_poll with no pollable items has nothing to wake it",
    ),
    (
        "omq-tokio/src/transport/ipc.rs",
        "sleep(std::time::Duration::from_millis(5))",
        "Windows ERROR_PIPE_BUSY has no wakeup; connect setup only",
    ),
    (
        "omq-tokio/src/socket/actor/endpoint_resolution.rs",
        "sleep(Duration::from_millis(10))",
        "setup retry while bind or connect preflight would block",
    ),
    (
        "omq-tokio/src/socket/actor/listener.rs",
        "sleep(Duration::from_millis(50))",
        "accept error backoff, not a readiness poll",
    ),
    (
        "omq-tokio/src/socket/actor/mod.rs",
        "sleep(Duration::from_millis(1))",
        "close drain: send queues have no became-empty signal",
    ),
];

#[test]
fn binding_and_libzmq_waits_do_not_sleep_poll() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest.parent().expect("omq-tokio lives under repo root");
    let mut violations = Vec::new();
    for root in ["omq-tokio/src", "omq-libzmq/src", "bindings"] {
        collect(repo, &repo.join(root), &mut violations);
    }
    assert!(
        violations.is_empty(),
        "timer polling outside the allowlist; park on a wakeup instead:\n{}",
        violations.join("\n")
    );
}

fn collect(repo: &Path, dir: &Path, violations: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("read source entry").path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if path.is_dir() {
            // Tests, benches, peers, and build output may sleep.
            if !matches!(
                name,
                "target" | "node_modules" | "tests" | "test" | "bin" | "benches"
            ) {
                collect(repo, &path, violations);
            }
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs")
            || name == "tests.rs"
            || name.ends_with("_tests.rs")
        {
            continue;
        }
        let rel = path
            .strip_prefix(repo)
            .expect("source under repo")
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(&path).expect("read source file");
        for (index, line) in text.lines().enumerate() {
            // Inline unit test modules sit at the end of the file.
            if line.starts_with("#[cfg(test)]") {
                break;
            }
            if !FORBIDDEN.iter().any(|needle| line.contains(needle)) {
                continue;
            }
            let allowed = ALLOWED
                .iter()
                .any(|(file, needle, _)| *file == rel && line.contains(needle));
            if !allowed {
                violations.push(format!("{rel}:{}: {}", index + 1, line.trim()));
            }
        }
    }
}
