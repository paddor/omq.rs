//! Rejects busy-waiting and blocking name lookups in the backend and the
//! libzmq API.
//!
//! A spin on a runtime thread can wait for a task that needs that same
//! thread, which livelocks a single-IO-thread context. Producers must enqueue
//! without waiting, and consumers must park on a signal. Every remaining spin
//! is bounded and runs where it cannot block the task it waits for. System
//! lookups run only on the shared DNS workers.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &["thread::yield_now(", "spin_loop(", "to_socket_addrs("];

/// (path relative to repo root, line substring, reason)
const ALLOWED: &[(&str, &str, &str)] = &[
    (
        "omq-tokio/src/transport/dns.rs",
        ".to_socket_addrs()",
        "DNS worker threads own every system lookup",
    ),
    (
        "omq-tokio/src/blocking.rs",
        "std::thread::yield_now();",
        "blocking dispatch yields its own OS thread between budgets",
    ),
    (
        "omq-tokio/src/engine/receive_cell.rs",
        "std::thread::yield_now();",
        "overlapping calls hand off a !Send guard that never spans an await",
    ),
    (
        "omq-tokio/src/socket/recv/blocking.rs",
        "std::hint::spin_loop();",
        "configured latency spin, bounded by a deadline and interrupt checks",
    ),
    (
        "omq-tokio/src/socket/handle/receive/blocking.rs",
        "std::thread::yield_now();",
        "blocking receive yields its own OS thread",
    ),
    (
        "omq-tokio/src/transport/dart/worker.rs",
        "std::hint::spin_loop();",
        "configured DART latency spin, bounded by its spin window",
    ),
    (
        "omq-libzmq/src/send_recv.rs",
        "std::hint::spin_loop();",
        "four bounded spins on the application thread before parking",
    ),
    (
        "omq-libzmq/src/send_recv.rs",
        "std::thread::yield_now();",
        "four bounded yields on the application thread before parking",
    ),
    (
        "omq-libzmq/src/socket.rs",
        "std::hint::spin_loop();",
        "application thread waits for a concurrent materialization to publish",
    ),
];

#[test]
fn runtime_code_does_not_busy_wait_or_block_on_lookups() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest.parent().expect("omq-tokio lives under repo root");
    let mut violations = Vec::new();
    for root in ["omq-tokio/src", "omq-libzmq/src"] {
        collect(repo, &repo.join(root), &mut violations);
    }
    assert!(
        violations.is_empty(),
        "busy-wait or blocking lookup outside the allowlist; enqueue without waiting, park on a signal, or use transport::dns:\n{}",
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
            // Benchmark peers may spin.
            if name != "bin" {
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
