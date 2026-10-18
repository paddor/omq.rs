//! Requires HWM-sized queues to use the clamped capacity accessors.
//!
//! Queues preallocate their capacity, so a raw HWM such as `i32::MAX` used
//! as a size aborts the process on allocation. `Options::send_queue_capacity`,
//! `Options::recv_queue_capacity`, and `options::queue_capacity` clamp it.

use std::fs;
use std::path::{Path, PathBuf};

// Field accesses only: locals derived from a clamped capacity are fine.
const FORBIDDEN: &[&str] = &[
    ".send_hwm.max(",
    ".recv_hwm.max(",
    ".send_hwm as usize",
    ".recv_hwm as usize",
];

#[test]
fn hwm_sized_queues_use_clamped_capacity() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest.parent().expect("omq-tokio lives under repo root");
    let mut violations = Vec::new();
    for root in ["omq-tokio/src", "omq-libzmq/src"] {
        collect(repo, &repo.join(root), &mut violations);
    }
    assert!(
        violations.is_empty(),
        "raw HWM used as a queue size; use the clamped capacity accessors:\n{}",
        violations.join("\n")
    );
}

fn collect(repo: &Path, dir: &Path, violations: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect(repo, &path, violations);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let rel = path
            .strip_prefix(repo)
            .expect("source under repo")
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(&path).expect("read source file");
        for (index, line) in text.lines().enumerate() {
            if FORBIDDEN.iter().any(|needle| line.contains(needle)) {
                violations.push(format!("{rel}:{}: {}", index + 1, line.trim()));
            }
        }
    }
}
