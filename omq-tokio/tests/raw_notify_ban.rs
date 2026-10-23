use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN_SUBSTRINGS: &[&str] = &[
    "tokio::sync::Notify",
    "tokio::sync::futures::Notified",
    ".notify_one(",
    ".notify_waiters(",
    ".notified(",
];

const FORBIDDEN_TOKENS: &[&str] = &["Notify::new("];

#[test]
fn raw_tokio_notify_is_confined_to_signal_module() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .parent()
        .expect("omq-tokio should live under repo root");
    let allowed = manifest.join("src/engine/signal.rs");
    let roots = [
        manifest.join("src"),
        repo.join("omq-libzmq/src"),
        repo.join("bindings/pyomq/src"),
    ];

    let mut violations = Vec::new();
    for root in roots {
        if root.exists() {
            collect_violations(&root, &allowed, &mut violations);
        }
    }

    assert!(
        violations.is_empty(),
        "raw tokio Notify must stay behind engine::signal:\n{}",
        violations.join("\n")
    );
}

fn collect_violations(root: &Path, allowed: &Path, violations: &mut Vec<String>) {
    for entry in fs::read_dir(root).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_violations(&path, allowed, violations);
            continue;
        }
        // The signal module and its child modules, such as its tests.
        let in_signal_module = path == allowed || path.starts_with(allowed.with_extension(""));
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") || in_signal_module {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read source file");
        for (index, line) in text.lines().enumerate() {
            let raw_hit = FORBIDDEN_SUBSTRINGS
                .iter()
                .any(|needle| line.contains(needle))
                || FORBIDDEN_TOKENS
                    .iter()
                    .any(|needle| contains_token(line, needle));
            if raw_hit {
                violations.push(format!("{}:{}", path.display(), index + 1));
            }
        }
    }
}

fn contains_token(line: &str, needle: &str) -> bool {
    let mut offset = 0;
    while let Some(pos) = line[offset..].find(needle) {
        let start = offset + pos;
        let previous = line[..start].chars().next_back();
        if !previous.is_some_and(is_ident_char) {
            return true;
        }
        offset = start + needle.len();
    }
    false
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
