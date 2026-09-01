use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub(crate) fn cache_dir() -> PathBuf {
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            format!("{home}/.cache")
        });
    PathBuf::from(base).join("omq")
}

pub(crate) fn load_jsonl<T: DeserializeOwned>(path: &Path) -> Vec<(usize, T)> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let Ok(line) = line else { continue };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if let Ok(row) = serde_json::from_str(&line) {
            rows.push((i, row));
        }
    }
    rows
}

/// Append a single row to a JSONL file, creating directories as needed.
pub(crate) fn append_jsonl<T: Serialize>(path: &Path, row: &T) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("failed to open JSONL file");
    let json = serde_json::to_string(row).expect("failed to serialize row");
    writeln!(file, "{json}").expect("failed to write JSONL row");
    file.flush().ok();
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::fsync(file.as_raw_fd());
        }
    }
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ComparisonRow {
    pub run_id: String,
    #[serde(rename = "impl")]
    pub impl_name: String,
    pub kind: String,
    pub transport: String,
    pub msg_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msgs_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pub_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p999_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_p10: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_p25: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_median: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_p75: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_p90: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zero_transport: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PushpullLz4Row {
    pub run_id: String,
    pub pattern: String,
    pub transport: String,
    pub peers: u64,
    pub msg_size: u64,
    pub wire_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_count: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msgs_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dict_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompressionRow {
    pub run_id: String,
    pub pattern: String,
    pub transport: String,
    pub peers: u64,
    pub msg_size: u64,
    pub wire_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_count: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msgs_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dict_size: Option<u64>,
}
