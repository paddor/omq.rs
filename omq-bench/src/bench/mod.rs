pub(crate) mod comparisons;
pub(crate) mod compression;
pub(crate) mod datagram;
pub(crate) mod pushpull_compression;
pub(crate) mod pushpull_lz4;
pub(crate) mod pushpull_rate_limit;
pub(crate) mod pushpull_zstd;

use std::time::{SystemTime, UNIX_EPOCH};

/// UTC run id `YYYYmmddTHHMMSSZ`, with `-name` appended when given.
pub(crate) fn make_run_id(name: Option<&str>) -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .expect("failed to run date");
    let ts = String::from_utf8_lossy(&output.stdout).trim().to_string();
    match name {
        Some(n) => format!("{ts}-{n}"),
        None => ts,
    }
}

/// Nanoseconds since the Unix epoch, for run ids and row timestamps.
pub(crate) fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos()
        .try_into()
        .expect("timestamp fits in u64")
}
