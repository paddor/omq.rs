//! Gate thresholds from `.perf_hw` or the smoke defaults, and the check of
//! one sample against them.

use std::collections::HashMap;

use super::{Sample, contract, draft};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ThresholdMode {
    Hardware,
    Smoke,
}

#[derive(Debug)]
pub(super) struct ThresholdConfig {
    pub(super) mode: ThresholdMode,
    pub(super) values: HashMap<String, f64>,
}

fn smoke_thresholds() -> HashMap<String, f64> {
    let mut thresholds = HashMap::from([
        ("reqrep_ct.p50_256b_us".to_string(), 1_000.0),
        ("pushpull_1io.16b_msgs_s".to_string(), 1_000_000.0),
        ("pubsub_1io.16b_msgs_s".to_string(), 500_000.0),
        ("inproc_pushpull_1io.16b_msgs_s".to_string(), 1_000_000.0),
    ]);
    draft::smoke_thresholds(&mut thresholds);
    thresholds
}

pub(super) fn read_thresholds() -> ThresholdConfig {
    let contents = match std::fs::read_to_string(".perf_hw") {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ThresholdConfig {
                mode: ThresholdMode::Smoke,
                values: smoke_thresholds(),
            };
        }
        Err(error) => panic!("cannot read .perf_hw: {error}"),
    };
    println!("threshold file: .perf_hw");
    let mut section = String::new();
    let mut thresholds = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            section = name.trim().to_string();
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if let Ok(value) = value.trim().parse() {
            thresholds.insert(format!("{section}.{}", key.trim()), value);
        }
    }
    ThresholdConfig {
        mode: ThresholdMode::Hardware,
        values: thresholds,
    }
}

pub(super) fn should_measure(name: &str, config: &ThresholdConfig) -> bool {
    // Profile contracts compare within one run and need no thresholds.
    config.values.contains_key(name)
        || (config.mode == ThresholdMode::Hardware
            && !name.starts_with("quic_")
            && !name.starts_with("dart_"))
        || name.starts_with("contract_")
}

pub(super) fn verify(sample: &Sample, thresholds: &HashMap<String, f64>) -> bool {
    let key = &sample.name;
    println!("{:<24} {:>12.2} {}", sample.name, sample.value, sample.unit);
    let contract = (sample.unit == "ratio").then_some(contract::MIN_RATIO);
    match thresholds.get(key).or(contract.as_ref()) {
        Some(limit) if sample.unit == "us" && sample.value > *limit => {
            eprintln!(
                "FAIL {key}: {:.2} above configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) if sample.unit == "us" => {
            println!("  threshold (max): {:.2} {}", limit, sample.unit);
            true
        }
        Some(limit) if sample.value < *limit => {
            eprintln!(
                "FAIL {key}: {:.2} below configured {:.2} {}",
                sample.value, limit, sample.unit
            );
            false
        }
        Some(limit) => {
            let direction = if sample.unit == "us" { "max" } else { "min" };
            println!("  threshold ({direction}): {:.2} {}", limit, sample.unit);
            true
        }
        None => true,
    }
}
