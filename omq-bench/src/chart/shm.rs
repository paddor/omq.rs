//! Matched, verified RADIO/DISH transport and storage comparison.

use std::collections::BTreeMap;
use std::path::Path;

use plotters::style::RGBColor;
use serde_json::Value;

use super::common::{
    self, ChartSpec, CpuColumns, FairnessEntry, FairnessMap, Impl, ThroughputRow, ValMap,
};
use crate::jsonl;

mod point_to_point;

const READERS: &[u64] = &[4, 32];

const SIZES: &[u64] = &[
    16, 64, 256, 1024, 4096, 16384, 131_072, 1_048_576, 8_388_608,
];

#[cfg(test)]
const LARGE_SIZES: &[u64] = &[1_048_576, 8_388_608];

const HWM: u64 = 1000;
const SLAB_BYTES: u64 = 4096;
const POOL_BYTES: u64 = 64 * 1024 * 1024;
const LARGE_POOL_BYTES: u64 = 256 * 1024 * 1024;
const LARGE_POOL_MIN_BODY: u64 = 1024 * 1024;
const POOLED_BATCH_COPY_THRESHOLD: u64 = 256;
const FANOUT_POOLED_BATCH_COPY_THRESHOLD: u64 = 1024;

const IMPLS: &[Impl] = &[
    Impl {
        key: "inproc",
        label: "OMQ / inproc pool",
        threads: "direct queues",
        color: common::C_OMQ_CT,
    },
    Impl {
        key: "ipc",
        label: "OMQ / IPC",
        threads: "1 IO",
        color: common::C_OMQ_1T,
    },
    Impl {
        key: "shm",
        label: "OMQ / SHM",
        threads: "1 IO",
        color: RGBColor(59, 130, 246),
    },
    Impl {
        key: "shm-pool",
        label: "OMQ / SHM pool",
        threads: "1 IO",
        color: RGBColor(147, 197, 253),
    },
];

type ChartResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Cohorts = BTreeMap<(String, String), Vec<Value>>;

/// Keep the standard machine/tuning subtitle without CPU counts.
fn hardware_subtitle() -> Option<String> {
    common::detect_hardware().map(|label| {
        label
            .split(", ")
            .filter(|field| {
                !field.contains(" cores") && !field.to_ascii_lowercase().contains("vcpu")
            })
            .collect::<Vec<_>>()
            .join(", ")
    })
}

fn pool_bytes(size: u64) -> u64 {
    if size >= LARGE_POOL_MIN_BODY {
        LARGE_POOL_BYTES
    } else {
        POOL_BYTES
    }
}

fn size_blocks(sizes: &[u64], cached_baselines: bool) -> Vec<&[u64]> {
    let split = sizes.partition_point(|&size| size < LARGE_POOL_MIN_BODY);
    if cached_baselines && split > 0 && split < sizes.len() {
        vec![&sizes[..split], &sizes[split..]]
    } else {
        vec![sizes]
    }
}

fn storage_matches(row: &Value, size: u64, latency: bool) -> bool {
    let slot_bytes = size.max(SLAB_BYTES);
    let slots = (pool_bytes(size) / slot_bytes).max(1);

    let slab_bytes = if !latency && (56..SLAB_BYTES).contains(&size) {
        SLAB_BYTES
    } else {
        0
    };

    row["send_hwm"] == HWM
        && row["recv_hwm"] == HWM
        && row["pool_slots"] == slots
        && row["pool_slot_bytes"] == slot_bytes
        && row["payload_pool_bytes"] == slots * slot_bytes
        && row["slab_bytes"] == slab_bytes
        && (latency || size < LARGE_POOL_MIN_BODY || row["payload_fill"] == "memset")
        && (latency
            || !row["impl"]
                .as_str()
                .is_some_and(|name| name.starts_with("shm"))
            || row["pooled_batch_copy_threshold"]
                == if row["socket_pair"] == "radio-dish" {
                    FANOUT_POOLED_BATCH_COPY_THRESHOLD
                } else {
                    POOLED_BATCH_COPY_THRESHOLD
                })
}

/// One chart point: implementation, message size, and a family-specific
/// mode (the reader count for RADIO/DISH).
#[derive(Clone, Copy)]
pub(super) struct Target<'a, M> {
    pub(super) implementation: &'a str,
    pub(super) size: u64,
    pub(super) mode: M,
}

pub(super) fn verified(row: &Value, target: Target<'_, u64>) -> bool {
    if setup_rejected(row, target) {
        return false;
    }

    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };

    rounds
        .iter()
        .enumerate()
        .all(|(index, round)| round_verified(round, index, target.mode))
}

/// Rejects rows from another setup.
fn setup_rejected(row: &Value, target: Target<'_, u64>) -> bool {
    let readers = target.mode;
    layout_rejected(row, target.implementation)
        || row["kind"] != "shm_pubsub_verified"
        || row["profiled"] == true
        || row["msg_size"] != target.size
        || row["peers"] != readers
        || row["socket_pair"] != "radio-dish"
        || row["subscriber_threads"] != readers
        || row["receiver_context"] != "shared"
        || row["warmup_seconds"] != 1
        || row["seconds"] != 3
        || !storage_matches(row, target.size, false)
}

/// Rejects rows of another implementation or CPU, process, or storage
/// layout. Inproc runs in one process across all four CPUs; the other
/// transports pin the sender and the shared receiver process to two CPUs
/// each.
fn layout_rejected(row: &Value, implementation: &str) -> bool {
    let inproc = implementation == "inproc";
    row["impl"] != implementation
        || row["subscriber_processes"] != u64::from(!inproc)
        || row["process_layout"]
            != if inproc {
                "one-process-shared-context"
            } else {
                "two-process-shared-receiver"
            }
        || row["publisher_cpus"] != if inproc { "1,2,3,4" } else { "1,2" }
        || row["subscriber_cpus"] != if inproc { "1,2,3,4" } else { "3,4" }
        || row["io_threads_per_context"] != 1
        || row["storage"]
            != match implementation {
                "shm-pool" => "preallocated-shm",
                _ => "preallocated-heap",
            }
}

/// Round `index` (zero-based) is valid when every reader got every offered
/// message intact and its rate is consistent with the measured times.
fn round_verified(round: &Value, index: usize, readers: u64) -> bool {
    let offered = round["offered"].as_u64().unwrap_or(0);
    let Some(counts) = round["reader_counts"].as_array() else {
        return false;
    };

    let Some(times) = round["reader_elapsed"].as_array() else {
        return false;
    };

    let sender = round["sender_elapsed"].as_f64().unwrap_or(0.0);
    let elapsed = round["elapsed"].as_f64().unwrap_or(0.0);
    let rate = round["msgs_s"].as_f64().unwrap_or(0.0);

    // The round ends with its slowest side, so elapsed must equal the max of
    // the sender and reader times.
    let slowest = times
        .iter()
        .filter_map(Value::as_f64)
        .fold(sender, f64::max);

    round["round"] == index + 1
        && offered > 0
        && counts.len() as u64 == readers
        && counts.iter().all(|count| count.as_u64() == Some(offered))
        && times.len() as u64 == readers
        && times
            .iter()
            .all(|time| time.as_f64().is_some_and(|v| v.is_finite() && v > 0.0))
        && sender.is_finite()
        && sender >= 3.0
        && elapsed.is_finite()
        && (elapsed - slowest).abs() < 1e-6
        && rate.is_finite()
        && rate > 0.0
        && (rate - offered as f64 / elapsed).abs() < rate * 1e-6
        && ["missing", "duplicates", "gaps", "corrupt"]
            .iter()
            .all(|field| round[field] == 0)
}

fn find<'a>(rows: &'a [Value], target: Target<'_, u64>) -> Option<&'a Value> {
    rows.iter().rev().find(|row| verified(row, target))
}

/// Complete when every implementation, reader count, and size has a
/// verified row.
fn pubsub_complete(rows: &[Value], implementations: &[Impl], sizes: &[u64]) -> bool {
    implementations.iter().all(|implementation| {
        READERS.iter().all(|&readers| {
            sizes.iter().all(|&size| {
                let target = Target {
                    implementation: implementation.key,
                    size,
                    mode: readers,
                };

                find(rows, target).is_some()
            })
        })
    })
}

/// Newest cohort that `complete` accepts for `implementations` and `sizes`.
fn latest_complete<'a>(
    cohorts: &'a Cohorts,
    implementations: &[Impl],
    sizes: &[u64],
    complete: &impl Fn(&[Value], &[Impl], &[u64]) -> bool,
) -> Option<&'a [Value]> {
    cohorts
        .values()
        .rev()
        .find(|rows| complete(rows, implementations, sizes))
        .map(Vec::as_slice)
}

/// Rows of the newest complete cohort per implementation group and size
/// block, logged as `SHM {label}`. With cached baselines, inproc/IPC and SHM
/// may come from different runs, and so may sizes below and above the
/// large-pool threshold. `None` when a group or block has no complete cohort.
fn select_cohorts(
    cohorts: &Cohorts,
    label: &str,
    sizes: &[u64],
    cached_baselines: bool,
    complete: impl Fn(&[Value], &[Impl], &[u64]) -> bool,
) -> Option<Vec<Value>> {
    let groups = if cached_baselines {
        vec![&IMPLS[..2], &IMPLS[2..]]
    } else {
        vec![IMPLS]
    };

    let mut selected = Vec::new();
    for implementations in groups {
        for sizes in size_blocks(sizes, cached_baselines) {
            let cohort = latest_complete(cohorts, implementations, sizes, &complete)?;
            eprintln!(
                "SHM {label}: implementations={}, sizes={}..{}, run={}, binary={}",
                implementations
                    .iter()
                    .map(|item| item.key)
                    .collect::<Vec<_>>()
                    .join(","),
                sizes[0],
                sizes[sizes.len() - 1],
                cohort[0]["run_id"],
                cohort[0]["binary_sha256"],
            );

            selected.extend(
                cohort
                    .iter()
                    .filter(|row| {
                        implementations
                            .iter()
                            .any(|implementation| row["impl"] == implementation.key)
                            && row["msg_size"]
                                .as_u64()
                                .is_some_and(|size| sizes.contains(&size))
                    })
                    .cloned(),
            );
        }
    }

    Some(selected)
}

fn load_cohorts(cache: &Path, family: &str) -> Cohorts {
    let mut cohorts = Cohorts::new();
    for (_, row) in jsonl::load_jsonl::<Value>(&cache.join(format!("shm-{family}.jsonl"))) {
        if let (Some(run), Some(digest)) = (row["run_id"].as_str(), row["binary_sha256"].as_str()) {
            cohorts
                .entry((run.into(), digest.into()))
                .or_default()
                .push(row);
        }
    }

    cohorts
}

/// Bandwidth, message rate, and round spread by size and implementation.
#[derive(Default)]
pub(super) struct RateMaps {
    bandwidth: ValMap,
    messages: ValMap,
    spread: FairnessMap,
}

impl RateMaps {
    /// Inserts the median of the row's three round rates. The spread box
    /// spans the midpoints between min, median, and max.
    pub(super) fn insert_round_rates(&mut self, row: &Value, implementation: &str, size: u64) {
        let mut rates: Vec<_> = row["rounds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|round| round["msgs_s"].as_f64().unwrap())
            .collect();
        rates.sort_by(f64::total_cmp);
        self.messages
            .entry(size)
            .or_default()
            .insert(implementation.into(), rates[1]);
        self.bandwidth
            .entry(size)
            .or_default()
            .insert(implementation.into(), rates[1] * size as f64 / 1e6);

        self.spread.entry(size).or_default().insert(
            implementation.into(),
            FairnessEntry {
                min: rates[0],
                p25: rates[0].midpoint(rates[1]),
                median: rates[1],
                p75: rates[1].midpoint(rates[2]),
                max: rates[2],
            },
        );
    }

    pub(super) fn row(&self, peers: u64) -> ThroughputRow<'_> {
        ThroughputRow {
            peers,
            tput: &self.bandwidth,
            msgs: &self.messages,
            fairness: Some(&self.spread),
        }
    }
}

fn build_panels(rows: &[Value]) -> Vec<(u64, RateMaps)> {
    READERS
        .iter()
        .map(|&readers| {
            let mut maps = RateMaps::default();
            for implementation in IMPLS {
                for &size in SIZES {
                    let target = Target {
                        implementation: implementation.key,
                        size,
                        mode: readers,
                    };
                    maps.insert_round_rates(find(rows, target).unwrap(), implementation.key, size);
                }
            }

            (readers, maps)
        })
        .collect()
}

pub(crate) fn generate(cached_baselines: bool) -> ChartResult {
    let directory = common::out_dir().join("shm");
    std::fs::create_dir_all(&directory)?;
    generate_from(&jsonl::cache_dir(), &directory, cached_baselines)
}

fn generate_from(cache: &Path, directory: &Path, cached_baselines: bool) -> ChartResult {
    // Attempt every family before reporting incomplete cohorts or IO errors.
    let results = [
        generate_pubsub(cache, directory, cached_baselines),
        point_to_point::generate(cache, directory, false, cached_baselines),
        point_to_point::generate(cache, directory, true, cached_baselines),
    ];
    let errors: Vec<_> = results
        .into_iter()
        .filter_map(Result::err)
        .map(|error| error.to_string())
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

fn generate_pubsub(cache: &Path, directory: &Path, cached_baselines: bool) -> ChartResult {
    let cohorts = load_cohorts(cache, "pubsub");
    if cohorts.is_empty() {
        return Ok(());
    }

    let selected = select_cohorts(&cohorts, "pubsub", SIZES, cached_baselines, pubsub_complete)
        .ok_or("missing complete SHM comparison cohort; run scripts/run_shm_pubsub.rb")?;

    let panels = build_panels(&selected);
    let rows: Vec<ThroughputRow<'_>> = panels
        .iter()
        .map(|(readers, maps)| maps.row(*readers))
        .collect();
    let path = directory.join("pubsub.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "RADIO/DISH throughput, inproc / IPC / SHM",
        sizes: SIZES,
        impls: IMPLS,
    };

    common::draw_multirow_throughput(
        &spec,
        &rows,
        &CpuColumns::NONE,
        &|readers| format!("{readers} subscribers"),
        hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());

    Ok(())
}

#[cfg(test)]
mod tests;
