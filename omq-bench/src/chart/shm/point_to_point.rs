//! Verified SCATTER/GATHER throughput and CLIENT/SERVER latency cohorts.

use super::{ChartResult, IMPLS, SIZES, common, jsonl};
use common::{FairnessEntry, FairnessMap, LatencyEntry, LatencyMap, ValMap};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

fn verified(row: &Value, implementation: &str, size: u64, latency: bool, spin: u64) -> bool {
    let inproc = implementation == "inproc";
    let hwm = (32 * 1024 * 1024 / size).clamp(4, 256);
    let slots = if matches!(implementation, "inproc" | "shm-pool") {
        (64 * 1024 * 1024 / size).clamp(4, 1024)
    } else {
        0
    };
    if row["kind"]
        != if latency {
            "shm_reqrep_verified"
        } else {
            "shm_pushpull_verified"
        }
        || row["socket_pair"]
            != if latency {
                "client-server"
            } else {
                "scatter-gather"
            }
        || row["impl"] != implementation
        || row["msg_size"] != size
        || row["peers"] != 1
        || row["recv_mode"] != "blocking"
        || row["recv_spin_us"] != spin
        || row["send_spin_us"].as_u64().unwrap_or(0) != 0
        || row["workload_profile"] != if latency { "latency" } else { "throughput" }
        || row["publisher_cpus"] != if inproc { "1,2,3,4" } else { "1,2" }
        || row["subscriber_cpus"] != if inproc { "1,2,3,4" } else { "3,4" }
        || row["process_layout"]
            != if inproc {
                "one-process-shared-context"
            } else {
                "two-process-shared-receiver"
            }
        || row["subscriber_processes"] != u64::from(!inproc)
        || row["subscriber_threads"] != 1
        || row["io_threads_per_context"] != 1
        || row["send_hwm"] != hwm
        || row["recv_hwm"] != hwm
        || row["pool_slots"] != slots
        || row["storage"]
            != match implementation {
                "inproc" => "preallocated-heap",
                "shm-pool" => "preallocated-shm",
                _ => "ordinary",
            }
    {
        return false;
    }
    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };
    rounds.iter().enumerate().all(|(index, round)| {
        if round["round"] != index + 1 || round["verified"] != true {
            return false;
        }
        if latency {
            let quantiles: Option<Vec<f64>> = ["p50_us", "p99_us", "p999_us", "max_us"]
                .iter()
                .map(|key| round[key].as_f64())
                .collect();
            round["samples"] == 100_000
                && round["warmup_iterations"] == 20_000
                && quantiles.is_some_and(|values| {
                    values.iter().all(|value| value.is_finite() && *value > 0.0)
                        && values.windows(2).all(|pair| pair[0] <= pair[1])
                })
        } else {
            let offered = round["offered"].as_u64().unwrap_or(0);
            let sender = round["sender_elapsed"].as_f64().unwrap_or(0.0);
            let receiver = round["reader_elapsed"][0].as_f64().unwrap_or(0.0);
            let elapsed = round["elapsed"].as_f64().unwrap_or(0.0);
            let rate = round["msgs_s"].as_f64().unwrap_or(0.0);
            offered > 0
                && round["reader_counts"]
                    .as_array()
                    .is_some_and(|counts| counts.len() == 1 && counts[0] == offered)
                && sender.is_finite()
                && sender >= 3.0
                && receiver.is_finite()
                && receiver > 0.0
                && (elapsed - sender.max(receiver)).abs() < 1e-6
                && rate.is_finite()
                && rate > 0.0
                && (rate - offered as f64 / elapsed).abs() < rate * 1e-6
                && ["missing", "duplicates", "gaps", "corrupt"]
                    .iter()
                    .all(|field| round[field] == 0)
        }
    })
}

fn find<'a>(
    rows: &'a [Value],
    implementation: &str,
    size: u64,
    latency: bool,
    spin: u64,
) -> Option<&'a Value> {
    rows.iter()
        .rev()
        .find(|row| verified(row, implementation, size, latency, spin))
}

fn load(cache: &Path, latency: bool) -> ChartResult<Option<Vec<Value>>> {
    let name = if latency { "reqrep" } else { "pushpull" };
    let sizes = if latency {
        common::COMPARISON_LATENCY_SIZES
    } else {
        SIZES
    };
    let spins: &[u64] = if latency { &[0, 50] } else { &[0] };
    let mut cohorts: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
    for (_, row) in jsonl::load_jsonl::<Value>(&cache.join(format!("shm-{name}.jsonl"))) {
        if let (Some(run), Some(digest)) = (row["run_id"].as_str(), row["binary_sha256"].as_str()) {
            cohorts
                .entry((run.into(), digest.into()))
                .or_default()
                .push(row);
        }
    }
    if cohorts.is_empty() {
        return Ok(None);
    }
    let cohort = cohorts
        .into_values()
        .rev()
        .find(|rows| {
            IMPLS.iter().all(|implementation| {
                sizes.iter().all(|&size| {
                    spins
                        .iter()
                        .all(|&spin| find(rows, implementation.key, size, latency, spin).is_some())
                })
            })
        })
        .ok_or_else(|| format!("missing complete verified SHM {name} cohort"))?;
    eprintln!(
        "SHM {name}: run={}, binary={}",
        cohort[0]["run_id"], cohort[0]["binary_sha256"]
    );
    Ok(Some(cohort))
}

fn throughput(directory: &Path, rows: &[Value]) -> ChartResult {
    let mut messages = ValMap::new();
    let mut bandwidth = ValMap::new();
    let mut spread = FairnessMap::new();
    for implementation in IMPLS {
        for &size in SIZES {
            let row = find(rows, implementation.key, size, false, 0).unwrap();
            let mut rates: Vec<_> = row["rounds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|round| round["msgs_s"].as_f64().unwrap())
                .collect();
            rates.sort_by(f64::total_cmp);
            messages
                .entry(size)
                .or_default()
                .insert(implementation.key.into(), rates[1]);
            bandwidth
                .entry(size)
                .or_default()
                .insert(implementation.key.into(), rates[1] * size as f64 / 1e6);
            spread.entry(size).or_default().insert(
                implementation.key.into(),
                FairnessEntry {
                    min: rates[0],
                    p25: rates[0].midpoint(rates[1]),
                    median: rates[1],
                    p75: rates[1].midpoint(rates[2]),
                    max: rates[2],
                },
            );
        }
    }
    let path = directory.join("pushpull.svg");
    common::draw_multirow_throughput(
        &path,
        "SCATTER/GATHER throughput, inproc / IPC / SHM",
        &[(1, &bandwidth, &messages)],
        SIZES,
        IMPLS,
        &BTreeMap::new(),
        &|_| "1 sender / 1 receiver".into(),
        "",
        "",
        Some(&[&spread]),
        common::GbsScale::Logarithmic,
        super::hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}

fn latency(directory: &Path, rows: &[Value]) -> ChartResult {
    let mut panels = [LatencyMap::new(), LatencyMap::new()];
    for implementation in IMPLS {
        for &size in common::COMPARISON_LATENCY_SIZES {
            for (panel, spin) in panels.iter_mut().zip([0, 50]) {
                let row = find(rows, implementation.key, size, true, spin).unwrap();
                let mut rounds: Vec<_> = row["rounds"].as_array().unwrap().iter().collect();
                rounds.sort_by(|a, b| {
                    a["p99_us"]
                        .as_f64()
                        .unwrap()
                        .total_cmp(&b["p99_us"].as_f64().unwrap())
                });
                let round = rounds[1];
                panel.entry(size).or_default().insert(
                    implementation.key.into(),
                    LatencyEntry {
                        p50: round["p50_us"].as_f64().unwrap(),
                        p99: round["p99_us"].as_f64().unwrap(),
                        p999: round["p999_us"].as_f64().unwrap(),
                    },
                );
            }
        }
    }
    let path = directory.join("reqrep.svg");
    common::draw_multirow_latency(
        &path,
        "CLIENT/SERVER round-trip latency, inproc / IPC / SHM",
        common::COMPARISON_LATENCY_SIZES,
        IMPLS,
        &[
            (
                "Non-spinning: p99 RTT; whiskers: p50 to p99.9 (lower is better)",
                &panels[0],
            ),
            (
                "Spinning (50 \u{03bc}s): p99 RTT; whiskers: p50 to p99.9 (lower is better)",
                &panels[1],
            ),
        ],
        120,
        20,
        super::hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}

pub(super) fn generate(cache: &Path, directory: &Path, is_latency: bool) -> ChartResult {
    let Some(rows) = load(cache, is_latency)? else {
        return Ok(());
    };
    if is_latency {
        latency(directory, &rows)
    } else {
        throughput(directory, &rows)
    }
}
