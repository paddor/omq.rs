//! Verified SCATTER/GATHER throughput and CLIENT/SERVER latency cohorts.

use super::{ChartResult, IMPLS, SIZES, common};
use common::{
    ChartSpec, CpuColumns, FairnessEntry, FairnessMap, LatencyEntry, LatencyMap, ThroughputRow,
    ValMap,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

const TIMED_BATCH_MESSAGES: u64 = 256;
const TIMED_BATCH_BYTES: u64 = 1024 * 1024;

fn verified(row: &Value, implementation: &str, size: u64, latency: bool, spin: u64) -> bool {
    if setup_rejected(row, implementation, size, latency, spin) {
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
            latency_round_verified(round)
        } else {
            throughput_round_verified(round)
        }
    })
}

/// Rejects rows from another setup. Same CPU and process layout as the
/// RADIO/DISH chart, with one blocking receiver.
fn setup_rejected(row: &Value, implementation: &str, size: u64, latency: bool, spin: u64) -> bool {
    let inproc = implementation == "inproc";
    row["kind"]
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
        || !super::storage_matches(row, size, latency)
        || (!latency && row["sender_clock_batch"] != sender_clock_batch(size))
        || row["storage"]
            != match implementation {
                "shm-pool" => "preallocated-shm",
                _ => "preallocated-heap",
            }
}

/// Standard sample counts and positive, monotonic quantiles.
fn latency_round_verified(round: &Value) -> bool {
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
}

/// The receiver got every offered message intact, and the rate matches
/// offered / elapsed, where elapsed is the slower of the two sides.
fn throughput_round_verified(round: &Value) -> bool {
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

pub(super) fn sender_clock_batch(size: u64) -> u64 {
    if (56..super::SLAB_BYTES).contains(&size) {
        super::SLAB_BYTES / size
    } else {
        (TIMED_BATCH_BYTES / size).clamp(1, TIMED_BATCH_MESSAGES)
    }
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

fn load(cache: &Path, latency: bool, cached_baselines: bool) -> ChartResult<Option<Vec<Value>>> {
    let name = if latency { "reqrep" } else { "pushpull" };
    let sizes = if latency {
        common::COMPARISON_LATENCY_SIZES
    } else {
        SIZES
    };
    let spins: &[u64] = if latency { &[0, 50] } else { &[0] };
    let cohorts = super::load_cohorts(cache, name);
    if cohorts.is_empty() {
        return Ok(None);
    }
    let groups = if cached_baselines {
        vec![&IMPLS[..2], &IMPLS[2..]]
    } else {
        vec![IMPLS]
    };
    let mut selected = Vec::new();
    for implementations in groups {
        for sizes in super::size_blocks(sizes, cached_baselines) {
            let cohort = cohorts
                .values()
                .rev()
                .find(|rows| complete(rows, implementations, sizes, latency, spins))
                .ok_or_else(|| format!("missing complete verified SHM {name} cohort"))?;
            eprintln!(
                "SHM {name}: implementations={}, sizes={}..{}, run={}, binary={}",
                implementations
                    .iter()
                    .map(|item| item.key)
                    .collect::<Vec<_>>()
                    .join(","),
                sizes[0],
                sizes[sizes.len() - 1],
                cohort[0]["run_id"],
                cohort[0]["binary_sha256"]
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
    Ok(Some(selected))
}

fn complete(
    rows: &[Value],
    implementations: &[common::Impl],
    sizes: &[u64],
    latency: bool,
    spins: &[u64],
) -> bool {
    implementations.iter().all(|implementation| {
        sizes.iter().all(|&size| {
            spins
                .iter()
                .all(|&spin| find(rows, implementation.key, size, latency, spin).is_some())
        })
    })
}

fn throughput(directory: &Path, rows: &[Value]) -> ChartResult {
    let (bandwidth, messages, spread) = throughput_maps(rows);
    let path = directory.join("pushpull.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "SCATTER/GATHER throughput, inproc / IPC / SHM",
        sizes: SIZES,
        impls: IMPLS,
    };
    let row = ThroughputRow {
        peers: 1,
        tput: &bandwidth,
        msgs: &messages,
        fairness: Some(&spread),
    };
    let columns = CpuColumns {
        cpu: &BTreeMap::new(),
        snd_label: "",
        rcv_label: "",
    };

    common::draw_multirow_throughput(
        &spec,
        &[row],
        &columns,
        &|_| "1 sender / 1 receiver".into(),
        super::hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}

/// Bandwidth, message rate, and round spread per size from the median of
/// three rounds.
fn throughput_maps(rows: &[Value]) -> (ValMap, ValMap, FairnessMap) {
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

    (bandwidth, messages, spread)
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
    let spec = ChartSpec {
        out_path: &path,
        title: "CLIENT/SERVER round-trip latency, inproc / IPC / SHM",
        sizes: common::COMPARISON_LATENCY_SIZES,
        impls: IMPLS,
    };
    common::draw_multirow_latency(
        &spec,
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

pub(super) fn generate(
    cache: &Path,
    directory: &Path,
    is_latency: bool,
    cached_baselines: bool,
) -> ChartResult {
    let Some(rows) = load(cache, is_latency, cached_baselines)? else {
        return Ok(());
    };
    if is_latency {
        latency(directory, &rows)
    } else {
        throughput(directory, &rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn throughput_rejects_the_old_per_message_clock_measurements() {
        let rows = super::super::tests::point_cohort(false);
        let mut row = rows
            .iter()
            .find(|row| row["msg_size"] == 16)
            .unwrap()
            .clone();
        let implementation = row["impl"].as_str().unwrap().to_owned();
        assert!(verified(&row, &implementation, 16, false, 0));
        row.as_object_mut().unwrap().remove("sender_clock_batch");
        assert!(!verified(&row, &implementation, 16, false, 0));
        row["sender_clock_batch"] = json!(1);
        assert!(!verified(&row, &implementation, 16, false, 0));
    }

    fn write_rows(path: &Path, rows: &[Value]) {
        use std::io::Write;
        let mut file = std::fs::File::create(path).unwrap();
        for row in rows {
            serde_json::to_writer(&mut file, row).unwrap();
            writeln!(file).unwrap();
        }
    }

    #[test]
    fn cached_baselines_select_complete_shared_cohorts_and_reject_partial_refreshes() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let cache =
            std::env::temp_dir().join(format!("omq-shm-point-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&cache).unwrap();
        for latency in [false, true] {
            let path = cache.join(if latency {
                "shm-reqrep.jsonl"
            } else {
                "shm-pushpull.jsonl"
            });
            let mut rows = super::super::tests::point_cohort(latency);
            for row in &mut rows {
                let shared = row["impl"].as_str().unwrap().starts_with("shm");
                row["run_id"] = json!(if shared { "b-shared" } else { "a-baseline" });
                row["binary_sha256"] = row["run_id"].clone();
            }
            let mut partial = rows
                .iter()
                .find(|row| row["impl"] == "shm")
                .unwrap()
                .clone();
            partial["run_id"] = json!("c-incomplete");
            partial["binary_sha256"] = json!("newer-binary");
            rows.push(partial);
            write_rows(&path, &rows);
            assert!(load(&cache, latency, false).is_err());
            let selected = load(&cache, latency, true).unwrap().unwrap();
            assert_eq!(selected.len(), rows.len() - 1);
            for row in selected {
                assert_eq!(
                    row["binary_sha256"],
                    if row["impl"].as_str().unwrap().starts_with("shm") {
                        "b-shared"
                    } else {
                        "a-baseline"
                    }
                );
            }
            let missing = rows
                .iter()
                .position(|row| row["impl"] == "shm-pool")
                .unwrap();
            rows.remove(missing);
            write_rows(&path, &rows);
            assert!(load(&cache, latency, true).is_err());
        }
        std::fs::remove_dir_all(cache).unwrap();
    }
}
