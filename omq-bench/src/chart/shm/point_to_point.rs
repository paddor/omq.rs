//! Verified SCATTER/GATHER throughput and CLIENT/SERVER latency cohorts.

use super::{ChartResult, IMPLS, RateMaps, SIZES, Target, common};
use common::{ChartSpec, CpuColumns, LatencyEntry, LatencyMap};
use serde_json::Value;
use std::path::Path;

const TIMED_BATCH_MESSAGES: u64 = 256;
const TIMED_BATCH_BYTES: u64 = 1024 * 1024;

/// Throughput, or latency with a receive spin in microseconds.
#[derive(Clone, Copy)]
struct Mode {
    latency: bool,
    spin: u64,
}

const THROUGHPUT: Mode = Mode {
    latency: false,
    spin: 0,
};

fn verified(row: &Value, target: Target<'_, Mode>) -> bool {
    if setup_rejected(row, target) {
        return false;
    }
    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };
    rounds.iter().enumerate().all(|(index, round)| {
        round["verified"] == true
            && if target.mode.latency {
                round["round"] == index + 1 && latency_round_verified(round)
            } else {
                super::round_verified(round, index, 1)
            }
    })
}

/// Rejects rows from another setup. Same CPU and process layout as the
/// RADIO/DISH chart, with one blocking receiver.
fn setup_rejected(row: &Value, target: Target<'_, Mode>) -> bool {
    let Mode { latency, spin } = target.mode;
    super::layout_rejected(row, target.implementation)
        || row["kind"]
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
        || row["msg_size"] != target.size
        || row["peers"] != 1
        || row["recv_mode"] != "blocking"
        || row["recv_spin_us"] != spin
        || row["send_spin_us"].as_u64().unwrap_or(0) != 0
        || row["workload_profile"] != if latency { "latency" } else { "throughput" }
        || row["subscriber_threads"] != 1
        || !super::storage_matches(row, target.size, latency)
        || (!latency && row["sender_clock_batch"] != sender_clock_batch(target.size))
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

pub(super) fn sender_clock_batch(size: u64) -> u64 {
    if (56..super::SLAB_BYTES).contains(&size) {
        super::SLAB_BYTES / size
    } else {
        (TIMED_BATCH_BYTES / size).clamp(1, TIMED_BATCH_MESSAGES)
    }
}

fn find<'a>(rows: &'a [Value], target: Target<'_, Mode>) -> Option<&'a Value> {
    rows.iter().rev().find(|row| verified(row, target))
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
    // Complete when every implementation, size, and spin has a verified row.
    let complete = |rows: &[Value], implementations: &[common::Impl], sizes: &[u64]| {
        implementations.iter().all(|implementation| {
            sizes.iter().all(|&size| {
                spins.iter().all(|&spin| {
                    let target = Target {
                        implementation: implementation.key,
                        size,
                        mode: Mode { latency, spin },
                    };
                    find(rows, target).is_some()
                })
            })
        })
    };
    let selected = super::select_cohorts(&cohorts, name, sizes, cached_baselines, complete)
        .ok_or_else(|| format!("missing complete verified SHM {name} cohort"))?;
    Ok(Some(selected))
}

/// SCATTER/GATHER chart from the median of three rounds per point.
fn throughput(directory: &Path, rows: &[Value]) -> ChartResult {
    let mut maps = RateMaps::default();
    for implementation in IMPLS {
        for &size in SIZES {
            let target = Target {
                implementation: implementation.key,
                size,
                mode: THROUGHPUT,
            };
            maps.insert_round_rates(find(rows, target).unwrap(), implementation.key, size);
        }
    }
    let path = directory.join("pushpull.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "SCATTER/GATHER throughput, inproc / IPC / SHM",
        sizes: SIZES,
        impls: IMPLS,
    };

    common::draw_multirow_throughput(
        &spec,
        &[maps.row(1)],
        &CpuColumns::NONE,
        &|_| "1 sender / 1 receiver".into(),
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
                let target = Target {
                    implementation: implementation.key,
                    size,
                    mode: Mode {
                        latency: true,
                        spin,
                    },
                };
                let row = find(rows, target).unwrap();
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
        let target = |implementation| Target {
            implementation,
            size: 16,
            mode: THROUGHPUT,
        };
        assert!(verified(&row, target(&implementation)));
        row.as_object_mut().unwrap().remove("sender_clock_batch");
        assert!(!verified(&row, target(&implementation)));
        row["sender_clock_batch"] = json!(1);
        assert!(!verified(&row, target(&implementation)));
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
