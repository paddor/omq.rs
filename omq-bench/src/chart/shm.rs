//! Matched, verified RADIO/DISH transport and storage comparison.

use std::collections::BTreeMap;
use std::path::Path;

use plotters::style::RGBColor;
use serde_json::Value;

use super::common::{self, FairnessEntry, FairnessMap, Impl, ValMap};
use crate::jsonl;

mod point_to_point;

type ChartResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const READERS: &[u64] = &[4, 32];
const SIZES: &[u64] = &[
    16, 64, 256, 1024, 4096, 16384, 131_072, 1_048_576, 8_388_608,
];
const LARGE_SIZES: &[u64] = &[131_072, 1_048_576, 8_388_608];
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

fn verified(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    let inproc = implementation == "inproc";
    if row["kind"] != "shm_pubsub_verified"
        || row["impl"] != implementation
        || row["msg_size"] != size
        || row["peers"] != readers
        || row["socket_pair"] != "radio-dish"
        || row["subscriber_threads"] != readers
        || row["subscriber_processes"] != u64::from(!inproc)
        || row["process_layout"]
            != if inproc {
                "one-process-shared-context"
            } else {
                "two-process-shared-receiver"
            }
        || row["publisher_cpus"] != if inproc { "1,2,3,4" } else { "1,2" }
        || row["subscriber_cpus"] != if inproc { "1,2,3,4" } else { "3,4" }
        || row["receiver_context"] != "shared"
        || row["io_threads_per_context"] != 1
        || row["storage"]
            != match implementation {
                "inproc" => "preallocated-heap",
                "shm-pool" => "preallocated-shm",
                _ => "ordinary",
            }
        || row["warmup_seconds"] != 1
        || row["seconds"] != 3
    {
        return false;
    }
    if inproc || LARGE_SIZES.contains(&size) {
        let hwm = (32 * 1024 * 1024 / size).clamp(4, 256);
        let slots = if matches!(implementation, "inproc" | "shm-pool") {
            (64 * 1024 * 1024 / size).clamp(4, 1024)
        } else {
            0
        };
        if row["send_hwm"] != hwm || row["recv_hwm"] != hwm || row["pool_slots"] != slots {
            return false;
        }
    }
    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };
    rounds.iter().enumerate().all(|(index, round)| {
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
    })
}

fn find<'a>(rows: &'a [Value], implementation: &str, size: u64, readers: u64) -> Option<&'a Value> {
    rows.iter()
        .rev()
        .find(|row| verified(row, implementation, size, readers))
}

fn latest_complete<'a>(
    cohorts: &'a BTreeMap<(String, String), Vec<Value>>,
    implementations: &[Impl],
    sizes: &[u64],
) -> Option<&'a [Value]> {
    cohorts
        .values()
        .rev()
        .find(|rows| {
            implementations.iter().all(|implementation| {
                READERS.iter().all(|&readers| {
                    sizes
                        .iter()
                        .all(|&size| find(rows, implementation.key, size, readers).is_some())
                })
            })
        })
        .map(Vec::as_slice)
}

fn build_panels(
    inproc: &[Value],
    baseline: &[Value],
    shm: &[Value],
    large_baseline: &[Value],
    large_shm: &[Value],
) -> Vec<(u64, ValMap, ValMap, FairnessMap)> {
    let mut panels = Vec::new();
    for &readers in READERS {
        let mut bandwidth = ValMap::new();
        let mut messages = ValMap::new();
        let mut spread = FairnessMap::new();
        for implementation in IMPLS {
            for &size in SIZES {
                let cohort = match (implementation.key, LARGE_SIZES.contains(&size)) {
                    ("inproc", _) => inproc,
                    ("shm" | "shm-pool", true) => large_shm,
                    ("shm" | "shm-pool", false) => shm,
                    ("ipc", true) => large_baseline,
                    ("ipc", false) => baseline,
                    _ => unreachable!("known SHM comparison implementation"),
                };
                let row = find(cohort, implementation.key, size, readers).unwrap();
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
        panels.push((readers, bandwidth, messages, spread));
    }
    panels
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
        point_to_point::generate(cache, directory, false),
        point_to_point::generate(cache, directory, true),
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
    let mut cohorts: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
    for (_, row) in jsonl::load_jsonl::<Value>(&cache.join("shm-pubsub.jsonl")) {
        if let (Some(run), Some(digest)) = (row["run_id"].as_str(), row["binary_sha256"].as_str()) {
            cohorts
                .entry((run.into(), digest.into()))
                .or_default()
                .push(row);
        }
    }
    if cohorts.is_empty() {
        return Ok(());
    }
    let missing = "missing complete SHM comparison cohort; run scripts/run_shm_pubsub.rb";
    let baseline = latest_complete(
        &cohorts,
        if cached_baselines {
            &IMPLS[1..2]
        } else {
            IMPLS
        },
        if cached_baselines {
            common::COMPARISON_SIZES
        } else {
            SIZES
        },
    )
    .ok_or(missing)?;
    let inproc = if cached_baselines {
        latest_complete(&cohorts, &IMPLS[..1], SIZES).ok_or(missing)?
    } else {
        baseline
    };
    let shm = if cached_baselines {
        latest_complete(&cohorts, &IMPLS[2..], common::COMPARISON_SIZES).ok_or(missing)?
    } else {
        baseline
    };
    let large_baseline = if cached_baselines {
        latest_complete(&cohorts, &IMPLS[1..2], LARGE_SIZES).ok_or(missing)?
    } else {
        baseline
    };
    let large_shm = if cached_baselines {
        latest_complete(&cohorts, &IMPLS[2..], LARGE_SIZES).ok_or(missing)?
    } else {
        shm
    };
    for (label, rows) in [
        ("Inproc pool", inproc),
        ("IPC through 16 KiB", baseline),
        ("SHM through 16 KiB", shm),
        ("IPC from 128 KiB", large_baseline),
        ("SHM from 128 KiB", large_shm),
    ] {
        eprintln!(
            "{label}: run={}, host={}, binary={}",
            rows[0]["run_id"].as_str().unwrap(),
            rows[0]["host"].as_str().unwrap_or("unknown"),
            rows[0]["binary_sha256"].as_str().unwrap(),
        );
    }
    let panels = build_panels(inproc, baseline, shm, large_baseline, large_shm);
    let rows: Vec<_> = panels
        .iter()
        .map(|(readers, bandwidth, messages, _)| (*readers, bandwidth, messages))
        .collect();
    let spread: Vec<_> = panels.iter().map(|(_, _, _, spread)| spread).collect();
    let path = directory.join("pubsub.svg");
    common::draw_multirow_throughput(
        &path,
        "RADIO/DISH throughput, inproc / IPC / SHM",
        &rows,
        SIZES,
        IMPLS,
        &BTreeMap::new(),
        &|readers| format!("{readers} subscribers"),
        "",
        "",
        Some(&spread),
        common::GbsScale::Logarithmic,
        hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cohort(run: &str, sizes: &[u64]) -> Vec<Value> {
        let mut rows = Vec::new();
        for implementation in IMPLS {
            let inproc = implementation.key == "inproc";
            for &readers in READERS {
                for &size in sizes {
                    let rounds: Vec<_> = (1..=3).map(|round| json!({
                        "round": round, "offered": 3, "reader_counts": vec![3; readers as usize],
                        "reader_elapsed": vec![3.0; readers as usize], "sender_elapsed": 3.0,
                        "elapsed": 3.0, "msgs_s": 1.0,
                        "missing": 0, "duplicates": 0, "gaps": 0, "corrupt": 0,
                    })).collect();
                    rows.push(json!({
                        "run_id": run, "kind": "shm_pubsub_verified", "impl": implementation.key,
                        "msg_size": size, "peers": readers, "socket_pair": "radio-dish",
                        "subscriber_threads": readers, "subscriber_processes": u64::from(!inproc),
                        "process_layout": if inproc { "one-process-shared-context" } else { "two-process-shared-receiver" },
                        "publisher_cpus": if inproc { "1,2,3,4" } else { "1,2" },
                        "subscriber_cpus": if inproc { "1,2,3,4" } else { "3,4" },
                        "receiver_context": "shared", "io_threads_per_context": 1,
                        "storage": match implementation.key { "inproc" => "preallocated-heap", "shm-pool" => "preallocated-shm", _ => "ordinary" },
                        "warmup_seconds": 1, "seconds": 3, "rounds": rounds,
                        "send_hwm": (32 * 1024 * 1024 / size).clamp(4, 256),
                        "recv_hwm": (32 * 1024 * 1024 / size).clamp(4, 256),
                        "pool_slots": if matches!(implementation.key, "inproc" | "shm-pool") { (64 * 1024 * 1024 / size).clamp(4, 1024) } else { 0 },
                    }));
                }
            }
        }
        rows
    }

    fn point_cohort(latency: bool) -> Vec<Value> {
        let sizes = if latency {
            common::COMPARISON_LATENCY_SIZES
        } else {
            SIZES
        };
        let spins: &[u64] = if latency { &[0, 50] } else { &[0] };
        cohort("point", sizes)
            .into_iter()
            .filter(|row| row["peers"] == 4)
            .flat_map(|row| {
                spins.iter().map(move |&spin| {
                    let mut row = row.clone();
                    row["kind"] = json!(if latency {
                        "shm_reqrep_verified"
                    } else {
                        "shm_pushpull_verified"
                    });
                    row["socket_pair"] = json!(if latency {
                        "client-server"
                    } else {
                        "scatter-gather"
                    });
                    row["peers"] = json!(1);
                    row["subscriber_threads"] = json!(1);
                    row["recv_mode"] = json!("blocking");
                    row["recv_spin_us"] = json!(spin);
                    row["workload_profile"] = json!(if latency { "latency" } else { "throughput" });
                    for round in row["rounds"].as_array_mut().unwrap() {
                        round["reader_counts"] = json!([3]);
                        round["reader_elapsed"] = json!([3.0]);
                        round["verified"] = json!(true);
                        round["samples"] = json!(100_000);
                        round["warmup_iterations"] = json!(20_000);
                        for (key, value) in [
                            ("p50_us", 1.0),
                            ("p99_us", 2.0),
                            ("p999_us", 3.0),
                            ("max_us", 4.0),
                        ] {
                            round[key] = json!(value);
                        }
                    }
                    row
                })
            })
            .collect()
    }

    #[test]
    fn missing_or_incomplete_families_do_not_block_other_charts() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("omq-shm-charts-{}-{unique}", std::process::id()));
        for incomplete in [None, Some("pubsub"), Some("pushpull"), Some("reqrep")] {
            let directory = root.join(incomplete.unwrap_or("missing-pubsub"));
            let cache = directory.join("cache");
            let output = directory.join("charts");
            std::fs::create_dir_all(&cache).unwrap();
            std::fs::create_dir_all(&output).unwrap();
            for (family, mut rows) in [
                ("pubsub", cohort("pubsub", SIZES)),
                ("pushpull", point_cohort(false)),
                ("reqrep", point_cohort(true)),
            ] {
                if family == "pubsub" && incomplete.is_none() {
                    continue;
                }
                if incomplete == Some(family) {
                    rows.pop();
                }
                let mut file =
                    std::fs::File::create(cache.join(format!("shm-{family}.jsonl"))).unwrap();
                for mut row in rows {
                    use std::io::Write;
                    row["binary_sha256"] = json!("chart-test");
                    serde_json::to_writer(&mut file, &row).unwrap();
                    writeln!(file).unwrap();
                }
            }
            assert_eq!(
                generate_from(&cache, &output, false).is_err(),
                incomplete.is_some()
            );
            for family in ["pubsub", "pushpull", "reqrep"] {
                let expected =
                    incomplete != Some(family) && !(family == "pubsub" && incomplete.is_none());
                assert_eq!(output.join(format!("{family}.svg")).exists(), expected);
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_size_extension_preserves_the_complete_original_block() {
        let cohorts = BTreeMap::from([
            (
                ("a".into(), "old-binary".into()),
                cohort("a", common::COMPARISON_SIZES),
            ),
            (("b".into(), "new-binary".into()), cohort("b", LARGE_SIZES)),
        ]);
        assert_eq!(
            latest_complete(&cohorts, IMPLS, common::COMPARISON_SIZES).unwrap()[0]["run_id"],
            "a"
        );
        assert_eq!(
            latest_complete(&cohorts, IMPLS, LARGE_SIZES).unwrap()[0]["run_id"],
            "b"
        );
        assert!(latest_complete(&cohorts, IMPLS, SIZES).is_none());
    }

    #[test]
    fn incomplete_or_mismatched_large_points_do_not_replace_a_complete_block() {
        let complete = cohort("a", LARGE_SIZES);
        let mut incomplete = cohort("b", LARGE_SIZES);
        incomplete.pop();
        let mut cohorts = BTreeMap::from([
            (("a".into(), "first-binary".into()), complete),
            (("b".into(), "second-binary".into()), incomplete),
        ]);
        assert_eq!(
            latest_complete(&cohorts, IMPLS, LARGE_SIZES).unwrap()[0]["run_id"],
            "a"
        );
        let mut mismatched = cohort("c", LARGE_SIZES);
        mismatched.last_mut().unwrap()["pool_slots"] = json!(1024);
        cohorts.insert(("c".into(), "third-binary".into()), mismatched);
        assert_eq!(
            latest_complete(&cohorts, IMPLS, LARGE_SIZES).unwrap()[0]["run_id"],
            "a"
        );
        cohorts.remove(&("a".into(), "first-binary".into()));
        assert!(latest_complete(&cohorts, IMPLS, LARGE_SIZES).is_none());
    }

    #[test]
    fn ordinary_inproc_rows_cannot_supply_the_pooled_series() {
        let mut rows = cohort("old", SIZES);
        for row in &mut rows {
            if row["impl"] == "inproc" {
                row["storage"] = json!("ordinary");
                row["pool_slots"] = json!(0);
            }
        }
        let cohorts = BTreeMap::from([(("old".into(), "old-binary".into()), rows)]);
        assert!(latest_complete(&cohorts, &IMPLS[..1], SIZES).is_none());
        assert!(latest_complete(&cohorts, &IMPLS[1..], SIZES).is_some());
    }
}
