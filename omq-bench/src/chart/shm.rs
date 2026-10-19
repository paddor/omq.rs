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

type ChartResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Cohorts = BTreeMap<(String, String), Vec<Value>>;

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

pub(super) fn verified(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    if setup_rejected(row, implementation, size, readers) {
        return false;
    }
    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };
    rounds
        .iter()
        .enumerate()
        .all(|(index, round)| round_verified(round, index, readers))
}

/// Rejects rows from another setup. Inproc runs in one process across all
/// four CPUs; the other transports pin the publisher and the shared
/// receiver process to two CPUs each.
fn setup_rejected(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    let inproc = implementation == "inproc";
    row["kind"] != "shm_pubsub_verified"
        || row["profiled"] == true
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
                "shm-pool" => "preallocated-shm",
                _ => "preallocated-heap",
            }
        || row["warmup_seconds"] != 1
        || row["seconds"] != 3
        || !storage_matches(row, size, false)
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

fn find<'a>(rows: &'a [Value], implementation: &str, size: u64, readers: u64) -> Option<&'a Value> {
    rows.iter()
        .rev()
        .find(|row| verified(row, implementation, size, readers))
}

fn latest_complete<'a>(
    cohorts: &'a Cohorts,
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

fn build_panels(rows: &[Value]) -> Vec<(u64, ValMap, ValMap, FairnessMap)> {
    let mut panels = Vec::new();
    for &readers in READERS {
        let mut bandwidth = ValMap::new();
        let mut messages = ValMap::new();
        let mut spread = FairnessMap::new();
        for implementation in IMPLS {
            for &size in SIZES {
                let row = find(rows, implementation.key, size, readers).unwrap();
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
    let selected = select_rows(&cohorts, cached_baselines)?;

    let panels = build_panels(&selected);
    let rows: Vec<ThroughputRow<'_>> = panels
        .iter()
        .map(|(readers, bandwidth, messages, spread)| ThroughputRow {
            peers: *readers,
            tput: bandwidth,
            msgs: messages,
            fairness: Some(spread),
        })
        .collect();
    let path = directory.join("pubsub.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "RADIO/DISH throughput, inproc / IPC / SHM",
        sizes: SIZES,
        impls: IMPLS,
    };
    let columns = CpuColumns {
        cpu: &BTreeMap::new(),
        snd_label: "",
        rcv_label: "",
    };

    common::draw_multirow_throughput(
        &spec,
        &rows,
        &columns,
        &|readers| format!("{readers} subscribers"),
        hardware_subtitle().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}

/// Rows of the newest complete cohort per implementation group and size
/// block. With cached baselines, inproc/IPC and SHM may come from different
/// runs, and so may sizes below and above the large-pool threshold.
fn select_rows(cohorts: &Cohorts, cached_baselines: bool) -> ChartResult<Vec<Value>> {
    let missing = "missing complete SHM comparison cohort; run scripts/run_shm_pubsub.rb";
    let groups = if cached_baselines {
        vec![&IMPLS[..2], &IMPLS[2..]]
    } else {
        vec![IMPLS]
    };
    let mut selected = Vec::new();

    for implementations in groups {
        for sizes in size_blocks(SIZES, cached_baselines) {
            let cohort = latest_complete(cohorts, implementations, sizes).ok_or(missing)?;
            eprintln!(
                "SHM pubsub: implementations={}, sizes={}..{}, run={}, binary={}",
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

    Ok(selected)
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
                        "storage": if implementation.key == "shm-pool" { "preallocated-shm" } else { "preallocated-heap" },
                        "warmup_seconds": 1, "seconds": 3, "rounds": rounds,
                        "send_hwm": HWM, "recv_hwm": HWM,
                        "pool_slots": (pool_bytes(size) / size.max(SLAB_BYTES)).max(1),
                        "pool_slot_bytes": size.max(SLAB_BYTES),
                        "payload_pool_bytes": (pool_bytes(size) / size.max(SLAB_BYTES)).max(1) * size.max(SLAB_BYTES),
                        "slab_bytes": if (56..SLAB_BYTES).contains(&size) { SLAB_BYTES } else { 0 },
                        "pooled_batch_copy_threshold": FANOUT_POOLED_BATCH_COPY_THRESHOLD,
                        "payload_fill": "memset",
                    }));
                }
            }
        }
        rows
    }

    pub(super) fn point_cohort(latency: bool) -> Vec<Value> {
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
                    row["pooled_batch_copy_threshold"] = json!(POOLED_BATCH_COPY_THRESHOLD);
                    if latency {
                        row["slab_bytes"] = json!(0);
                        row["payload_fill"] = json!("memcpy");
                    } else {
                        row["sender_clock_batch"] = json!(point_to_point::sender_clock_batch(
                            row["msg_size"].as_u64().unwrap()
                        ));
                    }
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
    fn historical_hwm_and_pool_settings_cannot_supply_the_new_charts() {
        let rows = cohort("current", SIZES);
        for original in rows {
            let implementation = original["impl"].as_str().unwrap();
            let size = original["msg_size"].as_u64().unwrap();
            let readers = original["peers"].as_u64().unwrap();
            assert!(verified(&original, implementation, size, readers));
            for field in [
                "send_hwm",
                "recv_hwm",
                "pool_slot_bytes",
                "payload_pool_bytes",
                "slab_bytes",
            ] {
                let mut changed = original.clone();
                changed[field] = json!(changed[field].as_u64().unwrap() + 1);
                assert!(!verified(&changed, implementation, size, readers));
            }
            if implementation.starts_with("shm") {
                let mut historical = original.clone();
                historical["pooled_batch_copy_threshold"] = Value::Null;
                assert!(!verified(&historical, implementation, size, readers));
            }
            if size >= LARGE_POOL_MIN_BODY {
                let mut copied = original.clone();
                copied["payload_fill"] = json!("memcpy");
                assert!(!verified(&copied, implementation, size, readers));
            }
        }
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
                cohort("a", &SIZES[..SIZES.len() - LARGE_SIZES.len()]),
            ),
            (("b".into(), "new-binary".into()), cohort("b", LARGE_SIZES)),
        ]);
        assert_eq!(
            latest_complete(&cohorts, IMPLS, &SIZES[..SIZES.len() - LARGE_SIZES.len()]).unwrap()[0]
                ["run_id"],
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
