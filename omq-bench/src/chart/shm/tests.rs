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
        let target = Target {
            implementation,
            size,
            mode: original["peers"].as_u64().unwrap(),
        };
        assert!(verified(&original, target));

        for field in [
            "send_hwm",
            "recv_hwm",
            "pool_slot_bytes",
            "payload_pool_bytes",
            "slab_bytes",
        ] {
            let mut changed = original.clone();
            changed[field] = json!(changed[field].as_u64().unwrap() + 1);
            assert!(!verified(&changed, target));
        }

        if implementation.starts_with("shm") {
            let mut historical = original.clone();
            historical["pooled_batch_copy_threshold"] = Value::Null;
            assert!(!verified(&historical, target));
        }

        if size >= LARGE_POOL_MIN_BODY {
            let mut copied = original.clone();
            copied["payload_fill"] = json!("memcpy");
            assert!(!verified(&copied, target));
        }
    }
}

#[test]
fn missing_or_incomplete_families_do_not_block_other_charts() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("omq-shm-charts-{}-{unique}", std::process::id()));

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
        latest_complete(
            &cohorts,
            IMPLS,
            &SIZES[..SIZES.len() - LARGE_SIZES.len()],
            &pubsub_complete
        )
        .unwrap()[0]["run_id"],
        "a"
    );

    assert_eq!(
        latest_complete(&cohorts, IMPLS, LARGE_SIZES, &pubsub_complete).unwrap()[0]["run_id"],
        "b"
    );
    assert!(latest_complete(&cohorts, IMPLS, SIZES, &pubsub_complete).is_none());
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
        latest_complete(&cohorts, IMPLS, LARGE_SIZES, &pubsub_complete).unwrap()[0]["run_id"],
        "a"
    );

    let mut mismatched = cohort("c", LARGE_SIZES);
    mismatched.last_mut().unwrap()["pool_slots"] = json!(1024);
    cohorts.insert(("c".into(), "third-binary".into()), mismatched);
    assert_eq!(
        latest_complete(&cohorts, IMPLS, LARGE_SIZES, &pubsub_complete).unwrap()[0]["run_id"],
        "a"
    );

    cohorts.remove(&("a".into(), "first-binary".into()));
    assert!(latest_complete(&cohorts, IMPLS, LARGE_SIZES, &pubsub_complete).is_none());
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
    assert!(latest_complete(&cohorts, &IMPLS[..1], SIZES, &pubsub_complete).is_none());
    assert!(latest_complete(&cohorts, &IMPLS[1..], SIZES, &pubsub_complete).is_some());
}
