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
    let cache = std::env::temp_dir().join(format!("omq-shm-point-{}-{unique}", std::process::id()));
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
