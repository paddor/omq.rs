//! Matched RADIO/DISH fan-out with verified delivery to every subscriber.

use std::collections::BTreeMap;

use serde_json::Value;

use super::super::common::{
    self, ChartSpec, CpuColumns, FairnessEntry, FairnessMap, ThroughputRow, ValMap,
};
use super::super::shm;
use crate::jsonl;

const READERS: &[u64] = &[4, 32];
const SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];

fn pooled_receive(row: &Value, size: u64) -> bool {
    row["source_pool"] == true
        && row["receive_pool"] == true
        && row["receive_pool_slots"] == 1024
        && row["receive_pool_slot_bytes"] == size.max(64)
        && row["receive_pool_batch_recycle"] == true
}

fn eligible(row: &Value) -> bool {
    let Some(implementation) = row["impl"].as_str() else {
        return false;
    };
    let size = row["msg_size"].as_u64().unwrap_or(0);
    let readers = row["peers"].as_u64().unwrap_or(0);
    super::IMPLS[..3]
        .iter()
        .any(|item| item.key == implementation)
        && SIZES.contains(&size)
        && READERS.contains(&readers)
        && row["profiled"] == false
        && row["recv_mode"] == "yield-poll"
        && row["recv_spin_us"] == 0
        && row["io_spin_us"] == 0
        && row["workload_profile"] == "throughput"
        && shm::verified(row, implementation, size, readers)
        && pooled_receive(row, size)
        && row["rounds"]
            .as_array()
            .is_some_and(|rounds| rounds.iter().all(|round| pooled_receive(round, size)))
        && (implementation == "tcp"
            || (row["dart_wire_version"] == super::DART_WIRE_VERSION
                && row["dart_window_messages"] == 256
                && row["dart_congestion"] == implementation.trim_start_matches("dart-")
                && row["rounds"].as_array().is_some_and(|rounds| {
                    rounds.iter().all(|round| round["dart_send_failures"] == 0)
                })))
}

/// Rows of one run and binary, keyed by readers, size, and impl.
type Cohort = BTreeMap<(u64, u64, String), Value>;

/// Groups eligible rows by run ID and binary digest.
fn cohorts(rows: Vec<(usize, Value)>) -> BTreeMap<(String, String), Cohort> {
    let mut cohorts = BTreeMap::<(String, String), Cohort>::new();
    for (_, row) in rows {
        if !eligible(&row) {
            continue;
        }
        let (Some(run), Some(digest)) = (row["run_id"].as_str(), row["binary_sha256"].as_str())
        else {
            continue;
        };
        cohorts
            .entry((run.into(), digest.into()))
            .or_default()
            .insert(
                (
                    row["peers"].as_u64().unwrap(),
                    row["msg_size"].as_u64().unwrap(),
                    row["impl"].as_str().unwrap().into(),
                ),
                row,
            );
    }

    cohorts
}

/// Bandwidth, message rate, and round spread per size for one reader count.
fn reader_panel(readers: u64, cohort: &Cohort) -> (ValMap, ValMap, FairnessMap) {
    let mut messages = ValMap::new();
    let mut bandwidth = ValMap::new();
    let mut spread = FairnessMap::new();

    for &size in SIZES {
        for implementation in &super::IMPLS[..3] {
            let row = &cohort[&(readers, size, implementation.key.into())];
            let mut rates: Vec<_> = row["rounds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|round| round["msgs_s"].as_f64().unwrap())
                .collect();
            rates.sort_by(f64::total_cmp);

            // Three rounds give min, median, and max. The box spans the
            // midpoints between them.
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

pub(super) fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let rows = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("dart-radiodish.jsonl"));
    if rows.is_empty() {
        return Ok(());
    }

    let cohorts = cohorts(rows);

    // Run IDs start with a UTC timestamp, so the last complete cohort is the
    // newest one.
    let implementations = &super::IMPLS[..3];
    let ((run, digest), cohort) = cohorts
        .iter()
        .rev()
        .find(|(_, cohort)| cohort.len() == READERS.len() * SIZES.len() * implementations.len())
        .ok_or("missing complete DART RADIO/DISH cohort; run scripts/run_shm_pubsub.rb --dart --recv-pool")?;
    eprintln!("DART RADIO/DISH: run={run}, binary={digest}");

    let panels: Vec<_> = READERS
        .iter()
        .map(|&readers| (readers, reader_panel(readers, cohort)))
        .collect();
    let rows: Vec<ThroughputRow<'_>> = panels
        .iter()
        .map(|(readers, (bandwidth, messages, spread))| ThroughputRow {
            peers: *readers,
            tput: bandwidth,
            msgs: messages,
            fairness: Some(spread),
        })
        .collect();

    let directory = common::out_dir().join("dart");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("radiodish.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "RADIO/DISH throughput per subscriber, DART / TCP loopback",
        sizes: SIZES,
        impls: implementations,
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
        &|readers| format!("{readers} subscribers, payload pools, median and range of 3"),
        common::detect_hardware().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}
