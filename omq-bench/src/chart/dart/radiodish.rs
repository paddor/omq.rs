//! Matched RADIO/DISH fan-out with verified delivery to every subscriber.

use std::collections::BTreeMap;

use serde_json::Value;

use super::super::common::{self, FairnessEntry, FairnessMap, ValMap};
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

pub(super) fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let rows = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("dart-radiodish.jsonl"));
    if rows.is_empty() {
        return Ok(());
    }
    let mut cohorts = BTreeMap::<(String, String), BTreeMap<(u64, u64, String), Value>>::new();
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
    let implementations = &super::IMPLS[..3];
    let ((run, digest), cohort) = cohorts
        .iter()
        .rev()
        .find(|(_, cohort)| cohort.len() == READERS.len() * SIZES.len() * implementations.len())
        .ok_or("missing complete DART RADIO/DISH cohort; run scripts/run_shm_pubsub.rb --dart --recv-pool")?;
    eprintln!("DART RADIO/DISH: run={run}, binary={digest}");
    let mut panels = Vec::new();
    for &readers in READERS {
        let mut messages = ValMap::new();
        let mut bandwidth = ValMap::new();
        let mut spread = FairnessMap::new();
        for &size in SIZES {
            for implementation in implementations {
                let row = &cohort[&(readers, size, implementation.key.into())];
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
    let rows: Vec<_> = panels
        .iter()
        .map(|(readers, bandwidth, messages, _)| (*readers, bandwidth, messages))
        .collect();
    let spread: Vec<_> = panels.iter().map(|(_, _, _, spread)| spread).collect();
    let directory = common::out_dir().join("dart");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("radiodish.svg");
    common::draw_multirow_throughput(
        &path,
        "RADIO/DISH throughput per subscriber, DART / TCP loopback",
        &rows,
        SIZES,
        implementations,
        &BTreeMap::new(),
        &|readers| format!("{readers} subscribers, payload pools, median and range of 3"),
        "",
        "",
        Some(&spread),
        common::detect_hardware().as_deref(),
    )?;
    eprintln!("Written: {}", path.display());
    Ok(())
}
