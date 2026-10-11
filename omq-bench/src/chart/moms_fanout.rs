//! Verified delivery to every subscriber socket, normalized per subscriber.

use std::collections::BTreeMap;

use plotters::style::RGBColor;
use serde_json::Value;

use super::common::{self, FairnessEntry, FairnessMap, Impl, ValMap};
use crate::jsonl;

const SIZES: &[u64] = common::COMPARISON_SIZES;
const READERS: &[u64] = &[1, 4, 32];
const NETWORK: &[Impl] = &[
    Impl {
        key: "omq-tcp",
        label: "OMQ / TCP",
        threads: "1 IO/context",
        color: common::C_OMQ_1T,
    },
    Impl {
        key: "omq-quic",
        label: "OMQ / QUIC",
        threads: "1 IO/context",
        color: common::C_OMQ_QUIC,
    },
    Impl {
        key: "omq-dart",
        label: "OMQ / DART",
        threads: "1 IO/context; RADIO/DISH",
        color: RGBColor(255, 183, 77),
    },
    Impl {
        key: "libzmq-tcp",
        label: "libzmq 4.3.4 / TCP",
        threads: "1 IO/context",
        color: common::C_LIBZMQ,
    },
    Impl {
        key: "zenoh-tcp",
        label: "zenoh 1.10.1 / TCP",
        threads: "one shared receiver session",
        color: common::C_ZENOH,
    },
    Impl {
        key: "aeron-udp",
        label: "Aeron 1.51.0 / UDP",
        threads: "SHARED driver/process; shared image",
        color: common::C_AERON,
    },
];
const LOCAL: &[Impl] = &[
    Impl {
        key: "omq-ipc",
        label: "OMQ / IPC",
        threads: "1 IO/context",
        color: RGBColor(239, 83, 168),
    },
    Impl {
        key: "libzmq-ipc",
        label: "libzmq 4.3.4 / IPC",
        threads: "1 IO/context",
        color: common::C_LIBZMQ,
    },
    Impl {
        key: "iceoryx2-shm",
        label: "iceoryx2 0.10.0 / SHM",
        threads: "polling; copy-in/borrowed",
        color: RGBColor(206, 147, 216),
    },
    Impl {
        key: "aeron-ipc",
        label: "Aeron 1.51.0 / IPC",
        threads: "one shared SHARED driver",
        color: common::C_AERON,
    },
];

pub(super) struct Chart {
    filename: &'static str,
    title: &'static str,
    implementations: &'static [Impl],
    rows: Vec<(u64, ValMap, ValMap, FairnessMap)>,
}

fn verified(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    if row["kind"] != "pub_sub_verified"
        || row["impl"] != implementation
        || row["msg_size"] != size
        || row["peers"] != readers
        || row["subscriber_processes"] != 1
        || row["subscriber_threads"] != readers
        || row["process_layout"] != "two-process-shared-receiver"
        || row["warmup_seconds"] != 1
        || row["seconds"] != 3
        || row["publisher_cpus"] != "1,2"
        || row["subscriber_cpus"] != "3,4"
        || row["copy_in"] != true
        || row["binary_sha256"].as_str().is_none()
    {
        return false;
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

pub(super) fn prepare() -> Vec<Chart> {
    let cached: Vec<_> = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("mom-fanout.jsonl"))
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let mut charts = Vec::new();
    for (filename, title, implementations) in [
        (
            "fanout_network.svg",
            "Verified pub/sub fan-out, network protocols on loopback",
            NETWORK,
        ),
        (
            "fanout_local.svg",
            "Verified pub/sub fan-out, local IPC / shared memory",
            LOCAL,
        ),
    ] {
        let mut rows = Vec::new();
        for &readers in READERS {
            let mut bandwidth = ValMap::new();
            let mut messages = ValMap::new();
            let mut spread = FairnessMap::new();
            for implementation in implementations {
                for &size in SIZES {
                    let row = cached
                        .iter()
                        .rev()
                        .find(|row| verified(row, implementation.key, size, readers))
                        .unwrap_or_else(|| {
                            panic!(
                                "missing verified fan-out cohort: {} {size} B {readers} readers; run scripts/rivals/run_fanout.rb",
                                implementation.key
                            )
                        });
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
            rows.push((readers, bandwidth, messages, spread));
        }
        charts.push(Chart {
            filename,
            title,
            implementations,
            rows,
        });
    }
    charts
}

pub(super) fn draw(charts: &[Chart]) {
    for chart in charts {
        let rows: Vec<_> = chart
            .rows
            .iter()
            .map(|(readers, bandwidth, messages, _)| (*readers, bandwidth, messages))
            .collect();
        let spread: Vec<_> = chart.rows.iter().map(|(_, _, _, spread)| spread).collect();
        let path = common::out_dir().join("moms").join(chart.filename);
        common::draw_multirow_throughput(
            &path,
            chart.title,
            &rows,
            SIZES,
            chart.implementations,
            &BTreeMap::new(),
            &|readers| {
                let suffix = if readers == 1 { "" } else { "s" };
                format!(
                    "{readers} subscriber{suffix}; 2 processes, shared receiver; publications/s per reader"
                )
            },
            "",
            "",
            Some(&spread),
        )
        .expect("draw verified MOM fan-out chart");
        eprintln!("Written: {}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn complete() -> Value {
        let rounds: Vec<_> = (1..=3)
            .map(|round| {
                json!({
                    "round":round, "offered":3000, "reader_counts":[3000,3000,3000,3000],
                    "reader_elapsed":[3.0,3.0,3.0,4.0], "sender_elapsed":3.0,
                    "elapsed":4.0,"msgs_s":750.0,"missing":0,"duplicates":0,"gaps":0,"corrupt":0
                })
            })
            .collect();
        json!({"kind":"pub_sub_verified","impl":"omq-tcp","msg_size":16,"peers":4,
            "subscriber_processes":1,"subscriber_threads":4,"process_layout":"two-process-shared-receiver","warmup_seconds":1,"seconds":3,
            "publisher_cpus":"1,2","subscriber_cpus":"3,4","copy_in":true,
            "binary_sha256":"test","rounds":rounds})
    }

    #[test]
    fn requires_every_reader_and_includes_slowest_drain() {
        let mut row = complete();
        assert!(verified(&row, "omq-tcp", 16, 4));
        row["rounds"][0]["msgs_s"] = json!(1000.0);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["rounds"][1]["reader_counts"][3] = json!(2999);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["rounds"][2]["gaps"] = json!(1);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["subscriber_processes"] = json!(4);
        assert!(!verified(&row, "omq-tcp", 16, 4));
    }
}
