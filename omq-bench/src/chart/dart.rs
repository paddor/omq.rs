use std::collections::BTreeMap;

use plotters::style::RGBColor;

use super::common::{self, Impl, LatencyEntry, LatencyMap, ValMap};
use crate::jsonl;

const SIZES: &[u64] = &[16, 64, 256, 512, 1024, 4096, 16384];
// The chart binary builds without the transport feature. Match the codec version.
const DART_WIRE_VERSION: u64 = 1;
const IMPLS: &[Impl] = &[
    Impl {
        key: "dart-lan",
        label: "OMQ / Dart-LAN",
        threads: "1 IO",
        color: common::C_OMQ_MT,
    },
    Impl {
        key: "dart-adaptive",
        label: "OMQ / Dart-adaptive",
        threads: "1 IO",
        color: RGBColor(255, 183, 77),
    },
    Impl {
        key: "tcp",
        label: "OMQ / TCP",
        threads: "1 IO",
        color: common::C_OMQ_1T,
    },
    Impl {
        key: "aeron",
        label: "Aeron v1.53.3 / UDP",
        threads: "1 IO",
        color: common::C_MONOCOQUE,
    },
];

type Runs = BTreeMap<(u64, String, String), Vec<serde_json::Value>>;

fn eligible_runs() -> Runs {
    let rows = jsonl::load_jsonl::<serde_json::Value>(&jsonl::cache_dir().join("dart.jsonl"));
    let mut groups = Runs::new();
    for (_, row) in rows {
        if row["profiled"] != false
            || row["diagnostic"] == true
            || row["continuous_spin"] == true
            || row["continuous_io_spin"] == true
            || row["runtime"]
                .as_str()
                .is_some_and(|runtime| runtime != "owned")
            || row["spin_us"] != 50
            || row["io_spin_us"] != 50
            || row["workload_profile"] != row["kind"]
        {
            continue;
        }
        let Some(transport @ ("dart" | "tcp")) = row["transport"].as_str() else {
            continue;
        };
        if transport == "dart"
            && (row["dart_wire_version"] != DART_WIRE_VERSION
                || row["dart_window_messages"].as_u64().unwrap_or(256) != 256)
        {
            continue;
        }
        let Some(size) = row["msg_size"].as_u64().filter(|size| SIZES.contains(size)) else {
            continue;
        };
        if transport == "dart"
            && size > 1024
            && row["experimental_wire_version"]
                .as_u64()
                .is_some_and(|version| version < 7)
        {
            continue;
        }
        let Some(kind @ ("throughput" | "latency")) = row["kind"].as_str() else {
            continue;
        };
        if row["binary_sha256"].as_str().is_none() {
            continue;
        }
        let key = match transport {
            "dart" => match row["congestion"].as_str() {
                Some("lan") => "dart-lan",
                Some("adaptive") => "dart-adaptive",
                _ => continue,
            },
            _ => "tcp",
        };
        if kind == "throughput" {
            if row["recv_batching"] != true
                || row["sender"]["seconds"] != 3
                || row["missing_count"] != 0
                || row["excess_count"] != 0
                || row["sender"]["unacknowledged"] != 0
                || ["duplicates", "gaps", "corrupt"]
                    .iter()
                    .any(|field| row["receiver"][field] != 0)
            {
                continue;
            }
        } else if row["timeouts"] != 0
            || row["iterations"] != 100_000
            || row["warmup_iterations"] != 200_000
            || row["measurement_order"] != "rotate"
        {
            continue;
        }
        add_run(&mut groups, size, key, kind.to_owned(), row);
    }
    aeron_runs(&mut groups);
    groups
}

fn add_run(groups: &mut Runs, size: u64, key: &str, kind: String, row: serde_json::Value) {
    let group = groups.entry((size, key.into(), kind)).or_default();
    if group.last().is_some_and(|last| {
        last["binary_sha256"] != row["binary_sha256"] || last["cpus"] != row["cpus"]
    }) {
        group.clear();
    }
    group.push(row);
    if group.len() > 3 {
        group.remove(0);
    }
}

fn aeron_runs(groups: &mut Runs) {
    let rows = jsonl::load_jsonl::<serde_json::Value>(&jsonl::cache_dir().join("dart-aeron.jsonl"));
    for (_, row) in rows {
        if row["profiled"] != false
            || row["verified"] != true
            || row["pinned_driver"] != true
            || row["threading"] != "shared"
            || row["aeron_version"] != "1.53.3"
            || row["binary_sha256"].as_str().is_none()
        {
            continue;
        }
        let Some(size) = row["msg_size"].as_u64().filter(|size| SIZES.contains(size)) else {
            continue;
        };
        let Some(kind @ ("throughput" | "latency")) = row["kind"].as_str() else {
            continue;
        };
        if kind == "throughput" {
            if row["elapsed"].as_f64() != Some(3.0)
                || row["warmup_seconds"] != 3
                || row["drain_seconds"] != 2
            {
                continue;
            }
        } else if row["iterations"] != 100_000 || row["warmup_iterations"] != 200_000 {
            continue;
        }
        add_run(groups, size, "aeron", kind.to_owned(), row);
    }
}

pub(crate) fn generate() {
    let mut messages = ValMap::new();
    let mut bandwidth = ValMap::new();
    let mut latency = LatencyMap::new();
    for ((size, key, kind), mut group) in eligible_runs() {
        if group.len() != 3 {
            continue;
        }
        let metric = if kind == "throughput" {
            "msgs_s"
        } else {
            "p99_us"
        };
        if group
            .iter()
            .any(|row| row[metric].as_f64().is_none_or(|value| value <= 0.0))
        {
            continue;
        }
        group.sort_by(|a, b| {
            a[metric]
                .as_f64()
                .unwrap()
                .total_cmp(&b[metric].as_f64().unwrap())
        });
        let row = &group[1];
        if kind == "throughput" {
            let rate = row["msgs_s"].as_f64().unwrap();
            messages.entry(size).or_default().insert(key.clone(), rate);
            bandwidth
                .entry(size)
                .or_default()
                .insert(key, rate * size as f64 / 1e6);
        } else if let (Some(p50), Some(p99), Some(p999)) = (
            row["p50_us"].as_f64(),
            row["p99_us"].as_f64(),
            row["p999_us"].as_f64(),
        ) {
            latency
                .entry(size)
                .or_default()
                .insert(key, LatencyEntry { p50, p99, p999 });
        }
    }
    let directory = common::out_dir().join("dart");
    std::fs::create_dir_all(&directory).expect("create DART chart directory");
    if !messages.is_empty() {
        let path = directory.join("scattergather.svg");
        common::draw_throughput_single_panel(
            &path,
            "Received throughput, Dart / TCP / Aeron UDP loopback, 2-process, median of 3",
            SIZES,
            IMPLS,
            &bandwidth,
            &messages,
        )
        .expect("draw DART throughput chart");
        eprintln!("Written: {}", path.display());
    }
    if !latency.is_empty() {
        let path = directory.join("clientserver.svg");
        common::draw_latency_single_panel_100us(
            &path,
            "Echo RTT, Dart / TCP / Aeron UDP loopback, 2-process, median-p99 run of 3",
            SIZES,
            IMPLS,
            &latency,
            &BTreeMap::new(),
        )
        .expect("draw DART latency chart");
        eprintln!("Written: {}", path.display());
    }
}
