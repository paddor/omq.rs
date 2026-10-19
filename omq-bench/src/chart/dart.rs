use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use plotters::style::RGBColor;

use super::common::{self, ChartSpec, CpuColumns, Impl, LatencyEntry, LatencyMap, ValMap};
use crate::jsonl;

mod radiodish;

use crate::bench::datagram::{LATENCY_SIZES, THROUGHPUT_SIZES};
// The chart binary builds without the transport feature. Match the codec version.
const DART_WIRE_VERSION: u64 = 1;
const IMPLS: &[Impl] = &[
    Impl {
        key: "dart-lan",
        label: "OMQ / DART-LAN",
        threads: "1 IO",
        color: common::C_OMQ_MT,
    },
    Impl {
        key: "dart-adaptive",
        label: "OMQ / DART-adaptive",
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
        threads: "1 IO (SHARED)",
        color: common::C_MONOCOQUE,
    },
];

type Runs = BTreeMap<(u64, String, String), Vec<serde_json::Value>>;

fn eligible_runs() -> Runs {
    let rows = jsonl::load_jsonl::<serde_json::Value>(&jsonl::cache_dir().join("dart.jsonl"));
    let mut groups = Runs::new();

    for (_, row) in rows {
        let Some((size, key, kind)) = run_slot(&row) else {
            continue;
        };
        let kind = kind.to_owned();
        add_run(&mut groups, size, key, kind, row);
    }

    aeron_runs(&mut groups);
    groups
}

/// Size, series key, and kind of a chartable DART or TCP row, or `None`
/// for rows from other settings, diagnostics, or with transfer errors.
fn run_slot(row: &serde_json::Value) -> Option<(u64, &'static str, &str)> {
    if settings_rejected(row) {
        return None;
    }
    let Some(transport @ ("dart" | "tcp")) = row["transport"].as_str() else {
        return None;
    };
    if transport == "dart"
        && (row["dart_wire_version"] != DART_WIRE_VERSION
            || row["dart_window_messages"].as_u64().unwrap_or(256) != 256)
    {
        return None;
    }

    let size = row["msg_size"]
        .as_u64()
        .filter(|size| THROUGHPUT_SIZES.contains(size))?;
    if transport == "dart"
        && size > 1024
        && row["experimental_wire_version"]
            .as_u64()
            .is_some_and(|version| version < 7)
    {
        return None;
    }

    let Some(kind @ ("throughput" | "latency")) = row["kind"].as_str() else {
        return None;
    };
    // Runs are grouped by binary, so rows without a digest cannot be used.
    row["binary_sha256"].as_str()?;

    let key = match transport {
        "dart" => match row["congestion"].as_str() {
            Some("lan") => "dart-lan",
            Some("adaptive") => "dart-adaptive",
            _ => return None,
        },
        _ => "tcp",
    };
    let rejected = if kind == "throughput" {
        throughput_rejected(row)
    } else {
        latency_rejected(row)
    };
    (!rejected).then_some((size, key, kind))
}

/// Charts compare one setup: unprofiled owned-runtime runs with 50 us app
/// and IO spin, no continuous spin, and the workload profile of the kind.
fn settings_rejected(row: &serde_json::Value) -> bool {
    row["profiled"] != false
        || row["diagnostic"] == true
        || row["continuous_spin"] == true
        || row["continuous_io_spin"] == true
        || row["runtime"]
            .as_str()
            .is_some_and(|runtime| runtime != "owned")
        || row["spin_us"] != 50
        || row["io_spin_us"] != 50
        || row["workload_profile"] != row["kind"]
}

/// Throughput rows need the standard timing and a loss-free transfer.
fn throughput_rejected(row: &serde_json::Value) -> bool {
    row["recv_batching"] != true
        || row["sender"]["seconds"] != 3
        || row["warmup_seconds"] != 0.2
        || row["drain_seconds"].as_f64().is_some_and(|seconds| {
            Duration::try_from_secs_f64(seconds) != Ok(Duration::from_secs(2))
        })
        || row["missing_count"] != 0
        || row["excess_count"] != 0
        || row["sender"]["unacknowledged"] != 0
        || ["duplicates", "gaps", "corrupt"]
            .iter()
            .any(|field| row["receiver"][field] != 0)
}

/// Latency rows need the standard iteration counts and no timeouts.
fn latency_rejected(row: &serde_json::Value) -> bool {
    row["timeouts"] != 0
        || row["iterations"] != 100_000
        || row["warmup_iterations"] != 200_000
        || row["measurement_order"] != "rotate"
}

/// Keeps the last three runs per slot. A new binary or CPU set restarts the
/// group, so the median never mixes builds or pinning.
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
        let Some(size) = row["msg_size"]
            .as_u64()
            .filter(|size| THROUGHPUT_SIZES.contains(size))
        else {
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

/// Median run of each complete group: messages/s, MB/s, and latency.
fn median_runs() -> (ValMap, ValMap, LatencyMap) {
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

        // Sort by the charted metric; the middle run is the median.
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

    (messages, bandwidth, latency)
}

pub(crate) fn generate() {
    radiodish::generate().expect("generate DART RADIO/DISH chart");
    let (messages, bandwidth, latency) = median_runs();

    let directory = common::out_dir().join("dart");
    std::fs::create_dir_all(&directory).expect("create DART chart directory");

    if !messages.is_empty() {
        draw_throughput(&directory, &bandwidth, &messages);
    }
    if !latency.is_empty() {
        draw_latency(&directory, &latency);
    }
}

fn draw_throughput(directory: &Path, bandwidth: &ValMap, messages: &ValMap) {
    let path = directory.join("scattergather.svg");
    let spec = ChartSpec {
        out_path: &path,
        title: "Received throughput, DART / TCP / Aeron UDP loopback, 2-process, median of 3",
        sizes: THROUGHPUT_SIZES,
        impls: IMPLS,
    };
    let columns = CpuColumns {
        cpu: &BTreeMap::new(),
        snd_label: "",
        rcv_label: "",
    };
    common::draw_throughput_dual_panel(&spec, bandwidth, messages, &columns)
        .expect("draw DART throughput chart");
    eprintln!("Written: {}", path.display());
}

fn draw_latency(directory: &Path, latency: &LatencyMap) {
    let path = directory.join("clientserver.svg");

    // Latency runs spin, so the threads column names the spin setup.
    let implementations: Vec<_> = IMPLS
        .iter()
        .map(|implementation| Impl {
            key: implementation.key,
            label: implementation.label,
            threads: match implementation.key {
                "aeron" => implementation.threads,
                "tcp" => "1 IO, recv spin 50 us",
                _ => "1 IO, app/IO spin 50 us",
            },
            color: implementation.color,
        })
        .collect();

    let spec = ChartSpec {
        out_path: &path,
        title: "Echo RTT, DART / TCP / Aeron UDP loopback, 2-process, median-p99 run of 3",
        sizes: LATENCY_SIZES,
        impls: &implementations,
    };
    common::draw_latency_single_panel_100us(&spec, latency, &BTreeMap::new())
        .expect("draw DART latency chart");
    eprintln!("Written: {}", path.display());
}
