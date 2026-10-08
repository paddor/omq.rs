use std::collections::BTreeMap;

use super::common::{self, Impl, LatencyEntry, LatencyMap, ValMap};
use crate::jsonl;

const SIZES: &[u64] = &[16, 1024];
const IMPLS: &[Impl] = &[
    Impl {
        key: "stock",
        label: "Quinn DATAGRAM stock Tokio UDP",
        threads: "1 CT, no spin",
        color: common::C_OMQ_1T,
    },
    Impl {
        key: "poll",
        label: "Quinn DATAGRAM polling UDP",
        threads: "1 CT, 50 \u{03bc}s IO spin",
        color: common::C_OMQ_EXCLUSIVE,
    },
];

pub(crate) fn generate() {
    let mut messages = ValMap::new();
    let mut bandwidth = ValMap::new();
    let mut latency = LatencyMap::new();
    let rows =
        jsonl::load_jsonl::<serde_json::Value>(&jsonl::cache_dir().join("quinn-datagram.jsonl"));
    for (_, row) in rows {
        if row["profiled"] != false
            || row["transport"] != "quinn-datagram"
            || row["quinn_version"] != "0.11.12"
            || row["cipher"] != "TLS_AES_128_GCM_SHA256"
            || row["controller"] != "default Cubic"
            || row["layout"] != "inline"
            || row["application_future"] != "spawned task"
            || row["app_spin_us"] != 0
            || row["event_interval"] != 61
            || row["batch_messages"] != 256
            || row["duration_seconds"].as_f64() != Some(3.0)
        {
            continue;
        }
        let key = match (row["io_adapter"].as_str(), row["io_spin_us"].as_u64()) {
            (Some("stock Tokio"), Some(0)) => "stock",
            (Some("speculative UDP"), Some(50)) => "poll",
            _ => continue,
        };
        let Some(size) = row["msg_size"].as_u64().filter(|size| SIZES.contains(size)) else {
            continue;
        };
        match row["kind"].as_str() {
            Some("throughput") => {
                if let Some(rate) = row["msgs_s"].as_f64().filter(|rate| *rate > 0.0) {
                    messages.entry(size).or_default().insert(key.into(), rate);
                    bandwidth
                        .entry(size)
                        .or_default()
                        .insert(key.into(), rate * size as f64 / 1e6);
                }
            }
            Some("latency")
                if row["timeouts"] == 0
                    && row["iterations"] == 100_000
                    && row["warmup_iterations"] == 20_000 =>
            {
                if let (Some(p50), Some(p99), Some(p999)) = (
                    row["p50_us"].as_f64(),
                    row["p99_us"].as_f64(),
                    row["p999_us"].as_f64(),
                ) {
                    latency
                        .entry(size)
                        .or_default()
                        .insert(key.into(), LatencyEntry { p50, p99, p999 });
                }
            }
            _ => {}
        }
    }
    let directory = common::out_dir().join("quinn-datagram");
    std::fs::create_dir_all(&directory).expect("create Quinn DATAGRAM chart directory");
    if !messages.is_empty() {
        let path = directory.join("throughput.svg");
        common::draw_throughput_dual_panel(
            &path,
            "Raw Quinn DATAGRAM received throughput, AES-GCM, 2-process loopback",
            SIZES,
            IMPLS,
            &bandwidth,
            &messages,
            &BTreeMap::new(),
            "",
            "",
        )
        .expect("draw Quinn DATAGRAM throughput chart");
        eprintln!("Written: {}", path.display());
    }
    if !latency.is_empty() {
        let path = directory.join("latency.svg");
        common::draw_latency_single_panel(
            &path,
            "Raw Quinn DATAGRAM echo RTT, AES-GCM, 2-process loopback",
            SIZES,
            IMPLS,
            &latency,
            &BTreeMap::new(),
            common::auto_lat_range(&latency),
        )
        .expect("draw Quinn DATAGRAM latency chart");
        eprintln!("Written: {}", path.display());
    }
}
