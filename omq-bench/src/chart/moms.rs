//! Matched OMQ transport cohorts and protocol comparison charts.

use std::collections::BTreeMap;

use serde_json::Value;

use super::common::{self, CpuData, Impl, LatencyEntry, LatencyMap, ValMap};
use crate::jsonl;

const THROUGHPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
const LATENCY_SIZES: &[u64] = &[16, 32, 64, 256, 1024, 4096];
const LOCAL_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384, 8_388_608];
const LOCAL_LATENCY_SIZES: &[u64] = &[16, 64, 256, 1024, 4096];
const NETWORK: &[Impl] = &[
    Impl {
        key: "omq-tokio-1t",
        label: "OMQ / TCP",
        threads: "1 IO",
        color: common::C_OMQ_1T,
    },
    Impl {
        key: "omq-quic",
        label: "OMQ / QUIC",
        threads: "1 IO",
        color: common::C_OMQ_QUIC,
    },
    Impl {
        key: "omq-dart",
        label: "OMQ / DART",
        threads: "1 IO",
        color: plotters::style::RGBColor(255, 183, 77),
    },
    Impl {
        key: "grpc-rust",
        label: "gRPC over HTTP/2",
        threads: "",
        color: common::C_GRPC,
    },
    Impl {
        key: "rabbitmq",
        label: "AMQP 0-9-1",
        threads: "RabbitMQ",
        color: common::C_RABBITMQ,
    },
    Impl {
        key: "aeron-udp-2proc",
        label: "Aeron / UDP",
        threads: "1 IO (SHARED)",
        color: common::C_AERON,
    },
    Impl {
        key: "nats",
        label: "NATS",
        threads: "nats-server",
        color: common::C_NATS,
    },
    Impl {
        key: "redis-streams",
        label: "Redis Streams",
        threads: "Redis",
        color: common::C_REDIS,
    },
    Impl {
        key: "zenoh-tcp-2proc",
        label: "zenoh / TCP",
        threads: "",
        color: common::C_ZENOH,
    },
    Impl {
        key: "iroh-quic-2proc",
        label: "iroh / QUIC",
        threads: "",
        color: common::C_IROH,
    },
];
const LOCAL: &[Impl] = &[
    Impl {
        key: "omq-ipc",
        label: "OMQ / IPC",
        threads: "1 IO",
        color: plotters::style::RGBColor(239, 83, 168),
    },
    Impl {
        key: "omq-shm",
        label: "OMQ / SHM",
        threads: "1 IO",
        color: plotters::style::RGBColor(29, 78, 216),
    },
    Impl {
        key: "omq-shm-pool",
        label: "OMQ / SHM pool",
        threads: "1 IO",
        color: plotters::style::RGBColor(0, 191, 165),
    },
    Impl {
        key: "iceoryx2-shm-2proc",
        label: "iceoryx2 / SHM",
        threads: "polling; copy-in",
        color: plotters::style::RGBColor(206, 147, 216),
    },
];

fn transport(key: &str) -> &str {
    match key {
        "omq-dart" => "dart",
        "omq-ipc" => "ipc",
        "omq-quic" | "iroh-quic-2proc" => "quic",
        "aeron-udp-2proc" => "udp",
        "iceoryx2-shm-2proc" => "shm",
        _ => "tcp",
    }
}

fn latency_impls(base: &[Impl]) -> Vec<Impl> {
    let mut impls = Vec::new();
    for imp in base {
        if imp.key.starts_with("omq-") {
            impls.push(*imp);
            impls.push(Impl {
                key: match imp.key {
                    "omq-tokio-1t" => "omq-tokio-1t-spin50",
                    "omq-quic" => "omq-quic-spin50",
                    "omq-dart" => "omq-dart-spin50",
                    "omq-ipc" => "omq-ipc-spin50",
                    "omq-shm" => "omq-shm-spin50",
                    "omq-shm-pool" => "omq-shm-pool-spin50",
                    _ => unreachable!(),
                },
                threads: if imp.key == "omq-dart" {
                    "1 IO; 50 \u{03bc}s app/IO"
                } else {
                    "1 IO; 50 \u{03bc}s recv"
                },
                ..*imp
            });
        } else {
            impls.push(*imp);
        }
    }
    impls
}

fn valid_omq_row(row: &Value, kind: &str, transport: &str, spin: u64) -> bool {
    if row["kind"] != kind
        || row["transport"] != transport
        || row["profiled"] != false
        || row["diagnostic"] != false
        || row["dirty"] != false
        || row["continuous_spin"] != false
        || row["continuous_io_spin"] != false
        || row["runtime"] != "owned"
        || row["runtime_polling"] != false
        || row["io_threads"] != 1
        || row["spin_us"] != spin
        || row["io_spin_us"] != if transport == "ipc" { 0 } else { spin }
        || row["workload_profile"] != kind
        || row["measurement_order"] != "rotate"
        || row["cpus"] != "1,2,0,3,5,4"
        || row["binary_sha256"].as_str().is_none()
    {
        return false;
    }
    if transport == "dart"
        && (row["dart_wire_version"] != 1
            || row["congestion"] != "adaptive"
            || row["dart_window_messages"] != if kind == "throughput" { 512 } else { 256 })
    {
        return false;
    }
    for side in ["sender", "receiver"] {
        for field in ["send_failures", "receive_failures", "invalid_datagrams"] {
            if row[side][field].as_u64().unwrap_or(0) != 0 {
                return false;
            }
        }
    }
    if kind == "throughput" {
        row["sender"]["seconds"] == 3
            && row["receiver"]["seconds"] == 3
            && row["warmup_seconds"] == 1.0
            && row["drain_seconds"] == 2.0
            && row["recv_batching"] == true
            && row["missing_count"] == 0
            && row["excess_count"] == 0
            && row["sender"]["offered"] == row["receiver"]["received_total"]
            && row["sender"]["unacknowledged"] == 0
            && ["duplicates", "gaps", "corrupt", "receive_overflow"]
                .iter()
                .all(|field| row["receiver"][field].as_u64().unwrap_or(0) == 0)
    } else {
        row["iterations"] == 10_000
            && row["warmup_iterations"] == 2_000
            && row["recv_batching"] == false
            && row["timeouts"] == 0
            && row["p50_us"]
                .as_f64()
                .zip(row["p99_us"].as_f64())
                .zip(row["p999_us"].as_f64())
                .is_some_and(|((p50, p99), p999)| p50 > 0.0 && p50 <= p99 && p99 <= p999)
    }
}

fn omq_rows(kind: &str, transport: &str, sizes: &[u64], spin: u64) -> BTreeMap<u64, Value> {
    let rows = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("mom-omq.jsonl"));
    let mut groups: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
    let mut complete = BTreeMap::new();
    let metric = if kind == "throughput" {
        "msgs_s"
    } else {
        "p99_us"
    };
    for (_, row) in rows {
        let Some(size) = row["msg_size"].as_u64().filter(|size| sizes.contains(size)) else {
            continue;
        };
        if !valid_omq_row(&row, kind, transport, spin)
            || row[metric]
                .as_f64()
                .is_none_or(|value| !value.is_finite() || value <= 0.0)
        {
            continue;
        }
        let group = groups.entry(size).or_default();
        if row["repeat"] == 1
            || group.last().is_some_and(|last| {
                last["binary_sha256"] != row["binary_sha256"] || last["revision"] != row["revision"]
            })
        {
            group.clear();
        }
        if row["repeat"].as_u64() != Some(group.len() as u64 + 1) {
            continue;
        }
        group.push(row);
        if group.len() == 3 {
            let mut selected = group.clone();
            selected.sort_by(|a, b| {
                a[metric]
                    .as_f64()
                    .unwrap()
                    .total_cmp(&b[metric].as_f64().unwrap())
            });
            complete.insert(size, selected.remove(1));
        }
    }
    complete
}

fn throughput(impls: &[Impl]) -> (ValMap, ValMap, BTreeMap<String, CpuData>) {
    let (mut bandwidth, mut messages, mut cpu) = (ValMap::new(), ValMap::new(), BTreeMap::new());
    for imp in impls {
        if let Some(shm_key) = local_shm_key(imp.key) {
            for (size, row) in local_shm_rows("shm-pushpull", shm_key, LOCAL_SIZES, None) {
                let rate = row["msgs_s"].as_f64().unwrap();
                messages
                    .entry(size)
                    .or_default()
                    .insert(imp.key.into(), rate);
                bandwidth
                    .entry(size)
                    .or_default()
                    .insert(imp.key.into(), rate * size as f64 / 1e6);
            }
        } else if imp.key == "omq-dart" {
            for (size, row) in omq_rows("throughput", "dart", THROUGHPUT_SIZES, 50) {
                let rate = row["msgs_s"].as_f64().unwrap();
                messages
                    .entry(size)
                    .or_default()
                    .insert(imp.key.into(), rate);
                bandwidth
                    .entry(size)
                    .or_default()
                    .insert(imp.key.into(), rate * size as f64 / 1e6);
            }
        } else {
            let (b, m, c) = common::load_tput(
                "throughput",
                transport(imp.key),
                None,
                std::slice::from_ref(imp),
            );
            for (size, values) in b {
                bandwidth.entry(size).or_default().extend(values);
            }
            for (size, values) in m {
                messages.entry(size).or_default().extend(values);
            }
            cpu.extend(c);
        }
    }
    (bandwidth, messages, cpu)
}

fn local_shm_key(key: &str) -> Option<&'static str> {
    match key {
        "omq-ipc" | "omq-ipc-spin50" => Some("ipc"),
        "omq-shm" | "omq-shm-spin50" => Some("shm"),
        "omq-shm-pool" | "omq-shm-pool-spin50" => Some("shm-pool"),
        _ => None,
    }
}

fn local_shm_rows(
    family: &str,
    implementation: &str,
    sizes: &[u64],
    spin: Option<u64>,
) -> BTreeMap<u64, Value> {
    let path = jsonl::cache_dir().join(format!("{family}.jsonl"));
    jsonl::load_jsonl::<Value>(&path)
        .into_iter()
        .filter_map(|(_, row)| {
            let size = row["msg_size"].as_u64()?;
            (sizes.contains(&size)
                && row["kind"]
                    == if family == "shm-pushpull" {
                        "shm_pushpull_verified"
                    } else {
                        "shm_reqrep_verified"
                    }
                && row["impl"] == implementation
                && row["socket_pair"]
                    == if spin.is_some() {
                        "client-server"
                    } else {
                        "scatter-gather"
                    }
                && row["recv_spin_us"] == spin.unwrap_or(0)
                && row["peers"] == 1
                && row["binary_sha256"].as_str().is_some())
            .then_some((size, row))
        })
        .collect()
}

fn latest_complete_comparison<'a>(
    rows: &'a [(usize, jsonl::ComparisonRow)],
    imp: &Impl,
    sizes: &[u64],
) -> Option<BTreeMap<u64, &'a jsonl::ComparisonRow>> {
    let mut groups: BTreeMap<&str, (usize, BTreeMap<u64, &jsonl::ComparisonRow>)> = BTreeMap::new();
    for (seq, row) in rows {
        if row.kind != "latency"
            || row.impl_name != imp.key
            || row.transport != transport(imp.key)
            || !sizes.contains(&row.msg_size)
            || row
                .latency_pair
                .as_deref()
                .is_some_and(|pair| pair != "req-rep")
            || row
                .workload_profile
                .as_deref()
                .is_some_and(|profile| profile != "default")
            || !row
                .p50_us
                .zip(row.p99_us)
                .zip(row.p999_us)
                .is_some_and(|((p50, p99), p999)| p50 > 0.0 && p50 <= p99 && p99 <= p999)
        {
            continue;
        }
        let group = groups.entry(&row.run_id).or_default();
        group.0 = *seq;
        group.1.insert(row.msg_size, row);
    }
    groups
        .into_values()
        .filter(|(_, group)| sizes.iter().all(|size| group.contains_key(size)))
        .max_by_key(|(seq, _)| *seq)
        .map(|(_, group)| group)
}

fn latency(impls: &[Impl], sizes: &[u64]) -> (LatencyMap, BTreeMap<String, CpuData>) {
    let mut latency = LatencyMap::new();
    let comparison_rows: Vec<(usize, jsonl::ComparisonRow)> =
        jsonl::load_jsonl(&jsonl::cache_dir().join("comparisons.jsonl"));
    for imp in impls {
        let spin = if imp.key.ends_with("-spin50") { 50 } else { 0 };
        if let Some(shm_key) = local_shm_key(imp.key) {
            for (size, row) in local_shm_rows("shm-reqrep", shm_key, sizes, Some(spin)) {
                latency.entry(size).or_default().insert(
                    imp.key.into(),
                    LatencyEntry {
                        p50: row["p50_us"].as_f64().unwrap(),
                        p99: row["p99_us"].as_f64().unwrap(),
                        p999: row["p999_us"].as_f64().unwrap(),
                    },
                );
            }
        } else if imp.key.starts_with("omq-") {
            let base = imp.key.strip_suffix("-spin50").unwrap_or(imp.key);
            for (size, row) in omq_rows("latency", transport(base), sizes, spin) {
                latency.entry(size).or_default().insert(
                    imp.key.into(),
                    LatencyEntry {
                        p50: row["p50_us"].as_f64().unwrap(),
                        p99: row["p99_us"].as_f64().unwrap(),
                        p999: row["p999_us"].as_f64().unwrap(),
                    },
                );
            }
        } else if let Some(group) = latest_complete_comparison(&comparison_rows, imp, sizes) {
            for (size, row) in group {
                latency.entry(size).or_default().insert(
                    imp.key.into(),
                    LatencyEntry {
                        p50: row.p50_us.unwrap(),
                        p99: row.p99_us.unwrap(),
                        p999: row.p999_us.unwrap(),
                    },
                );
            }
        }
    }
    (latency, BTreeMap::new())
}

fn require_points<T>(
    values: &BTreeMap<u64, BTreeMap<String, T>>,
    sizes: &[u64],
    impls: &[Impl],
    kind: &str,
) {
    for imp in impls.iter().filter(|imp| {
        kind == "latency" || imp.key.starts_with("omq-") || imp.key.ends_with("-2proc")
    }) {
        for size in sizes {
            assert!(
                values
                    .get(size)
                    .is_some_and(|point| point.contains_key(imp.key)),
                "missing complete {kind} cohort: {} {size} B; see RUNNING_BENCHMARKS.md",
                imp.key,
            );
        }
    }
}

pub(crate) fn generate() {
    let fanout = super::moms_fanout::prepare();
    let directory = common::out_dir().join("moms");
    std::fs::create_dir_all(&directory).expect("create MOM chart directory");
    for (name, sizes, base) in [
        ("network", THROUGHPUT_SIZES, NETWORK),
        ("local", LOCAL_SIZES, LOCAL),
    ] {
        let (bandwidth, messages, cpu) = throughput(base);
        require_points(&messages, sizes, base, "throughput");
        let path = directory.join(format!("throughput_{name}.svg"));
        let (sender_cpu_label, broker_cpu_label, receiver_cpu_label) = if name == "local" {
            ("", "", "")
        } else {
            ("snd CPU%", "broker CPU%", "rcv CPU%")
        };
        let title = format!("Producer/consumer throughput, {name}, one flow");
        let spec = common::ChartSpec {
            out_path: &path,
            title: &title,
            sizes,
            impls: base,
        };
        let columns = common::CpuColumns {
            cpu: &cpu,
            snd_label: sender_cpu_label,
            rcv_label: receiver_cpu_label,
        };
        common::draw_throughput_dual_panel_brokered_with_versions(
            &spec,
            &bandwidth,
            &messages,
            &columns,
            broker_cpu_label,
        )
        .expect("draw MOM throughput chart");
        eprintln!("Written: {}", path.display());
    }
    for (name, sizes, base) in [
        ("network", LATENCY_SIZES, NETWORK),
        ("local", LOCAL_LATENCY_SIZES, LOCAL),
    ] {
        let impls = latency_impls(base);
        let (latency, cpu) = latency(&impls, sizes);
        require_points(&latency, sizes, &impls, "latency");
        let path = directory.join(format!("latency_{name}.svg"));
        let title = if name == "network" {
            "Echo RTT, network"
        } else {
            "Echo RTT, local"
        };
        let spec = common::ChartSpec {
            out_path: &path,
            title,
            sizes,
            impls: &impls,
        };
        common::draw_latency_brokered_with_versions(&spec, &latency, &cpu, name == "local")
            .expect("draw MOM latency chart");
        eprintln!("Written: {}", path.display());
    }
    super::moms_fanout::draw(&fanout);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_newer_latency_run_does_not_fill_a_complete_cohort() {
        let imp = NETWORK.iter().find(|imp| imp.key == "nats").unwrap();
        let rows: Vec<_> = [
            (0, "complete", 16, 10.0),
            (1, "complete", 32, 11.0),
            (2, "complete", 64, 12.0),
            (3, "partial", 16, 20.0),
            (4, "partial", 64, 22.0),
        ]
        .into_iter()
        .map(|(seq, run, size, p99)| {
            (
                seq,
                jsonl::ComparisonRow {
                    run_id: run.into(),
                    impl_name: "nats".into(),
                    kind: "latency".into(),
                    transport: "tcp".into(),
                    msg_size: size,
                    p50_us: Some(p99 / 2.0),
                    p99_us: Some(p99),
                    p999_us: Some(p99 * 2.0),
                    ..Default::default()
                },
            )
        })
        .collect();
        let cohort = latest_complete_comparison(&rows, imp, &[16, 32, 64]).unwrap();
        assert_eq!(cohort[&16].p99_us, Some(10.0));
        assert_eq!(cohort[&32].p99_us, Some(11.0));
        assert_eq!(cohort[&64].p99_us, Some(12.0));
    }
}
