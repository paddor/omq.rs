//! Matched OMQ transport cohorts and protocol comparison charts.

use std::collections::BTreeMap;

use serde_json::Value;

use super::common::{self, CpuData, Impl, LatencyEntry, LatencyMap, ValMap};
use crate::jsonl;

const THROUGHPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
const LATENCY_SIZES: &[u64] = &[16, 32, 64, 256, 1024, 4096];
const IMPLS: &[Impl] = &[
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

fn transport(key: &str) -> &str {
    match key {
        "omq-dart" => "dart",
        "omq-quic" | "iroh-quic-2proc" => "quic",
        "aeron-udp-2proc" => "udp",
        _ => "tcp",
    }
}

fn latency_impls(spinning: bool) -> Vec<Impl> {
    IMPLS
        .iter()
        .copied()
        .filter_map(|mut imp| {
            if imp.key.starts_with("omq-") {
                if spinning {
                    imp.threads = match imp.key {
                        "omq-tokio-1t" | "omq-quic" => "1 IO; 50 us recv",
                        "omq-dart" => "1 IO; 50 us app/IO",
                        _ => unreachable!(),
                    };
                }
                Some(imp)
            } else if (imp.key == "aeron-udp-2proc") == spinning {
                Some(imp)
            } else {
                None
            }
        })
        .collect()
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
        || row["io_spin_us"] != spin
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

fn throughput() -> (ValMap, ValMap, BTreeMap<String, CpuData>) {
    let (mut bandwidth, mut messages, mut cpu) = (ValMap::new(), ValMap::new(), BTreeMap::new());
    for imp in IMPLS {
        if imp.key == "omq-dart" {
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

fn latency(spinning: bool, impls: &[Impl]) -> (LatencyMap, BTreeMap<String, CpuData>) {
    let (mut latency, mut cpu) = (LatencyMap::new(), BTreeMap::new());
    for imp in impls {
        if imp.key.starts_with("omq-") {
            for (size, row) in omq_rows(
                "latency",
                transport(imp.key),
                LATENCY_SIZES,
                if spinning { 50 } else { 0 },
            ) {
                latency.entry(size).or_default().insert(
                    imp.key.into(),
                    LatencyEntry {
                        p50: row["p50_us"].as_f64().unwrap(),
                        p99: row["p99_us"].as_f64().unwrap(),
                        p999: row["p999_us"].as_f64().unwrap(),
                    },
                );
            }
        } else {
            let (other, c) =
                common::load_latency(transport(imp.key), LATENCY_SIZES, std::slice::from_ref(imp));
            for (size, values) in other {
                latency.entry(size).or_default().extend(values);
            }
            cpu.extend(c);
        }
    }
    (latency, cpu)
}

fn require_omq_points<T>(values: &BTreeMap<u64, BTreeMap<String, T>>, sizes: &[u64], kind: &str) {
    for imp in IMPLS.iter().filter(|imp| imp.key.starts_with("omq-")) {
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
    let directory = common::out_dir().join("moms");
    std::fs::create_dir_all(&directory).expect("create MOM chart directory");
    let (bandwidth, messages, cpu) = throughput();
    require_omq_points(&messages, THROUGHPUT_SIZES, "throughput");
    let latencies: Vec<_> = [false, true]
        .into_iter()
        .map(|spinning| {
            let impls = latency_impls(spinning);
            let (values, cpu) = latency(spinning, &impls);
            require_omq_points(
                &values,
                LATENCY_SIZES,
                if spinning { "spin latency" } else { "latency" },
            );
            (spinning, impls, values, cpu)
        })
        .collect();
    let path = directory.join("throughput.svg");
    common::draw_throughput_dual_panel_brokered_with_versions(
        &path,
        "Producer/consumer throughput, loopback, one flow",
        THROUGHPUT_SIZES,
        IMPLS,
        &bandwidth,
        &messages,
        &cpu,
        "snd CPU%",
        "broker CPU%",
        "rcv CPU%",
    )
    .expect("draw MOM throughput chart");
    eprintln!("Written: {}", path.display());
    for (spinning, impls, latency, cpu) in latencies {
        let path = directory.join(if spinning {
            "latency_spin.svg"
        } else {
            "latency.svg"
        });
        let title = if spinning {
            "Echo RTT, loopback, receive spinning"
        } else {
            "Echo RTT, loopback, no explicit receive spinning"
        };
        common::draw_latency_brokered_with_versions(
            &path,
            title,
            LATENCY_SIZES,
            &impls,
            &latency,
            &cpu,
        )
        .expect("draw MOM latency chart");
        eprintln!("Written: {}", path.display());
    }
}
