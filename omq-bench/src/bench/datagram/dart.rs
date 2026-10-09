use std::process::Command;
use std::time::Duration;

use serde_json::{Value, json};

use super::args::DartArgs;
use super::peers::{self, Peers, Side};
use super::{
    LATENCY_SIZES, THROUGHPUT_SIZES, count, number, provenance, record, rounds, timestamp,
    validate_cpus,
};

pub(crate) fn run(mut args: DartArgs) {
    validate_cpus(&args.common.cpus);
    assert!(args.duration.is_finite() && args.duration > 0.0 && args.iterations > 0);
    assert!(args.warmup_seconds.is_finite() && args.warmup_seconds >= 0.0);
    assert!(args.drain_seconds.is_finite() && args.drain_seconds > 0.0);
    assert!(args.spin <= 50 && args.io_spin <= 50);
    assert!(args.window_messages.is_power_of_two() && args.window_messages <= 65_536);
    if args.continuous_io_spin {
        assert!(args.transport.iter().all(|transport| transport == "dart"));
    }
    if args.check_gates {
        validate_gates(&args);
    }
    if args.runtime != "owned" {
        assert!(args.common.kind == "latency" && args.spin == 0 && args.io_spin == 0);
        assert!(!args.continuous_spin && !args.continuous_io_spin);
    }
    if let Some(path) = &args.latency_samples {
        assert_eq!(args.common.kind, "latency");
        std::fs::create_dir_all(path).unwrap();
        args.latency_samples = Some(path.canonicalize().unwrap());
    }
    args.binary = args.binary.canonicalize().expect("built DART peer binary");
    if args.transport.iter().any(|transport| transport == "quic") {
        crate::tls::install_bench_credentials();
    }
    let provenance = provenance(&args.binary);
    let mut results = Vec::new();
    for transport in &args.transport {
        rounds(
            &args.common,
            THROUGHPUT_SIZES,
            LATENCY_SIZES,
            |kind, size, repeat, position| {
                let mut row = measure(&args, transport, kind, size);
                row["repeat"] = json!(repeat);
                row["size_position"] = json!(position);
                row.as_object_mut()
                    .unwrap()
                    .extend(provenance.as_object().unwrap().clone());
                record(&args.common, "dart.jsonl", &row);
                results.push(row.clone());
                row
            },
        );
    }
    if args.check_gates {
        check_gates(&results);
    }
}

fn command(
    args: &DartArgs,
    peer_side: Side,
    role: &str,
    endpoint: &str,
    size: u64,
) -> (Command, Option<std::path::PathBuf>) {
    let mut command = Command::new(&args.binary);
    command
        .args([role, endpoint])
        .args([
            size.to_string(),
            args.duration.to_string(),
            args.iterations.to_string(),
            args.warmup.to_string(),
            spin_argument(args.spin, args.continuous_spin),
            spin_argument(args.io_spin, args.continuous_io_spin),
        ])
        .arg(&args.congestion)
        .env("OMQ_DART_WARMUP_SECS", args.warmup_seconds.to_string())
        .env("OMQ_DART_DRAIN_SECS", args.drain_seconds.to_string())
        .env("OMQ_DART_WINDOW_MESSAGES", args.window_messages.to_string())
        .env("OMQ_PERF_CPUS", args.common.cpu_csv());
    if args.runtime != "owned" {
        command.arg(&args.runtime);
    }
    let samples = args
        .latency_samples
        .as_ref()
        .filter(|_| role == "client")
        .map(|path| {
            let transport = endpoint.split(':').next().unwrap();
            let path = path.join(format!("{transport}-{size}-{}.csv", timestamp()));
            command.env("OMQ_DART_RTT_SAMPLES", &path);
            path
        });
    let profile_side = if args.common.profile_side.as_deref().unwrap_or("receive") == "receive" {
        Side::Receive
    } else {
        Side::Send
    };
    let profile = args
        .common
        .profile
        .as_deref()
        .filter(|_| peer_side == profile_side);
    (peers::profiled(command, profile, role, size), samples)
}

fn spin_argument(budget: u64, continuous: bool) -> String {
    if continuous {
        "continuous".into()
    } else {
        budget.to_string()
    }
}

fn measure(args: &DartArgs, transport: &str, kind: &str, size: u64) -> Value {
    let timeout = Duration::from_secs_f64(
        40.0_f64.max(args.warmup_seconds + args.duration + args.drain_seconds + 10.0),
    );
    let mut peers = Peers::new(timeout);
    let (receive_role, send_role) = if kind == "throughput" {
        ("gather", "scatter")
    } else {
        ("server", "client")
    };
    peers.spawn(
        Side::Receive,
        &mut command(
            args,
            Side::Receive,
            receive_role,
            &format!("{transport}://127.0.0.1:0"),
            size,
        )
        .0,
    );
    let bound = peers.wait_json(Side::Receive, "bound");
    let (mut send, samples) = command(
        args,
        Side::Send,
        send_role,
        bound["endpoint"].as_str().unwrap(),
        size,
    );
    peers.spawn(Side::Send, &mut send);
    let receive_ready = peers.wait_json(Side::Receive, "ready");
    let send_ready = peers.wait_json(Side::Send, "ready");
    assert_eq!(
        receive_ready["dart_wire_version"],
        send_ready["dart_wire_version"]
    );
    if transport == "dart" {
        for ready in [&receive_ready, &send_ready] {
            assert_eq!(
                ready["dart_window_messages"].as_u64().unwrap_or(256),
                args.window_messages as u64,
                "peer receive/retention window"
            );
        }
    }
    let start = timestamp() + 200_000_000;
    peers.command(Side::Receive, start);
    peers.command(Side::Send, start);
    let sender = peers.wait_json(Side::Send, "result");
    if kind == "latency" {
        peers.command(Side::Receive, "STOP");
    }
    let receiver = peers.wait_json(Side::Receive, "result");
    peers.finish();
    let mut row = json!({"timestamp_ns":timestamp(), "kind":kind, "transport":transport,
        "msg_size":size, "spin_us":args.spin, "io_spin_us":args.io_spin,
        "continuous_spin":args.continuous_spin, "continuous_io_spin":args.continuous_io_spin,
        "cpus":args.common.cpu_csv(), "sender":sender, "receiver":receiver,
        "profiled":args.common.profile.is_some(), "diagnostic":samples.is_some(),
        "workload_profile":kind, "recv_batching":kind == "throughput", "runtime":args.runtime,
        "io_threads":usize::from(args.runtime == "owned"), "runtime_polling":args.runtime == "current-poll",
        "measurement_order":args.common.order, "dart_wire_version":receive_ready["dart_wire_version"],
        "dart_window_messages": (transport == "dart").then_some(args.window_messages),
        "dart_pool_buffers": (transport == "dart").then_some(&receive_ready["dart_pool_buffers"]),
        "dart_buffer_capacity": (transport == "dart").then_some(&receive_ready["dart_buffer_capacity"]),
        "congestion": if transport == "dart" { Some(&args.congestion) } else { None },
        "offloads_before":{"receive":receive_ready["offloads"], "send":send_ready["offloads"]}});
    verify(&mut row, args, samples);
    row
}

fn verify(row: &mut Value, args: &DartArgs, samples: Option<std::path::PathBuf>) {
    for side in ["sender", "receiver"] {
        for field in ["send_failures", "receive_failures", "invalid_datagrams"] {
            assert_eq!(
                row[side][field].as_u64().unwrap_or(0),
                0,
                "peer IO/protocol failure"
            );
        }
    }
    if row["kind"] == "latency" {
        row["iterations"] = json!(args.iterations);
        row["warmup_iterations"] = json!(args.warmup);
        for field in ["p50_us", "p99_us", "p999_us", "max_us", "timeouts"] {
            row[field] = row["sender"][field].clone();
        }
        assert_eq!(count(row, "timeouts"), 0);
        if let Some(path) = samples {
            row["latency_samples"] = json!(path);
        }
    } else {
        let offered = count(&row["sender"], "offered");
        let total = count(&row["receiver"], "received_total");
        let received = count(&row["receiver"], "received");
        assert!(offered > 0 && received > 0, "empty measurement window");
        assert_eq!(
            offered, total,
            "delivery mismatch after bounded drain: {row}"
        );
        assert_eq!(
            count(&row["sender"], "unacknowledged"),
            0,
            "unacknowledged messages after bounded drain: {row}"
        );
        for field in ["duplicates", "gaps", "corrupt"] {
            assert_eq!(count(&row["receiver"], field), 0);
        }
        row["warmup_seconds"] = json!(args.warmup_seconds);
        row["drain_seconds"] = json!(args.drain_seconds);
        row["msgs_s"] = json!(received as f64 / args.duration);
        row["offered_msgs_s"] = json!(offered as f64 / args.duration);
        row["missing_count"] = json!(0);
        row["excess_count"] = json!(0);
    }
}

fn validate_gates(args: &DartArgs) {
    assert!(args.runtime == "owned" && args.congestion == "lan" && args.common.profile.is_none());
    assert!(
        args.common.kind == "both" && args.common.repeats == 3 && args.common.order == "rotate"
    );
    assert_eq!(
        Duration::from_secs_f64(args.duration),
        Duration::from_secs(3)
    );
    assert!(args.iterations == 100_000 && args.warmup == 200_000);
    assert_eq!(
        Duration::from_secs_f64(args.warmup_seconds),
        Duration::from_millis(200)
    );
    assert_eq!(
        Duration::from_secs_f64(args.drain_seconds),
        Duration::from_secs(2)
    );
    assert!(args.spin == 50 && args.io_spin == 50);
    assert!(!args.continuous_spin && !args.continuous_io_spin);
    assert_eq!(args.window_messages, 256);
    let mut sizes = args.common.sizes(THROUGHPUT_SIZES);
    sizes.sort_unstable();
    assert_eq!(sizes, THROUGHPUT_SIZES);
    assert!(args.transport.iter().any(|transport| transport == "dart"));
    assert!(args.latency_samples.is_none());
}

fn check_gates(rows: &[Value]) {
    for kind in ["throughput", "latency"] {
        for size in THROUGHPUT_SIZES.iter().filter(|size| **size <= 1024) {
            let metric = if kind == "latency" {
                "p99_us"
            } else {
                "msgs_s"
            };
            let mut values: Vec<_> = rows
                .iter()
                .filter(|row| {
                    row["transport"] == "dart" && row["kind"] == kind && row["msg_size"] == *size
                })
                .map(|row| number(row, metric))
                .collect();
            assert_eq!(values.len(), 3);
            values.sort_by(f64::total_cmp);
            let passed = if kind == "latency" {
                values[1] <= 25.0
            } else {
                match size {
                    16 => values[1] >= 5_000_000.0,
                    1024 => values[1] * 1024.0 >= 1e9,
                    _ => true,
                }
            };
            assert!(
                passed,
                "LAN performance gate missed: {kind} {size} B; profile before collecting more results"
            );
        }
    }
}
