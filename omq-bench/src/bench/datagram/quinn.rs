use std::path::Path;
use std::process::Command;
use std::time::Duration;

use serde_json::{Value, json};

use super::args::QuinnArgs;
use super::peers::{self, Peers, Side};
use super::{TempDir, capture, count, provenance, record, rounds, timestamp, validate_cpus};

pub(crate) fn run(mut args: QuinnArgs) {
    validate_cpus(&args.common.cpus);
    assert!(args.duration.is_finite() && args.duration > 0.0 && args.iterations > 0);
    assert!(args.layout == "split" || args.app_spin == 0);
    assert!([0, 50].contains(&args.app_spin) && [0, 50].contains(&args.io_spin));
    assert!((1..=4096).contains(&args.batch) && args.event_interval > 0);
    args.binary = args.binary.canonicalize().expect("built Quinn peer binary");
    let provenance = provenance(&args.binary);
    let versions = versions();
    let directory = TempDir::new("quinn-datagram");
    let mut certificates = Command::new(&args.binary);
    certificates.arg("certs").arg(&directory.0);
    print!("{}", capture(&mut certificates));
    rounds(
        &args.common,
        &[16, 1024],
        &[16, 1024],
        |kind, size, repeat, position| {
            let mut row = measure(&args, &directory.0, kind, size);
            row.as_object_mut()
                .unwrap()
                .extend(provenance.as_object().unwrap().clone());
            row["quinn_version"] = versions["quinn"].clone();
            row["dependency_versions"] = versions.clone();
            row["dependency_versions_source"] = json!("workspace Cargo.lock at measurement time");
            row["run"] = json!(repeat);
            row["size_position"] = json!(position);
            record(&args.common, "quinn-datagram.jsonl", &row);
            row
        },
    );
}

fn command(
    args: &QuinnArgs,
    directory: &Path,
    peer_side: Side,
    role: &str,
    endpoint: &str,
    size: u64,
) -> Command {
    let (app, io) = if peer_side == Side::Send {
        (0, 1)
    } else {
        (5, 3)
    };
    let mut command = Command::new(&args.binary);
    command
        .args([
            role,
            endpoint,
            &size.to_string(),
            &args.duration.to_string(),
            &args.iterations.to_string(),
            &args.warmup.to_string(),
            &args.layout,
            &args.app_spin.to_string(),
            &args.io_spin.to_string(),
        ])
        .arg(directory)
        .args([
            args.common.cpus[app].to_string(),
            args.common.cpus[io].to_string(),
            args.batch.to_string(),
            args.event_interval.to_string(),
        ]);
    let profile_side = if args.common.profile_side.as_deref().unwrap_or("send") == "receive" {
        Side::Receive
    } else {
        Side::Send
    };
    peers::profiled(
        command,
        args.common
            .profile
            .as_deref()
            .filter(|_| peer_side == profile_side),
        role,
        size,
    )
}

fn measure(args: &QuinnArgs, directory: &Path, kind: &str, size: u64) -> Value {
    let mut peers = Peers::new(Duration::from_secs_f64(60.0_f64.max(args.duration + 10.0)));
    let (receive_role, send_role) = if kind == "throughput" {
        ("receive", "send")
    } else {
        ("server", "client")
    };
    peers.spawn(
        Side::Receive,
        &mut command(
            args,
            directory,
            Side::Receive,
            receive_role,
            "127.0.0.1:0",
            size,
        ),
    );
    let bound = peers.wait_json(Side::Receive, "bound");
    peers.spawn(
        Side::Send,
        &mut command(
            args,
            directory,
            Side::Send,
            send_role,
            bound["endpoint"].as_str().unwrap(),
            size,
        ),
    );
    let ready = json!({"receive":peers.wait_json(Side::Receive, "ready"), "send":peers.wait_json(Side::Send, "ready")});
    let start = timestamp() + 200_000_000;
    peers.command(Side::Receive, start);
    peers.command(Side::Send, start);
    let sender = peers.wait_json(Side::Send, "result");
    if kind == "latency" {
        peers.command(Side::Receive, "STOP");
    }
    let receiver = peers.wait_json(Side::Receive, "result");
    peers.command(Side::Send, "STOP");
    peers.finish();
    let mut row = json!({"timestamp_ns":timestamp(), "kind":kind, "transport":"quinn-datagram",
        "msg_size":size, "duration_seconds":args.duration, "layout":args.layout,
        "io_threads":if args.layout == "multi" { 2 } else { 1 },
        "application_future":if args.layout == "split" { "Handle::block_on" } else { "spawned task" },
        "app_spin_us":args.app_spin, "io_spin_us":args.io_spin, "cpus":args.common.cpu_csv(),
        "event_interval":args.event_interval, "batch_messages":args.batch, "batch_bytes":65536,
        "io_adapter":if args.io_spin == 0 { "stock Tokio" } else { "speculative UDP" },
        "datagram_queue_bytes":8 * 1024 * 1024, "send_api":"send_datagram_wait (throughput); send_datagram (RTT)",
        "throughput_payload":"immutable reusable Bytes; warmup tag 0, measured tag 1",
        "cipher":"TLS_AES_128_GCM_SHA256", "controller":"default Cubic",
        "ready":ready, "sender":sender, "receiver":receiver, "profiled":args.common.profile.is_some(),
        "measurement_order":args.common.order});
    verify(&mut row, args, kind, size);
    row
}

fn verify(row: &mut Value, args: &QuinnArgs, kind: &str, size: u64) {
    if kind == "throughput" {
        let offered = count(&row["sender"], "offered");
        let received = count(&row["receiver"], "received");
        let total = count(&row["receiver"], "received_total");
        assert!(offered > 0 && received > 0, "empty measurement window");
        row["msgs_s"] = json!(received as f64 / args.duration);
        row["payload_gb_s"] = json!(received as f64 * size as f64 / args.duration / 1e9);
        row["offered_msgs_s"] = json!(offered as f64 / args.duration);
        row["missing_count"] = json!(offered.saturating_sub(total));
        row["excess_count"] = json!(total.saturating_sub(offered));
        row["warmup_seconds"] = json!(0.2);
        row["tail_seconds"] = json!(0.3);
    } else {
        row["iterations"] = json!(args.iterations);
        row["warmup_iterations"] = json!(args.warmup);
        for field in ["p50_us", "p99_us", "p999_us", "max_us", "timeouts"] {
            row[field] = row["sender"][field].clone();
        }
        assert_eq!(
            count(&row["receiver"], "echoed"),
            args.iterations + args.warmup,
            "echo count mismatch"
        );
        assert_eq!(count(row, "timeouts"), 0);
    }
}

fn versions() -> Value {
    let lock = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("Cargo.lock");
    let lock = std::fs::read_to_string(lock).expect("workspace Cargo.lock");
    let mut result = json!({});
    for package in lock.split("[[package]]").skip(1) {
        let field = |key: &str| {
            package.lines().find_map(|line| {
                line.strip_prefix(&format!("{key} = \""))
                    .and_then(|value| value.strip_suffix('"'))
            })
        };
        if let Some(name @ ("quinn" | "quinn-proto" | "quinn-udp" | "rustls" | "tokio" | "ring")) =
            field("name")
        {
            result[name] = json!(field("version").expect("dependency version"));
        }
    }
    assert_eq!(result.as_object().unwrap().len(), 6);
    result
}
