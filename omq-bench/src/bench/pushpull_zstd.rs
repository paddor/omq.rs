use crate::cli::PushpullZstdArgs;
use crate::jsonl::{self, PushpullLz4Row};
use crate::process;

use std::path::PathBuf;
use std::time::Duration;

const CHART_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384, 65536, 262_144];
const QUICK_SIZES: &[u64] = &[64, 1024, 16384];

fn size_label(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{} MiB", n / 1_048_576)
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

static mut PORT_COUNTER: u16 = 17600;

fn next_port() -> u16 {
    unsafe {
        let p = PORT_COUNTER;
        PORT_COUNTER += 1;
        p
    }
}

struct Peers {
    blocking: PathBuf,
    tokio: PathBuf,
}

fn build_peers() -> Peers {
    eprintln!("  building omq_bench_peer_blocking (zstd)...");
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "omq-tokio",
            "--bin",
            "omq_bench_peer_blocking",
            "--features",
            "zstd",
            "-q",
        ])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "build failed");

    eprintln!("  building omq_bench_peer_tokio (zstd)...");
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "omq-tokio",
            "--bin",
            "omq_bench_peer_tokio",
            "--features",
            "zstd",
            "-q",
        ])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "build failed");

    Peers {
        blocking: PathBuf::from("target/release/omq_bench_peer_blocking"),
        tokio: PathBuf::from("target/release/omq_bench_peer_tokio"),
    }
}

fn get_wire_size(
    binary: &str,
    transport: &str,
    size: u64,
    dict_file: Option<&str>,
    level: Option<i32>,
) -> u64 {
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");
    let size_str = size.to_string();
    let mut env = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        env.push(("OMQ_BENCH_DICT_FILE", df));
    }
    let level_val = level.map(|l| l.to_string());
    if let Some(level) = &level_val {
        env.push(("OMQ_BENCH_ZSTD_LEVEL", level.as_str()));
    }
    let output = process::capture(
        &[binary, "wire-size", &ep, &size_str],
        &env,
        None,
        Duration::from_secs(10),
    )
    .unwrap_or_default();
    output.trim().parse().unwrap_or(size)
}

fn train_dict(binary: &str, path: &str, capacity: u64) {
    let cap_str = capacity.to_string();
    process::capture(
        &[binary, "train-zstd-dict", path, &cap_str],
        &[],
        None,
        Duration::from_secs(30),
    );
}

fn run_cell(
    binary: &str,
    transport: &str,
    size: u64,
    duration: f64,
    dict_file: Option<&str>,
    level: Option<i32>,
) -> Option<(f64, f64, f64)> {
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");

    let mut push_env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        push_env.push(("OMQ_BENCH_DICT_FILE", df));
    }
    let level_val = level.map(|l| l.to_string());
    if let Some(level) = &level_val {
        push_env.push(("OMQ_BENCH_ZSTD_LEVEL", level.as_str()));
    }

    let mut push_proc = process::spawn(
        &[binary, "push", &ep, &size_str],
        &push_env,
        Some(process::MEASURED_CPU),
    );

    std::thread::sleep(Duration::from_millis(200));

    let pull_transport = if transport.contains("zstd") {
        "tcp"
    } else {
        transport
    };
    let pull_ep = format!("{pull_transport}://127.0.0.1:{port}");

    let mut pull_env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        pull_env.push(("OMQ_BENCH_DICT_FILE", df));
    }
    if let Some(level) = &level_val {
        pull_env.push(("OMQ_BENCH_ZSTD_LEVEL", level.as_str()));
    }

    let output = process::capture(
        &[binary, "pull", &pull_ep, &size_str, &dur_str],
        &pull_env,
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    push_proc.kill();

    let output = output?;
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let count: f64 = parts[0].parse().ok()?;
    let elapsed: f64 = parts[1].parse().ok()?;
    if elapsed <= 0.0 || count <= 0.0 {
        return None;
    }
    let msgs_s = count / elapsed;
    let mbps = (count * size as f64) / elapsed / 1_000_000.0;
    Some((msgs_s, mbps, push_cpu))
}

fn make_run_id() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .expect("failed to run date");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[allow(clippy::needless_pass_by_value)]
#[expect(clippy::too_many_lines)]
pub(crate) fn run(args: PushpullZstdArgs) {
    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else if args.quick {
        QUICK_SIZES.to_vec()
    } else {
        CHART_SIZES.to_vec()
    };

    let duration = if args.quick { 1.5 } else { args.duration };
    let rounds = if args.quick { 1 } else { args.rounds };

    let transports: Vec<&str> = args.transports.split(',').collect();
    let peers = build_peers();
    let bench_bin = peers.blocking.to_str().unwrap();
    let util_bin = peers.tokio.to_str().unwrap();
    let run_id = make_run_id();
    let jsonl_path = jsonl::cache_dir().join("results_pushpull_zstd.jsonl");

    eprintln!("PUSH/PULL Zstd benchmark");
    eprintln!(
        "Transports: {transports:?}, sizes: {}, rounds: {rounds}, level: {}",
        sizes.len(),
        args.level.map_or("default".to_string(), |l| l.to_string()),
    );

    for transport in &transports {
        eprintln!("\n=== {transport} ===");

        for &size in &sizes {
            eprint!("{:>8}", size_label(size));

            let wire_bytes = get_wire_size(util_bin, transport, size, None, args.level);

            let mut best: Option<(f64, f64, f64)> = None;
            for _ in 0..rounds {
                if let Some(result) =
                    run_cell(bench_bin, transport, size, duration, None, args.level)
                    && best.as_ref().is_none_or(|b| result.0 > b.0)
                {
                    best = Some(result);
                }
            }

            if let Some((msgs_s, mbps, cpu_time)) = best {
                let row = PushpullLz4Row {
                    run_id: run_id.clone(),
                    pattern: "pushpull_zstd".to_string(),
                    transport: transport.to_string(),
                    peers: 1,
                    msg_size: size,
                    wire_bytes,
                    msg_count: Some(msgs_s * duration),
                    elapsed: Some(duration),
                    cpu_time: Some(cpu_time),
                    msgs_s: Some(msgs_s),
                    mbps: Some(mbps),
                    dict_size: None,
                    compression_level: args.level,
                };
                jsonl::append_jsonl(&jsonl_path, &row);
                eprint!("  {msgs_s:>10.0} msg/s  {mbps:>8.1} MB/s");
            } else {
                eprint!("  {:>10}", "-");
            }
            eprintln!();
        }
    }

    // Dict: train once up front from diverse JSON records, then reuse it.
    for &dict_cap in &args.dict_sizes {
        let dict_path = format!("/tmp/omq-bench-zstd-dict-{dict_cap}.bin");
        eprintln!("\nTraining zstd dict (capacity={dict_cap})...");
        train_dict(util_bin, &dict_path, dict_cap);

        for transport in &transports {
            if !transport.contains("zstd") {
                continue;
            }
            eprintln!("--- {transport} + dict (dict_size={dict_cap}) ---");

            for &size in &sizes {
                eprint!("{:>8}", size_label(size));

                let wire_bytes =
                    get_wire_size(util_bin, transport, size, Some(&dict_path), args.level);

                let mut best: Option<(f64, f64, f64)> = None;
                for _ in 0..rounds {
                    if let Some(result) = run_cell(
                        bench_bin,
                        transport,
                        size,
                        duration,
                        Some(&dict_path),
                        args.level,
                    ) && best.as_ref().is_none_or(|b| result.0 > b.0)
                    {
                        best = Some(result);
                    }
                }

                if let Some((msgs_s, mbps, cpu_time)) = best {
                    let row = PushpullLz4Row {
                        run_id: run_id.clone(),
                        pattern: "pushpull_zstd_dict".to_string(),
                        transport: transport.to_string(),
                        peers: 1,
                        msg_size: size,
                        wire_bytes,
                        msg_count: Some(msgs_s * duration),
                        elapsed: Some(duration),
                        cpu_time: Some(cpu_time),
                        msgs_s: Some(msgs_s),
                        mbps: Some(mbps),
                        dict_size: Some(dict_cap),
                        compression_level: args.level,
                    };
                    jsonl::append_jsonl(&jsonl_path, &row);
                    eprint!("  {msgs_s:>10.0} msg/s  {mbps:>8.1} MB/s  wire {wire_bytes}");
                } else {
                    eprint!("  {:>10}", "-");
                }
                eprintln!();
            }
        }
        let _ = std::fs::remove_file(&dict_path);
    }

    eprintln!("\nResults appended to {}", jsonl_path.display());
}
