use crate::chart::fmt_size;
use crate::cli::CompressionArgs;
use crate::jsonl::{self, CompressionRow};
use crate::{parse, process};

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

const CHART_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131_072, 262_144,
    524_288,
];

static PORT_COUNTER: AtomicU16 = AtomicU16::new(18500);

fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn get_wire_size(binary: &str, transport: &str, size: u64, dict_file: Option<&str>) -> u64 {
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");
    let size_str = size.to_string();
    let mut env = vec![("OMQ_BENCH_PAYLOAD", "json")];

    if let Some(df) = dict_file {
        env.push(("OMQ_BENCH_DICT_FILE", df));
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
        &[binary, "train-dict", path, &cap_str],
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
) -> Option<(f64, f64, f64)> {
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let port = next_port();
    let ep = format!("{transport}://127.0.0.1:{port}");

    let mut env: Vec<(&str, &str)> = vec![("OMQ_BENCH_PAYLOAD", "json")];
    if let Some(df) = dict_file {
        env.push(("OMQ_BENCH_DICT_FILE", df));
    }

    let mut push_proc = process::spawn(
        &[binary, "push", &ep, &size_str],
        &env,
        Some(process::MEASURED_CPU),
    );

    std::thread::sleep(Duration::from_millis(200));

    let pull_output = process::capture(
        &[binary, "pull", &ep, &size_str, &dur_str],
        &env,
        Some(process::OTHER_CPU),
        process::peer_timeout(duration),
    );

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    push_proc.kill();

    let result = parse::parse_throughput(&pull_output?, size)?;
    let cpu_time = push_cpu + result.pull_cpu.unwrap_or(0.0);
    Some((result.msgs_s, result.mbps, cpu_time))
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run(args: CompressionArgs) {
    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else {
        CHART_SIZES.to_vec()
    };

    let transports: Vec<&str> = args.transports.split(',').collect();
    eprintln!("  building omq_bench_peer_tokio (lz4)...");
    let binary = process::build_omq_peer("omq_bench_peer_tokio", &["lz4"]);
    let binary_str = binary.to_str().unwrap();
    let run_id = super::make_run_id(None);
    let jsonl_path = jsonl::cache_dir().join("results_compression_tokio.jsonl");
    let sweep = Sweep {
        binary: binary_str,
        sizes: &sizes,
        duration: args.duration,
        run_id: &run_id,
        jsonl_path: &jsonl_path,
    };

    eprintln!("Compression benchmark");

    // Non-dict sweep.
    sweep.run(&transports, "compression_json", None);

    // Dict sweeps.
    for &dict_cap in &args.dict_sizes {
        let dict_path = format!("/tmp/omq-bench-comp-dict-{dict_cap}.bin");
        eprintln!("\nTraining dict (capacity={dict_cap})...");
        train_dict(binary_str, &dict_path, dict_cap);

        let dict = Dict {
            path: &dict_path,
            capacity: dict_cap,
        };
        sweep.run(&["lz4+tcp"], "compression_json_dict", Some(&dict));

        std::fs::remove_file(&dict_path).ok();
    }

    eprintln!("\nResults appended to {}", jsonl_path.display());
}

/// Settings shared by every sweep of one compression run.
struct Sweep<'a> {
    binary: &'a str,
    sizes: &'a [u64],
    duration: f64,
    run_id: &'a str,
    jsonl_path: &'a std::path::Path,
}

/// Trained dictionary file and the capacity recorded in each row.
struct Dict<'a> {
    path: &'a str,
    capacity: u64,
}

impl Sweep<'_> {
    fn run(&self, transports: &[&str], pattern: &str, dict: Option<&Dict<'_>>) {
        let dict_file = dict.map(|dict| dict.path);
        let dict_size = dict.map(|dict| dict.capacity);

        for transport in transports {
            eprintln!("\n=== {transport} {pattern} ===");

            for &size in self.sizes {
                eprint!("{:>8}", fmt_size(size));

                let wire_bytes = get_wire_size(self.binary, transport, size, dict_file);

                match run_cell(self.binary, transport, size, self.duration, dict_file) {
                    Some((msgs_s, mbps, cpu_time)) => {
                        let row = CompressionRow {
                            run_id: self.run_id.to_string(),
                            pattern: pattern.to_string(),
                            transport: transport.to_string(),
                            peers: 1,
                            msg_size: size,
                            wire_bytes,
                            msg_count: Some(msgs_s * self.duration),
                            elapsed: Some(self.duration),
                            cpu_time: Some(cpu_time),
                            mbps: Some(mbps),
                            msgs_s: Some(msgs_s),
                            dict_size,
                        };

                        jsonl::append_jsonl(self.jsonl_path, &row);
                        eprint!("  {msgs_s:>10.0} msg/s  {mbps:>8.1} MB/s");
                    }
                    None => {
                        eprint!("  {:>10}", "-");
                    }
                }

                eprintln!();
            }
        }
    }
}
