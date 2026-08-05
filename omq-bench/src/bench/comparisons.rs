use crate::cli::ComparisonsArgs;
use crate::coord::CoordSocket;
use crate::jsonl::{self, ComparisonRow};
use crate::parse;
use crate::process;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const COMPARISON_CHART_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
const MAIN_EXTRA_CHART_SIZES: &[u64] = &[32, 128, 512, 2048, 8192, 32768, 262_144, 4_194_304];
const QUICK_SIZES: &[u64] = &[64, 1024, 4096];

const LATENCY_MAX_SIZE: u64 = 4096;

const DEFAULT_DURATION: f64 = 3.0;
const QUICK_DURATION: f64 = 1.5;
const DEFAULT_ROUNDS: u32 = 1;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImplClass {
    Classic,
    IoUring,
    Curve,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportKind {
    Tcp,
    Ipc,
    Inproc,
    Ws,
}

impl TransportKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Ipc => "ipc",
            Self::Inproc => "inproc",
            Self::Ws => "ws",
        }
    }
}

#[expect(clippy::struct_excessive_bools)]
pub(crate) struct ImplDef {
    pub name: &'static str,
    pub binary_from: Option<&'static str>,
    pub prefix: &'static str,
    pub class: Option<ImplClass>,
    pub main: bool,
    pub transports: &'static [TransportKind],
    pub inproc_tput_subcmd: &'static str,
    pub inproc_lat_subcmd: &'static str,
    pub inproc_pubsub_subcmd: &'static str,
    pub pub_needs_peer_count: bool,
    pub fanout_subcmd: &'static str,
    pub fanio_needs_peer_count: bool,
    pub supports_pubsub: bool,
    pub env: &'static [(&'static str, &'static str)],
}

use TransportKind::{Inproc, Ipc, Tcp, Ws};

static IMPLS: &[ImplDef] = &[
    ImplDef {
        name: "omq-tokio-ct",
        binary_from: None,
        prefix: "t",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "pub-fanout",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "omq-tokio-1t",
        binary_from: None,
        prefix: "b",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Inproc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "omq-tokio-2t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "u",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "2")],
    },
    ImplDef {
        name: "omq-tokio-2ut",
        binary_from: Some("omq-tokio-1t"),
        prefix: "v",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Inproc],
        inproc_tput_subcmd: "inproc-2ut",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_IO_THREADS", "1")],
    },
    ImplDef {
        name: "libzmq",
        binary_from: None,
        prefix: "z",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "libzmq-2t",
        binary_from: Some("libzmq"),
        prefix: "Y",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Ws],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "2")],
    },
    ImplDef {
        name: "tmq",
        binary_from: None,
        prefix: "m",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "zmq.rs",
        binary_from: None,
        prefix: "q",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "rzmq",
        binary_from: None,
        prefix: "r",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[],
    },
    ImplDef {
        name: "rzmq-iouring",
        binary_from: Some("rzmq"),
        prefix: "R",
        class: Some(ImplClass::IoUring),
        main: true,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("RZMQ_IO_URING", "1")],
    },
    ImplDef {
        name: "libzmq-curve-1t",
        binary_from: Some("libzmq"),
        prefix: "lc1",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "1"), ("ZMQ_BENCH_CURVE", "1")],
    },
    ImplDef {
        name: "libzmq-curve-2t",
        binary_from: Some("libzmq"),
        prefix: "lc2",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: false,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("ZMQ_IO_THREADS", "2"), ("ZMQ_BENCH_CURVE", "1")],
    },
    ImplDef {
        name: "omq-curve-1t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "oc1",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: false,
        supports_pubsub: true,
        env: &[("OMQ_BENCH_MECHANISM", "curve")],
    },
    ImplDef {
        name: "omq-curve-2t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "oc2",
        class: Some(ImplClass::Curve),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "pub-fanout",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "2"), ("OMQ_BENCH_MECHANISM", "curve")],
    },
];

fn find_impl(name: &str) -> Option<&'static ImplDef> {
    IMPLS.iter().find(|i| i.name == name)
}

#[cfg(test)]
fn impl_io_threads(def: &ImplDef) -> &'static str {
    def.env
        .iter()
        .find_map(|&(k, v)| matches!(k, "OMQ_IO_THREADS" | "ZMQ_IO_THREADS").then_some(v))
        .unwrap_or("1")
}

fn all_chart_sizes() -> Vec<u64> {
    let mut sizes: Vec<u64> = COMPARISON_CHART_SIZES.to_vec();
    sizes.extend(MAIN_EXTRA_CHART_SIZES);
    sizes.sort_unstable();
    sizes.dedup();
    sizes
}

fn latency_sizes_from(sizes: &[u64]) -> Vec<u64> {
    sizes
        .iter()
        .copied()
        .filter(|&s| s <= LATENCY_MAX_SIZE)
        .collect()
}

// ---- Address generation ---------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};
static ADDR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_addr_id() -> u64 {
    ADDR_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn uses_filesystem_ipc(impl_name: &str) -> bool {
    matches!(impl_name, "zmq.rs" | "rzmq" | "rzmq-iouring")
}

fn addr_for(
    transport: TransportKind,
    prefix: &str,
    idx: u64,
    base_port: u16,
    impl_name: &str,
) -> String {
    let uid = next_addr_id();
    match transport {
        TransportKind::Tcp => "0".to_string(),
        TransportKind::Ws => {
            let offset: u16 = match prefix {
                "t" => 0,
                "u" => 100,
                "z" => 200,
                "Y" => 300,
                "Z" => 400,
                "q" => 500,
                "r" => 600,
                "R" => 700,
                _ => 800,
            };
            let port = base_port + offset + idx as u16;
            format!("ws://127.0.0.1:{port}/")
        }
        TransportKind::Ipc => {
            if uses_filesystem_ipc(impl_name) {
                format!("ipc:///tmp/omq-bench-cmp-{prefix}-{uid}")
            } else {
                format!("ipc://@omq-bench-cmp-{prefix}-{uid}")
            }
        }
        TransportKind::Inproc => {
            format!("bench-cmp-{prefix}-{uid}")
        }
    }
}

// ---- Build ----------------------------------------------------------------

#[expect(clippy::too_many_lines)]
fn build_peers(impl_names: &[&str], needs_ws: bool, needs_curve: bool) -> HashMap<String, PathBuf> {
    let mut binaries: HashMap<String, PathBuf> = HashMap::new();
    let mut built: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let sources: Vec<&str> = impl_names
        .iter()
        .map(|&name| {
            let def = find_impl(name).unwrap();
            def.binary_from.unwrap_or(def.name)
        })
        .collect();
    for source in sources {
        if built.contains(source) {
            continue;
        }
        built.insert(source);

        match source {
            "omq-tokio-ct" => {
                let mut features = Vec::new();
                if needs_ws {
                    features.push("ws");
                }
                if needs_curve {
                    features.push("curve");
                }
                let mut cmd = vec![
                    "cargo",
                    "build",
                    "--release",
                    "-p",
                    "omq-tokio",
                    "--bin",
                    "omq_bench_peer_tokio",
                    "-q",
                ];
                let feat_str;
                if !features.is_empty() {
                    feat_str = features.join(",");
                    cmd.push("--features");
                    cmd.push(&feat_str);
                }
                run_build(&cmd);
                binaries.insert(
                    source.to_string(),
                    PathBuf::from("target/release/omq_bench_peer_tokio"),
                );
            }
            "omq-tokio-1t" => {
                let mut features = Vec::new();
                if needs_ws {
                    features.push("ws");
                }
                if needs_curve {
                    features.push("curve");
                }
                let mut cmd = vec![
                    "cargo",
                    "build",
                    "--release",
                    "-p",
                    "omq-tokio",
                    "--bin",
                    "omq_bench_peer_blocking",
                    "-q",
                ];
                let feat_str;
                if !features.is_empty() {
                    feat_str = features.join(",");
                    cmd.push("--features");
                    cmd.push(&feat_str);
                }
                run_build(&cmd);
                binaries.insert(
                    source.to_string(),
                    PathBuf::from("target/release/omq_bench_peer_blocking"),
                );
            }
            "libzmq" => {
                let src = "scripts/libzmq_bench_peer.c";
                let out = "scripts/libzmq_bench_peer";
                run_build(&["gcc", "-O2", "-o", out, src, "-lzmq", "-lpthread"]);
                binaries.insert(source.to_string(), PathBuf::from(out));
            }
            "zmq.rs" => {
                run_build_in_dir(
                    &["cargo", "build", "--release", "-q"],
                    "scripts/zmqrs_bench_peer",
                );
                binaries.insert(
                    source.to_string(),
                    PathBuf::from("scripts/zmqrs_bench_peer/target/release/zmqrs_bench_peer"),
                );
            }
            "tmq" => {
                run_build_in_dir_with_env(
                    &["cargo", "build", "--release", "-q"],
                    "scripts/tmq_bench_peer",
                    &[("CXXFLAGS", "-std=gnu++11")],
                );
                binaries.insert(
                    source.to_string(),
                    PathBuf::from("scripts/tmq_bench_peer/target/release/tmq_bench_peer"),
                );
            }
            "rzmq" => {
                run_build_in_dir(
                    &["cargo", "build", "--release", "-q"],
                    "scripts/rzmq_bench_peer",
                );
                binaries.insert(
                    source.to_string(),
                    PathBuf::from("scripts/rzmq_bench_peer/target/release/rzmq_bench_peer"),
                );
            }
            _ => panic!("unknown impl source: {source}"),
        }
    }

    // Map each impl name to its binary path.
    let mut result = HashMap::new();
    for &name in impl_names {
        let def = find_impl(name).unwrap();
        let source = def.binary_from.unwrap_or(def.name);
        result.insert(name.to_string(), binaries[source].clone());
    }
    if binaries.contains_key("omq-tokio-1t") {
        result.insert("omq-tokio-1t".to_string(), binaries["omq-tokio-1t"].clone());
    }
    result
}

fn run_build(cmd: &[&str]) {
    eprintln!("  building: {}", cmd.join(" "));
    let status = std::process::Command::new(cmd[0])
        .args(&cmd[1..])
        .status()
        .unwrap_or_else(|e| panic!("failed to run {cmd:?}: {e}"));
    assert!(status.success(), "build failed: {cmd:?}");
}

fn run_build_in_dir(cmd: &[&str], dir: &str) {
    eprintln!("  building: {} (in {dir})", cmd.join(" "));
    let status = std::process::Command::new(cmd[0])
        .args(&cmd[1..])
        .current_dir(dir)
        .status()
        .unwrap_or_else(|e| panic!("failed to run {cmd:?} in {dir}: {e}"));
    assert!(status.success(), "build failed: {cmd:?} in {dir}");
}

fn run_build_in_dir_with_env(cmd: &[&str], dir: &str, env: &[(&str, &str)]) {
    eprintln!("  building: {} (in {dir})", cmd.join(" "));
    let status = std::process::Command::new(cmd[0])
        .args(&cmd[1..])
        .current_dir(dir)
        .envs(env.iter().copied())
        .status()
        .unwrap_or_else(|e| panic!("failed to run {cmd:?} in {dir}: {e}"));
    assert!(status.success(), "build failed: {cmd:?} in {dir}");
}

// ---- Measurement integrity ------------------------------------------------

struct MeasurementTracker {
    issues: Vec<String>,
}

impl MeasurementTracker {
    fn new() -> Self {
        Self { issues: Vec::new() }
    }

    #[allow(dead_code)]
    fn note(&mut self, present: bool, ctx: &str, what: &str) -> bool {
        if !present {
            self.issues.push(format!("{ctx}: missing {what}"));
        }
        present
    }

    fn check(&self) {
        if !self.issues.is_empty() {
            eprintln!("\nMeasurement issues:");
            for issue in &self.issues {
                eprintln!("  - {issue}");
            }
            std::process::exit(1);
        }
    }
}

// ---- Size formatting ------------------------------------------------------

pub(crate) fn size_label(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{} MiB", n / 1_048_576)
    } else if n >= 1024 {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

// ---- Run ID ---------------------------------------------------------------

fn make_run_id(name: Option<&str>) -> String {
    let ts = chrono_like_utc_now();
    match name {
        Some(n) => format!("{ts}-{n}"),
        None => ts,
    }
}

fn chrono_like_utc_now() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .expect("failed to run date");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

// ---- Cell functions -------------------------------------------------------

struct CellResult {
    msgs_s: f64,
    mbps: f64,
    elapsed: f64,
    push_cpu: Option<f64>,
    pull_cpu: Option<f64>,
    peer_min: Option<f64>,
    peer_max: Option<f64>,
    peer_p10: Option<f64>,
    peer_p25: Option<f64>,
    peer_median: Option<f64>,
    peer_p75: Option<f64>,
    peer_p90: Option<f64>,
}

fn zero_result(duration: f64) -> CellResult {
    CellResult {
        msgs_s: 0.0,
        mbps: 0.0,
        elapsed: duration,
        push_cpu: None,
        pull_cpu: None,
        peer_min: None,
        peer_max: None,
        peer_p10: None,
        peer_p25: None,
        peer_median: None,
        peer_p75: None,
        peer_p90: None,
    }
}

#[expect(clippy::too_many_arguments)]
fn run_throughput_cell(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    duration: f64,
    rounds: u32,
    base_port: u16,
) -> CellResult {
    representative_of(rounds, |_| {
        run_throughput_once(
            binary,
            peer_binary,
            def,
            transport,
            size,
            duration,
            base_port,
        )
    })
}

#[allow(clippy::too_many_lines)]
fn run_throughput_once(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    duration: f64,
    base_port: u16,
) -> CellResult {
    let binary_str = binary.to_str().unwrap();
    let peer_binary_str = peer_binary.to_str().unwrap();
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");

    if transport == TransportKind::Inproc {
        let name = addr_for(transport, def.prefix, 0, base_port, def.name);
        let cmd = vec![
            binary_str,
            def.inproc_tput_subcmd,
            &name,
            &size_str,
            &dur_str,
        ];
        let _ = cmd; // used below
        let env: Vec<(&str, &str)> = def.env.to_vec();
        if let Some((out, cpu)) = process::capture_with_cpu(
            &[
                binary_str,
                def.inproc_tput_subcmd,
                &name,
                &size_str,
                &dur_str,
            ],
            &env,
            Some(process::MEASURED_CPU),
            Duration::from_secs(duration as u64 + 30),
        ) && let Some(r) = parse::parse_throughput(&out, size)
        {
            return CellResult {
                msgs_s: r.msgs_s,
                mbps: r.mbps,
                elapsed: r.elapsed,
                push_cpu: Some(cpu),
                pull_cpu: None,
                peer_min: None,
                peer_max: None,
                peer_p10: None,
                peer_p25: None,
                peer_median: None,
                peer_p75: None,
                peer_p90: None,
            };
        }
        return zero_result(duration);
    }

    let addr = addr_for(transport, def.prefix, 0, base_port, def.name);
    let connect_addr;

    let push_env: Vec<(&str, &str)> = def.env.to_vec();
    let pull_env: Vec<(&str, &str)> = def.env.to_vec();

    let push_cmd: Vec<&str>;
    let mut push_proc;
    let mut _coord_socket = None;
    match transport {
        TransportKind::Tcp => {
            let coord = CoordSocket::bind_new();
            push_cmd = vec![binary_str, "push", "tcp://127.0.0.1:0", &size_str];
            let mut env = push_env.clone();
            env.push(("OMQ_BENCH_COORD", coord.endpoint()));
            push_proc = process::spawn(&push_cmd, &env, Some(process::MEASURED_CPU));
            let port = coord
                .recv_ready_port(Duration::from_secs(10))
                .expect("coord: no READY from push peer");
            connect_addr = format!("tcp://127.0.0.1:{port}");
            _coord_socket = Some(coord);
        }
        TransportKind::Ws => {
            push_cmd = vec![binary_str, "push", &addr, &size_str];
            push_proc = process::spawn(&push_cmd, &push_env, Some(process::MEASURED_CPU));
            std::thread::sleep(Duration::from_millis(200));
            connect_addr = addr.clone();
        }
        _ => {
            push_cmd = vec![binary_str, "push", &addr, &size_str];
            push_proc = process::spawn(&push_cmd, &push_env, Some(process::MEASURED_CPU));
            std::thread::sleep(Duration::from_millis(100));
            connect_addr = addr.clone();
        }
    }

    let pull_output = process::capture(
        &[peer_binary_str, "pull", &connect_addr, &size_str, &dur_str],
        &pull_env,
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    push_proc.kill();

    if transport == TransportKind::Ipc {
        cleanup_ipc_addr(&addr, def.name);
    }

    let Some(output) = pull_output else {
        return zero_result(duration);
    };

    match parse::parse_throughput(&output, size) {
        Some(r) => CellResult {
            msgs_s: r.msgs_s,
            mbps: r.mbps,
            elapsed: r.elapsed,
            push_cpu: Some(push_cpu),
            pull_cpu: r.pull_cpu,
            peer_min: None,
            peer_max: None,
            peer_p10: None,
            peer_p25: None,
            peer_median: None,
            peer_p75: None,
            peer_p90: None,
        },
        None => zero_result(duration),
    }
}

#[expect(clippy::too_many_arguments)]
fn run_pubsub_cell(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    rounds: u32,
    base_port: u16,
) -> CellResult {
    representative_of(rounds, |_| {
        run_pubsub_once(
            binary,
            peer_binary,
            def,
            transport,
            size,
            peers,
            duration,
            base_port,
        )
    })
}

#[allow(clippy::needless_late_init, clippy::too_many_lines)]
#[expect(clippy::too_many_arguments)]
fn run_pubsub_once(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    base_port: u16,
) -> CellResult {
    let binary_str = binary.to_str().unwrap();
    let peer_binary_str = peer_binary.to_str().unwrap();
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();

    if transport == TransportKind::Inproc {
        let name = addr_for(transport, def.prefix, 0, base_port, def.name);
        let cmd = vec![
            binary_str,
            def.inproc_pubsub_subcmd,
            &name,
            &size_str,
            &dur_str,
            &peers_str,
        ];
        let env: Vec<(&str, &str)> = def.env.to_vec();
        if let Some((out, cpu)) = process::capture_with_cpu(
            &cmd,
            &env,
            Some(process::MEASURED_CPU),
            Duration::from_secs(duration as u64 + 30),
        ) && let Some(r) = parse::parse_throughput(&out, size)
        {
            return CellResult {
                msgs_s: r.msgs_s,
                mbps: r.mbps,
                elapsed: r.elapsed,
                push_cpu: Some(cpu),
                pull_cpu: None,
                peer_min: None,
                peer_max: None,
                peer_p10: None,
                peer_p25: None,
                peer_median: None,
                peer_p75: None,
                peer_p90: None,
            };
        }
        return zero_result(duration);
    }

    let addr = addr_for(transport, def.prefix, 0, base_port, def.name);
    let connect_addr;

    let mut pub_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut sub_env: Vec<(&str, &str)> = def.env.to_vec();
    let start_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs_f64()
        + 2.0;
    let start_at_str = format!("{start_at:.6}");
    pub_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    sub_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    pub_env.push(("OMQ_BENCH_WARMUP_MS", "500"));
    sub_env.push(("OMQ_BENCH_WARMUP_MS", "500"));

    let mut pub_cmd: Vec<&str> = vec![binary_str, "pub"];
    if transport == TransportKind::Tcp {
        pub_cmd.extend(["tcp://127.0.0.1:0", &size_str]);
    } else {
        pub_cmd.extend([addr.as_str(), &size_str]);
    }
    if def.pub_needs_peer_count {
        pub_cmd.push(&peers_str);
    }

    let coord = (transport == TransportKind::Tcp).then(CoordSocket::bind_new);
    let mut spawn_env = pub_env.clone();
    if let Some(ref c) = coord {
        spawn_env.push(("OMQ_BENCH_COORD", c.endpoint()));
    }
    let mut pub_proc = process::spawn(&pub_cmd, &spawn_env, Some(process::MEASURED_CPU));

    if let Some(ref c) = coord {
        let port = c
            .recv_ready_port(Duration::from_secs(10))
            .expect("coord: no READY from pub peer");
        connect_addr = format!("tcp://127.0.0.1:{port}");
    } else {
        std::thread::sleep(Duration::from_millis(100));
        connect_addr = addr.clone();
    }

    let sub_dur_str = format!("{:.1}", duration + 3.0);
    let sub_output = process::capture(
        &[
            peer_binary_str,
            "multi-sub",
            &connect_addr,
            &size_str,
            &sub_dur_str,
            &peers_str,
        ],
        &sub_env,
        Some(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let pub_cpu = process::read_proc_cpu(pub_proc.pid());
    pub_proc.kill();

    if transport == TransportKind::Ipc {
        cleanup_ipc_addr(&addr, def.name);
    }

    let Some(output) = sub_output else {
        return zero_result(duration);
    };

    match parse::parse_multi_throughput(&output, size, peers) {
        Some(r) => CellResult {
            msgs_s: r.msgs_s,
            mbps: r.mbps,
            elapsed: r.elapsed,
            push_cpu: Some(pub_cpu),
            pull_cpu: r.pull_cpu,
            peer_min: r.peer_min,
            peer_max: r.peer_max,
            peer_p10: r.peer_p10,
            peer_p25: r.peer_p25,
            peer_median: r.peer_median,
            peer_p75: r.peer_p75,
            peer_p90: r.peer_p90,
        },
        None => zero_result(duration),
    }
}

#[expect(clippy::too_many_arguments)]
fn run_fanout_cell(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    rounds: u32,
    base_port: u16,
) -> CellResult {
    representative_of(rounds, |_| {
        run_fanout_once(
            binary,
            peer_binary,
            def,
            transport,
            size,
            peers,
            duration,
            base_port,
        )
    })
}

#[allow(clippy::needless_late_init)]
#[expect(clippy::too_many_arguments)]
#[expect(clippy::too_many_lines)]
fn run_fanout_once(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    base_port: u16,
) -> CellResult {
    let binary_str = binary.to_str().unwrap();
    let peer_binary_str = peer_binary.to_str().unwrap();
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();

    let addr = addr_for(transport, def.prefix, 0, base_port, def.name);
    let connect_addr;

    let mut push_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut pull_env: Vec<(&str, &str)> = def.env.to_vec();
    let start_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs_f64()
        + 2.0;
    let start_at_str = format!("{start_at:.6}");
    push_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    pull_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    push_env.push(("OMQ_BENCH_WARMUP_MS", "500"));
    pull_env.push(("OMQ_BENCH_WARMUP_MS", "500"));

    let fanout_subcmd = def.fanout_subcmd;
    let mut push_cmd: Vec<&str> = vec![binary_str, fanout_subcmd];
    if transport == TransportKind::Tcp {
        push_cmd.extend(["tcp://127.0.0.1:0", &size_str]);
    } else {
        push_cmd.extend([addr.as_str(), &size_str]);
    }
    if def.fanio_needs_peer_count {
        push_cmd.push(&peers_str);
    }

    let coord = (transport == TransportKind::Tcp).then(CoordSocket::bind_new);
    let mut spawn_env = push_env.clone();
    if let Some(ref c) = coord {
        spawn_env.push(("OMQ_BENCH_COORD", c.endpoint()));
    }
    let mut push_proc = process::spawn(&push_cmd, &spawn_env, Some(process::MEASURED_CPU));

    if let Some(ref c) = coord {
        let port = c
            .recv_ready_port(Duration::from_secs(10))
            .expect("coord: no READY from push peer");
        connect_addr = format!("tcp://127.0.0.1:{port}");
    } else {
        std::thread::sleep(Duration::from_millis(100));
        connect_addr = addr.clone();
    }

    let receiver_processes = std::env::var("OMQ_BENCH_RECEIVER_PROCS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1);
    assert!(receiver_processes > 0 && peers.is_multiple_of(receiver_processes));
    let peers_per_process = peers / receiver_processes;
    let local_peers_str = peers_per_process.to_string();
    let mut pull_procs = Vec::new();
    for _ in 0..receiver_processes {
        pull_procs.push(process::spawn(
            &[
                peer_binary_str,
                "multi-pull",
                &connect_addr,
                &size_str,
                &dur_str,
                &local_peers_str,
            ],
            &pull_env,
            Some(process::OTHER_CPU),
        ));
    }
    let timeout = Duration::from_secs(duration as u64 + 30);
    let mut pull_results = Vec::new();
    for pull_proc in &mut pull_procs {
        if let Some(output) = pull_proc.wait_with_output(timeout)
            && let Some(result) = parse::parse_multi_throughput(&output, size, peers_per_process)
        {
            pull_results.push(result);
        }
    }

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    push_proc.kill();

    if transport == TransportKind::Ipc {
        cleanup_ipc_addr(&addr, def.name);
    }

    if pull_results.is_empty() {
        return zero_result(duration);
    }

    let total_msgs: f64 = pull_results
        .iter()
        .map(|r| r.msgs_s * peers_per_process as f64 * r.elapsed)
        .sum();
    let elapsed = pull_results.iter().map(|r| r.elapsed).fold(0.0, f64::max);
    let mut peer_rates: Vec<f64> = pull_results
        .iter()
        .flat_map(|r| r.peer_rates.iter().copied())
        .collect();
    peer_rates.sort_unstable_by(f64::total_cmp);
    let cpu: f64 = pull_results.iter().filter_map(|r| r.pull_cpu).sum();
    let quantile = |p: f64| {
        if peer_rates.is_empty() {
            None
        } else {
            Some(peer_rates[((peer_rates.len() - 1) as f64 * p).round() as usize])
        }
    };

    CellResult {
        msgs_s: total_msgs / elapsed / peers as f64,
        mbps: total_msgs * size as f64 / elapsed / 1_000_000.0,
        elapsed,
        push_cpu: Some(push_cpu),
        pull_cpu: Some(cpu),
        peer_min: quantile(0.0),
        peer_max: quantile(1.0),
        peer_p10: quantile(0.10),
        peer_p25: quantile(0.25),
        peer_median: quantile(0.50),
        peer_p75: quantile(0.75),
        peer_p90: quantile(0.90),
    }
}

#[expect(clippy::too_many_arguments)]
fn run_fanin_cell(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    rounds: u32,
    base_port: u16,
) -> CellResult {
    representative_of(rounds, |_| {
        run_fanin_once(
            binary,
            peer_binary,
            def,
            transport,
            size,
            peers,
            duration,
            base_port,
        )
    })
}

#[allow(clippy::needless_late_init)]
#[expect(clippy::too_many_arguments)]
fn run_fanin_once(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    peers: u64,
    duration: f64,
    base_port: u16,
) -> CellResult {
    let binary_str = binary.to_str().unwrap();
    let peer_binary_str = peer_binary.to_str().unwrap();
    let size_str = size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();

    let addr = addr_for(transport, def.prefix, 0, base_port, def.name);
    let connect_addr;

    let mut pull_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut push_env: Vec<(&str, &str)> = def.env.to_vec();
    let start_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs_f64()
        + 2.0;
    let start_at_str = format!("{start_at:.6}");
    pull_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    push_env.push(("OMQ_BENCH_START_AT", &start_at_str));
    pull_env.push(("OMQ_BENCH_WARMUP_MS", "500"));
    push_env.push(("OMQ_BENCH_WARMUP_MS", "500"));

    // pull-bind binds on the measured CPU.
    let mut pull_cmd = vec![binary_str, "pull-bind"];
    if transport == TransportKind::Tcp {
        pull_cmd.extend(["tcp://127.0.0.1:0", &size_str, &dur_str]);
    } else {
        pull_cmd.extend([addr.as_str(), &size_str, &dur_str]);
    }

    let coord = (transport == TransportKind::Tcp).then(CoordSocket::bind_new);
    let mut spawn_env = pull_env.clone();
    if let Some(ref c) = coord {
        spawn_env.push(("OMQ_BENCH_COORD", c.endpoint()));
    }
    let mut pull_proc = process::spawn(&pull_cmd, &spawn_env, Some(process::MEASURED_CPU));

    if let Some(c) = coord {
        let port = c
            .recv_ready_port(Duration::from_secs(10))
            .expect("coord: no READY from pull-bind peer");
        connect_addr = format!("tcp://127.0.0.1:{port}");
    } else {
        std::thread::sleep(Duration::from_millis(100));
        connect_addr = addr.clone();
    }

    let mut push_proc = process::spawn(
        &[
            peer_binary_str,
            "multi-push",
            &connect_addr,
            &size_str,
            &peers_str,
            &dur_str,
        ],
        &push_env,
        Some(process::OTHER_CPU),
    );

    let pull_output = pull_proc.wait_with_output(Duration::from_secs(duration as u64 + 30));

    let push_cpu = process::read_proc_cpu(push_proc.pid());
    let timed_push = def.name != "zmq.rs" && def.name != "rzmq" && def.name != "rzmq-iouring";
    let push_output = if timed_push {
        push_proc.wait_with_output(Duration::from_secs(duration as u64 + 30))
    } else {
        push_proc.kill();
        None
    };

    let pull_cpu_proc = process::read_proc_cpu(pull_proc.pid());
    // pull_proc dropped by wait_with_output

    if transport == TransportKind::Ipc {
        cleanup_ipc_addr(&addr, def.name);
    }

    let Some(output) = pull_output else {
        return zero_result(duration);
    };

    let Some(r) = parse::parse_throughput(&output, size) else {
        return zero_result(duration);
    };
    let p = push_output
        .as_deref()
        .and_then(|output| parse::parse_multi_throughput(output, size, peers));
    CellResult {
        msgs_s: r.msgs_s,
        mbps: r.mbps,
        elapsed: r.elapsed,
        push_cpu: Some(push_cpu),
        pull_cpu: Some(r.pull_cpu.unwrap_or(pull_cpu_proc)),
        peer_min: p.as_ref().and_then(|p| p.peer_min),
        peer_max: p.as_ref().and_then(|p| p.peer_max),
        peer_p10: p.as_ref().and_then(|p| p.peer_p10),
        peer_p25: p.as_ref().and_then(|p| p.peer_p25),
        peer_median: p.as_ref().and_then(|p| p.peer_median),
        peer_p75: p.as_ref().and_then(|p| p.peer_p75),
        peer_p90: p.as_ref().and_then(|p| p.peer_p90),
    }
}

struct LatencyResult {
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    iterations: u64,
    cpu_time: Option<f64>,
    req_cpu: Option<f64>,
    elapsed: Option<f64>,
}

#[expect(clippy::too_many_arguments)]
#[allow(clippy::needless_late_init, clippy::similar_names)]
fn run_latency_cell(
    binary: &Path,
    peer_binary: &Path,
    def: &ImplDef,
    transport: TransportKind,
    size: u64,
    iterations: u64,
    warmup: u64,
    timeout: u64,
    base_port: u16,
) -> Option<LatencyResult> {
    let binary_str = binary.to_str().unwrap();
    let peer_binary_str = peer_binary.to_str().unwrap();
    let size_str = size.to_string();
    let iters_str = iterations.to_string();
    let warmup_str = warmup.to_string();

    if transport == TransportKind::Inproc {
        let name = addr_for(transport, def.prefix, 0, base_port, def.name);
        let env: Vec<(&str, &str)> = def.env.to_vec();
        let (out, cpu) = process::capture_with_cpu(
            &[
                binary_str,
                def.inproc_lat_subcmd,
                &name,
                &size_str,
                &iters_str,
                &warmup_str,
            ],
            &env,
            Some(process::MEASURED_CPU),
            Duration::from_secs(timeout + 30),
        )?;
        let r = parse::parse_latency(&out)?;
        return Some(LatencyResult {
            p50_us: r.p50_us,
            p99_us: r.p99_us,
            p999_us: r.p999_us,
            max_us: r.max_us,
            iterations: r.iterations,
            cpu_time: Some(cpu),
            req_cpu: Some(cpu),
            elapsed: r.elapsed,
        });
    }

    let addr = addr_for(transport, def.prefix, 0, base_port, def.name);
    let connect_addr;

    let rep_env: Vec<(&str, &str)> = def.env.to_vec();
    let req_env: Vec<(&str, &str)> = def.env.to_vec();

    let mut rep_cmd = vec![peer_binary_str, "rep"];
    if transport == TransportKind::Tcp {
        rep_cmd.extend(["tcp://127.0.0.1:0", &size_str]);
    } else {
        rep_cmd.extend([addr.as_str(), &size_str]);
    }

    let coord = (transport == TransportKind::Tcp).then(CoordSocket::bind_new);
    let mut spawn_env = rep_env.clone();
    if let Some(ref c) = coord {
        spawn_env.push(("OMQ_BENCH_COORD", c.endpoint()));
    }
    let mut rep_proc = process::spawn(&rep_cmd, &spawn_env, Some(process::OTHER_CPU));

    if let Some(c) = coord {
        let port = c
            .recv_ready_port(Duration::from_secs(10))
            .expect("coord: no READY from rep peer");
        connect_addr = format!("tcp://127.0.0.1:{port}");
    } else {
        std::thread::sleep(Duration::from_millis(100));
        connect_addr = addr.clone();
    }

    let req_output = process::capture(
        &[
            binary_str,
            "req",
            &connect_addr,
            &size_str,
            &iters_str,
            &warmup_str,
        ],
        &req_env,
        Some(process::MEASURED_CPU),
        Duration::from_secs(timeout + 30),
    );

    let rep_cpu = process::read_proc_cpu(rep_proc.pid());
    rep_proc.kill();

    if transport == TransportKind::Ipc {
        cleanup_ipc_addr(&addr, def.name);
    }

    let output = req_output?;
    let r = parse::parse_latency(&output)?;

    let req_cpu = r.req_cpu;
    let cpu_time = match (req_cpu, rep_cpu) {
        (Some(rc), _) => Some(rc + rep_cpu),
        _ => None,
    };

    Some(LatencyResult {
        p50_us: r.p50_us,
        p99_us: r.p99_us,
        p999_us: r.p999_us,
        max_us: r.max_us,
        iterations: r.iterations,
        cpu_time,
        req_cpu,
        elapsed: r.elapsed,
    })
}

fn representative_of(rounds: u32, mut f: impl FnMut(u32) -> CellResult) -> CellResult {
    let mut results = Vec::new();
    for i in 0..rounds {
        results.push(f(i));
    }
    results.sort_by(|a, b| a.msgs_s.total_cmp(&b.msgs_s));
    results
        .into_iter()
        .nth((rounds.saturating_sub(1) / 2) as usize)
        .unwrap_or_else(|| zero_result(0.0))
}

fn cleanup_ipc_addr(addr: &str, impl_name: &str) {
    if uses_filesystem_ipc(impl_name)
        && let Some(path) = addr.strip_prefix("ipc://")
        && !path.starts_with('@')
    {
        std::fs::remove_file(path).ok();
    }
}

// ---- Orchestration --------------------------------------------------------

#[expect(clippy::too_many_lines)]
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run(args: ComparisonsArgs) {
    process::install_reaper();
    process::cleanup_ipc_sockets();

    let duration = if args.quick_run {
        QUICK_DURATION
    } else {
        args.duration
            .or_else(|| std::env::var("OMQ_BENCH_DURATION").ok()?.parse().ok())
            .unwrap_or(DEFAULT_DURATION)
    };

    let rounds = if args.quick_run {
        1
    } else {
        args.rounds
            .or_else(|| std::env::var("OMQ_BENCH_ROUNDS").ok()?.parse().ok())
            .unwrap_or(DEFAULT_ROUNDS)
    };

    let sizes = if let Some(ref s) = args.sizes {
        s.clone()
    } else if args.quick_run {
        QUICK_SIZES.to_vec()
    } else {
        all_chart_sizes()
    };

    // Validate sizes against chart sizes if not allowed.
    if !args.allow_non_chart_sizes {
        let chart = all_chart_sizes();
        for &s in &sizes {
            if !chart.contains(&s) {
                eprintln!(
                    "warning: size {s} is not a chart size, use --allow-non-chart-sizes to override"
                );
            }
        }
    }

    let mut impl_names: Vec<&str> = if args.omq {
        let mut v = vec!["omq-tokio-1t", "omq-tokio-2t"];
        for name in &args.impls {
            if !v.contains(&name.as_str()) {
                v.push(name.as_str());
            }
        }
        v
    } else if !args.impls.is_empty() {
        args.impls.iter().map(std::string::String::as_str).collect()
    } else {
        IMPLS.iter().filter(|i| i.main).map(|i| i.name).collect()
    };

    if args.curve {
        let families: Vec<&str> = impl_names
            .iter()
            .filter_map(|name| name.split('-').next())
            .collect();
        for imp in IMPLS {
            if imp.class == Some(ImplClass::Curve)
                && !impl_names.contains(&imp.name)
                && imp
                    .name
                    .split('-')
                    .next()
                    .is_some_and(|f| families.contains(&f))
            {
                impl_names.push(imp.name);
            }
        }
    }

    // Validate impl names.
    for &name in &impl_names {
        if find_impl(name).is_none() {
            eprintln!("unknown impl: {name}");
            eprintln!(
                "available: {}",
                IMPLS.iter().map(|i| i.name).collect::<Vec<_>>().join(", ")
            );
            std::process::exit(1);
        }
    }

    let transports: Vec<TransportKind> = args
        .transport
        .iter()
        .map(|t| match t {
            crate::cli::Transport::Tcp => TransportKind::Tcp,
            crate::cli::Transport::Ipc => TransportKind::Ipc,
            crate::cli::Transport::Inproc => TransportKind::Inproc,
            crate::cli::Transport::Ws => TransportKind::Ws,
        })
        .collect();

    let needs_ws = transports.contains(&TransportKind::Ws);
    let needs_curve = args.curve || impl_names.iter().any(|n| n.contains("curve"));

    eprintln!("Building peers...");
    let binaries = build_peers(&impl_names, needs_ws, needs_curve);

    let base_port = args.base_port.unwrap_or_else(|| {
        let mut buf = [0u8; 2];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut f| {
                use std::io::Read;
                f.read_exact(&mut buf)?;
                Ok(())
            })
            .ok();
        let port = u16::from_le_bytes(buf);
        20000 + (port % 20000)
    });

    let run_id = make_run_id(args.id.as_deref());
    let jsonl_path = jsonl::cache_dir().join("comparisons.jsonl");

    let tracker = MeasurementTracker::new();

    let latency_iters = args.latency_iterations;
    let latency_warmup = args.latency_warmup;
    let latency_timeout = args.latency_timeout;

    for &transport in &transports {
        let transport_str = transport.as_str();
        let active_impls: Vec<&str> = impl_names
            .iter()
            .filter(|&&name| {
                let def = find_impl(name).unwrap();
                def.transports.contains(&transport)
            })
            .copied()
            .collect();

        if active_impls.is_empty() {
            continue;
        }

        // Throughput
        if !args.no_throughput {
            eprintln!("\n=== Throughput / {transport_str} ===");
            print_throughput_header(&active_impls);

            for &size in &sizes {
                let mut cells = Vec::with_capacity(active_impls.len());
                for &impl_name in &active_impls {
                    let def = find_impl(impl_name).unwrap();
                    let binary = binaries[impl_name].as_path();
                    let peer_binary = binary;

                    let result = run_throughput_cell(
                        binary,
                        peer_binary,
                        def,
                        transport,
                        size,
                        duration,
                        rounds,
                        base_port,
                    );

                    let cpu_time = match (result.push_cpu, result.pull_cpu) {
                        (Some(pc), Some(rc)) => Some(pc + rc),
                        (Some(pc), None) => Some(pc),
                        _ => None,
                    };

                    let row = ComparisonRow {
                        run_id: run_id.clone(),
                        impl_name: impl_name.to_string(),
                        kind: "throughput".to_string(),
                        transport: transport_str.to_string(),
                        msg_size: size,
                        peers: None,
                        msgs_s: Some(result.msgs_s),
                        mbps: Some(result.mbps),
                        elapsed: Some(result.elapsed),
                        cpu_time,
                        push_cpu_time: result.push_cpu,
                        pull_cpu_time: result.pull_cpu,
                        pub_cpu_time: None,
                        req_cpu_time: None,
                        p50_us: None,
                        p99_us: None,
                        p999_us: None,
                        max_us: None,
                        iterations: None,
                        peer_min: None,
                        peer_max: None,
                        peer_p10: None,
                        peer_p25: None,
                        peer_median: None,
                        peer_p75: None,
                        peer_p90: None,
                        zero_transport: if result.msgs_s == 0.0 {
                            Some(true)
                        } else {
                            None
                        },
                    };
                    jsonl::append_jsonl(&jsonl_path, &row);

                    if size >= 1024 {
                        cells.push(fmt_gbps(result.mbps));
                    } else {
                        cells.push(fmt_rate(result.msgs_s));
                    }
                }
                print_table_row(&size_label(size), &cells, 14);
            }
        }

        // Latency
        if !args.no_latency {
            let latency_sizes = latency_sizes_from(&sizes);
            eprintln!("\n=== Latency / {transport_str} ===");
            print_latency_header(&active_impls);

            for &size in &latency_sizes {
                let mut cells = Vec::with_capacity(active_impls.len());
                for &impl_name in &active_impls {
                    let def = find_impl(impl_name).unwrap();
                    let binary = binaries[impl_name].as_path();

                    let result = run_latency_cell(
                        binary,
                        binary,
                        def,
                        transport,
                        size,
                        latency_iters,
                        latency_warmup,
                        latency_timeout,
                        base_port,
                    );

                    match result {
                        Some(lat) => {
                            let row = ComparisonRow {
                                run_id: run_id.clone(),
                                impl_name: impl_name.to_string(),
                                kind: "latency".to_string(),
                                transport: transport_str.to_string(),
                                msg_size: size,
                                peers: None,
                                msgs_s: None,
                                mbps: None,
                                elapsed: lat.elapsed,
                                cpu_time: lat.cpu_time,
                                push_cpu_time: None,
                                pull_cpu_time: None,
                                pub_cpu_time: None,
                                req_cpu_time: lat.req_cpu,
                                p50_us: Some(lat.p50_us),
                                p99_us: Some(lat.p99_us),
                                p999_us: Some(lat.p999_us),
                                max_us: Some(lat.max_us),
                                iterations: Some(lat.iterations),
                                peer_min: None,
                                peer_max: None,
                                peer_p10: None,
                                peer_p25: None,
                                peer_median: None,
                                peer_p75: None,
                                peer_p90: None,
                                zero_transport: None,
                            };
                            jsonl::append_jsonl(&jsonl_path, &row);
                            cells.push(format!("{:.1}", lat.p50_us));
                        }
                        None => {
                            cells.push("-".to_string());
                        }
                    }
                }
                print_table_row(&size_label(size), &cells, 14);
            }
        }

        // Pub/sub
        if !args.no_pubsub && transport != TransportKind::Inproc {
            let pubsub_impls: Vec<&str> = active_impls
                .iter()
                .filter(|&&name| {
                    let def = find_impl(name).unwrap();
                    def.supports_pubsub && def.class != Some(ImplClass::Curve)
                })
                .copied()
                .collect();

            let pubsub_sizes: Vec<u64> = sizes
                .iter()
                .copied()
                .filter(|s| COMPARISON_CHART_SIZES.contains(s))
                .collect();

            for &peer_count in &args.pubsub_peers {
                eprintln!("\n=== PubSub {peer_count}p / {transport_str} ===");
                print_throughput_header(&pubsub_impls);

                for &size in &pubsub_sizes {
                    let mut cells = Vec::with_capacity(pubsub_impls.len());
                    for &impl_name in &pubsub_impls {
                        let def = find_impl(impl_name).unwrap();
                        let binary = binaries[impl_name].as_path();
                        let peer_binary = binary;

                        let result = run_pubsub_cell(
                            binary,
                            peer_binary,
                            def,
                            transport,
                            size,
                            peer_count,
                            duration,
                            rounds,
                            base_port,
                        );

                        let cpu_time = match (result.push_cpu, result.pull_cpu) {
                            (Some(pc), Some(rc)) => Some(pc + rc),
                            (Some(pc), None) => Some(pc),
                            _ => None,
                        };

                        let row = ComparisonRow {
                            run_id: run_id.clone(),
                            impl_name: impl_name.to_string(),
                            kind: "pub_sub".to_string(),
                            transport: transport_str.to_string(),
                            msg_size: size,
                            peers: Some(peer_count),
                            msgs_s: Some(result.msgs_s),
                            mbps: Some(result.mbps),
                            elapsed: Some(result.elapsed),
                            cpu_time,
                            push_cpu_time: None,
                            pull_cpu_time: None,
                            pub_cpu_time: result.push_cpu,
                            req_cpu_time: None,
                            p50_us: None,
                            p99_us: None,
                            p999_us: None,
                            max_us: None,
                            iterations: None,
                            peer_min: result.peer_min,
                            peer_max: result.peer_max,
                            peer_p10: result.peer_p10,
                            peer_p25: result.peer_p25,
                            peer_median: result.peer_median,
                            peer_p75: result.peer_p75,
                            peer_p90: result.peer_p90,
                            zero_transport: if result.msgs_s == 0.0 {
                                Some(true)
                            } else {
                                None
                            },
                        };
                        jsonl::append_jsonl(&jsonl_path, &row);

                        if size >= 1024 {
                            cells.push(format!("{:.1}", result.mbps / 1000.0));
                        } else {
                            cells.push(format!("{:.0}", result.msgs_s / 1000.0));
                        }
                    }
                    print_table_row(&size_label(size), &cells, 8);
                }
            }
        }

        // Fan-out (TCP only)
        if args.fanout && transport == TransportKind::Tcp {
            let fanout_impls: Vec<&str> = active_impls
                .iter()
                .filter(|&&name| {
                    let def = find_impl(name).unwrap();
                    def.class != Some(ImplClass::Curve)
                })
                .copied()
                .collect();

            let fanout_sizes: Vec<u64> = sizes
                .iter()
                .copied()
                .filter(|s| COMPARISON_CHART_SIZES.contains(s))
                .collect();

            for &peer_count in &args.fanout_peers {
                eprintln!("\n=== FanOut {peer_count}p / {transport_str} ===");
                print_throughput_header(&fanout_impls);

                for &size in &fanout_sizes {
                    let mut cells = Vec::with_capacity(fanout_impls.len());
                    for &impl_name in &fanout_impls {
                        let def = find_impl(impl_name).unwrap();
                        let binary = binaries[impl_name].as_path();
                        let peer_binary = binary;

                        let result = run_fanout_cell(
                            binary,
                            peer_binary,
                            def,
                            transport,
                            size,
                            peer_count,
                            duration,
                            rounds,
                            base_port,
                        );

                        let row = ComparisonRow {
                            run_id: run_id.clone(),
                            impl_name: impl_name.to_string(),
                            kind: "fan_out".to_string(),
                            transport: transport_str.to_string(),
                            msg_size: size,
                            peers: Some(peer_count),
                            msgs_s: Some(result.msgs_s),
                            mbps: Some(result.mbps),
                            elapsed: Some(result.elapsed),
                            cpu_time: result.push_cpu,
                            push_cpu_time: result.push_cpu,
                            pull_cpu_time: result.pull_cpu,
                            pub_cpu_time: None,
                            req_cpu_time: None,
                            p50_us: None,
                            p99_us: None,
                            p999_us: None,
                            max_us: None,
                            iterations: None,
                            peer_min: result.peer_min,
                            peer_max: result.peer_max,
                            peer_p10: result.peer_p10,
                            peer_p25: result.peer_p25,
                            peer_median: result.peer_median,
                            peer_p75: result.peer_p75,
                            peer_p90: result.peer_p90,
                            zero_transport: if result.msgs_s == 0.0 {
                                Some(true)
                            } else {
                                None
                            },
                        };
                        jsonl::append_jsonl(&jsonl_path, &row);

                        if size >= 1024 {
                            cells.push(format!("{:.1}", result.mbps / 1000.0));
                        } else {
                            cells.push(format!("{:.0}", result.msgs_s / 1000.0));
                        }
                    }
                    print_table_row(&size_label(size), &cells, 8);
                }
            }
        }

        // Fan-in (TCP only)
        if args.fanin && transport == TransportKind::Tcp {
            let fanin_impls: Vec<&str> = active_impls
                .iter()
                .filter(|&&name| {
                    let def = find_impl(name).unwrap();
                    def.class != Some(ImplClass::Curve)
                })
                .copied()
                .collect();

            let fanin_sizes: Vec<u64> = sizes
                .iter()
                .copied()
                .filter(|s| COMPARISON_CHART_SIZES.contains(s))
                .collect();

            for &peer_count in &args.fanin_peers {
                eprintln!("\n=== FanIn {peer_count}p / {transport_str} ===");
                print_throughput_header(&fanin_impls);

                for &size in &fanin_sizes {
                    let mut cells = Vec::with_capacity(fanin_impls.len());
                    for &impl_name in &fanin_impls {
                        let def = find_impl(impl_name).unwrap();
                        let binary = binaries[impl_name].as_path();
                        let peer_binary = binary;

                        let result = run_fanin_cell(
                            binary,
                            peer_binary,
                            def,
                            transport,
                            size,
                            peer_count,
                            duration,
                            rounds,
                            base_port,
                        );

                        let cpu_time = match (result.push_cpu, result.pull_cpu) {
                            (Some(pc), Some(rc)) => Some(pc + rc),
                            _ => result.pull_cpu,
                        };

                        let row = ComparisonRow {
                            run_id: run_id.clone(),
                            impl_name: impl_name.to_string(),
                            kind: "fan_in".to_string(),
                            transport: transport_str.to_string(),
                            msg_size: size,
                            peers: Some(peer_count),
                            msgs_s: Some(result.msgs_s),
                            mbps: Some(result.mbps),
                            elapsed: Some(result.elapsed),
                            cpu_time,
                            push_cpu_time: result.push_cpu,
                            pull_cpu_time: result.pull_cpu,
                            pub_cpu_time: None,
                            req_cpu_time: None,
                            p50_us: None,
                            p99_us: None,
                            p999_us: None,
                            max_us: None,
                            iterations: None,
                            peer_min: result.peer_min,
                            peer_max: result.peer_max,
                            peer_p10: result.peer_p10,
                            peer_p25: result.peer_p25,
                            peer_median: result.peer_median,
                            peer_p75: result.peer_p75,
                            peer_p90: result.peer_p90,
                            zero_transport: if result.msgs_s == 0.0 {
                                Some(true)
                            } else {
                                None
                            },
                        };
                        jsonl::append_jsonl(&jsonl_path, &row);

                        if size >= 1024 {
                            cells.push(format!("{:.1}", result.mbps / 1000.0));
                        } else {
                            cells.push(format!("{:.0}", result.msgs_s / 1000.0));
                        }
                    }
                    print_table_row(&size_label(size), &cells, 8);
                }
            }
        }
    }

    // CURVE pub/sub
    if args.curve {
        let curve_impls: Vec<&str> = impl_names
            .iter()
            .filter(|&&name| {
                let def = find_impl(name).unwrap();
                def.class == Some(ImplClass::Curve) && def.transports.contains(&TransportKind::Tcp)
            })
            .copied()
            .collect();

        if !curve_impls.is_empty() {
            let peer_count = args.curve_peers;
            let curve_sizes: Vec<u64> = sizes
                .iter()
                .copied()
                .filter(|s| COMPARISON_CHART_SIZES.contains(s))
                .collect();
            eprintln!("\n=== CURVE PubSub {peer_count}p / tcp ===");
            print_throughput_header(&curve_impls);

            for &size in &curve_sizes {
                let mut cells = Vec::with_capacity(curve_impls.len());
                for &impl_name in &curve_impls {
                    let def = find_impl(impl_name).unwrap();
                    let binary = binaries[impl_name].as_path();
                    let peer_binary = binary;

                    let result = run_pubsub_cell(
                        binary,
                        peer_binary,
                        def,
                        TransportKind::Tcp,
                        size,
                        peer_count,
                        duration,
                        rounds,
                        base_port,
                    );

                    let cpu_time = match (result.push_cpu, result.pull_cpu) {
                        (Some(pc), Some(rc)) => Some(pc + rc),
                        (Some(pc), None) => Some(pc),
                        _ => None,
                    };

                    let row = ComparisonRow {
                        run_id: run_id.clone(),
                        impl_name: impl_name.to_string(),
                        kind: "pub_sub".to_string(),
                        transport: "tcp".to_string(),
                        msg_size: size,
                        peers: Some(peer_count),
                        msgs_s: Some(result.msgs_s),
                        mbps: Some(result.mbps),
                        elapsed: Some(result.elapsed),
                        cpu_time,
                        push_cpu_time: None,
                        pull_cpu_time: None,
                        pub_cpu_time: result.push_cpu,
                        req_cpu_time: None,
                        p50_us: None,
                        p99_us: None,
                        p999_us: None,
                        max_us: None,
                        iterations: None,
                        peer_min: result.peer_min,
                        peer_max: result.peer_max,
                        peer_p10: result.peer_p10,
                        peer_p25: result.peer_p25,
                        peer_median: result.peer_median,
                        peer_p75: result.peer_p75,
                        peer_p90: result.peer_p90,
                        zero_transport: if result.msgs_s == 0.0 {
                            Some(true)
                        } else {
                            None
                        },
                    };
                    jsonl::append_jsonl(&jsonl_path, &row);

                    if size >= 1024 {
                        cells.push(fmt_gbps(result.mbps));
                    } else {
                        cells.push(fmt_rate(result.msgs_s));
                    }
                }
                print_table_row(&size_label(size), &cells, 14);
            }
        }
    }

    tracker.check();

    eprintln!("\nResults appended to {}", jsonl_path.display());
}

fn print_throughput_header(impls: &[&str]) {
    eprint!("{:>8}", "");
    for &name in impls {
        eprint!("  {name:>14}");
    }
    eprintln!();
}

fn print_latency_header(impls: &[&str]) {
    eprint!("{:>8}", "");
    for &name in impls {
        eprint!("  {name:>14}");
    }
    eprintln!("  (p50 us)");
}

fn print_table_row(label: &str, cells: &[String], width: usize) {
    use std::fmt::Write as _;

    let mut line = format!("{label:>8}");
    for cell in cells {
        write!(&mut line, "  {cell:>width$}").expect("write to string");
    }
    eprintln!("{line}");
}

fn fmt_rate(val: f64) -> String {
    if val >= 1_000_000.0 {
        format!("{:.2}M msg/s", val / 1_000_000.0)
    } else if val >= 1000.0 {
        format!("{:.0}K msg/s", val / 1000.0)
    } else {
        format!("{val:.0} msg/s")
    }
}

fn fmt_gbps(val: f64) -> String {
    if val >= 1000.0 {
        format!("{:.2} GB/s", val / 1000.0)
    } else {
        format!("{val:.0} MB/s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_threads_from_env<'a>(env: &'a [(&'a str, &'a str)]) -> &'a str {
        env.iter()
            .find_map(|&(k, v)| matches!(k, "OMQ_IO_THREADS" | "ZMQ_IO_THREADS").then_some(v))
            .unwrap_or("1")
    }

    #[test]
    fn two_io_omq_impls_use_blocking_binary_source() {
        let ct = find_impl("omq-tokio-ct").unwrap();
        let two_thread = find_impl("omq-tokio-2t").unwrap();
        let curve_two_thread = find_impl("omq-curve-2t").unwrap();

        assert_eq!(ct.binary_from, None);
        assert_eq!(two_thread.binary_from, Some("omq-tokio-1t"));
        assert_eq!(curve_two_thread.binary_from, Some("omq-tokio-1t"));
    }

    #[test]
    fn paired_benchmark_envs_match_exactly() {
        for def in IMPLS {
            let sender_env = def.env.to_vec();
            let receiver_env = def.env.to_vec();
            let sender_io = io_threads_from_env(&sender_env);
            let receiver_io = io_threads_from_env(&receiver_env);

            assert_eq!(sender_env, receiver_env, "{}", def.name);
            assert_eq!(sender_io, receiver_io, "{}", def.name);
        }
    }

    #[test]
    fn two_io_impls_configure_two_io_threads() {
        for name in [
            "omq-tokio-2t",
            "libzmq-2t",
            "omq-curve-2t",
            "libzmq-curve-2t",
        ] {
            let def = find_impl(name).unwrap();
            assert_eq!(impl_io_threads(def), "2", "{name}");
        }
    }

    #[test]
    fn latency_runner_caps_sizes_at_4kib() {
        assert_eq!(
            latency_sizes_from(&[16, 1024, 4096, 8192, 16384]),
            vec![16, 1024, 4096]
        );
    }
}
