use crate::cli::ComparisonsArgs;
use crate::jsonl::{self, ComparisonRow};
use crate::process;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

mod build;
mod cells;
mod impls;

use build::build_peers;
use cells::{
    Cell, CellResult, LatencyResult, LatencyWorkload, run_fanin_cell, run_fanout_cell,
    run_latency_cell, run_pubsub_cell, run_throughput_cell,
};
use impls::{
    IMPLS, ImplClass, ImplDef, TransportKind, find_impl, latency_env, supports_fanio,
    supports_latency_pair, supports_pushpull,
};

const COMPARISON_CHART_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
const MAIN_EXTRA_CHART_SIZES: &[u64] = &[
    32, 128, 512, 2048, 8192, 32768, 262_144, 4_194_304, 8_388_608,
];
const QUICK_SIZES: &[u64] = &[64, 1024, 4096];

const LATENCY_MAX_SIZE: u64 = 4096;

const DEFAULT_DURATION: f64 = 3.0;
const QUICK_DURATION: f64 = 1.5;
const DEFAULT_ROUNDS: u32 = 1;

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
        TransportKind::Tcp | TransportKind::Quic => "0".to_string(),
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

fn cleanup_ipc_addr(addr: &str, impl_name: &str) {
    if uses_filesystem_ipc(impl_name)
        && let Some(path) = addr.strip_prefix("ipc://")
        && !path.starts_with('@')
    {
        std::fs::remove_file(path).ok();
    }
}

// ---- Orchestration --------------------------------------------------------

fn selected_sizes(args: &ComparisonsArgs) -> Vec<u64> {
    let sizes = if let Some(sizes) = &args.sizes {
        sizes.clone()
    } else if args.quick_run {
        QUICK_SIZES.to_vec()
    } else {
        all_chart_sizes()
    };
    if !args.allow_non_chart_sizes {
        let chart = all_chart_sizes();
        for &size in &sizes {
            if !chart.contains(&size) {
                eprintln!(
                    "warning: size {size} is not a chart size, use --allow-non-chart-sizes to override"
                );
            }
        }
    }
    sizes
}

fn selected_impl_names(args: &ComparisonsArgs) -> Vec<&str> {
    let mut names: Vec<&str> = if args.omq {
        let mut names = vec!["omq-tokio-1t", "omq-tokio-2t"];
        for name in &args.impls {
            if !names.contains(&name.as_str()) {
                names.push(name);
            }
        }
        names
    } else if !args.impls.is_empty() {
        args.impls.iter().map(String::as_str).collect()
    } else {
        IMPLS
            .iter()
            .filter(|imp| imp.main)
            .map(|imp| imp.name)
            .collect()
    };
    if args.curve {
        let families: Vec<&str> = names
            .iter()
            .filter_map(|name| name.split('-').next())
            .collect();
        for imp in IMPLS {
            if imp.class == Some(ImplClass::Curve)
                && !names.contains(&imp.name)
                && imp
                    .name
                    .split('-')
                    .next()
                    .is_some_and(|family| families.contains(&family))
            {
                names.push(imp.name);
            }
        }
    }
    names
}

fn random_base_port() -> u16 {
    let mut buf = [0u8; 2];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| {
            use std::io::Read;
            file.read_exact(&mut buf)
        })
        .ok();
    20_000 + (u16::from_le_bytes(buf) % 20_000)
}

#[derive(Clone, Copy, Debug)]
enum CellKind {
    Throughput,
    PubSub,
    FanOut,
    FanIn,
}

impl CellKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::PubSub => "pub_sub",
            Self::FanOut => "fan_out",
            Self::FanIn => "fan_in",
        }
    }
}

struct ComparisonRun<'a> {
    args: &'a ComparisonsArgs,
    sizes: &'a [u64],
    impl_names: &'a [&'a str],
    binaries: &'a HashMap<String, PathBuf>,
    duration: f64,
    rounds: u32,
    base_port: u16,
    run_id: &'a str,
    jsonl_path: &'a Path,
}

impl ComparisonRun<'_> {
    fn row(&self, name: &str, kind: &str, transport: TransportKind, size: u64) -> ComparisonRow {
        ComparisonRow {
            run_id: self.run_id.to_owned(),
            impl_name: name.to_owned(),
            kind: kind.to_owned(),
            transport: transport.as_str().to_owned(),
            msg_size: size,
            runtime_workers: find_impl(name)
                .is_some_and(ImplDef::uses_mt_runtime)
                .then(|| {
                    std::env::var("TOKIO_WORKER_THREADS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .filter(|workers| *workers > 0)
                        .unwrap_or_else(|| {
                            std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
                        })
                }),
            ..ComparisonRow::default()
        }
    }

    /// Cell of `name` at `size`. Both sides run the same peer binary.
    fn cell(&self, name: &str, transport: TransportKind, size: u64) -> Cell<'_> {
        let binary = self.binaries[name].as_path();
        Cell {
            binary,
            peer_binary: binary,
            def: find_impl(name).unwrap(),
            transport,
            size,
            base_port: self.base_port,
        }
    }

    fn record_cell(
        &self,
        cell: &Cell<'_>,
        kind: CellKind,
        peers: Option<u64>,
        result: &CellResult,
    ) {
        let mut row = self.row(cell.def.name, kind.as_str(), cell.transport, cell.size);
        row.peers = peers;
        row.blocking_inproc.clone_from(&result.blocking_inproc);
        row.msgs_s = Some(result.msgs_s);
        row.mbps = Some(result.mbps);
        row.elapsed = Some(result.elapsed);
        row.cpu_time = match kind {
            CellKind::Throughput | CellKind::PubSub => match (result.push_cpu, result.pull_cpu) {
                (Some(push), Some(pull)) => Some(push + pull),
                (Some(push), None) => Some(push),
                _ => None,
            },
            CellKind::FanOut => result.push_cpu,
            CellKind::FanIn => match (result.push_cpu, result.pull_cpu) {
                (Some(push), Some(pull)) => Some(push + pull),
                _ => result.pull_cpu,
            },
        };
        match kind {
            CellKind::Throughput | CellKind::FanOut | CellKind::FanIn => {
                row.push_cpu_time = result.push_cpu;
                row.pull_cpu_time = result.pull_cpu;
            }
            CellKind::PubSub => {
                row.pub_cpu_time = result.push_cpu;
                row.pull_cpu_time = result.pull_cpu;
                row.receiver_io_threads = self.args.pubsub_receiver_io_threads;
                row.receiver_workers = self.args.pubsub_receiver_workers;
                row.receiver_cpus
                    .clone_from(&self.args.pubsub_receiver_cpus);
            }
        }
        row.peer_min = result.peer_min;
        row.peer_max = result.peer_max;
        row.peer_p10 = result.peer_p10;
        row.peer_p25 = result.peer_p25;
        row.peer_median = result.peer_median;
        row.peer_p75 = result.peer_p75;
        row.peer_p90 = result.peer_p90;
        row.zero_transport = (result.msgs_s == 0.0).then_some(true);
        jsonl::append_jsonl(self.jsonl_path, &row);
    }

    fn record_latency(
        &self,
        cell: &Cell<'_>,
        workload: &LatencyWorkload<'_>,
        result: &LatencyResult,
    ) {
        let name = cell.def.name;
        let mut row = self.row(name, "latency", cell.transport, cell.size);
        row.elapsed = result.elapsed;
        row.cpu_time = result.cpu_time;
        row.req_cpu_time = result.req_cpu;
        row.p50_us = Some(result.p50_us);
        row.p99_us = Some(result.p99_us);
        row.p999_us = Some(result.p999_us);
        row.max_us = Some(result.max_us);
        row.iterations = Some(result.iterations);
        row.latency_pair = Some(workload.pair.to_owned());
        row.workload_profile = Some(workload.profile.to_owned());
        if name.starts_with("omq-") {
            row.recv_spin_us = Some(
                latency_env(cell.def, workload.pair, workload.profile)
                    .iter()
                    .find(|&&(key, _)| key == "OMQ_BENCH_RECV_SPIN_US")
                    .unwrap()
                    .1
                    .parse()
                    .unwrap(),
            );
        }
        jsonl::append_jsonl(self.jsonl_path, &row);
    }

    fn multi_peer(&self, name: &str) -> bool {
        find_impl(name).unwrap().class != Some(ImplClass::Curve)
            || self.args.impls.iter().any(|imp| imp == name)
    }

    fn chart_sizes(&self) -> Vec<u64> {
        self.sizes
            .iter()
            .copied()
            .filter(|size| COMPARISON_CHART_SIZES.contains(size))
            .collect()
    }

    fn run_transport(&self, transport: TransportKind) {
        let active: Vec<&str> = self
            .impl_names
            .iter()
            .copied()
            .filter(|name| find_impl(name).unwrap().transports.contains(&transport))
            .collect();
        if active.is_empty() {
            return;
        }
        if !self.args.no_throughput {
            self.run_throughput(transport, &active);
        }
        if !self.args.no_latency {
            self.run_latency(transport, &active);
        }
        if !self.args.no_pubsub && transport != TransportKind::Inproc {
            let pubsub_impls: Vec<&str> = active
                .iter()
                .copied()
                .filter(|name| find_impl(name).unwrap().supports_pubsub && self.multi_peer(name))
                .collect();
            self.run_pubsub(transport, &pubsub_impls, &self.args.pubsub_peers, false);
        }
        if matches!(transport, TransportKind::Tcp | TransportKind::Quic) {
            if self.args.fanout {
                self.run_fanio(transport, &active, CellKind::FanOut);
            }
            if self.args.fanin {
                self.run_fanio(transport, &active, CellKind::FanIn);
            }
        }
    }

    fn run_throughput(&self, transport: TransportKind, active: &[&str]) {
        let impls: Vec<&str> = active
            .iter()
            .copied()
            .filter(|name| supports_pushpull(find_impl(name).unwrap()))
            .collect();
        if impls.is_empty() {
            return;
        }
        eprintln!("\n=== Throughput / {} ===", transport.as_str());
        print_throughput_header(&impls);
        for &size in self.sizes {
            let mut cells = Vec::with_capacity(impls.len());
            for &name in &impls {
                let cell = self.cell(name, transport, size);
                let result = run_throughput_cell(&cell, self.duration, self.rounds);
                self.record_cell(&cell, CellKind::Throughput, None, &result);
                cells.push(if size >= 1024 {
                    fmt_gbps(result.mbps)
                } else {
                    fmt_rate(result.msgs_s)
                });
            }
            print_table_row(&size_label(size), &cells, 14);
        }
    }

    fn run_latency(&self, transport: TransportKind, active: &[&str]) {
        let sizes = latency_sizes_from(self.sizes);
        for pair in &self.args.latency_pairs {
            for profile in &self.args.latency_profiles {
                let impls: Vec<&str> = active
                    .iter()
                    .copied()
                    .filter(|name| {
                        let def = find_impl(name).unwrap();
                        supports_latency_pair(def, pair, profile)
                            && (transport != TransportKind::Inproc
                                || !def.inproc_lat_subcmd.is_empty())
                    })
                    .collect();
                if impls.is_empty() {
                    continue;
                }

                eprintln!(
                    "\n=== Latency / {pair} / {profile} / {} (p99 us) ===",
                    transport.as_str()
                );
                print_latency_header(&impls);
                let workload = LatencyWorkload {
                    pair,
                    profile,
                    iterations: self.args.latency_iterations,
                    warmup: self.args.latency_warmup,
                    timeout: self.args.latency_timeout,
                };

                for &size in &sizes {
                    let mut cells = Vec::with_capacity(impls.len());
                    for &name in &impls {
                        let cell = self.cell(name, transport, size);
                        let result = self.measure_latency(&cell, &workload);
                        self.record_latency(&cell, &workload, &result);
                        cells.push(format!("{:.1}", result.p99_us));
                    }
                    print_table_row(&size_label(size), &cells, 14);
                }
            }
        }
    }

    /// Runs every latency round of one cell and keeps the median-p99 round.
    fn measure_latency(&self, cell: &Cell<'_>, workload: &LatencyWorkload<'_>) -> LatencyResult {
        let mut results = Vec::new();

        for round in 0..self.rounds {
            let result = run_latency_cell(cell, workload).unwrap_or_else(|| {
                panic!(
                    "latency measurement failed: {} {} {} {} {} B",
                    cell.def.name,
                    workload.pair,
                    workload.profile,
                    cell.transport.as_str(),
                    cell.size
                )
            });

            if self.rounds > 1 {
                eprintln!(
                    "  {} {} B round {}/{}: p50/p99/p99.9 {:.3}/{:.3}/{:.3} us",
                    cell.def.name,
                    cell.size,
                    round + 1,
                    self.rounds,
                    result.p50_us,
                    result.p99_us,
                    result.p999_us
                );
            }

            results.push(result);
        }

        results.sort_by(|a, b| a.p99_us.total_cmp(&b.p99_us));
        results
            .into_iter()
            .nth((self.rounds.saturating_sub(1) / 2) as usize)
            .expect("at least one latency round")
    }

    fn run_pubsub(
        &self,
        transport: TransportKind,
        impls: &[&str],
        peer_counts: &[u64],
        curve: bool,
    ) {
        let sizes = self.chart_sizes();
        for &peers in peer_counts {
            let label = if curve { "CURVE PubSub" } else { "PubSub" };
            eprintln!("\n=== {label} {peers}p / {} ===", transport.as_str());
            print_throughput_header(impls);
            for &size in &sizes {
                let mut cells = Vec::with_capacity(impls.len());
                for &name in impls {
                    let cell = self.cell(name, transport, size);
                    let result =
                        run_pubsub_cell(&cell, peers, self.duration, self.rounds, self.args);
                    self.record_cell(&cell, CellKind::PubSub, Some(peers), &result);
                    cells.push(if curve {
                        if size >= 1024 {
                            fmt_gbps(result.mbps)
                        } else {
                            fmt_rate(result.msgs_s)
                        }
                    } else if size >= 1024 {
                        format!("{:.1}", result.mbps / 1000.0)
                    } else {
                        format!("{:.0}", result.msgs_s / 1000.0)
                    });
                }
                print_table_row(&size_label(size), &cells, if curve { 14 } else { 8 });
            }
        }
    }

    fn run_curve_pubsub(&self) {
        let impls: Vec<&str> = self
            .impl_names
            .iter()
            .copied()
            .filter(|name| {
                let def = find_impl(name).unwrap();
                def.class == Some(ImplClass::Curve) && def.transports.contains(&TransportKind::Tcp)
            })
            .collect();
        if !impls.is_empty() {
            self.run_pubsub(TransportKind::Tcp, &impls, &[self.args.curve_peers], true);
        }
    }

    fn run_fanio(&self, transport: TransportKind, active: &[&str], kind: CellKind) {
        let impls: Vec<&str> = active
            .iter()
            .copied()
            .filter(|name| supports_fanio(find_impl(name).unwrap()) && self.multi_peer(name))
            .collect();
        let sizes = self.chart_sizes();
        let (label, peer_counts) = match kind {
            CellKind::FanOut => ("FanOut", &self.args.fanout_peers),
            CellKind::FanIn => ("FanIn", &self.args.fanin_peers),
            _ => unreachable!(),
        };
        for &peers in peer_counts {
            eprintln!("\n=== {label} {peers}p / {} ===", transport.as_str());
            print_throughput_header(&impls);
            for &size in &sizes {
                let mut cells = Vec::with_capacity(impls.len());
                for &name in &impls {
                    let cell = self.cell(name, transport, size);
                    let result = match kind {
                        CellKind::FanOut => {
                            run_fanout_cell(&cell, peers, self.duration, self.rounds)
                        }
                        CellKind::FanIn => run_fanin_cell(&cell, peers, self.duration, self.rounds),
                        _ => unreachable!(),
                    };
                    self.record_cell(&cell, kind, Some(peers), &result);
                    cells.push(if size >= 1024 {
                        format!("{:.1}", result.mbps / 1000.0)
                    } else {
                        format!("{:.0}", result.msgs_s / 1000.0)
                    });
                }
                print_table_row(&size_label(size), &cells, 8);
            }
        }
    }
}

pub(crate) fn run(args: &ComparisonsArgs) {
    assert!(
        args.pubsub_receiver_cpus.is_none() || std::env::var_os("OMQ_BENCH_TASKSET").is_some(),
        "--pubsub-receiver-cpus requires OMQ_BENCH_TASKSET"
    );
    process::install_reaper();
    process::cleanup_ipc_sockets();
    let (duration, rounds) = duration_and_rounds(args);
    let sizes = selected_sizes(args);
    let impl_names = selected_impl_names(args);
    exit_on_unknown_impls(&impl_names);
    let transports = selected_transports(args);

    let needs_ws = transports.contains(&TransportKind::Ws);
    let needs_quic = transports.contains(&TransportKind::Quic);
    if needs_quic {
        crate::tls::install_bench_credentials();
    }
    let needs_curve = args.curve || impl_names.iter().any(|name| name.contains("curve"));
    eprintln!("Building peers...");
    let binaries = build_peers(&impl_names, needs_ws, needs_quic, needs_curve);

    let base_port = args.base_port.unwrap_or_else(random_base_port);
    let run_id = make_run_id(args.id.as_deref());
    let jsonl_path = jsonl::cache_dir().join("comparisons.jsonl");
    let tracker = MeasurementTracker::new();
    let run = ComparisonRun {
        args,
        sizes: &sizes,
        impl_names: &impl_names,
        binaries: &binaries,
        duration,
        rounds,
        base_port,
        run_id: &run_id,
        jsonl_path: &jsonl_path,
    };

    for &transport in &transports {
        run.run_transport(transport);
    }
    if args.curve {
        run.run_curve_pubsub();
    }
    tracker.check();
    eprintln!("\nResults appended to {}", jsonl_path.display());
}

/// Seconds per cell and rounds per cell: CLI, then environment, then
/// defaults. Quick runs override both.
fn duration_and_rounds(args: &ComparisonsArgs) -> (f64, u32) {
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
    (duration, rounds)
}

fn exit_on_unknown_impls(impl_names: &[&str]) {
    for &name in impl_names {
        if find_impl(name).is_none() {
            eprintln!("unknown impl: {name}");
            eprintln!(
                "available: {}",
                IMPLS
                    .iter()
                    .map(|imp| imp.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            std::process::exit(1);
        }
    }
}

fn selected_transports(args: &ComparisonsArgs) -> Vec<TransportKind> {
    args.transport
        .iter()
        .map(|transport| match transport {
            crate::cli::Transport::Tcp => TransportKind::Tcp,
            crate::cli::Transport::Ipc => TransportKind::Ipc,
            crate::cli::Transport::Inproc => TransportKind::Inproc,
            crate::cli::Transport::Ws => TransportKind::Ws,
            crate::cli::Transport::Quic => TransportKind::Quic,
        })
        .collect()
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
    eprintln!("  (p99 us)");
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
mod tests;
