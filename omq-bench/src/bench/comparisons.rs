use super::make_run_id;
use crate::cli::ComparisonsArgs;
use crate::jsonl::{self, ComparisonRow};
use crate::process;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

mod build;
mod cells;
mod impls;
mod select;
mod table;

use build::build_peers;
use cells::{
    Cell, CellResult, LatencyResult, LatencyWorkload, run_fanin_cell, run_fanout_cell,
    run_latency_cell, run_pubsub_cell, run_throughput_cell,
};
use impls::{
    ImplClass, ImplDef, TransportKind, find_impl, latency_env, supports_fanio,
    supports_latency_pair, supports_pushpull,
};
use select::{
    COMPARISON_CHART_SIZES, duration_and_rounds, exit_on_unknown_impls, latency_sizes_from,
    peer_features, random_base_port, selected_impl_names, selected_sizes, selected_transports,
};
use table::{fmt_compact, fmt_full, print_latency_header, print_throughput_header, run_table};

// ---- Address generation ---------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};
static ADDR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_addr_id() -> u64 {
    ADDR_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn uses_filesystem_ipc(impl_name: &str) -> bool {
    matches!(impl_name, "zmq.rs" | "rzmq" | "rzmq-iouring")
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
        row.set_spread(result.spread);
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
        run_table(self.sizes, &impls, 14, |name, size| {
            let cell = self.cell(name, transport, size);
            let result = run_throughput_cell(&cell, self.duration, self.rounds);
            self.record_cell(&cell, CellKind::Throughput, None, &result);
            fmt_full(&result, size)
        });
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

                run_table(&sizes, &impls, 14, |name, size| {
                    let cell = self.cell(name, transport, size);
                    let result = self.measure_latency(&cell, &workload);
                    self.record_latency(&cell, &workload, &result);
                    format!("{:.1}", result.p99_us)
                });
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
            run_table(&sizes, impls, if curve { 14 } else { 8 }, |name, size| {
                let cell = self.cell(name, transport, size);
                let result = run_pubsub_cell(&cell, peers, self.duration, self.rounds, self.args);
                self.record_cell(&cell, CellKind::PubSub, Some(peers), &result);
                if curve {
                    fmt_full(&result, size)
                } else {
                    fmt_compact(&result, size)
                }
            });
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
            run_table(&sizes, &impls, 8, |name, size| {
                let cell = self.cell(name, transport, size);
                let result = match kind {
                    CellKind::FanOut => run_fanout_cell(&cell, peers, self.duration, self.rounds),
                    CellKind::FanIn => run_fanin_cell(&cell, peers, self.duration, self.rounds),
                    _ => unreachable!(),
                };
                self.record_cell(&cell, kind, Some(peers), &result);
                fmt_compact(&result, size)
            });
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

    if transports.contains(&TransportKind::Quic) {
        crate::tls::install_bench_credentials();
    }
    eprintln!("Building peers...");
    let features = peer_features(args, &impl_names, &transports);
    let binaries = build_peers(&impl_names, &features);

    let base_port = args.base_port.unwrap_or_else(random_base_port);
    let run_id = make_run_id(args.id.as_deref());
    let jsonl_path = jsonl::cache_dir().join("comparisons.jsonl");
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
    eprintln!("\nResults appended to {}", jsonl_path.display());
}

#[cfg(test)]
mod tests;
