//! Fan-out and fan-in cells: one binder against many connecting peers.

use super::{
    BinderCmd, Cell, CellResult, push_start_env, representative_of, size_and_peers, spawn_binder,
    start_at, zero_result,
};
use crate::bench::comparisons::impls::ImplDef;
use crate::parse;
use crate::process;
use std::time::Duration;

pub(crate) fn run_fanout_cell(
    cell: &Cell<'_>,
    peers: u64,
    duration: f64,
    rounds: u32,
) -> CellResult {
    representative_of(rounds, |_| run_fanout_once(cell, peers, duration))
}

fn run_fanout_once(cell: &Cell<'_>, peers: u64, duration: f64) -> CellResult {
    let def = cell.def;
    let size_str = cell.size.to_string();
    let peers_str = peers.to_string();
    let addr = cell.addr();

    let start_at = start_at();
    let mut push_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut pull_env: Vec<(&str, &str)> = def.env.to_vec();
    push_start_env(&mut push_env, &start_at);
    push_start_env(&mut pull_env, &start_at);

    let tail = size_and_peers(&size_str, &peers_str, def.fanio_needs_peer_count);
    let binder = BinderCmd {
        head: [cell.binary.to_str().unwrap(), def.fanout_subcmd],
        tail: &tail,
        env: &push_env,
        cpus: def.affinity(process::MEASURED_CPU),
        role: "push",
        settle: Duration::from_millis(100),
    };
    let mut pusher = spawn_binder(cell.transport, &addr, &binder);

    let (mut pull_procs, peers_per_process) =
        spawn_fanout_receivers(cell, &pusher.connect_addr, peers, duration, &pull_env);

    let timeout = process::peer_timeout(duration);
    let mut pull_results = Vec::new();
    for pull_proc in &mut pull_procs {
        if let Some(output) = pull_proc.wait_with_output(timeout)
            && let Some(result) =
                parse::parse_multi_throughput(&output, cell.size, peers_per_process)
        {
            pull_results.push(result);
        }
    }

    let push_cpu = process::read_proc_cpu(pusher.guard.pid());
    pusher.guard.kill();
    cell.cleanup(&addr);

    if pull_results.is_empty() {
        return zero_result(duration);
    }

    let receivers = FanoutReceivers {
        results: &pull_results,
        peers_per_process,
        peers,
    };
    receivers.combine(cell.size, push_cpu)
}

/// Spawns the PULL side, split across `OMQ_BENCH_RECEIVER_PROCS` processes
/// (default 1), and returns them with the peer count of each process.
fn spawn_fanout_receivers(
    cell: &Cell<'_>,
    connect_addr: &str,
    peers: u64,
    duration: f64,
    env: &[(&str, &str)],
) -> (Vec<process::ProcessGuard>, u64) {
    let receiver_processes = std::env::var("OMQ_BENCH_RECEIVER_PROCS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1);
    assert!(receiver_processes > 0 && peers.is_multiple_of(receiver_processes));
    let peers_per_process = peers / receiver_processes;

    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let local_peers_str = peers_per_process.to_string();
    let mut pull_procs = Vec::new();
    for _ in 0..receiver_processes {
        pull_procs.push(process::spawn(
            &[
                cell.peer_binary.to_str().unwrap(),
                "multi-pull",
                connect_addr,
                &size_str,
                &dur_str,
                &local_peers_str,
            ],
            env,
            cell.def.affinity(process::OTHER_CPU),
        ));
    }

    (pull_procs, peers_per_process)
}

/// Parsed results of all fan-out receiver processes.
struct FanoutReceivers<'a> {
    results: &'a [parse::MultiThroughputResult],
    peers_per_process: u64,
    peers: u64,
}

impl FanoutReceivers<'_> {
    /// One cell result: total messages over the slowest receiver's elapsed
    /// time, plus the spread of all per-peer rates.
    fn combine(&self, size: u64, push_cpu: f64) -> CellResult {
        let total_msgs: f64 = self
            .results
            .iter()
            .map(|r| r.rate.msgs_s * self.peers_per_process as f64 * r.rate.elapsed)
            .sum();
        let elapsed = self
            .results
            .iter()
            .map(|r| r.rate.elapsed)
            .fold(0.0, f64::max);
        let mut peer_rates: Vec<f64> = self
            .results
            .iter()
            .flat_map(|r| r.peer_rates.iter().copied())
            .collect();
        peer_rates.sort_unstable_by(f64::total_cmp);
        let cpu: f64 = self.results.iter().filter_map(|r| r.rate.pull_cpu).sum();

        CellResult {
            blocking_inproc: None,
            msgs_s: total_msgs / elapsed / self.peers as f64,
            mbps: total_msgs * size as f64 / elapsed / 1_000_000.0,
            elapsed,
            push_cpu: Some(push_cpu),
            pull_cpu: Some(cpu),
            spread: parse::PeerSpread::from_sorted(&peer_rates),
        }
    }
}

pub(crate) fn run_fanin_cell(
    cell: &Cell<'_>,
    peers: u64,
    duration: f64,
    rounds: u32,
) -> CellResult {
    representative_of(rounds, |_| run_fanin_once(cell, peers, duration))
}

fn run_fanin_once(cell: &Cell<'_>, peers: u64, duration: f64) -> CellResult {
    let def = cell.def;
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();
    let addr = cell.addr();

    let start_at = start_at();
    let mut pull_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut push_env: Vec<(&str, &str)> = def.env.to_vec();
    push_start_env(&mut pull_env, &start_at);
    push_start_env(&mut push_env, &start_at);

    // pull-bind binds on the measured CPU.
    let binder = BinderCmd {
        head: [cell.binary.to_str().unwrap(), "pull-bind"],
        tail: &[&size_str, &dur_str],
        env: &pull_env,
        cpus: def.affinity(process::MEASURED_CPU),
        role: "pull-bind",
        settle: Duration::from_millis(100),
    };
    let mut puller = spawn_binder(cell.transport, &addr, &binder);
    // Fan-in closes the coord socket as soon as the binder is ready.
    drop(puller.coord.take());

    let mut push_proc = process::spawn(
        &[
            cell.peer_binary.to_str().unwrap(),
            "multi-push",
            &puller.connect_addr,
            &size_str,
            &peers_str,
            &dur_str,
        ],
        &push_env,
        def.affinity(process::OTHER_CPU),
    );

    let timeout = process::peer_timeout(duration);
    let pull_output = puller.guard.wait_with_output(timeout);
    let push_cpu = process::read_proc_cpu(push_proc.pid());
    let push_output = finish_fanin_push(&mut push_proc, def, timeout);
    let pull_cpu_proc = process::read_proc_cpu(puller.guard.pid());
    cell.cleanup(&addr);

    let Some(r) = pull_output.and_then(|output| parse::parse_throughput(&output, cell.size)) else {
        return zero_result(duration);
    };
    // Push peers that were killed report no per-peer rates.
    let spread = push_output
        .as_deref()
        .and_then(|output| parse::parse_multi_throughput(output, cell.size, peers))
        .map(|multi| multi.spread)
        .unwrap_or_default();
    CellResult {
        msgs_s: r.msgs_s,
        mbps: r.mbps,
        elapsed: r.elapsed,
        push_cpu: Some(push_cpu),
        pull_cpu: Some(r.pull_cpu.unwrap_or(pull_cpu_proc)),
        spread,
        ..zero_result(duration)
    }
}

/// Waits for the fan-in PUSH side. The zmq.rs and rzmq push peers do not
/// stop on their own, so they are killed and report no per-peer rates.
fn finish_fanin_push(
    push_proc: &mut process::ProcessGuard,
    def: &ImplDef,
    timeout: Duration,
) -> Option<String> {
    let timed_push = !matches!(def.name, "zmq.rs" | "rzmq" | "rzmq-iouring");
    if timed_push {
        push_proc.wait_with_output(timeout)
    } else {
        push_proc.kill();
        None
    }
}
