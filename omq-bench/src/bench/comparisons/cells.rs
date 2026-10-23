//! Run one benchmark cell: spawn peers, measure, and collect the result.

use super::impls::{ImplDef, TransportKind, latency_env, latency_rep_subcmd, latency_req_subcmd};
use super::{addr_for, cleanup_ipc_addr};
use crate::cli::ComparisonsArgs;
use crate::coord::CoordSocket;
use crate::parse;
use crate::process;
use std::path::Path;
use std::time::Duration;

pub(super) struct CellResult {
    pub(super) blocking_inproc: Option<parse::BlockingInprocStats>,
    pub(super) msgs_s: f64,
    pub(super) mbps: f64,
    pub(super) elapsed: f64,
    pub(super) push_cpu: Option<f64>,
    pub(super) pull_cpu: Option<f64>,
    pub(super) peer_min: Option<f64>,
    pub(super) peer_max: Option<f64>,
    pub(super) peer_p10: Option<f64>,
    pub(super) peer_p25: Option<f64>,
    pub(super) peer_median: Option<f64>,
    pub(super) peer_p75: Option<f64>,
    pub(super) peer_p90: Option<f64>,
}

impl CellResult {
    /// Copies the per-peer rate spread of a multi-peer measurement.
    fn with_spread(self, spread: &parse::MultiThroughputResult) -> Self {
        Self {
            peer_min: spread.peer_min,
            peer_max: spread.peer_max,
            peer_p10: spread.peer_p10,
            peer_p25: spread.peer_p25,
            peer_median: spread.peer_median,
            peer_p75: spread.peer_p75,
            peer_p90: spread.peer_p90,
            ..self
        }
    }
}

fn zero_result(duration: f64) -> CellResult {
    CellResult {
        blocking_inproc: None,
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

/// One measured cell: implementation, transport, and message size.
pub(super) struct Cell<'a> {
    pub(super) binary: &'a Path,
    pub(super) peer_binary: &'a Path,
    pub(super) def: &'a ImplDef,
    pub(super) transport: TransportKind,
    pub(super) size: u64,
    pub(super) base_port: u16,
}

impl Cell<'_> {
    fn addr(&self) -> String {
        addr_for(
            self.transport,
            self.def.prefix,
            0,
            self.base_port,
            self.def.name,
        )
    }

    fn cleanup(&self, addr: &str) {
        if self.transport == TransportKind::Ipc {
            cleanup_ipc_addr(addr, self.def.name);
        }
    }
}

/// Command line, environment, and CPUs of the peer that binds. The bind
/// address goes between `head` and `tail`.
struct BinderCmd<'a> {
    head: [&'a str; 2],
    tail: &'a [&'a str],
    env: &'a [(&'a str, &'a str)],
    cpus: Option<&'a str>,
    /// Peer name for the missing-READY panic.
    role: &'a str,
    /// Startup wait for binders that cannot report readiness.
    settle: Duration,
}

/// Binding peer and the address the other side connects to.
struct BoundPeer {
    guard: process::ProcessGuard,
    connect_addr: String,
    coord: Option<CoordSocket>,
}

/// Spawns the binding peer. TCP and QUIC binders take port 0 and report the
/// real port over a coord socket. Other transports bind `fixed_addr` and get
/// `settle` to come up.
fn spawn_binder(transport: TransportKind, fixed_addr: &str, binder: &BinderCmd<'_>) -> BoundPeer {
    let scheme = transport.port_scheme();
    let bind_any = scheme.map(|scheme| format!("{scheme}://127.0.0.1:0"));
    let mut cmd = binder.head.to_vec();
    cmd.push(bind_any.as_deref().unwrap_or(fixed_addr));
    cmd.extend_from_slice(binder.tail);

    let coord = scheme.is_some().then(CoordSocket::bind_new);
    let mut env = binder.env.to_vec();
    if let Some(coord) = &coord {
        env.push(("OMQ_BENCH_COORD", coord.endpoint()));
    }

    let guard = process::spawn(&cmd, &env, binder.cpus);

    let connect_addr = if let (Some(scheme), Some(coord)) = (scheme, &coord) {
        let port = coord
            .recv_ready_port(Duration::from_secs(10))
            .unwrap_or_else(|| panic!("coord: no READY from {} peer", binder.role));
        format!("{scheme}://127.0.0.1:{port}")
    } else {
        std::thread::sleep(binder.settle);
        fixed_addr.to_owned()
    };

    BoundPeer {
        guard,
        connect_addr,
        coord,
    }
}

/// Start time two seconds out, shared by both sides so their warmups and
/// timed windows line up.
fn start_at() -> String {
    let start_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs_f64()
        + 2.0;
    format!("{start_at:.6}")
}

fn push_start_env<'a>(env: &mut Vec<(&'a str, &'a str)>, start_at: &'a str) {
    env.push(("OMQ_BENCH_START_AT", start_at));
    env.push(("OMQ_BENCH_WARMUP_MS", "500"));
}

/// Binder arguments after the address: the size, then the peer count for
/// peers that take it.
fn size_and_peers<'a>(size: &'a str, peers: &'a str, with_peers: bool) -> Vec<&'a str> {
    let mut tail = vec![size];
    if with_peers {
        tail.push(peers);
    }
    tail
}

pub(super) fn run_throughput_cell(cell: &Cell<'_>, duration: f64, rounds: u32) -> CellResult {
    representative_of(rounds, |_| run_throughput_once(cell, duration))
}

fn run_throughput_once(cell: &Cell<'_>, duration: f64) -> CellResult {
    if cell.transport == TransportKind::Inproc {
        return run_inproc_throughput(cell, duration);
    }

    let def = cell.def;
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let addr = cell.addr();
    let env: Vec<(&str, &str)> = def.env.to_vec();

    // WS binders cannot report readiness, so they get a longer head start.
    let settle = if cell.transport == TransportKind::Ws {
        Duration::from_millis(200)
    } else {
        Duration::from_millis(100)
    };
    let binder = BinderCmd {
        head: [cell.binary.to_str().unwrap(), "push"],
        tail: &[&size_str],
        env: &env,
        cpus: def.affinity(process::MEASURED_CPU),
        role: "push",
        settle,
    };
    let mut pusher = spawn_binder(cell.transport, &addr, &binder);

    let pull_output = process::capture(
        &[
            cell.peer_binary.to_str().unwrap(),
            "pull",
            &pusher.connect_addr,
            &size_str,
            &dur_str,
        ],
        &env,
        def.affinity(process::OTHER_CPU),
        Duration::from_secs(duration as u64 + 30),
    );

    let push_cpu = process::read_proc_cpu(pusher.guard.pid());
    pusher.guard.kill();
    cell.cleanup(&addr);

    let Some(output) = pull_output else {
        return zero_result(duration);
    };
    match parse::parse_throughput(&output, cell.size) {
        Some(r) => CellResult {
            msgs_s: r.msgs_s,
            mbps: r.mbps,
            elapsed: r.elapsed,
            push_cpu: Some(push_cpu),
            pull_cpu: r.pull_cpu,
            ..zero_result(duration)
        },
        None => zero_result(duration),
    }
}

/// Inproc runs both sides in one process on the measured CPUs.
fn run_inproc_throughput(cell: &Cell<'_>, duration: f64) -> CellResult {
    let def = cell.def;
    let name = cell.addr();
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let cmd = [
        cell.binary.to_str().unwrap(),
        def.inproc_tput_subcmd,
        &name,
        &size_str,
        &dur_str,
    ];
    let env: Vec<(&str, &str)> = def.env.to_vec();

    if let Some((out, cpu)) = process::capture_with_cpu(
        &cmd,
        &env,
        def.affinity(process::MEASURED_CPU),
        Duration::from_secs(duration as u64 + 30),
    ) && let Some(r) = parse::parse_throughput(&out, cell.size)
    {
        // Blocking inproc peers report their own CPU seconds; prefer those.
        let blocking_inproc = parse::parse_blocking_inproc_stats(&out);
        let measured_cpu = blocking_inproc
            .as_ref()
            .and_then(|stats| stats.cpu_seconds)
            .unwrap_or(cpu);
        return CellResult {
            blocking_inproc,
            msgs_s: r.msgs_s,
            mbps: r.mbps,
            elapsed: r.elapsed,
            push_cpu: Some(measured_cpu),
            ..zero_result(duration)
        };
    }

    zero_result(duration)
}

pub(super) fn run_pubsub_cell(
    cell: &Cell<'_>,
    peers: u64,
    duration: f64,
    rounds: u32,
    args: &ComparisonsArgs,
) -> CellResult {
    representative_of(rounds, |_| run_pubsub_once(cell, peers, duration, args))
}

fn run_pubsub_once(
    cell: &Cell<'_>,
    peers: u64,
    duration: f64,
    args: &ComparisonsArgs,
) -> CellResult {
    if cell.transport == TransportKind::Inproc {
        return run_inproc_pubsub(cell, peers, duration);
    }

    let def = cell.def;
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();
    let addr = cell.addr();

    let receiver_io = args.pubsub_receiver_io_threads.map(|n| n.to_string());
    let receiver_workers = args.pubsub_receiver_workers.map(|n| n.to_string());
    let start_at = start_at();
    let mut pub_env: Vec<(&str, &str)> = def.env.to_vec();
    let mut sub_env = subscriber_env(def, receiver_io.as_deref(), receiver_workers.as_deref());
    push_start_env(&mut pub_env, &start_at);
    push_start_env(&mut sub_env, &start_at);

    let tail = size_and_peers(&size_str, &peers_str, def.pub_needs_peer_count);
    let binder = BinderCmd {
        head: [cell.binary.to_str().unwrap(), "pub"],
        tail: &tail,
        env: &pub_env,
        cpus: def.affinity(process::MEASURED_CPU),
        role: "pub",
        settle: Duration::from_millis(100),
    };
    let mut publisher = spawn_binder(cell.transport, &addr, &binder);

    // --pubsub-receiver-cpus only replaces a pinned default.
    let sub_cpus = def
        .affinity(process::OTHER_CPU)
        .map(|default| args.pubsub_receiver_cpus.as_deref().unwrap_or(default));
    let sub_output = process::capture(
        &[
            cell.peer_binary.to_str().unwrap(),
            "multi-sub",
            &publisher.connect_addr,
            &size_str,
            &dur_str,
            &peers_str,
        ],
        &sub_env,
        sub_cpus,
        Duration::from_secs(duration as u64 + 30),
    );

    let pub_cpu = process::read_proc_cpu(publisher.guard.pid());
    publisher.guard.kill();
    cell.cleanup(&addr);

    let Some(r) =
        sub_output.and_then(|output| parse::parse_multi_throughput(&output, cell.size, peers))
    else {
        return zero_result(duration);
    };
    CellResult {
        msgs_s: r.msgs_s,
        mbps: r.mbps,
        elapsed: r.elapsed,
        push_cpu: Some(pub_cpu),
        pull_cpu: r.pull_cpu,
        ..zero_result(duration)
    }
    .with_spread(&r)
}

/// Subscriber environment with the optional receiver tuning from the CLI.
fn subscriber_env<'a>(
    def: &'a ImplDef,
    receiver_io: Option<&'a str>,
    receiver_workers: Option<&'a str>,
) -> Vec<(&'a str, &'a str)> {
    let mut env: Vec<(&str, &str)> = def.env.to_vec();
    if let Some(io) = receiver_io {
        env.push(("OMQ_IO_THREADS", io));
        env.push(("ZMQ_IO_THREADS", io));
    }

    if let Some(workers) = receiver_workers {
        env.push(("OMQ_BENCH_RECV_WORKERS", workers));
    }

    env
}

fn run_inproc_pubsub(cell: &Cell<'_>, peers: u64, duration: f64) -> CellResult {
    let def = cell.def;
    let name = cell.addr();
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();
    let cmd = [
        cell.binary.to_str().unwrap(),
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
        def.affinity(process::MEASURED_CPU),
        Duration::from_secs(duration as u64 + 30),
    ) && let Some(r) = parse::parse_throughput(&out, cell.size)
    {
        return CellResult {
            msgs_s: r.msgs_s,
            mbps: r.mbps,
            elapsed: r.elapsed,
            push_cpu: Some(cpu),
            ..zero_result(duration)
        };
    }

    zero_result(duration)
}

pub(super) fn run_fanout_cell(
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

    let timeout = Duration::from_secs(duration as u64 + 30);
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
            .map(|r| r.msgs_s * self.peers_per_process as f64 * r.elapsed)
            .sum();
        let elapsed = self.results.iter().map(|r| r.elapsed).fold(0.0, f64::max);
        let mut peer_rates: Vec<f64> = self
            .results
            .iter()
            .flat_map(|r| r.peer_rates.iter().copied())
            .collect();
        peer_rates.sort_unstable_by(f64::total_cmp);
        let cpu: f64 = self.results.iter().filter_map(|r| r.pull_cpu).sum();

        // Nearest-rank quantile over the sorted rates.
        let quantile = |p: f64| {
            if peer_rates.is_empty() {
                None
            } else {
                Some(peer_rates[((peer_rates.len() - 1) as f64 * p).round() as usize])
            }
        };

        CellResult {
            blocking_inproc: None,
            msgs_s: total_msgs / elapsed / self.peers as f64,
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
}

pub(super) fn run_fanin_cell(
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

    let timeout = Duration::from_secs(duration as u64 + 30);
    let pull_output = puller.guard.wait_with_output(timeout);
    let push_cpu = process::read_proc_cpu(push_proc.pid());
    let push_output = finish_fanin_push(&mut push_proc, def, timeout);
    let pull_cpu_proc = process::read_proc_cpu(puller.guard.pid());
    cell.cleanup(&addr);

    let Some(r) = pull_output.and_then(|output| parse::parse_throughput(&output, cell.size)) else {
        return zero_result(duration);
    };
    let result = CellResult {
        msgs_s: r.msgs_s,
        mbps: r.mbps,
        elapsed: r.elapsed,
        push_cpu: Some(push_cpu),
        pull_cpu: Some(r.pull_cpu.unwrap_or(pull_cpu_proc)),
        ..zero_result(duration)
    };

    match push_output
        .as_deref()
        .and_then(|output| parse::parse_multi_throughput(output, cell.size, peers))
    {
        Some(spread) => result.with_spread(&spread),
        None => result,
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

pub(super) struct LatencyResult {
    pub(super) p50_us: f64,
    pub(super) p99_us: f64,
    pub(super) p999_us: f64,
    pub(super) max_us: f64,
    pub(super) iterations: u64,
    pub(super) cpu_time: Option<f64>,
    pub(super) req_cpu: Option<f64>,
    pub(super) elapsed: Option<f64>,
}

/// Latency settings shared by every cell of one socket pair and profile.
pub(super) struct LatencyWorkload<'a> {
    pub(super) pair: &'a str,
    pub(super) profile: &'a str,
    pub(super) iterations: u64,
    pub(super) warmup: u64,
    /// Expected run time in seconds; peers get 30 s on top.
    pub(super) timeout: u64,
}

#[expect(clippy::similar_names)]
pub(super) fn run_latency_cell(
    cell: &Cell<'_>,
    workload: &LatencyWorkload<'_>,
) -> Option<LatencyResult> {
    if cell.transport == TransportKind::Inproc {
        return run_inproc_latency(cell, workload);
    }

    let def = cell.def;
    let size_str = cell.size.to_string();
    let iters_str = workload.iterations.to_string();
    let warmup_str = workload.warmup.to_string();
    let addr = cell.addr();
    let env = latency_env(def, workload.pair, workload.profile);

    // REP binds on the other CPUs; the timed REQ side runs on the measured
    // CPUs.
    let binder = BinderCmd {
        head: [cell.peer_binary.to_str().unwrap(), latency_rep_subcmd(def)],
        tail: &[&size_str],
        env: &env,
        cpus: def.affinity(process::OTHER_CPU),
        role: "rep",
        settle: Duration::from_millis(100),
    };
    let mut replier = spawn_binder(cell.transport, &addr, &binder);
    // Latency closes the coord socket as soon as the binder is ready.
    drop(replier.coord.take());

    let req_output = process::capture(
        &[
            cell.binary.to_str().unwrap(),
            latency_req_subcmd(def),
            &replier.connect_addr,
            &size_str,
            &iters_str,
            &warmup_str,
        ],
        &env,
        def.affinity(process::MEASURED_CPU),
        Duration::from_secs(workload.timeout + 30),
    );

    let rep_cpu = process::read_proc_cpu(replier.guard.pid());
    replier.guard.kill();
    cell.cleanup(&addr);

    let r = parse::parse_latency(&req_output?)?;
    validate_latency_result(&r, workload.iterations);

    let req_cpu = r.req_cpu;
    Some(LatencyResult {
        p50_us: r.p50_us,
        p99_us: r.p99_us,
        p999_us: r.p999_us,
        max_us: r.max_us,
        iterations: r.iterations,
        cpu_time: req_cpu.map(|req| req + rep_cpu),
        req_cpu,
        elapsed: r.elapsed,
    })
}

fn run_inproc_latency(cell: &Cell<'_>, workload: &LatencyWorkload<'_>) -> Option<LatencyResult> {
    let def = cell.def;
    let name = cell.addr();
    let size_str = cell.size.to_string();
    let iters_str = workload.iterations.to_string();
    let warmup_str = workload.warmup.to_string();
    let env = latency_env(def, workload.pair, workload.profile);

    let (out, cpu) = process::capture_with_cpu(
        &[
            cell.binary.to_str().unwrap(),
            def.inproc_lat_subcmd,
            &name,
            &size_str,
            &iters_str,
            &warmup_str,
        ],
        &env,
        def.affinity(process::MEASURED_CPU),
        Duration::from_secs(workload.timeout + 30),
    )?;
    let r = parse::parse_latency(&out)?;
    validate_latency_result(&r, workload.iterations);

    // One process runs both sides, so all of its CPU counts as REQ CPU.
    Some(LatencyResult {
        p50_us: r.p50_us,
        p99_us: r.p99_us,
        p999_us: r.p999_us,
        max_us: r.max_us,
        iterations: r.iterations,
        cpu_time: Some(cpu),
        req_cpu: Some(cpu),
        elapsed: r.elapsed,
    })
}

fn validate_latency_result(result: &parse::LatencyResult, iterations: u64) {
    assert_eq!(
        result.iterations, iterations,
        "incomplete latency measurement"
    );
    let quantiles = [result.p50_us, result.p99_us, result.p999_us, result.max_us];
    assert!(quantiles.iter().all(|v| v.is_finite() && *v >= 0.0));
    assert!(quantiles.windows(2).all(|pair| pair[0] <= pair[1]));
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
