//! Run one benchmark cell: spawn peers, measure, and collect the result.

use super::impls::{ImplDef, TransportKind};
use super::{cleanup_ipc_addr, next_addr_id, uses_filesystem_ipc};
use crate::cli::PubsubArgs;
use crate::coord::CoordSocket;
use crate::parse;
use crate::process;
use std::path::Path;
use std::time::Duration;

mod fanio;
mod latency;

pub(super) use fanio::{run_fanin_cell, run_fanout_cell};
pub(super) use latency::{LatencyResult, LatencyWorkload, run_latency_cell};

#[derive(Default)]
pub(super) struct CellResult {
    pub(super) blocking_inproc: Option<parse::BlockingInprocStats>,
    pub(super) msgs_s: f64,
    pub(super) mbps: f64,
    pub(super) elapsed: f64,
    pub(super) push_cpu: Option<f64>,
    pub(super) pull_cpu: Option<f64>,
    pub(super) spread: parse::PeerSpread,
}

fn zero_result(duration: f64) -> CellResult {
    CellResult {
        elapsed: duration,
        ..CellResult::default()
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
    /// Bind address. WS ports are offset by prefix; IPC and inproc names are
    /// unique per call.
    fn addr(&self) -> String {
        let prefix = self.def.prefix;
        let uid = next_addr_id();

        match self.transport {
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

                let port = self.base_port + offset;
                format!("ws://127.0.0.1:{port}/")
            }
            TransportKind::Ipc => {
                if uses_filesystem_ipc(self.def.name) {
                    format!("ipc:///tmp/omq-bench-cmp-{prefix}-{uid}")
                } else {
                    format!("ipc://@omq-bench-cmp-{prefix}-{uid}")
                }
            }
            TransportKind::Inproc => format!("bench-cmp-{prefix}-{uid}"),
        }
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
        return run_inproc_cell(cell, cell.def.inproc_tput_subcmd, &[], duration);
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
        process::peer_timeout(duration),
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

/// Inproc runs both sides in one process on the measured CPUs: `subcmd`
/// with the name, size, duration, then `extra` arguments.
fn run_inproc_cell(cell: &Cell<'_>, subcmd: &str, extra: &[&str], duration: f64) -> CellResult {
    let def = cell.def;
    let name = cell.addr();
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let mut cmd = vec![
        cell.binary.to_str().unwrap(),
        subcmd,
        &name,
        &size_str,
        &dur_str,
    ];
    cmd.extend_from_slice(extra);
    let env: Vec<(&str, &str)> = def.env.to_vec();

    if let Some((out, cpu)) = process::capture_with_cpu(
        &cmd,
        &env,
        def.affinity(process::MEASURED_CPU),
        process::peer_timeout(duration),
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
    pubsub: &PubsubArgs,
) -> CellResult {
    representative_of(rounds, |_| run_pubsub_once(cell, peers, duration, pubsub))
}

fn run_pubsub_once(cell: &Cell<'_>, peers: u64, duration: f64, pubsub: &PubsubArgs) -> CellResult {
    if cell.transport == TransportKind::Inproc {
        let peers = peers.to_string();
        return run_inproc_cell(cell, cell.def.inproc_pubsub_subcmd, &[&peers], duration);
    }

    let def = cell.def;
    let size_str = cell.size.to_string();
    let dur_str = format!("{duration:.1}");
    let peers_str = peers.to_string();
    let addr = cell.addr();

    let receiver_io = pubsub.pubsub_receiver_io_threads.map(|n| n.to_string());
    let receiver_workers = pubsub.pubsub_receiver_workers.map(|n| n.to_string());
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
        .map(|default| pubsub.pubsub_receiver_cpus.as_deref().unwrap_or(default));

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
        process::peer_timeout(duration),
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
        msgs_s: r.rate.msgs_s,
        mbps: r.rate.mbps,
        elapsed: r.rate.elapsed,
        push_cpu: Some(pub_cpu),
        pull_cpu: r.rate.pull_cpu,
        spread: r.spread,
        ..zero_result(duration)
    }
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
