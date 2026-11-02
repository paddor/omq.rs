//! Latency cells: REP binds, the timed REQ side connects.

use super::{BinderCmd, Cell, spawn_binder};
use crate::bench::comparisons::impls::{
    TransportKind, latency_env, latency_rep_subcmd, latency_req_subcmd,
};
use crate::cli::LatencyArgs;
use crate::parse;
use crate::process;
use std::time::Duration;

pub(crate) struct LatencyResult {
    pub(crate) p50_us: f64,
    pub(crate) p99_us: f64,
    pub(crate) p999_us: f64,
    pub(crate) max_us: f64,
    pub(crate) iterations: u64,
    pub(crate) cpu_time: Option<f64>,
    pub(crate) req_cpu: Option<f64>,
    pub(crate) elapsed: Option<f64>,
}

/// Latency settings shared by every cell of one socket pair and profile.
pub(crate) struct LatencyWorkload<'a> {
    pub(crate) pair: &'a str,
    pub(crate) profile: &'a str,
    pub(crate) iterations: u64,
    pub(crate) warmup: u64,
    /// Expected run time in seconds; peers get 30 s on top.
    pub(crate) timeout: u64,
}

impl<'a> LatencyWorkload<'a> {
    pub(crate) fn new(pair: &'a str, profile: &'a str, args: &LatencyArgs) -> Self {
        Self {
            pair,
            profile,
            iterations: args.latency_iterations,
            warmup: args.latency_warmup,
            timeout: args.latency_timeout,
        }
    }
}

#[expect(clippy::similar_names)]
pub(crate) fn run_latency_cell(
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
