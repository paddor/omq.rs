use crate::cli::ComparisonsArgs;
use crate::coord::CoordSocket;
use crate::jsonl::{self, ComparisonRow};
use crate::parse;
use crate::process;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const COMPARISON_CHART_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
const MAIN_EXTRA_CHART_SIZES: &[u64] = &[
    32, 128, 512, 2048, 8192, 32768, 262_144, 4_194_304, 8_388_608,
];
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
    Quic,
}

impl TransportKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Ipc => "ipc",
            Self::Inproc => "inproc",
            Self::Ws => "ws",
            Self::Quic => "quic",
        }
    }

    /// URI scheme for transports whose binder reports an ephemeral port.
    fn port_scheme(self) -> Option<&'static str> {
        match self {
            Self::Tcp => Some("tcp"),
            Self::Quic => Some("quic"),
            Self::Ipc | Self::Inproc | Self::Ws => None,
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

use TransportKind::{Inproc, Ipc, Quic, Tcp, Ws};

static IMPLS: &[ImplDef] = &[
    ImplDef {
        name: "omq-tokio-ct",
        binary_from: None,
        prefix: "t",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp, Inproc, Ipc, Ws, Quic],
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
        transports: &[Tcp, Ipc, Inproc, Quic],
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
        name: "omq-tokio-1t-spin50",
        binary_from: Some("omq-tokio-1t"),
        prefix: "s",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Ipc, Inproc, Quic],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_BENCH_RECV_SPIN_US", "50")],
    },
    ImplDef {
        name: "omq-tokio-mt",
        binary_from: Some("omq-tokio-ct"),
        prefix: "m",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_BENCH_MT_RUNTIME", "1")],
    },
    ImplDef {
        name: "omq-tokio-2t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "u",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc, Ws, Quic],
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
        name: "omq-tokio-3t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "w",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "3")],
    },
    ImplDef {
        name: "omq-tokio-4t",
        binary_from: Some("omq-tokio-1t"),
        prefix: "x",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Tcp, Inproc, Ipc],
        inproc_tput_subcmd: "inproc",
        inproc_lat_subcmd: "inproc-latency",
        inproc_pubsub_subcmd: "inproc-pubsub",
        pub_needs_peer_count: true,
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "4")],
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
        name: "omq-tokio-2ut-blocking",
        binary_from: Some("omq-tokio-1t"),
        prefix: "q",
        class: Some(ImplClass::Classic),
        main: false,
        transports: &[Inproc],
        inproc_tput_subcmd: "inproc-2ut-blocking",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[("OMQ_IO_THREADS", "1")],
    },
    ImplDef {
        name: "omq-tokio-exclusive",
        binary_from: Some("omq-tokio-ct"),
        prefix: "E",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: false,
        fanout_subcmd: "",
        fanio_needs_peer_count: false,
        supports_pubsub: false,
        env: &[],
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
        name: "r0z-async",
        binary_from: None,
        prefix: "n",
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
        name: "monocoque-tokio-ct",
        binary_from: None,
        prefix: "o",
        class: Some(ImplClass::Classic),
        main: true,
        transports: &[Tcp],
        inproc_tput_subcmd: "",
        inproc_lat_subcmd: "",
        inproc_pubsub_subcmd: "",
        pub_needs_peer_count: true,
        fanout_subcmd: "",
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
        fanio_needs_peer_count: true,
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
        fanout_subcmd: "push",
        fanio_needs_peer_count: true,
        supports_pubsub: true,
        env: &[("OMQ_IO_THREADS", "2"), ("OMQ_BENCH_MECHANISM", "curve")],
    },
];

fn find_impl(name: &str) -> Option<&'static ImplDef> {
    IMPLS.iter().find(|i| i.name == name)
}

impl ImplDef {
    fn uses_mt_runtime(&self) -> bool {
        matches!(
            self.name,
            "omq-tokio-mt" | "zmq.rs" | "rzmq" | "rzmq-iouring"
        )
    }

    fn affinity(&self, cpus: &'static str) -> Option<&'static str> {
        (!self.uses_mt_runtime()).then_some(cpus)
    }
}

fn supports_pushpull(def: &ImplDef) -> bool {
    !matches!(def.name, "omq-tokio-exclusive" | "omq-tokio-1t-spin50")
}

fn supports_latency_pair(def: &ImplDef, pair: &str, profile: &str) -> bool {
    (pair == "req-rep" && profile == "default")
        || (def.binary_from.unwrap_or(def.name) == "libzmq"
            && profile == "default"
            && matches!(pair, "router-dealer" | "router-router" | "pair"))
        || (def.name != "omq-tokio-exclusive"
            && matches!(
                def.binary_from.unwrap_or(def.name),
                "omq-tokio-1t" | "omq-tokio-ct"
            ))
}

fn latency_env<'a>(def: &ImplDef, pair: &'a str, profile: &'a str) -> Vec<(&'a str, &'a str)> {
    let mut env = def.env.to_vec();
    env.push(("OMQ_BENCH_LATENCY_PAIR", pair));
    env.push(("OMQ_BENCH_WORKLOAD_PROFILE", profile));
    // Keep inherited experiment settings from silently changing the baseline.
    if !env.iter().any(|&(key, _)| key == "OMQ_BENCH_RECV_SPIN_US") {
        env.push(("OMQ_BENCH_RECV_SPIN_US", "0"));
    }
    if !env.iter().any(|&(key, _)| key == "OMQ_IO_THREADS") {
        match def.binary_from.unwrap_or(def.name) {
            "omq-tokio-1t" => env.push(("OMQ_IO_THREADS", "1")),
            "omq-tokio-ct" => env.push(("OMQ_IO_THREADS", "0")),
            _ => {}
        }
    }
    env
}

fn supports_fanio(def: &ImplDef) -> bool {
    !def.fanout_subcmd.is_empty()
}

fn latency_req_subcmd(def: &ImplDef) -> &'static str {
    if def.name == "omq-tokio-exclusive" {
        "req-exclusive"
    } else {
        "req"
    }
}

fn latency_rep_subcmd(def: &ImplDef) -> &'static str {
    if def.name == "omq-tokio-exclusive" {
        "rep-exclusive"
    } else {
        "rep"
    }
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

fn build_peers(
    impl_names: &[&str],
    needs_ws: bool,
    needs_quic: bool,
    needs_curve: bool,
) -> HashMap<String, PathBuf> {
    let needs_mt_runtime = impl_names.contains(&"omq-tokio-mt");
    let peer_features: Vec<&str> = [
        (needs_ws, "ws"),
        (needs_quic, "quic"),
        (needs_curve, "curve"),
    ]
    .into_iter()
    .filter_map(|(needed, feature)| needed.then_some(feature))
    .collect();

    let sources: Vec<&str> = impl_names
        .iter()
        .map(|&name| {
            let def = find_impl(name).unwrap();
            def.binary_from.unwrap_or(def.name)
        })
        .collect();

    // Variants that share a source binary build it once.
    let mut binaries: HashMap<String, PathBuf> = HashMap::new();
    for source in sources {
        if !binaries.contains_key(source) {
            let binary = build_source(source, &peer_features, needs_mt_runtime);
            binaries.insert(source.to_string(), binary);
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

/// Builds the peer binary of one implementation source.
fn build_source(source: &str, peer_features: &[&str], needs_mt_runtime: bool) -> PathBuf {
    match source {
        "omq-tokio-ct" => {
            // The MT variant reuses this binary with the MT runtime built in.
            let mut features = peer_features.to_vec();
            if needs_mt_runtime {
                features.push("bench-mt-runtime");
            }
            process::build_omq_peer("omq_bench_peer_tokio", &features)
        }
        "omq-tokio-1t" => process::build_omq_peer("omq_bench_peer_blocking", peer_features),
        "libzmq" => {
            let src = "scripts/libzmq_bench_peer.c";
            let out = "scripts/omq_libzmq_baseline_peer";
            run_build(&["gcc", "-O2", "-o", out, src, "-lzmq", "-lpthread"]);
            PathBuf::from(out)
        }
        "zmq.rs" => {
            run_build_in_dir(
                &["cargo", "build", "--release", "-q"],
                "scripts/zmqrs_bench_peer",
            );
            PathBuf::from("scripts/zmqrs_bench_peer/target/release/omq_zmqrs_bench_peer")
        }
        "r0z-async" | "tmq" | "monocoque-tokio-ct" => build_external_comparison_peer(source),
        "rzmq" => {
            run_build_in_dir(
                &["cargo", "build", "--release", "-q"],
                "scripts/rzmq_bench_peer",
            );
            PathBuf::from("scripts/rzmq_bench_peer/target/release/omq_rzmq_bench_peer")
        }
        _ => panic!("unknown impl source: {source}"),
    }
}

fn build_external_comparison_peer(source: &str) -> PathBuf {
    match source {
        "r0z-async" => process::build_external_peer(
            "scripts/r0z_bench_peer",
            "omq_r0z_bench_peer",
            &[("CXXFLAGS", "-std=gnu++11")],
        ),
        "tmq" => process::build_external_peer(
            "scripts/tmq_bench_peer",
            "omq_tmq_bench_peer",
            &[("CXXFLAGS", "-std=gnu++11")],
        ),
        "monocoque-tokio-ct" => process::build_external_peer(
            "scripts/monocoque_bench_peer",
            "omq_monocoque_bench_peer",
            &[],
        ),
        _ => unreachable!("not an external comparison peer: {source}"),
    }
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
    blocking_inproc: Option<parse::BlockingInprocStats>,
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
struct Cell<'a> {
    binary: &'a Path,
    peer_binary: &'a Path,
    def: &'a ImplDef,
    transport: TransportKind,
    size: u64,
    base_port: u16,
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

fn run_throughput_cell(cell: &Cell<'_>, duration: f64, rounds: u32) -> CellResult {
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

fn run_pubsub_cell(
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

fn run_fanout_cell(cell: &Cell<'_>, peers: u64, duration: f64, rounds: u32) -> CellResult {
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

fn run_fanin_cell(cell: &Cell<'_>, peers: u64, duration: f64, rounds: u32) -> CellResult {
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

/// Latency settings shared by every cell of one socket pair and profile.
struct LatencyWorkload<'a> {
    pair: &'a str,
    profile: &'a str,
    iterations: u64,
    warmup: u64,
    /// Expected run time in seconds; peers get 30 s on top.
    timeout: u64,
}

#[expect(clippy::similar_names)]
fn run_latency_cell(cell: &Cell<'_>, workload: &LatencyWorkload<'_>) -> Option<LatencyResult> {
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
    fn mt_runtime_omq_uses_tokio_peer_and_runtime_env() {
        let def = find_impl("omq-tokio-mt").unwrap();

        assert_eq!(def.binary_from, Some("omq-tokio-ct"));
        assert_eq!(def.env, &[("OMQ_BENCH_MT_RUNTIME", "1")]);
        assert!(matches!(def.transports, [Tcp]));
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
    fn latency_variants_preserve_runtime_and_spin_configuration() {
        for (name, io, spin) in [
            ("omq-tokio-ct", "0", "0"),
            ("omq-tokio-1t", "1", "0"),
            ("omq-tokio-1t-spin50", "1", "50"),
            ("omq-tokio-2t", "2", "0"),
        ] {
            let def = find_impl(name).unwrap();
            let env = latency_env(def, "router-router", "latency");
            assert!(env.contains(&("OMQ_BENCH_LATENCY_PAIR", "router-router")));
            assert!(env.contains(&("OMQ_BENCH_WORKLOAD_PROFILE", "latency")));
            assert!(env.contains(&("OMQ_BENCH_RECV_SPIN_US", spin)));
            assert!(env.contains(&("OMQ_IO_THREADS", io)));
            assert!(supports_latency_pair(def, "router-router", "latency"));
        }
        for name in ["libzmq", "omq-tokio-exclusive", "zmq.rs"] {
            let def = find_impl(name).unwrap();
            assert!(supports_latency_pair(def, "req-rep", "default"));
            assert!(!supports_latency_pair(def, "router-router", "latency"));
        }
        for name in ["libzmq", "libzmq-2t"] {
            let def = find_impl(name).unwrap();
            for pair in ["router-dealer", "router-router", "pair"] {
                assert!(supports_latency_pair(def, pair, "default"));
            }
            for pair in ["client-server", "peer", "channel"] {
                assert!(!supports_latency_pair(def, pair, "default"));
            }
        }
        assert!(!supports_pushpull(
            find_impl("omq-tokio-1t-spin50").unwrap()
        ));
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

    #[test]
    fn exclusive_latency_uses_exclusive_req_and_rep() {
        let def = find_impl("omq-tokio-exclusive").unwrap();

        assert_eq!(latency_req_subcmd(def), "req-exclusive");
        assert_eq!(latency_rep_subcmd(def), "rep-exclusive");
    }
}
