use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cli::CompressionLinkArgs;
use crate::jsonl::{self, CompressionLink, PushpullLz4Row};
use crate::process;

const CHART_SIZES: &[u64] = &[16, 32, 64, 256, 1024, 4096, 16384, 65536, 262_144];
const QUICK_SIZES: &[u64] = &[64, 1024, 16384];

pub(crate) struct Config {
    pub feature: &'static str,
    pub transports: String,
    pub sizes: Option<Vec<u64>>,
    pub duration: f64,
    pub rounds: u32,
    pub quick: bool,
    pub dict_sizes: Vec<u64>,
    pub level: Option<i32>,
    pub link: CompressionLinkArgs,
}

struct Peers {
    blocking: PathBuf,
    utility: PathBuf,
    sha256: Option<String>,
}

pub(crate) struct Measurement {
    pub count: f64,
    pub elapsed: f64,
    pub sender_cpu: f64,
    pub receiver_cpu: f64,
}

// Positional shell arguments keep paths and user arguments out of shell code.
const NETEM_SETUP: &str = r#"
set -eu
export PATH="$PATH:/usr/sbin:/sbin"
rate=$1
delay=$2
shift 2
ip link set lo up
ip link set lo mtu 1500
ethtool -K lo gso off gro off tso off tx-udp-segmentation off tx-gso-list off
ip link add ifb0 type ifb
ip link set ifb0 up
tc qdisc add dev lo clsact
tc filter add dev lo ingress protocol ip pref 1 flower ip_proto tcp action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root netem limit 10000 delay "${delay}us" rate "${rate}mbit"
trap 'tc -s qdisc show dev ifb0' EXIT
"$@"
"#;

pub(crate) fn run_in_netem(args: &CompressionLinkArgs) -> bool {
    let Some(rate) = args.link_mbps else {
        return false;
    };
    assert_eq!(std::env::consts::OS, "linux", "netem requires Linux");
    if std::env::var_os("OMQ_BENCH_NETEM_READY").is_some() {
        return false;
    }
    let executable = std::env::current_exe().expect("benchmark executable");
    let status = Command::new("unshare")
        .args(["--user", "--map-root-user", "--net", "sh", "-c"])
        .arg(NETEM_SETUP)
        .arg("omq_netem")
        .arg(rate.to_string())
        .arg(args.link_delay_us.to_string())
        .arg(executable)
        .args(std::env::args_os().skip(1))
        .env("OMQ_BENCH_NETEM_READY", "1")
        .status()
        .expect("start private netem namespace");
    assert!(status.success(), "netem benchmark failed");
    true
}

fn utility(peers: &Peers, args: &[&str], env: &[(&str, &str)], timeout: Duration) -> String {
    let binary = peers.utility.to_str().expect("utility path");
    let mut command = vec![binary];
    command.extend_from_slice(args);
    process::capture(&command, env, None, timeout).expect("compression utility failed or timed out")
}

fn environment<'a>(dict: Option<&'a str>, level: Option<&'a str>) -> Vec<(&'static str, &'a str)> {
    let mut env = vec![
        ("OMQ_BENCH_PAYLOAD", "structured-varied-v1"),
        ("OMQ_BENCH_JSON_SEED", "4242"),
    ];
    if let Some(path) = dict {
        env.push(("OMQ_BENCH_DICT_FILE", path));
    }
    if let Some(level) = level {
        env.push(("OMQ_BENCH_ZSTD_LEVEL", level));
    }
    env
}

/// One timed PUSH/PULL measurement between two blocking peers.
pub(crate) struct Cell<'a> {
    pub blocking: &'a Path,
    pub transport: &'a str,
    pub size: u64,
    pub duration: f64,
    pub warmup: f64,
}

pub(crate) fn run_cell(
    cell: &Cell<'_>,
    sender_env: &[(&str, &str)],
    receiver_env: &[(&str, &str)],
) -> Measurement {
    let binary = cell.blocking.to_str().expect("peer path");
    // Measurements are serial; both peers exit before this port is reused.
    let endpoint = format!("{}://127.0.0.1:17500", cell.transport);
    let size_arg = cell.size.to_string();
    let duration_arg = cell.duration.to_string();
    let warmup_arg = Duration::from_secs_f64(cell.warmup).as_millis().to_string();

    let mut sender = process::spawn(
        &[binary, "push", &endpoint, &size_arg],
        sender_env,
        Some(process::MEASURED_CPU),
    );

    // The receiver verifies payloads and reads the sender's CPU time by PID.
    let sender_pid = sender.pid().to_string();
    let mut receiver_env = receiver_env.to_vec();
    receiver_env.extend([
        ("OMQ_BENCH_VERIFIED_PUSHPULL", "1"),
        ("OMQ_BENCH_SENDER_PID", sender_pid.as_str()),
        ("OMQ_BENCH_WARMUP_MS", warmup_arg.as_str()),
    ]);
    let output = process::capture(
        &[binary, "pull", &endpoint, &size_arg, &duration_arg],
        &receiver_env,
        Some(process::OTHER_CPU),
        Duration::from_secs_f64(cell.duration + cell.warmup + 15.0),
    )
    .expect("compression receiver failed or timed out");
    sender.kill();

    parse_measurement(&output, cell.size)
}

/// Parses `count elapsed size receiver_cpu sender_cpu` from the receiver.
fn parse_measurement(output: &str, size: u64) -> Measurement {
    let fields: Vec<f64> = output
        .split_whitespace()
        .map(|field| field.parse().expect("invalid compression measurement"))
        .collect();
    assert_eq!(fields.len(), 5, "incomplete compression measurement");
    assert!(
        fields
            .iter()
            .all(|field| field.is_finite() && *field >= 0.0)
    );
    assert!(fields[0] > 0.0 && fields[1] > 0.0);

    let reported_size: u64 = output
        .split_whitespace()
        .nth(2)
        .expect("reported payload size")
        .parse()
        .expect("integer payload size");
    assert_eq!(reported_size, size, "incorrect payload size");

    Measurement {
        count: fields[0],
        elapsed: fields[1],
        sender_cpu: fields[4],
        receiver_cpu: fields[3],
    }
}

/// Settings shared by every measurement of one benchmark run.
struct Sweep<'a> {
    config: &'a Config,
    peers: &'a Peers,
    sizes: &'a [u64],
    run_id: &'a str,
    rounds: u32,
    duration: f64,
}

impl Sweep<'_> {
    fn measure(&self, transport: &str, dictionary: Option<(&str, u64)>) {
        let config = self.config;
        let level = config.level.map(|value| value.to_string());
        let env = environment(dictionary.map(|(path, _)| path), level.as_deref());
        let cache = jsonl::cache_dir().join(format!("results_pushpull_{}.jsonl", config.feature));

        for &size in self.sizes {
            let wire_bytes = self.wire_bytes(transport, size, &env);
            // Slow links need enough large messages to avoid integer-count steps.
            let cell_duration = config.link.link_mbps.map_or(self.duration, |rate| {
                self.duration
                    .max((110.0 * wire_bytes as f64 * 8.0 / (f64::from(rate) * 1e6)).min(40.0))
            });
            let cell = Cell {
                blocking: &self.peers.blocking,
                transport,
                size,
                duration: cell_duration,
                warmup: config.link.warmup_seconds,
            };
            let base = self.base_row(&cell, wire_bytes, dictionary);

            for repeat in 1..=self.rounds {
                let sample = run_cell(&cell, &env, &env);
                let msgs_s = sample.count / sample.elapsed;
                let mbps = msgs_s * size as f64 / 1_000_000.0;
                let row = PushpullLz4Row {
                    msg_count: Some(sample.count),
                    elapsed: Some(sample.elapsed),
                    cpu_time: Some(sample.sender_cpu),
                    msgs_s: Some(msgs_s),
                    mbps: Some(mbps),
                    repeat: Some(repeat),
                    ..base.clone()
                };

                if !config.quick {
                    jsonl::append_jsonl(&cache, &row);
                }

                eprintln!(
                    "{transport}{} size={size} round={repeat}/{}: {msgs_s:.0} msg/s, \
                     {mbps:.3} MB/s, sender CPU={:.1}%",
                    if dictionary.is_some() { "+dict" } else { "" },
                    self.rounds,
                    sample.sender_cpu / sample.elapsed * 100.0,
                );
            }
        }
    }

    /// Encoded wire bytes of one message, as reported by the peer.
    fn wire_bytes(&self, transport: &str, size: u64, env: &[(&str, &str)]) -> u64 {
        let endpoint = format!("{transport}://127.0.0.1:17500");
        utility(
            self.peers,
            &["wire-size", &endpoint, &size.to_string()],
            env,
            Duration::from_secs(10),
        )
        .trim()
        .parse()
        .expect("invalid encoded wire size")
    }

    /// Row fields shared by every repeat of one cell.
    fn base_row(
        &self,
        cell: &Cell<'_>,
        wire_bytes: u64,
        dictionary: Option<(&str, u64)>,
    ) -> PushpullLz4Row {
        let config = self.config;
        let netem = config.link.link_mbps.map(|rate_mbps| CompressionLink {
            rate_mbps,
            delay_us: config.link.link_delay_us,
            mtu: 1500,
            shared_rate: true,
            segmentation_offloads: false,
            placement: "tcp-ingress-ifb".to_owned(),
        });
        let affinity = if std::env::var_os("OMQ_BENCH_TASKSET").is_some() {
            "sender=1-2,receiver=3-4"
        } else {
            "unbound"
        };

        PushpullLz4Row {
            run_id: self.run_id.to_owned(),
            pattern: format!(
                "pushpull_{}{}",
                config.feature,
                if dictionary.is_some() { "_dict" } else { "" }
            ),
            transport: cell.transport.to_owned(),
            peers: 1,
            msg_size: cell.size,
            wire_bytes,
            msg_count: None,
            elapsed: None,
            cpu_time: None,
            msgs_s: None,
            mbps: None,
            dict_size: dictionary.map(|(_, capacity)| capacity),
            compression_level: config.level,
            netem,
            repeat: None,
            rounds: Some(self.rounds),
            warmup_seconds: Some(config.link.warmup_seconds),
            duration_seconds: Some(cell.duration),
            binary_sha256: self.peers.sha256.clone(),
            cpu_affinity: Some(affinity.to_owned()),
            payload: Some("structured-varied-v1".to_owned()),
            payload_seed: Some(4242),
            payload_corpus_messages: Some((64 * 1024 * 1024 / cell.size).clamp(2, 4096)),
            arena_bytes: Some(arena_bytes(config.feature)),
        }
    }

    /// Trains a dictionary of `capacity` bytes and measures the compressed
    /// transport with it, if that transport was selected.
    fn measure_dictionary(&self, capacity: u64, compressed: &str, selected: bool) {
        let feature = self.config.feature;
        let path = format!(
            "/tmp/omq-bench-{feature}-dict-{capacity}-{}.bin",
            std::process::id()
        );
        let action = if feature == "lz4" {
            "train-dict"
        } else {
            "train-zstd-dict"
        };
        utility(
            self.peers,
            &[action, &path, &capacity.to_string()],
            &environment(None, None),
            Duration::from_secs(30),
        );

        if selected {
            self.measure(compressed, Some((&path, capacity)));
        }

        std::fs::remove_file(&path).expect("remove benchmark dictionary");
    }
}

/// Peer arena size: the `OMQ_<FEATURE>_ARENA_BYTES` override, else the
/// feature default, capped at 64 KiB.
fn arena_bytes(feature: &str) -> u64 {
    std::env::var(format!("OMQ_{}_ARENA_BYTES", feature.to_uppercase()))
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(if feature == "zstd" { 32768 } else { 16384 })
        .min(65536)
}

/// SHA-256 of the peer binary, so result rows identify the build.
pub(crate) fn peer_sha256(binary: &Path) -> String {
    let hash = Command::new("sha256sum")
        .arg(binary)
        .output()
        .expect("hash benchmark peer");
    assert!(hash.status.success(), "benchmark peer hash failed");
    String::from_utf8(hash.stdout)
        .expect("UTF-8 peer hash")
        .split_whitespace()
        .next()
        .expect("peer hash")
        .to_owned()
}

pub(crate) fn run(config: &Config) {
    assert!(config.duration.is_finite() && config.duration > 0.0);
    assert!(config.link.warmup_seconds.is_finite() && config.link.warmup_seconds >= 0.0);
    assert!(config.rounds > 0);
    let [blocking, utility_path] = process::build_compression_peers(config.feature);
    if run_in_netem(&config.link) {
        return;
    }

    let sha256 = config.link.link_mbps.map(|_| peer_sha256(&blocking));
    let peers = Peers {
        blocking,
        utility: utility_path,
        sha256,
    };
    let sizes = config.sizes.as_deref().unwrap_or(if config.quick {
        QUICK_SIZES
    } else {
        CHART_SIZES
    });
    assert!(sizes.iter().all(|size| *size > 0));
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("benchmark timestamp")
        .as_nanos()
        .to_string();
    let transports: Vec<_> = config.transports.split(',').collect();
    let compressed = format!("{}+tcp", config.feature);
    assert!(
        transports
            .iter()
            .all(|transport| *transport == "tcp" || *transport == compressed),
        "compression measurements require TCP transports"
    );

    let sweep = Sweep {
        config,
        peers: &peers,
        sizes,
        run_id: &run_id,
        rounds: if config.quick { 1 } else { config.rounds },
        duration: if config.quick { 1.5 } else { config.duration },
    };
    for transport in &transports {
        sweep.measure(transport, None);
    }
    for &capacity in &config.dict_sizes {
        sweep.measure_dictionary(
            capacity,
            &compressed,
            transports.contains(&compressed.as_str()),
        );
    }
}
