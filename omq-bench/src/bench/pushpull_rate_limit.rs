use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::pushpull_compression::{Cell, peer_sha256, run_cell, run_in_netem};
use crate::cli::{CompressionLinkArgs, PushpullRateLimitArgs};
use crate::jsonl::{self, CompressionLink};
use crate::process;

pub(crate) const SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
pub(crate) const RATE: u64 = 100_000_000;
pub(crate) const BURST: u64 = 1_000_000;
pub(crate) const CACHE: &str = "pushpull-rate-limit.jsonl";

pub(crate) fn link() -> CompressionLink {
    CompressionLink {
        rate_mbps: 1000,
        delay_us: 1000,
        mtu: 1500,
        shared_rate: true,
        segmentation_offloads: false,
        placement: "tcp-ingress-ifb".into(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Row {
    pub run_id: String,
    pub binary_sha256: String,
    pub transport: String,
    pub processes: u32,
    pub io_threads: u32,
    pub cpu_affinity: String,
    pub netem: CompressionLink,
    pub egress_bytes_per_second: Option<u64>,
    pub burst_bytes: Option<u64>,
    pub msg_size: u64,
    pub repeat: u32,
    pub rounds: u32,
    pub warmup_seconds: f64,
    pub duration_seconds: f64,
    pub count: f64,
    pub elapsed: f64,
    pub sender_cpu: f64,
    pub receiver_cpu: f64,
    pub payload_verified: bool,
}

impl Row {
    pub(crate) fn msgs_s(&self) -> f64 {
        self.count / self.elapsed
    }
}

pub(crate) fn run(args: &PushpullRateLimitArgs) {
    assert!(args.duration.is_finite() && args.duration > 0.0);
    assert!(args.rounds > 0);
    // Build before entering the namespace; the second build only resolves the
    // cached executable. No compilation overlaps the measurement interval.
    let binary = process::build_omq_peer("omq_bench_peer_blocking", &[]);
    let settings = link();
    if run_in_netem(&CompressionLinkArgs {
        link_mbps: Some(settings.rate_mbps),
        link_delay_us: settings.delay_us,
        warmup_seconds: 1.0,
        send_rate: None,
    }) {
        return;
    }

    let binary_sha256 = peer_sha256(&binary);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let sizes = args.sizes.as_deref().unwrap_or(SIZES);
    assert!(!sizes.is_empty() && sizes.iter().all(|size| *size > 0));
    let affinity = if std::env::var_os("OMQ_BENCH_TASKSET").is_some() {
        "sender=1-2,receiver=3-4"
    } else {
        "unbound"
    };
    let campaign = Campaign {
        binary: &binary,
        binary_sha256: &binary_sha256,
        run_id: &run_id,
        affinity,
        settings: &settings,
        rounds: args.rounds,
        cache: jsonl::cache_dir().join(CACHE),
    };

    for &size in sizes {
        // Use identical intervals for both series, including at large sizes.
        let duration = args.duration.max(110.0 * (size + 9) as f64 / RATE as f64);
        for repeat in 1..=args.rounds {
            // Alternate order so one series does not always follow startup.
            let order = if repeat % 2 == 1 {
                [false, true]
            } else {
                [true, false]
            };
            for limited in order {
                campaign.measure(size, duration, repeat, limited);
            }
        }
    }
}

/// Settings shared by every cell of one rate-limit campaign.
struct Campaign<'a> {
    binary: &'a Path,
    binary_sha256: &'a str,
    run_id: &'a str,
    affinity: &'a str,
    settings: &'a CompressionLink,
    rounds: u32,
    cache: PathBuf,
}

impl Campaign<'_> {
    /// Measures one cell, with or without the sender egress limit, and
    /// appends its row.
    fn measure(&self, size: u64, duration: f64, repeat: u32, limited: bool) {
        let limit = format!("{RATE},{BURST}");
        let receiver_env = [("OMQ_IO_THREADS", "1")];
        let mut sender_env = receiver_env.to_vec();
        if limited {
            sender_env.push(("OMQ_BENCH_EGRESS_BYTE_LIMIT", limit.as_str()));
        }

        let cell = Cell {
            blocking: self.binary,
            transport: "tcp",
            size,
            duration,
            warmup: 1.0,
        };
        let sample = run_cell(&cell, &sender_env, &receiver_env);

        let row = Row {
            run_id: self.run_id.to_owned(),
            binary_sha256: self.binary_sha256.to_owned(),
            transport: "tcp".into(),
            processes: 2,
            io_threads: 1,
            cpu_affinity: self.affinity.into(),
            netem: self.settings.clone(),
            egress_bytes_per_second: limited.then_some(RATE),
            burst_bytes: limited.then_some(BURST),
            msg_size: size,
            repeat,
            rounds: self.rounds,
            warmup_seconds: 1.0,
            duration_seconds: duration,
            count: sample.count,
            elapsed: sample.elapsed,
            sender_cpu: sample.sender_cpu,
            receiver_cpu: sample.receiver_cpu,
            payload_verified: true,
        };
        eprintln!(
            "{size:>8} B  {:>8}  round {repeat}/{}  {:>10.0} msg/s  {:>7.2} Mbit/s",
            if limited { "800 Mbit" } else { "uncapped" },
            self.rounds,
            row.msgs_s(),
            row.msgs_s() * size as f64 * 8.0 / 1e6,
        );
        jsonl::append_jsonl(&self.cache, &row);
    }
}
