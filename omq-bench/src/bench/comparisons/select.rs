//! Sizes, implementations, transports, durations, and ports selected from
//! the command line.

use super::impls::{IMPLS, ImplClass, TransportKind, find_impl};
use crate::cli::ComparisonsArgs;

pub(super) const COMPARISON_CHART_SIZES: &[u64] = &[16, 64, 256, 1024, 4096, 16384];
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

pub(super) fn latency_sizes_from(sizes: &[u64]) -> Vec<u64> {
    sizes
        .iter()
        .copied()
        .filter(|&s| s <= LATENCY_MAX_SIZE)
        .collect()
}

pub(super) fn selected_sizes(args: &ComparisonsArgs) -> Vec<u64> {
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

pub(super) fn selected_impl_names(args: &ComparisonsArgs) -> Vec<&str> {
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
    if args.curve.enabled {
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

pub(super) fn random_base_port() -> u16 {
    let mut buf = [0u8; 2];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| {
            use std::io::Read;
            file.read_exact(&mut buf)
        })
        .ok();
    20_000 + (u16::from_le_bytes(buf) % 20_000)
}

/// OMQ peer Cargo features that the selected transports and impls need.
pub(super) fn peer_features(
    args: &ComparisonsArgs,
    impl_names: &[&str],
    transports: &[TransportKind],
) -> Vec<&'static str> {
    let mut features = Vec::new();
    if transports.contains(&TransportKind::Ws) {
        features.push("ws");
    }
    if transports.contains(&TransportKind::Quic) {
        features.push("quic");
    }
    if args.curve.enabled || impl_names.iter().any(|name| name.contains("curve")) {
        features.push("curve");
    }
    features
}

/// Seconds per cell and rounds per cell: CLI, then environment, then
/// defaults. Quick runs override both.
pub(super) fn duration_and_rounds(args: &ComparisonsArgs) -> (f64, u32) {
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

pub(super) fn exit_on_unknown_impls(impl_names: &[&str]) {
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

pub(super) fn selected_transports(args: &ComparisonsArgs) -> Vec<TransportKind> {
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
