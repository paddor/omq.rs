//! Build comparison peers from their sources.

use super::impls::find_impl;
use crate::process;
use std::collections::HashMap;
use std::path::PathBuf;

pub(super) fn build_peers(
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
        "remoc" => {
            process::build_external_peer("scripts/remoc_bench_peer", "remoc_bench_peer", &[])
        }
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
