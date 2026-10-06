//! OMQ over QUIC against OMQ with CURVE over TCP, same runtime modes and
//! sizes. Both sides encrypt, so the comparison is fair.
//!
//! Series keys are `<transport>:<impl>`. Each transport loads separately and
//! its keys get the transport prefix before drawing.

use std::collections::BTreeMap;

use plotters::style::RGBColor;

use super::common::{
    self, C_OMQ_1T, C_OMQ_2T, COMPARISON_SIZES, CpuData, FairnessMap, Impl, LatencyMap, ValMap,
    draw_latency_single_panel, draw_multirow_throughput,
    draw_throughput_dual_panel_fixed_2m_msgs_with_versions, load_fairness, load_latency, load_tput,
    merge_cpu_data, out_dir,
};

const C_QUIC_1T: RGBColor = RGBColor(56, 189, 248);
const C_QUIC_2T: RGBColor = RGBColor(3, 105, 161);

const TPUT_SIZES: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];
const LAT_SIZES: &[u64] = &[16, 64, 256, 1024, 4096];
const PEER_COUNTS: &[u64] = &[4, 8];

const CURVE_1T: Impl = Impl {
    key: "tcp:omq-curve-1t",
    label: "omq CURVE",
    threads: "1 IO",
    color: C_OMQ_1T,
};
const CURVE_2T: Impl = Impl {
    key: "tcp:omq-curve-2t",
    label: "omq CURVE",
    threads: "2 IO",
    color: C_OMQ_2T,
};
const QUIC_1T: Impl = Impl {
    key: "quic:omq-tokio-1t",
    label: "omq QUIC",
    threads: "1 IO",
    color: C_QUIC_1T,
};
const QUIC_2T: Impl = Impl {
    key: "quic:omq-tokio-2t",
    label: "omq QUIC",
    threads: "2 IO",
    color: C_QUIC_2T,
};

const SINGLE_PEER_IMPLS: &[Impl] = &[CURVE_1T, QUIC_1T, QUIC_2T];
const MULTI_PEER_IMPLS: &[Impl] = &[CURVE_1T, CURVE_2T, QUIC_1T, QUIC_2T];

/// Impl keys of one transport with the prefix stripped, for the loaders.
fn transport_impls(impls: &[Impl], transport: &str) -> Vec<Impl> {
    impls
        .iter()
        .filter_map(|imp| {
            let (t, key) = imp.key.split_once(':')?;
            (t == transport).then_some(Impl {
                key,
                label: imp.label,
                threads: imp.threads,
                color: imp.color,
            })
        })
        .collect()
}

fn transports(impls: &[Impl]) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = impls
        .iter()
        .filter_map(|imp| imp.key.split_once(':').map(|(t, _)| t))
        .collect();
    out.dedup();
    out
}

fn prefixed(transport: &str, name: &str) -> String {
    format!("{transport}:{name}")
}

fn merge_sized<V>(
    into: &mut BTreeMap<u64, BTreeMap<String, V>>,
    from: BTreeMap<u64, BTreeMap<String, V>>,
    transport: &str,
) {
    for (size, by_impl) in from {
        let entry = into.entry(size).or_default();
        for (name, v) in by_impl {
            entry.insert(prefixed(transport, &name), v);
        }
    }
}

fn merge_cpu(
    into: &mut BTreeMap<String, CpuData>,
    from: BTreeMap<String, CpuData>,
    transport: &str,
) {
    for (name, v) in from {
        into.insert(prefixed(transport, &name), v);
    }
}

fn load_tput_all(
    kind: &str,
    peers: Option<u64>,
    impls: &[Impl],
) -> (ValMap, ValMap, BTreeMap<String, CpuData>) {
    let (mut tput, mut msgs, mut cpu) = (ValMap::new(), ValMap::new(), BTreeMap::new());
    for transport in transports(impls) {
        let (t, m, c) = load_tput(kind, transport, peers, &transport_impls(impls, transport));
        merge_sized(&mut tput, t, transport);
        merge_sized(&mut msgs, m, transport);
        merge_cpu(&mut cpu, c, transport);
    }
    (tput, msgs, cpu)
}

fn load_fairness_all(kind: &str, peers: Option<u64>, impls: &[Impl]) -> FairnessMap {
    let mut fair = FairnessMap::new();
    for transport in transports(impls) {
        let f = load_fairness(kind, transport, peers, &transport_impls(impls, transport));
        merge_sized(&mut fair, f, transport);
    }
    fair
}

fn load_latency_all(impls: &[Impl]) -> (LatencyMap, BTreeMap<String, CpuData>) {
    let (mut lat, mut cpu) = (LatencyMap::new(), BTreeMap::new());
    for transport in transports(impls) {
        let (l, c) = load_latency(transport, LAT_SIZES, &transport_impls(impls, transport));
        merge_sized(&mut lat, l, transport);
        merge_cpu(&mut cpu, c, transport);
    }
    (lat, cpu)
}

fn generate_multi_peer(
    kind: &str,
    file: &str,
    title: &str,
    row_title: &dyn Fn(u64) -> String,
    (snd_label, rcv_label): (&str, &str),
    with_fairness: bool,
) {
    #[expect(clippy::type_complexity)]
    let mut panel_data: Vec<(u64, ValMap, ValMap, BTreeMap<String, CpuData>, FairnessMap)> =
        Vec::new();
    for &peers in PEER_COUNTS {
        let (tput, msgs, cpu) = load_tput_all(kind, Some(peers), MULTI_PEER_IMPLS);
        let fair = if with_fairness {
            load_fairness_all(kind, Some(peers), MULTI_PEER_IMPLS)
        } else {
            FairnessMap::new()
        };
        if !tput.is_empty() {
            panel_data.push((peers, tput, msgs, cpu, fair));
        }
    }
    if panel_data.is_empty() {
        return;
    }
    let merged_cpu = merge_cpu_data(panel_data.iter().map(|(_, _, _, cpu, _)| cpu));
    let rows: Vec<(u64, &ValMap, &ValMap)> = panel_data
        .iter()
        .map(|(p, t, m, _, _)| (*p, t, m))
        .collect();
    let fair_refs: Vec<&FairnessMap> = panel_data.iter().map(|(_, _, _, _, f)| f).collect();
    let out = out_dir().join("quic").join(file);
    draw_multirow_throughput(
        &out,
        title,
        &rows,
        COMPARISON_SIZES,
        MULTI_PEER_IMPLS,
        &merged_cpu,
        row_title,
        snd_label,
        rcv_label,
        with_fairness.then_some(fair_refs.as_slice()),
    )
    .expect("draw QUIC multi-peer chart");
    eprintln!("Written: {}", out.display());
}

pub(crate) fn generate() {
    let dir = out_dir().join("quic");
    std::fs::create_dir_all(&dir).ok();

    let (tput, msgs, cpu) = load_tput_all("throughput", None, SINGLE_PEER_IMPLS);
    if !tput.is_empty() {
        let out = dir.join("pushpull.svg");
        draw_throughput_dual_panel_fixed_2m_msgs_with_versions(
            &out,
            "PUSH/PULL throughput, CURVE/TCP vs QUIC loopback, 2-process",
            TPUT_SIZES,
            SINGLE_PEER_IMPLS,
            &tput,
            &msgs,
            &cpu,
            "snd CPU%",
            "rcv CPU%",
        )
        .expect("draw QUIC pushpull chart");
        eprintln!("Written: {}", out.display());
    }

    let (lat, cpu) = load_latency_all(SINGLE_PEER_IMPLS);
    if !lat.is_empty() {
        let out = dir.join("reqrep.svg");
        draw_latency_single_panel(
            &out,
            "REQ/REP latency, CURVE/TCP vs QUIC loopback, 2-process",
            LAT_SIZES,
            SINGLE_PEER_IMPLS,
            &lat,
            &cpu,
            common::auto_lat_range(&lat),
        )
        .expect("draw QUIC reqrep chart");
        eprintln!("Written: {}", out.display());
    }

    generate_multi_peer(
        "pub_sub",
        "pubsub.svg",
        "PUB/SUB throughput, CURVE/TCP vs QUIC loopback, 2-process",
        &|peers| format!("{peers} subscribers"),
        ("snd CPU%", ""),
        false,
    );
    generate_multi_peer(
        "fan_out",
        "fanout.svg",
        "PUSH fan-out, CURVE/TCP vs QUIC loopback, 2-process",
        &|peers| format!("{peers} peers"),
        ("push CPU%", "pull CPU%"),
        true,
    );
    generate_multi_peer(
        "fan_in",
        "fanin.svg",
        "PUSH fan-in, CURVE/TCP vs QUIC loopback, 2-process",
        &|peers| format!("{peers} peers"),
        ("push CPU%", "pull CPU%"),
        true,
    );
}
