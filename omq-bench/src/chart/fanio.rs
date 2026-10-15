use std::collections::BTreeMap;

use super::common::{
    C_LIBZMQ, C_LIBZMQ_2T, C_OMQ_1T, C_OMQ_2T, COMPARISON_SIZES, CpuData, FairnessMap, Impl,
    ValMap, draw_multirow_throughput, load_fairness, load_tput, merge_cpu_data, out_dir,
};

const FANIO_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "libzmq-2t",
        label: "libzmq",
        threads: "2 IO",
        color: C_LIBZMQ_2T,
    },
    Impl {
        key: "omq-tokio-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-tokio-2t",
        label: "omq",
        threads: "2 IO",
        color: C_OMQ_2T,
    },
];

const PEER_COUNTS: &[u64] = &[4, 32];

fn generate_fanio(kind: &str, dir_name: &str, title_fn: &dyn Fn(u64) -> String) {
    let dir = out_dir();
    let sub = dir.join("pushpull").join(dir_name);
    std::fs::create_dir_all(&sub).ok();

    #[expect(clippy::type_complexity)]
    let mut panel_data: Vec<(u64, ValMap, ValMap, BTreeMap<String, CpuData>, FairnessMap)> =
        Vec::new();
    for &peers in PEER_COUNTS {
        let (tput, msgs, cpu) = load_tput(kind, "tcp", Some(peers), FANIO_IMPLS);
        let fair = load_fairness(kind, "tcp", Some(peers), FANIO_IMPLS);
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
    let out = sub.join("tcp.svg");
    let chart_title = title_fn(0);
    let _ = kind;
    let (snd_label, rcv_label) = ("push CPU%", "pull CPU%");
    draw_multirow_throughput(
        &out,
        &chart_title,
        &rows,
        COMPARISON_SIZES,
        FANIO_IMPLS,
        &merged_cpu,
        &|peers| format!("{peers} peers"),
        snd_label,
        rcv_label,
        Some(&fair_refs),
        super::common::GbsScale::Linear,
        super::common::detect_hardware().as_deref(),
    )
    .expect("draw fanio chart");
    eprintln!("Written: {}", out.display());
}

pub(crate) fn generate() {
    generate_fanio("fan_out", "fanout", &|_| {
        "PUSH fan-out, TCP loopback, 2-process".to_string()
    });
    generate_fanio("fan_in", "fanin", &|_| {
        "PUSH fan-in, TCP loopback, 2-process".to_string()
    });
}
