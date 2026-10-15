use std::collections::{BTreeMap, BTreeSet};

use super::common::{
    C_LIBZMQ, C_LIBZMQ_2T, C_OMQ_1T, C_OMQ_2T, COMPARISON_SIZES, CpuData, Impl, ValMap,
    draw_multirow_throughput, draw_throughput_dual_panel, load_tput, merge_cpu_data, out_dir,
};

const PUBSUB_IMPLS: &[Impl] = &[
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

const CURVE_IMPLS: &[Impl] = &[
    Impl {
        key: "libzmq-curve-1t",
        label: "libzmq",
        threads: "1 IO",
        color: C_LIBZMQ,
    },
    Impl {
        key: "libzmq-curve-2t",
        label: "libzmq",
        threads: "2 IO",
        color: C_LIBZMQ_2T,
    },
    Impl {
        key: "omq-curve-1t",
        label: "omq",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
    Impl {
        key: "omq-curve-2t",
        label: "omq",
        threads: "2 IO",
        color: C_OMQ_2T,
    },
];

const PUBSUB_PEERS: &[u64] = &[4, 32];
const CURVE_PEERS: u64 = 16;

pub(crate) fn generate() {
    let dir = out_dir();
    let sub = dir.join("pubsub");
    std::fs::create_dir_all(&sub).ok();

    // PUB/SUB multi-panel
    for (transport, title) in [
        ("tcp", "PUB/SUB throughput, TCP loopback, 2-process"),
        ("ipc", "PUB/SUB throughput, IPC, 2-process"),
    ] {
        let mut panel_data: Vec<(u64, ValMap, ValMap, BTreeMap<String, CpuData>)> = Vec::new();
        for &peers in PUBSUB_PEERS {
            let (tput, msgs, cpu) = load_tput("pub_sub", transport, Some(peers), PUBSUB_IMPLS);
            if !tput.is_empty() {
                panel_data.push((peers, tput, msgs, cpu));
            }
        }
        if !panel_data.is_empty() {
            let title = format!("{title}{}", receiver_label(transport));
            let merged_cpu = merge_cpu_data(panel_data.iter().map(|(_, _, _, cpu)| cpu));
            let rows: Vec<(u64, &ValMap, &ValMap)> =
                panel_data.iter().map(|(p, t, m, _)| (*p, t, m)).collect();
            let out = sub.join(format!("{transport}.svg"));
            draw_multirow_throughput(
                &out,
                &title,
                &rows,
                COMPARISON_SIZES,
                PUBSUB_IMPLS,
                &merged_cpu,
                &|peers| {
                    if peers == 1 {
                        "1 subscriber".to_string()
                    } else {
                        format!("{peers} subscribers")
                    }
                },
                "snd CPU%",
                "",
                None,
                super::common::GbsScale::Linear,
                super::common::detect_hardware().as_deref(),
            )
            .expect("draw pubsub chart");
            eprintln!("Written: {}", out.display());
        }
    }

    // CURVE PUB/SUB
    let (tput, msgs, cpu) = load_tput("pub_sub", "tcp", Some(CURVE_PEERS), CURVE_IMPLS);
    if !tput.is_empty() {
        let out = sub.join("curve_tcp.svg");
        draw_throughput_dual_panel(
            &out,
            &format!("CURVE PUB/SUB throughput ({CURVE_PEERS} peers), TCP loopback, 2-process"),
            COMPARISON_SIZES,
            CURVE_IMPLS,
            &tput,
            &msgs,
            &cpu,
            "snd CPU%",
            "rcv CPU%",
        )
        .expect("draw curve chart");
        eprintln!("Written: {}", out.display());
    }
}

fn receiver_label(transport: &str) -> String {
    let path = crate::jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(_, crate::jsonl::ComparisonRow)> = crate::jsonl::load_jsonl(&path);
    let mut latest = BTreeMap::new();
    for (_, row) in rows {
        if row.kind == "pub_sub"
            && row.transport == transport
            && row.peers.is_some_and(|peers| PUBSUB_PEERS.contains(&peers))
            && COMPARISON_SIZES.contains(&row.msg_size)
            && PUBSUB_IMPLS.iter().any(|def| def.key == row.impl_name)
        {
            latest.insert(
                (row.impl_name, row.peers, row.msg_size),
                (row.receiver_io_threads, row.receiver_workers),
            );
        }
    }
    let io_counts: BTreeSet<_> = latest.values().map(|(io, _)| *io).collect();
    let Some(Some(io)) = io_counts.first() else {
        return String::new();
    };
    if io_counts.len() != 1 {
        return String::new();
    }
    let workers: BTreeSet<_> = latest
        .into_values()
        .map(|(_, workers)| workers.unwrap_or(1))
        .collect();
    match (workers.first(), workers.last()) {
        (Some(min), Some(max)) if min != max => {
            format!(", receiver {io} IO / {min}-{max} workers")
        }
        (Some(count), _) => format!(", receiver {io} IO / {count} workers"),
        _ => format!(", receiver {io} IO"),
    }
}
