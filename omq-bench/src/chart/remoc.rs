use std::collections::BTreeMap;

use super::common::{
    self, C_OMQ_CT, C_REMOC, ChartSpec, CpuColumns, Impl, LatencyEntry, LatencyMap, ValMap,
    draw_latency_single_panel, draw_throughput_dual_panel, out_dir,
};
use crate::jsonl::{self, ComparisonRow};

const IMPLS: &[Impl] = &[
    Impl {
        key: "omq-tokio-ct",
        label: "OMQ 0.22.1 (raw messages)",
        threads: "CT",
        color: C_OMQ_CT,
    },
    Impl {
        key: "remoc",
        label: "Remoc 0.20.5 (Postbag)",
        threads: "CT",
        color: C_REMOC,
    },
];

fn load_data() -> Option<(String, ValMap, ValMap, LatencyMap)> {
    let rows: Vec<(usize, ComparisonRow)> =
        jsonl::load_jsonl(&jsonl::cache_dir().join("comparisons.jsonl"));
    let run_id = rows
        .iter()
        .rev()
        .find(|(_, row)| row.impl_name == "remoc" && row.transport == "tcp")
        .map(|(_, row)| row.run_id.clone())?;
    // Keep both implementations from the same run instead of mixing baselines.
    let mut throughput = ValMap::new();
    let mut messages = ValMap::new();
    let mut latency = LatencyMap::new();
    for (_, row) in rows {
        if row.run_id != run_id
            || row.transport != "tcp"
            || !IMPLS.iter().any(|imp| imp.key == row.impl_name)
        {
            continue;
        }
        if row.kind == "throughput" {
            if let (Some(mbps), Some(msgs_s)) = (row.mbps, row.msgs_s) {
                throughput
                    .entry(row.msg_size)
                    .or_default()
                    .insert(row.impl_name.clone(), mbps);
                messages
                    .entry(row.msg_size)
                    .or_default()
                    .insert(row.impl_name, msgs_s);
            }
        } else if row.kind == "latency"
            && let (Some(p50), Some(p99), Some(p999)) = (row.p50_us, row.p99_us, row.p999_us)
        {
            latency
                .entry(row.msg_size)
                .or_default()
                .insert(row.impl_name, LatencyEntry { p50, p99, p999 });
        }
    }
    Some((run_id, throughput, messages, latency))
}

pub(crate) fn generate() {
    let Some((run_id, throughput, messages, latency)) = load_data() else {
        return;
    };
    let dir = out_dir();
    if !throughput.is_empty() {
        let out = dir.join("remoc_pushpull_tcp.svg");
        let spec = ChartSpec {
            out_path: &out,
            title: "Remoc MPSC vs OMQ PUSH/PULL: TCP loopback, 2 processes",
            sizes: &throughput.keys().copied().collect::<Vec<_>>(),
            impls: IMPLS,
        };
        let columns = CpuColumns {
            cpu: &BTreeMap::new(),
            snd_label: "",
            rcv_label: "",
        };
        draw_throughput_dual_panel(&spec, &throughput, &messages, &columns)
            .expect("draw Remoc throughput chart");
        eprintln!("Written: {} (run {run_id})", out.display());
    }
    if !latency.is_empty() {
        let out = dir.join("remoc_rpc_tcp.svg");
        let spec = ChartSpec {
            out_path: &out,
            title: "Remoc RPC vs OMQ REQ/REP: one request in flight",
            sizes: &latency.keys().copied().collect::<Vec<_>>(),
            impls: IMPLS,
        };
        draw_latency_single_panel(
            &spec,
            &latency,
            &BTreeMap::new(),
            common::auto_lat_range(&latency),
        )
        .expect("draw Remoc latency chart");
        eprintln!("Written: {} (run {run_id})", out.display());
    }
}
