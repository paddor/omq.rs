use std::collections::BTreeMap;

use super::common::{
    C_OMQ_1T, C_OMQ_CT, CpuData, Impl, ValMap, draw_throughput_dual_panel, out_dir,
};
use crate::bench::pushpull_rate_limit::{BURST, CACHE, RATE, Row, SIZES, link};
use crate::jsonl;

const SERIES: &[Impl] = &[
    Impl {
        key: "uncapped",
        label: "omq uncapped",
        threads: "1 IO",
        color: C_OMQ_CT,
    },
    Impl {
        key: "limited",
        label: "omq 800 Mbit/s",
        threads: "1 IO",
        color: C_OMQ_1T,
    },
];

#[expect(clippy::float_cmp, reason = "warmup is exact recorded configuration")]
fn eligible(row: &Row) -> bool {
    row.transport == "tcp"
        && row.processes == 2
        && row.io_threads == 1
        && row.cpu_affinity == "sender=1-2,receiver=3-4"
        && row.netem == link()
        && matches!(
            (row.egress_bytes_per_second, row.burst_bytes),
            (None, None) | (Some(RATE), Some(BURST))
        )
        && row.binary_sha256.len() == 64
        && SIZES.contains(&row.msg_size)
        && row.rounds == 3
        && (1..=3).contains(&row.repeat)
        && row.warmup_seconds == 1.0
        && row.duration_seconds.is_finite()
        && row.duration_seconds >= 3.0_f64.max(110.0 * (row.msg_size + 9) as f64 / RATE as f64)
        && row.elapsed.is_finite()
        && row.elapsed >= row.duration_seconds
        && row.elapsed <= row.duration_seconds * 1.1
        && row.count.is_finite()
        && row.count > 0.0
        && row.sender_cpu.is_finite()
        && row.sender_cpu >= 0.0
        && row.receiver_cpu.is_finite()
        && row.receiver_cpu >= 0.0
        && row.payload_verified
}

// Keep both series, every size and all repeats in one campaign/binary cohort.
// A partial diagnostic run must not replace any published points.
fn select(rows: Vec<Row>) -> Vec<Row> {
    type Cells = BTreeMap<(u64, bool), BTreeMap<u32, Row>>;
    let mut campaigns: BTreeMap<(String, String), (usize, Cells)> = BTreeMap::new();
    for (index, row) in rows
        .into_iter()
        .enumerate()
        .filter(|(_, row)| eligible(row))
    {
        let (last, cells) = campaigns
            .entry((row.run_id.clone(), row.binary_sha256.clone()))
            .or_default();
        *last = index;
        cells
            .entry((row.msg_size, row.egress_bytes_per_second.is_some()))
            .or_default()
            .insert(row.repeat, row);
    }
    let Some((_, cells)) = campaigns
        .into_values()
        .filter(|(_, cells)| {
            cells.len() == SIZES.len() * 2 && cells.values().all(|runs| runs.len() == 3)
        })
        .max_by_key(|(last, _)| *last)
    else {
        return Vec::new();
    };
    cells
        .into_values()
        .map(|runs| {
            let mut runs: Vec<_> = runs.into_values().collect();
            runs.sort_by(|a, b| a.msgs_s().total_cmp(&b.msgs_s()));
            runs.swap_remove(1)
        })
        .collect()
}

pub(crate) fn generate() {
    let rows = jsonl::load_jsonl::<Row>(&jsonl::cache_dir().join(CACHE))
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let selected = select(rows);
    if selected.is_empty() {
        eprintln!("No complete TCP rate-limit campaign; chart unchanged");
        return;
    }
    let mut msgs = ValMap::new();
    let mut tput = ValMap::new();
    let mut cpu = BTreeMap::new();
    for series in SERIES {
        let rows: Vec<_> = selected
            .iter()
            .filter(|row| row.egress_bytes_per_second.is_some() == (series.key == "limited"))
            .collect();
        for row in &rows {
            msgs.entry(row.msg_size)
                .or_default()
                .insert(series.key.into(), row.msgs_s());
            tput.entry(row.msg_size)
                .or_default()
                .insert(series.key.into(), row.msgs_s() * row.msg_size as f64 / 1e6);
        }
        cpu.insert(
            series.key.into(),
            CpuData {
                sender: Some(
                    rows.iter()
                        .map(|row| row.sender_cpu / row.elapsed * 100.0)
                        .sum::<f64>()
                        / rows.len() as f64,
                ),
                receiver: Some(
                    rows.iter()
                        .map(|row| row.receiver_cpu / row.elapsed * 100.0)
                        .sum::<f64>()
                        / rows.len() as f64,
                ),
                runtime_workers: None,
            },
        );
    }
    let dir = out_dir().join("pushpull");
    std::fs::create_dir_all(&dir).expect("chart directory");
    let path = dir.join("tcp_rate_limit.svg");
    draw_throughput_dual_panel(
        &path,
        "PUSH/PULL TCP, 2-process, 1000 Mbit/s netem; limiter: 800 Mbit/s, 1 MB burst",
        SIZES,
        SERIES,
        &tput,
        &msgs,
        &cpu,
        "snd CPU%",
        "rcv CPU%",
    )
    .expect("draw TCP rate-limit chart");
    eprintln!("Written: {}", path.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn campaign(id: &str) -> Vec<Row> {
        SIZES
            .iter()
            .flat_map(|&size| {
                [false, true].into_iter().flat_map(move |limited| {
                    (1..=3).map(move |repeat| Row {
                        run_id: id.into(),
                        binary_sha256: "a".repeat(64),
                        transport: "tcp".into(),
                        processes: 2,
                        io_threads: 1,
                        cpu_affinity: "sender=1-2,receiver=3-4".into(),
                        netem: link(),
                        egress_bytes_per_second: limited.then_some(RATE),
                        burst_bytes: limited.then_some(BURST),
                        msg_size: size,
                        repeat,
                        rounds: 3,
                        warmup_seconds: 1.0,
                        duration_seconds: 10.0,
                        elapsed: 10.0,
                        count: f64::from(repeat * 100),
                        sender_cpu: f64::from(repeat),
                        receiver_cpu: 1.0,
                        payload_verified: true,
                    })
                })
            })
            .collect()
    }

    #[test]
    fn retains_complete_campaign_and_cpu_from_median_sample() {
        let mut rows = campaign("complete");
        let mut partial = campaign("partial");
        partial.pop();
        rows.extend(partial);
        let selected = select(rows);
        assert_eq!(selected.len(), SIZES.len() * 2);
        assert!(
            selected
                .iter()
                .all(|row| row.run_id == "complete" && row.repeat == 2)
        );
        for row in selected {
            assert!((row.sender_cpu - 2.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn rejects_mixed_binaries_and_mismatched_network_settings() {
        let mut rows = campaign("mixed");
        rows[0].binary_sha256 = "b".repeat(64);
        assert!(select(rows).is_empty());
        let mut rows = campaign("wrong-burst");
        rows.iter_mut()
            .find(|row| row.burst_bytes.is_some())
            .unwrap()
            .burst_bytes = Some(BURST * 2);
        assert!(select(rows).is_empty());
        let mut rows = campaign("wrong-link");
        rows[0].netem.segmentation_offloads = true;
        assert!(select(rows).is_empty());
    }
}
