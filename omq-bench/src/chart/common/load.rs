//! Load benchmark JSONL rows into chart maps.

use super::{CpuData, FairnessEntry, FairnessMap, Impl, LatencyEntry, LatencyMap, ValMap};
use std::collections::BTreeMap;

#[derive(Default)]
struct CpuAccum {
    sender_sum: f64,
    broker_sum: f64,
    receiver_sum: f64,
    sender_count: u32,
    broker_count: u32,
    receiver_count: u32,
    runtime_workers: Option<usize>,
}

impl CpuAccum {
    fn add_sender_pct(&mut self, pct: f64) {
        self.sender_sum += pct;
        self.sender_count += 1;
    }

    fn add_receiver_pct(&mut self, pct: f64) {
        self.receiver_sum += pct;
        self.receiver_count += 1;
    }

    fn add_broker_pct(&mut self, pct: f64) {
        self.broker_sum += pct;
        self.broker_count += 1;
    }

    fn add_sender(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_sender_pct(cpu_time / elapsed * 100.0);
    }

    fn add_receiver(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_receiver_pct(cpu_time / elapsed * 100.0);
    }

    fn add_broker(&mut self, cpu_time: f64, elapsed: f64) {
        self.add_broker_pct(cpu_time / elapsed * 100.0);
    }

    fn into_data(self) -> CpuData {
        CpuData {
            runtime_workers: self.runtime_workers,
            sender: (self.sender_count > 0).then(|| self.sender_sum / f64::from(self.sender_count)),
            broker: (self.broker_count > 0).then(|| self.broker_sum / f64::from(self.broker_count)),
            receiver: (self.receiver_count > 0)
                .then(|| self.receiver_sum / f64::from(self.receiver_count)),
        }
    }
}

pub(crate) fn merge_cpu_data<'a>(
    panel_cpus: impl IntoIterator<Item = &'a BTreeMap<String, CpuData>>,
) -> BTreeMap<String, CpuData> {
    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for cpu in panel_cpus {
        for (name, data) in cpu {
            let accum = cpu_sums.entry(name.clone()).or_default();
            accum.runtime_workers = data.runtime_workers.or(accum.runtime_workers);
            if let Some(sender) = data.sender {
                accum.add_sender_pct(sender);
            }
            if let Some(broker) = data.broker {
                accum.add_broker_pct(broker);
            }
            if let Some(receiver) = data.receiver {
                accum.add_receiver_pct(receiver);
            }
        }
    }

    cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect()
}

pub(crate) fn load_tput(
    kind: &str,
    transport: &str,
    peers: Option<u64>,
    impls: &[Impl],
) -> (ValMap, ValMap, BTreeMap<String, CpuData>) {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);

    let mut tput: ValMap = BTreeMap::new();
    let mut msgs: ValMap = BTreeMap::new();
    let mut latest: BTreeMap<(String, u64), ComparisonRow> = BTreeMap::new();

    for (_, row) in rows {
        if row.transport != transport || row.kind != kind {
            continue;
        }
        let Some(chart_key) = chart_key_for(impls, &row.impl_name, transport) else {
            continue;
        };
        if let Some(p) = peers
            && row.peers != Some(p)
        {
            continue;
        }
        let key = (chart_key.to_string(), row.msg_size);
        latest.insert(key, row);
    }

    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for ((chart_key, _), row) in latest {
        if let Some(v) = row.mbps {
            tput.entry(row.msg_size)
                .or_default()
                .insert(chart_key.clone(), v);
        }
        if let Some(v) = row.msgs_s {
            msgs.entry(row.msg_size)
                .or_default()
                .insert(chart_key.clone(), v);
        }
        if let Some(elapsed) = row.elapsed
            && elapsed > 0.0
        {
            let e = cpu_sums.entry(chart_key).or_default();
            e.runtime_workers = row.runtime_workers.or(e.runtime_workers);
            if let Some(push) = row.push_cpu_time.or(row.pub_cpu_time) {
                e.add_sender(push, elapsed);
            }
            if let Some(broker) = row.broker_cpu_time {
                e.add_broker(broker, elapsed);
            }
            if let Some(pull) = row.pull_cpu_time {
                e.add_receiver(pull, elapsed);
            } else if let (Some(total), Some(push)) =
                (row.cpu_time, row.push_cpu_time.or(row.pub_cpu_time))
            {
                e.add_receiver(total - push, elapsed);
            }
        }
    }

    let cpu = cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect();

    (tput, msgs, cpu)
}

/// Chart key whose series `impl_name` rows feed on `transport`. Charts with
/// an `omq-quic` series read OMQ QUIC rows recorded as `omq-tokio-1t`.
fn chart_key_for(impls: &[Impl], impl_name: &str, transport: &str) -> Option<&'static str> {
    let map_omq_quic = impls.iter().any(|imp| imp.key == "omq-quic");
    impls
        .iter()
        .map(|imp| imp.key)
        .find(|&key| comparison_impl_key(key, transport, map_omq_quic) == Some(impl_name))
}

fn comparison_impl_key<'a>(
    chart_key: &'a str,
    transport: &str,
    map_omq_quic: bool,
) -> Option<&'a str> {
    if !map_omq_quic {
        return Some(chart_key);
    }
    match (chart_key, transport) {
        ("omq-tokio-1t" | "omq-tokio-1t-spin50", "quic") => None,
        ("omq-quic", "quic") => Some("omq-tokio-1t"),
        _ => Some(chart_key),
    }
}

pub(crate) fn load_fairness(
    kind: &str,
    transport: &str,
    peers: Option<u64>,
    impls: &[Impl],
) -> FairnessMap {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);
    let keys: Vec<&str> = impls.iter().map(|i| i.key).collect();

    let mut fairness: FairnessMap = BTreeMap::new();
    let mut seen: BTreeMap<(String, u64), usize> = BTreeMap::new();

    for (seq, row) in &rows {
        if row.transport != transport || row.kind != kind {
            continue;
        }
        if !keys.contains(&row.impl_name.as_str()) {
            continue;
        }
        if let Some(p) = peers
            && row.peers != Some(p)
        {
            continue;
        }
        let key = (row.impl_name.clone(), row.msg_size);
        if seen.get(&key).is_some_and(|&prev| *seq < prev) {
            continue;
        }
        seen.insert(key, *seq);
        if let (Some(min), Some(p25), Some(median), Some(p75), Some(max)) = (
            row.peer_min,
            row.peer_p25,
            row.peer_median,
            row.peer_p75,
            row.peer_max,
        ) && median > 0.0
        {
            fairness.entry(row.msg_size).or_default().insert(
                row.impl_name.clone(),
                FairnessEntry {
                    min,
                    p25,
                    median,
                    p75,
                    max,
                },
            );
        }
    }

    fairness
}

pub(crate) fn load_latency(
    transport: &str,
    sizes: &[u64],
    impls: &[Impl],
) -> (LatencyMap, BTreeMap<String, CpuData>) {
    use crate::jsonl::{self, ComparisonRow};

    let path = jsonl::cache_dir().join("comparisons.jsonl");
    let rows: Vec<(usize, ComparisonRow)> = jsonl::load_jsonl(&path);

    let mut lat: LatencyMap = BTreeMap::new();
    let mut latest: BTreeMap<(String, u64), ComparisonRow> = BTreeMap::new();

    for (_, row) in rows {
        if row.transport != transport || row.kind != "latency" {
            continue;
        }
        if row
            .latency_pair
            .as_deref()
            .is_some_and(|pair| pair != "req-rep")
            || row
                .workload_profile
                .as_deref()
                .is_some_and(|profile| profile != "default")
        {
            continue;
        }
        let Some(chart_key) = chart_key_for(impls, &row.impl_name, transport) else {
            continue;
        };
        if !sizes.contains(&row.msg_size) {
            continue;
        }
        let key = (chart_key.to_string(), row.msg_size);
        latest.insert(key, row);
    }

    let mut cpu_sums: BTreeMap<String, CpuAccum> = BTreeMap::new();

    for ((chart_key, _), row) in latest {
        if let (Some(p50), Some(p99), Some(p999)) = (row.p50_us, row.p99_us, row.p999_us) {
            lat.entry(row.msg_size)
                .or_default()
                .insert(chart_key.clone(), LatencyEntry { p50, p99, p999 });
        }
        if let Some(elapsed) = row.elapsed
            && elapsed > 0.0
        {
            let e = cpu_sums.entry(chart_key).or_default();
            if let Some(req) = row.req_cpu_time {
                e.add_sender(req, elapsed);
            }
            if let Some(broker) = row.broker_cpu_time {
                e.add_broker(broker, elapsed);
            }
            if let (Some(total), Some(req)) = (row.cpu_time, row.req_cpu_time) {
                e.add_receiver(total - req, elapsed);
            }
        }
    }

    let cpu = cpu_sums
        .into_iter()
        .map(|(name, accum)| (name, accum.into_data()))
        .collect();

    (lat, cpu)
}
