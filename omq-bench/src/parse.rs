pub(crate) struct ThroughputResult {
    pub msgs_s: f64,
    pub mbps: f64,
    pub elapsed: f64,
    pub pull_cpu: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct BlockingInprocStats {
    pub hwm: u32,
    pub ring_capacity: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_switches: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_switches_per_message: Option<f64>,
}

pub(crate) fn parse_blocking_inproc_stats(output: &str) -> Option<BlockingInprocStats> {
    let count: f64 = output.split_whitespace().next()?.parse().ok()?;
    if count <= 0.0 || !count.is_finite() {
        return None;
    }
    let stats = output
        .lines()
        .find_map(|line| line.strip_prefix("blocking-stats "))?;
    let field = |name: &str| {
        stats.split_whitespace().find_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            (key == name).then_some(value)
        })
    };
    let context_switches: Option<u64> =
        field("context_switches").and_then(|value| value.parse().ok());
    Some(BlockingInprocStats {
        hwm: field("hwm")?.parse().ok()?,
        ring_capacity: field("ring_capacity")?.parse().ok()?,
        cpu_seconds: field("cpu_seconds").and_then(|value| value.parse().ok()),
        context_switches,
        context_switches_per_message: context_switches.map(|switches| switches as f64 / count),
    })
}

pub(crate) struct MultiThroughputResult {
    pub msgs_s: f64,
    pub mbps: f64,
    pub elapsed: f64,
    pub pull_cpu: Option<f64>,
    pub peer_min: Option<f64>,
    pub peer_max: Option<f64>,
    pub peer_p10: Option<f64>,
    pub peer_p25: Option<f64>,
    pub peer_median: Option<f64>,
    pub peer_p75: Option<f64>,
    pub peer_p90: Option<f64>,
    pub peer_rates: Vec<f64>,
}

pub(crate) struct LatencyResult {
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub iterations: u64,
    pub req_cpu: Option<f64>,
    pub elapsed: Option<f64>,
}

/// Parse single-stream throughput output.
///
/// Format: `count elapsed size [cpu]`
pub(crate) fn parse_throughput(output: &str, size: u64) -> Option<ThroughputResult> {
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let count: f64 = parts[0].parse().ok()?;
    let elapsed: f64 = parts[1].parse().ok()?;
    if elapsed <= 0.0 || count <= 0.0 {
        return None;
    }
    let pull_cpu: Option<f64> = parts.get(3).and_then(|s| s.parse().ok());
    let msgs_s = count / elapsed;
    let mbps = (count * size as f64) / elapsed / 1_000_000.0;
    Some(ThroughputResult {
        msgs_s,
        mbps,
        elapsed,
        pull_cpu,
    })
}

/// Parse multi-socket throughput output.
///
/// Format: `total elapsed size cpu sockets min_rate max_rate`
pub(crate) fn parse_multi_throughput(
    output: &str,
    size: u64,
    peers: u64,
) -> Option<MultiThroughputResult> {
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 7 {
        return None;
    }
    let total: f64 = parts[0].parse().ok()?;
    let elapsed: f64 = parts[1].parse().ok()?;
    let cpu: f64 = parts[3].parse().ok()?;
    let min_rate: f64 = parts[5].parse().ok()?;
    let max_rate: f64 = parts[6].parse().ok()?;
    let percentile = |i: usize| parts.get(i).and_then(|s| s.parse().ok());
    if elapsed <= 0.0 || total <= 0.0 {
        return None;
    }
    let peers_f = peers as f64;
    let msgs_s = total / elapsed / peers_f;
    let mbps = (total * size as f64) / elapsed / 1_000_000.0;
    Some(MultiThroughputResult {
        msgs_s,
        mbps,
        elapsed,
        pull_cpu: Some(cpu),
        peer_min: Some(min_rate),
        peer_max: Some(max_rate),
        peer_p10: percentile(7),
        peer_p25: percentile(8),
        peer_median: percentile(9),
        peer_p75: percentile(10),
        peer_p90: percentile(11),
        peer_rates: parts
            .get(12..)
            .unwrap_or(&[])
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect(),
    })
}

/// Parse latency output.
///
/// Format: `p50 p99 p999 max iterations [cpu] [elapsed]`
#[allow(clippy::similar_names)]
pub(crate) fn parse_latency(output: &str) -> Option<LatencyResult> {
    let parts: Vec<&str> = output.split_whitespace().collect();
    if parts.len() < 5 {
        return None;
    }
    let p50_us: f64 = parts[0].parse().ok()?;
    let p99_us: f64 = parts[1].parse().ok()?;
    let p999_us: f64 = parts[2].parse().ok()?;
    let max_us: f64 = parts[3].parse().ok()?;
    let iterations: u64 = parts[4].parse().ok()?;
    let req_cpu: Option<f64> = parts.get(5).and_then(|s| s.parse().ok());
    let elapsed: Option<f64> = parts.get(6).and_then(|s| s.parse().ok());
    Some(LatencyResult {
        p50_us,
        p99_us,
        p999_us,
        max_us,
        iterations,
        req_cpu,
        elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_blocking_inproc_stats, parse_throughput};

    #[test]
    fn blocking_stats_preserve_throughput_and_normalize_switches() {
        let output = "1000 0.5 64\nblocking-stats hwm=8 ring_capacity=16 cpu_seconds=0.6 context_switches=50\n";
        let rate = parse_throughput(output, 64).unwrap();
        assert!((rate.msgs_s - 2000.0).abs() < f64::EPSILON);
        let stats = parse_blocking_inproc_stats(output).unwrap();
        assert_eq!(stats.hwm, 8);
        assert_eq!(stats.ring_capacity, 16);
        assert_eq!(stats.cpu_seconds, Some(0.6));
        assert_eq!(stats.context_switches_per_message, Some(0.05));
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"context_switches\":50"));
    }

    #[test]
    fn blocking_stats_allow_platforms_without_usage_counters() {
        let output = "256 1.0 16\nblocking-stats hwm=1000 ring_capacity=2048\n";
        let stats = parse_blocking_inproc_stats(output).unwrap();
        assert_eq!(stats.cpu_seconds, None);
        assert_eq!(stats.context_switches_per_message, None);
        assert!((parse_throughput(output, 16).unwrap().msgs_s - 256.0).abs() < f64::EPSILON);
        assert!(parse_blocking_inproc_stats("256 1.0 16").is_none());
        assert!(
            parse_blocking_inproc_stats("0 1.0 16\nblocking-stats hwm=8 ring_capacity=16")
                .is_none()
        );
    }
}
