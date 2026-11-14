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

/// Per-peer rate spread of a multi-peer run. Values a peer does not
/// report stay `None`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PeerSpread {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub p10: Option<f64>,
    pub p25: Option<f64>,
    pub median: Option<f64>,
    pub p75: Option<f64>,
    pub p90: Option<f64>,
}

impl PeerSpread {
    /// Nearest-rank quantiles of ascending `rates`; all `None` when empty.
    pub(crate) fn from_sorted(rates: &[f64]) -> Self {
        let quantile = |p: f64| {
            (!rates.is_empty()).then(|| rates[((rates.len() - 1) as f64 * p).round() as usize])
        };

        Self {
            min: quantile(0.0),
            max: quantile(1.0),
            p10: quantile(0.10),
            p25: quantile(0.25),
            median: quantile(0.50),
            p75: quantile(0.75),
            p90: quantile(0.90),
        }
    }
}

/// Multi-socket throughput: the per-peer `rate`, the spread across peers,
/// and every per-peer rate the peer printed.
pub(crate) struct MultiThroughputResult {
    pub rate: ThroughputResult,
    pub spread: PeerSpread,
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
        rate: ThroughputResult {
            msgs_s,
            mbps,
            elapsed,
            pull_cpu: Some(cpu),
        },
        spread: PeerSpread {
            min: Some(min_rate),
            max: Some(max_rate),
            p10: percentile(7),
            p25: percentile(8),
            median: percentile(9),
            p75: percentile(10),
            p90: percentile(11),
        },
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
mod tests;
