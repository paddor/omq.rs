use std::path::PathBuf;

use clap::Args;

#[derive(Args)]
pub(crate) struct CommonArgs {
    #[arg(long, default_value = "both", value_parser = ["throughput", "latency", "both"])]
    pub kind: String,
    #[arg(long, value_delimiter = ',')]
    pub sizes: Option<Vec<u64>>,
    #[arg(long, default_value_t = 3)]
    pub repeats: usize,
    #[arg(long, value_delimiter = ',', default_value = "0,1,2,3,4,5")]
    pub cpus: Vec<usize>,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub profile: Option<PathBuf>,
    #[arg(long, value_parser = ["send", "receive"])]
    pub profile_side: Option<String>,
    /// Rotate the first size between independent process pairs.
    #[arg(long, default_value = "rotate", value_parser = ["rotate", "fixed"])]
    pub order: String,
}

impl CommonArgs {
    pub(super) fn kinds(&self) -> Vec<&str> {
        if self.kind == "both" {
            vec!["throughput", "latency"]
        } else {
            vec![&self.kind]
        }
    }

    pub(super) fn sizes(&self, defaults: &[u64]) -> Vec<u64> {
        let sizes = self.sizes.clone().unwrap_or_else(|| defaults.to_vec());
        assert!(self.repeats > 0 && !sizes.is_empty());
        for (i, size) in sizes.iter().enumerate() {
            assert!((16..=8_388_608).contains(size) && !sizes[..i].contains(size));
        }
        sizes
    }

    pub(super) fn cpu_csv(&self) -> String {
        self.cpus
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) fn output(&self, name: &str) -> PathBuf {
        self.output
            .clone()
            .unwrap_or_else(|| crate::jsonl::cache_dir().join(name))
    }
}

#[derive(Args)]
pub(crate) struct DartArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long)]
    pub binary: PathBuf,
    #[arg(long, default_value = "dart,tcp", value_delimiter = ',', value_parser = ["dart", "tcp", "quic"])]
    pub transport: Vec<String>,
    #[arg(long, default_value_t = 3.0)]
    pub duration: f64,
    /// Unmeasured throughput warmup in seconds.
    #[arg(long, default_value_t = 0.2)]
    pub warmup_seconds: f64,
    /// Maximum postmeasurement delivery and acknowledgment drain in seconds.
    #[arg(long, default_value_t = 2.0)]
    pub drain_seconds: f64,
    #[arg(long, default_value_t = 100_000)]
    pub iterations: u64,
    #[arg(long, default_value_t = 200_000)]
    pub warmup: u64,
    #[arg(long, default_value_t = 50)]
    pub spin: u64,
    #[arg(long, default_value_t = 50)]
    pub io_spin: u64,
    /// Poll application receive queues continuously instead of parking.
    #[arg(long)]
    pub continuous_spin: bool,
    /// Poll DART continuously, yielding bounded turns to other IO tasks.
    #[arg(long)]
    pub continuous_io_spin: bool,
    /// Retained send and private receive slots per DART peer.
    #[arg(long, default_value_t = 256)]
    pub window_messages: usize,
    #[arg(long, default_value = "lan", value_parser = ["lan", "adaptive"])]
    pub congestion: String,
    #[arg(long, default_value = "owned", value_parser = ["owned", "current", "current-poll"])]
    pub runtime: String,
    /// Save chronological RTT CSVs after timing; excluded from charts.
    #[arg(long)]
    pub latency_samples: Option<PathBuf>,
    #[arg(long)]
    pub check_gates: bool,
}

#[derive(Args)]
pub(crate) struct AeronArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long)]
    pub jar: PathBuf,
    #[arg(long)]
    pub classes: PathBuf,
}

#[derive(Args)]
pub(crate) struct QuinnArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long)]
    pub binary: PathBuf,
    #[arg(long, default_value_t = 3.0)]
    pub duration: f64,
    #[arg(long, default_value_t = 100_000)]
    pub iterations: u64,
    #[arg(long, default_value_t = 20_000)]
    pub warmup: u64,
    #[arg(long, default_value = "inline", value_parser = ["inline", "split", "multi"])]
    pub layout: String,
    #[arg(long, default_value_t = 61)]
    pub event_interval: u64,
    #[arg(long, default_value_t = 0)]
    pub app_spin: u64,
    #[arg(long, default_value_t = 0)]
    pub io_spin: u64,
    #[arg(long, default_value_t = 256)]
    pub batch: u64,
}
