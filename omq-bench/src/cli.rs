use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "omq_bench", about = "OMQ benchmark runner")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
#[expect(clippy::large_enum_variant)]
pub(crate) enum Command {
    /// Run benchmarks.
    Run {
        #[command(subcommand)]
        sub: RunSub,
    },
    /// Generate charts from cached JSONL data.
    Chart {
        #[command(subcommand)]
        sub: Option<ChartSub>,
    },
}

#[derive(Subcommand)]
pub(crate) enum ChartSub {
    /// Local RADIO/DISH, SCATTER/GATHER, and CLIENT/SERVER transport comparisons.
    Shm {
        /// Refresh SHM series while retaining cached inproc/IPC baselines.
        #[arg(long)]
        cached_baselines: bool,
    },
    /// Main TCP overview charts.
    Main,
    /// Per-transport PUSH/PULL and REQ/REP charts.
    Comparison,
    /// PUB/SUB multi-panel and CURVE charts.
    Pubsub,
    /// Fan-out and fan-in charts.
    Fanio,
    /// OMQ over QUIC against OMQ over TCP.
    Quic,
    /// DART against TCP, SCATTER/GATHER, CLIENT/SERVER, and RADIO/DISH.
    Dart,
    /// Dart and QUIC throughput and RTT over simulated lossy links.
    Lossy,
    /// PUSH/PULL LZ4 and Zstd charts: saturated links and fixed send rates.
    Compression,
    /// PUSH/PULL over a 1 Gbit/s link with an 800 Mbit/s egress limit.
    RateLimit,
}

#[derive(Subcommand)]
pub(crate) enum RunSub {
    /// Cross-implementation comparisons.
    Comparisons(ComparisonsArgs),
    /// PUSH/PULL with LZ4 compression.
    PushpullLz4(PushpullLz4Args),
    /// PUSH/PULL with experimental Zstd compression.
    PushpullZstd(PushpullZstdArgs),
    /// Two-process TCP PUSH/PULL with context byte pacing on a netem link.
    PushpullRateLimit(PushpullRateLimitArgs),
    /// Push/pull compression benchmarks.
    Compression(CompressionArgs),
    /// Pinned, two-process DART/TCP benchmarks.
    Dart(crate::bench::datagram::args::DartArgs),
    /// Aeron 1.53.3 UDP baseline with pinned shared drivers.
    AeronDart(crate::bench::datagram::args::AeronArgs),
    /// Standalone Quinn DATAGRAM API with real TLS.
    QuinnDatagram(crate::bench::datagram::args::QuinnArgs),
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Transport {
    Tcp,
    Ipc,
    Inproc,
    Ws,
    Quic,
}

impl Transport {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Ipc => "ipc",
            Self::Inproc => "inproc",
            Self::Ws => "ws",
            Self::Quic => "quic",
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Parser)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct ComparisonsArgs {
    /// Implementations to benchmark (repeatable).
    #[arg(long = "impl")]
    pub impls: Vec<String>,

    /// Shorthand: omq-tokio-1t + omq-tokio-2t.
    #[arg(long)]
    pub omq: bool,

    /// Transports to test (repeatable).
    #[arg(long, value_enum, default_values_t = [Transport::Tcp, Transport::Inproc, Transport::Ipc])]
    pub transport: Vec<Transport>,

    /// Message sizes (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub sizes: Option<Vec<u64>>,

    /// Allow non-chart sizes.
    #[arg(long)]
    pub allow_non_chart_sizes: bool,

    /// Duration per measurement in seconds.
    #[arg(long)]
    pub duration: Option<f64>,

    /// Measurement rounds. Defaults to 1; keeps the median throughput or p99-latency round.
    #[arg(long)]
    pub rounds: Option<u32>,

    /// Skip throughput measurements.
    #[arg(long)]
    pub no_throughput: bool,

    /// Skip latency measurements.
    #[arg(long)]
    pub no_latency: bool,

    /// Latency socket pairs (comma-separated). libzmq also supports ROUTER and PAIR pairs.
    #[arg(long, value_delimiter = ',', default_value = "req-rep", value_parser = ["req-rep", "router-dealer", "router-router", "pair", "client-server", "peer", "channel"])]
    pub latency_pairs: Vec<String>,

    /// OMQ latency benchmark profiles (comma-separated); default preserves socket defaults.
    #[arg(long, value_delimiter = ',', default_value = "default", value_parser = ["default", "latency", "throughput"])]
    pub latency_profiles: Vec<String>,

    #[command(flatten)]
    pub pubsub: PubsubArgs,

    #[command(flatten)]
    pub latency: LatencyArgs,

    #[command(flatten)]
    pub fanio: FanioArgs,

    #[command(flatten)]
    pub curve: CurveArgs,

    /// Base port for WS transport.
    #[arg(long)]
    pub base_port: Option<u16>,

    /// Run ID suffix.
    #[arg(long)]
    pub id: Option<String>,

    /// Quick run: 3 sizes, 1 round, 1.5s.
    #[arg(long)]
    pub quick_run: bool,
}

/// PUB/SUB comparison options.
#[derive(Args)]
pub(crate) struct PubsubArgs {
    /// Skip pub/sub measurements.
    #[arg(long)]
    pub no_pubsub: bool,

    /// Pub/sub peer counts (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [4, 32])]
    pub pubsub_peers: Vec<u64>,

    /// Fixed IO-thread count for OMQ/libzmq PUB/SUB receivers.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub pubsub_receiver_io_threads: Option<u32>,

    /// Fixed application worker count for blocking OMQ/libzmq PUB/SUB receivers.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub pubsub_receiver_workers: Option<u32>,

    /// Receiver CPU mask for PUB/SUB (requires `OMQ_BENCH_TASKSET`).
    #[arg(long)]
    pub pubsub_receiver_cpus: Option<String>,
}

/// Iterations, warmup, and timeout of every latency cell.
#[derive(Args)]
#[expect(
    clippy::struct_field_names,
    reason = "field names give the --latency-* flags and value names"
)]
pub(crate) struct LatencyArgs {
    /// Latency iterations.
    #[arg(long, default_value_t = 5000)]
    pub latency_iterations: u64,

    /// Latency warmup iterations.
    #[arg(long, default_value_t = 500)]
    pub latency_warmup: u64,

    /// Latency timeout in seconds.
    #[arg(long, default_value_t = 15)]
    pub latency_timeout: u64,
}

/// Fan-out and fan-in comparison options.
#[derive(Args)]
pub(crate) struct FanioArgs {
    /// Enable fan-out benchmarks.
    #[arg(long)]
    pub fanout: bool,

    /// Fan-out peer counts (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [4, 32])]
    pub fanout_peers: Vec<u64>,

    /// Enable fan-in benchmarks.
    #[arg(long)]
    pub fanin: bool,

    /// Fan-in peer counts (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [4, 32])]
    pub fanin_peers: Vec<u64>,
}

/// CURVE comparison options.
#[derive(Args)]
pub(crate) struct CurveArgs {
    /// Enable CURVE benchmarks.
    #[arg(long = "curve")]
    pub enabled: bool,

    /// CURVE pub/sub peer count.
    #[arg(long, default_value_t = 16)]
    pub curve_peers: u64,
}

/// Options shared by the LZ4 and Zstd PUSH/PULL compression benchmarks.
#[derive(Args)]
pub(crate) struct PushpullCompressionArgs {
    #[command(flatten)]
    pub link: CompressionLinkArgs,

    /// Transports (comma-separated).
    #[arg(long, default_value = "tcp,lz4+tcp")]
    pub transports: String,

    /// Message sizes (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub sizes: Option<Vec<u64>>,

    /// Duration per measurement in seconds.
    #[arg(long, default_value_t = 2.0)]
    pub duration: f64,

    /// Measurements per cell (all rounds are retained).
    #[arg(long, default_value_t = 3)]
    pub rounds: u32,

    /// Quick mode: 3 sizes, 1 round, 1.5s.
    #[arg(long)]
    pub quick: bool,

    /// Dict sizes to train (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub dict_sizes: Vec<u64>,
}

#[derive(Parser)]
pub(crate) struct PushpullLz4Args {
    #[command(flatten)]
    pub common: PushpullCompressionArgs,
}

#[derive(Parser)]
// The adaptive level settles within about 1 s.
#[command(mut_arg("warmup_seconds", |arg| arg.default_value("1")))]
#[command(mut_arg("transports", |arg| arg.default_value("zstd+tcp")))]
pub(crate) struct PushpullZstdArgs {
    #[command(flatten)]
    pub common: PushpullCompressionArgs,

    /// Fixed Zstd compression level (-7..=4) instead of `--level-range`.
    #[arg(long)]
    pub level: Option<i32>,

    /// Range the adaptive Zstd level moves within.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "-7,4",
        allow_negative_numbers = true
    )]
    pub level_range: Vec<i32>,
}

#[derive(Parser)]
pub(crate) struct PushpullRateLimitArgs {
    /// Message sizes (comma-separated); defaults to all 15 TCP chart sizes.
    #[arg(long, value_delimiter = ',')]
    pub sizes: Option<Vec<u64>>,

    /// Minimum measured seconds; extended to fit 110 large messages.
    #[arg(long, default_value_t = 3.0)]
    pub duration: f64,

    /// Measurements per cell (all rounds are retained).
    #[arg(long, default_value_t = 3)]
    pub rounds: u32,
}

#[derive(Parser)]
pub(crate) struct CompressionLinkArgs {
    /// Emulate a shared TCP link in a private Linux network namespace.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub link_mbps: Option<u32>,

    /// Netem delay in each direction, in microseconds.
    #[arg(long, default_value_t = 1000)]
    pub link_delay_us: u32,

    /// Active receiver warmup, in seconds.
    #[arg(long, default_value_t = 0.5)]
    pub warmup_seconds: f64,

    /// Pace the sender at this many messages per second over plain loopback.
    #[arg(long, conflicts_with = "link_mbps", value_parser = clap::value_parser!(u64).range(1..))]
    pub send_rate: Option<u64>,
}

#[derive(Parser)]
pub(crate) struct CompressionArgs {
    /// Transports (comma-separated).
    #[arg(long, default_value = "tcp,lz4+tcp")]
    pub transports: String,

    /// Message sizes (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub sizes: Option<Vec<u64>>,

    /// Duration per measurement in seconds.
    #[arg(long, default_value_t = 2.0)]
    pub duration: f64,

    /// Dict sizes to train (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [2048])]
    pub dict_sizes: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, RunSub};
    use clap::{CommandFactory, Parser};

    fn run(args: &[&str]) -> RunSub {
        let cli = Cli::try_parse_from([&["omq_bench", "run"], args].concat()).unwrap();
        let Command::Run { sub } = cli.command else {
            panic!("not a run command");
        };
        sub
    }

    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn compression_commands_keep_their_own_defaults() {
        let RunSub::PushpullLz4(lz4) = run(&["pushpull-lz4"]) else {
            panic!("not pushpull-lz4");
        };
        assert_eq!(lz4.common.transports, "tcp,lz4+tcp");
        assert!((lz4.common.link.warmup_seconds - 0.5).abs() < f64::EPSILON);
        let RunSub::PushpullZstd(zstd) = run(&["pushpull-zstd"]) else {
            panic!("not pushpull-zstd");
        };
        assert_eq!(zstd.common.transports, "zstd+tcp");
        assert!((zstd.common.link.warmup_seconds - 1.0).abs() < f64::EPSILON);
        assert_eq!(zstd.level_range, [-7, 4]);
    }

    #[test]
    fn grouped_comparison_flags_keep_their_names() {
        let RunSub::Comparisons(args) = run(&[
            "comparisons",
            "--curve",
            "--curve-peers=8",
            "--no-pubsub",
            "--pubsub-receiver-workers=2",
            "--latency-iterations=10",
            "--fanin",
            "--fanout-peers=2,3",
        ]) else {
            panic!("not comparisons");
        };
        assert!(args.curve.enabled);
        assert_eq!(args.curve.curve_peers, 8);
        assert!(args.pubsub.no_pubsub);
        assert_eq!(args.pubsub.pubsub_receiver_workers, Some(2));
        assert_eq!(args.latency.latency_iterations, 10);
        assert_eq!(args.latency.latency_warmup, 500);
        assert!(args.fanio.fanin && !args.fanio.fanout);
        assert_eq!(args.fanio.fanout_peers, [2, 3]);
    }
}
