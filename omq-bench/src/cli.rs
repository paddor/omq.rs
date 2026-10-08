use clap::{Parser, Subcommand, ValueEnum};

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
    /// DART against TCP, SCATTER/GATHER and CLIENT/SERVER.
    Dart,
    /// Dart and QUIC throughput and RTT over simulated lossy links.
    Lossy,
    /// PUB/SUB LZ4 compression chart.
    Lz4,
    /// PUSH/PULL Zstd compression chart.
    Zstd,
}

#[derive(Subcommand)]
pub(crate) enum RunSub {
    /// Cross-implementation comparisons.
    Comparisons(ComparisonsArgs),
    /// PUSH/PULL with LZ4 compression.
    PushpullLz4(PushpullLz4Args),
    /// PUSH/PULL with experimental Zstd compression.
    PushpullZstd(PushpullZstdArgs),
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

    /// Measurement rounds. Defaults to 1; N>1 keeps the median throughput round.
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

    /// Skip pub/sub measurements.
    #[arg(long)]
    pub no_pubsub: bool,

    /// Pub/sub peer counts (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [4, 32])]
    pub pubsub_peers: Vec<u64>,

    /// Latency iterations.
    #[arg(long, default_value_t = 5000)]
    pub latency_iterations: u64,

    /// Latency warmup iterations.
    #[arg(long, default_value_t = 500)]
    pub latency_warmup: u64,

    /// Latency timeout in seconds.
    #[arg(long, default_value_t = 15)]
    pub latency_timeout: u64,

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

    /// Enable CURVE benchmarks.
    #[arg(long)]
    pub curve: bool,

    /// CURVE pub/sub peer count.
    #[arg(long, default_value_t = 16)]
    pub curve_peers: u64,

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

#[derive(Parser)]
pub(crate) struct PushpullLz4Args {
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
    #[arg(long, value_delimiter = ',', default_values_t = [2048])]
    pub dict_sizes: Vec<u64>,
}

#[derive(Parser)]
pub(crate) struct PushpullZstdArgs {
    #[command(flatten)]
    pub link: CompressionLinkArgs,

    /// Transports (comma-separated).
    #[arg(long, default_value = "tcp,zstd+tcp")]
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
    #[arg(long, value_delimiter = ',', default_values_t = [2048])]
    pub dict_sizes: Vec<u64>,

    /// Zstd compression level (-8..=4). Omitted uses transport default.
    #[arg(long)]
    pub level: Option<i32>,
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
