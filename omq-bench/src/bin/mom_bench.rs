use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use serde_json::{Value, json};

#[path = "mom_bench/grpc.rs"]
pub mod grpc;
#[path = "mom_bench/kafka.rs"]
mod kafka;
#[path = "mom_bench/nats.rs"]
mod nats;
#[path = "mom_bench/rabbit.rs"]
mod rabbit;
#[path = "mom_bench/redis.rs"]
mod redis;

const CHART_SIZES: &[usize] = &[
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 262_144, 4_194_304, 8_388_608,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Role {
    Coordinator,
    Producer,
}

#[derive(Parser)]
struct Args {
    #[arg(long = "impl", required = true)]
    pub(crate) impls: Vec<String>,

    #[arg(long, default_value_t = default_sizes())]
    pub(crate) sizes: String,

    #[arg(long, default_value_t = 3.0)]
    pub(crate) duration: f64,

    #[arg(long, default_value_t = 1.0)]
    pub(crate) warmup: f64,

    #[arg(long, default_value = "mom-rust-20260901")]
    pub(crate) run_id: String,

    #[arg(long, value_enum, default_value_t = Role::Coordinator)]
    pub(crate) role: Role,

    #[arg(long)]
    pub(crate) token: Option<String>,

    #[arg(long)]
    pub(crate) start_file: Option<PathBuf>,

    #[arg(long)]
    pub(crate) stop_file: Option<PathBuf>,

    #[arg(long)]
    pub(crate) result_file: Option<PathBuf>,

    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    pub(crate) nats_url: String,

    #[arg(long, default_value = "amqp://guest:guest@127.0.0.1:5672/%2f")]
    pub(crate) rabbitmq_url: String,

    #[arg(long, default_value = "redis://127.0.0.1:6379/0")]
    pub(crate) redis_url: String,

    #[arg(long, default_value = "127.0.0.1:19092")]
    pub(crate) kafka_url: String,

    #[arg(long)]
    pub(crate) grpc_port_file: Option<PathBuf>,
}

pub(crate) struct BenchResult {
    pub(crate) count: u64,
    pub(crate) elapsed: f64,
    pub(crate) pull_cpu_time: f64,
    pub(crate) push_cpu_time: f64,
    pub(crate) broker_cpu_time: Option<f64>,
}

pub(crate) struct CpuWindow {
    start_at: Instant,
    cpu_start: Option<f64>,
}

#[derive(Clone, Copy)]
pub(crate) struct ProducerFiles<'a> {
    pub(crate) start: &'a Path,
    pub(crate) stop: &'a Path,
    pub(crate) result: &'a Path,
    pub(crate) grpc_port: Option<&'a Path>,
}

impl CpuWindow {
    pub(crate) fn new(warmup: Duration) -> Self {
        Self {
            start_at: Instant::now() + warmup,
            cpu_start: None,
        }
    }

    pub(crate) fn sample_start(&mut self) -> Result<()> {
        if self.cpu_start.is_none() && Instant::now() >= self.start_at {
            self.cpu_start = Some(self_cpu_secs()?);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<f64> {
        let start = self.cpu_start.unwrap_or(self_cpu_secs()?);
        Ok(self_cpu_secs()? - start)
    }
}

fn default_sizes() -> String {
    CHART_SIZES
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn cache_path() -> PathBuf {
    let base = std::env::var_os("XDG_CACHE_HOME").map_or_else(
        || {
            let home = std::env::var_os("HOME").expect("HOME set");
            PathBuf::from(home).join(".cache")
        },
        PathBuf::from,
    );
    base.join("omq").join("comparisons.jsonl")
}

fn append_row(run_id: &str, impl_name: &str, size: usize, r: &BenchResult) -> Result<()> {
    let path = cache_path();
    std::fs::create_dir_all(path.parent().expect("cache parent"))?;
    let msg_size = u64::try_from(size)?;
    let count_f = r.count as f64;
    let row = json!({
        "run_id": run_id,
        "impl": impl_name,
        "kind": "throughput",
        "transport": "tcp",
        "msg_size": msg_size,
        "msgs_s": count_f / r.elapsed,
        "mbps": count_f * msg_size as f64 / r.elapsed / 1_000_000.0,
        "elapsed": r.elapsed,
        "push_cpu_time": r.push_cpu_time,
        "pull_cpu_time": r.pull_cpu_time,
        "broker_cpu_time": r.broker_cpu_time
    });
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{row}")?;
    f.sync_data()?;
    Ok(())
}

pub(crate) fn write_push_cpu(path: &Path, cpu_time: f64) -> Result<()> {
    std::fs::write(path, json!({ "push_cpu_time": cpu_time }).to_string())?;
    Ok(())
}

pub(crate) fn read_push_cpu(path: &Path) -> Result<f64> {
    let value: Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    value
        .get("push_cpu_time")
        .and_then(Value::as_f64)
        .context("producer result missing push_cpu_time")
}

fn wait_for_file(path: &Path) {
    while !path.exists() {
        std::thread::sleep(Duration::from_millis(1));
    }
}

pub(crate) fn write_marker(path: &Path) -> Result<()> {
    std::fs::write(path, b"1")?;
    Ok(())
}

pub(crate) fn stop_requested(stop_file: &Path, sent: u64, check_every: u64) -> bool {
    sent.is_multiple_of(check_every) && stop_file.exists()
}

pub(crate) fn check_every(size: usize) -> u64 {
    u64::try_from((1024 * 1024 / size.max(1)).clamp(1, 1024)).unwrap()
}

fn ticks_per_second() -> f64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as f64 } else { 100.0 }
}

pub(crate) fn process_cpu_secs(pid: u32) -> Result<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat.rfind(") ").context("bad proc stat")? + 2;
    let fields: Vec<&str> = stat[end..].split_whitespace().collect();
    let utime: f64 = fields.get(11).context("missing utime")?.parse()?;
    let stime: f64 = fields.get(12).context("missing stime")?.parse()?;
    Ok((utime + stime) / ticks_per_second())
}

pub(crate) fn self_cpu_secs() -> Result<f64> {
    process_cpu_secs(std::process::id())
}

fn broker_container(impl_name: &str) -> Option<&'static str> {
    match impl_name {
        "nats" => Some("omq-bench-nats"),
        "rabbitmq" => Some("omq-bench-rabbit"),
        "kafka" => Some("omq-bench-redpanda"),
        "redis-streams" => Some("omq-bench-redis"),
        _ => None,
    }
}

pub(crate) fn broker_pid(impl_name: &str) -> Option<u32> {
    let container = broker_container(impl_name)?;
    let output = Command::new("podman")
        .args(["inspect", "--format", "{{.State.Pid}}", container])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    text.trim().parse().ok().filter(|pid| *pid > 0)
}

pub(crate) fn spawn_producer(
    args: &Args,
    impl_name: &str,
    size: usize,
    token: &str,
    files: ProducerFiles<'_>,
) -> Result<std::process::Child> {
    let exe = std::env::current_exe()?;
    let mut cmd = Command::new(exe);
    cmd.arg("--role")
        .arg("producer")
        .arg("--impl")
        .arg(impl_name)
        .arg("--sizes")
        .arg(size.to_string())
        .arg("--warmup")
        .arg(args.warmup.to_string())
        .arg("--duration")
        .arg(args.duration.to_string())
        .arg("--token")
        .arg(token)
        .arg("--start-file")
        .arg(files.start)
        .arg("--stop-file")
        .arg(files.stop)
        .arg("--result-file")
        .arg(files.result)
        .arg("--nats-url")
        .arg(&args.nats_url)
        .arg("--rabbitmq-url")
        .arg(&args.rabbitmq_url)
        .arg("--redis-url")
        .arg(&args.redis_url)
        .arg("--kafka-url")
        .arg(&args.kafka_url)
        .stdout(Stdio::null());
    if let Some(path) = files.grpc_port {
        cmd.arg("--grpc-port-file").arg(path);
    }
    cmd.spawn().context("spawn producer")
}

async fn run_producer(args: &Args) -> Result<()> {
    let impl_name = args.impls.first().context("impl missing")?;
    let size = args.sizes.parse::<usize>()?;
    let token = args.token.as_deref().context("token missing")?;
    let start_file = args.start_file.as_deref().context("start file missing")?;
    let stop_file = args.stop_file.as_deref().context("stop file missing")?;
    let result_file = args.result_file.as_deref().context("result file missing")?;
    if impl_name == "grpc-rust" {
        let port_file = args
            .grpc_port_file
            .as_deref()
            .context("gRPC port file missing")?;
        return grpc::producer(size, start_file, port_file).await;
    }
    wait_for_file(start_file);
    let warmup = Duration::from_secs_f64(args.warmup);
    let cpu_time = match impl_name.as_str() {
        "nats" => nats::producer(&args.nats_url, token, size, warmup, stop_file).await?,
        "rabbitmq" => rabbit::producer(&args.rabbitmq_url, token, size, warmup, stop_file).await?,
        "kafka" => kafka::producer(&args.kafka_url, token, size, warmup, stop_file)?,
        "redis-streams" => redis::producer(&args.redis_url, token, size, warmup, stop_file)?,
        other => bail!("unknown impl {other}"),
    };
    write_push_cpu(result_file, cpu_time)
}

pub(crate) async fn measure_receive<F, Fut>(
    args: &Args,
    stop_file: &Path,
    result_file: &Path,
    producer: &mut std::process::Child,
    impl_name: &str,
    receive: F,
) -> Result<BenchResult>
where
    F: FnOnce(Instant) -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let duration = Duration::from_secs_f64(args.duration);
    let broker_start_pid = broker_pid(impl_name);
    let broker_start = broker_start_pid.and_then(|pid| process_cpu_secs(pid).ok());
    let pull_start = self_cpu_secs()?;
    let start = Instant::now();
    let deadline = start + duration;
    let count = receive(deadline).await?;
    let elapsed = start.elapsed().as_secs_f64();
    let pull_cpu_time = self_cpu_secs()? - pull_start;
    let broker_cpu_time = broker_start_pid
        .zip(broker_start)
        .and_then(|(pid, start)| process_cpu_secs(pid).ok().map(|end| end - start));

    write_marker(stop_file)?;
    let status = producer.wait()?;
    if !status.success() {
        bail!("producer failed: {status}");
    }
    let push_cpu_time = read_push_cpu(result_file)?;

    Ok(BenchResult {
        count,
        elapsed,
        pull_cpu_time,
        push_cpu_time,
        broker_cpu_time,
    })
}

pub(crate) fn run_paths(token: &str, size: usize) -> (PathBuf, PathBuf, PathBuf) {
    let base = std::env::temp_dir();
    (
        base.join(format!("omq-mom-{token}-{size}.start")),
        base.join(format!("omq-mom-{token}-{size}.stop")),
        base.join(format!("omq-mom-{token}-{size}.json")),
    )
}

pub(crate) fn clean_paths(paths: &(PathBuf, PathBuf, PathBuf)) -> Result<()> {
    for path in [&paths.0, &paths.1, &paths.2] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.role == Role::Producer {
        return run_producer(&args).await;
    }

    let sizes = args
        .sizes
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::parse::<usize>)
        .collect::<Result<Vec<_>, _>>()?;
    for impl_name in &args.impls {
        for &size in &sizes {
            let token = format!(
                "{}-{}",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
                uuid::Uuid::new_v4().simple()
            );
            let result = match impl_name.as_str() {
                "nats" => nats::bench(&args, &token, size).await?,
                "grpc-rust" => grpc::bench(&args, &token, size).await?,
                "rabbitmq" => rabbit::bench(&args, &token, size).await?,
                "kafka" => kafka::bench(&args, &token, size).await?,
                "redis-streams" => redis::bench(&args, &token, size).await?,
                other => bail!("unknown impl {other}"),
            };
            append_row(&args.run_id, impl_name, size, &result)?;
            let msgs_s = result.count as f64 / result.elapsed;
            let mbps = result.count as f64 * size as f64 / result.elapsed / 1_000_000.0;
            let broker_cpu = result.broker_cpu_time.map_or(String::new(), |v| {
                format!(" broker_cpu={:.0}%", v / result.elapsed * 100.0)
            });
            println!(
                "{impl_name:13} {size:8} B  {msgs_s:12.0} msg/s  {mbps:10.1} MB/s  snd_cpu={:.0}%{} rcv_cpu={:.0}%",
                result.push_cpu_time / result.elapsed * 100.0,
                broker_cpu,
                result.pull_cpu_time / result.elapsed * 100.0
            );
        }
    }
    Ok(())
}
