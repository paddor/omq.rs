use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use serde_json::{Value, json};

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
    impls: Vec<String>,

    #[arg(long, default_value_t = default_sizes())]
    sizes: String,

    #[arg(long, default_value_t = 3.0)]
    duration: f64,

    #[arg(long, default_value_t = 1.0)]
    warmup: f64,

    #[arg(long, default_value = "mom-rust-20260901")]
    run_id: String,

    #[arg(long, value_enum, default_value_t = Role::Coordinator)]
    role: Role,

    #[arg(long)]
    token: Option<String>,

    #[arg(long)]
    start_file: Option<PathBuf>,

    #[arg(long)]
    stop_file: Option<PathBuf>,

    #[arg(long)]
    result_file: Option<PathBuf>,

    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, default_value = "amqp://guest:guest@127.0.0.1:5672/%2f")]
    rabbitmq_url: String,

    #[arg(long, default_value = "redis://127.0.0.1:6379/0")]
    redis_url: String,

    #[arg(long, default_value = "127.0.0.1:19092")]
    kafka_url: String,
}

struct BenchResult {
    count: u64,
    elapsed: f64,
    pull_cpu_time: f64,
    push_cpu_time: f64,
    broker_cpu_time: Option<f64>,
}

struct CpuWindow {
    start_at: Instant,
    cpu_start: Option<f64>,
}

impl CpuWindow {
    fn new(warmup: Duration) -> Self {
        Self {
            start_at: Instant::now() + warmup,
            cpu_start: None,
        }
    }

    fn sample_start(&mut self) -> Result<()> {
        if self.cpu_start.is_none() && Instant::now() >= self.start_at {
            self.cpu_start = Some(self_cpu_secs()?);
        }
        Ok(())
    }

    fn finish(self) -> Result<f64> {
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

fn write_push_cpu(path: &Path, cpu_time: f64) -> Result<()> {
    std::fs::write(path, json!({ "push_cpu_time": cpu_time }).to_string())?;
    Ok(())
}

fn read_push_cpu(path: &Path) -> Result<f64> {
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

fn write_marker(path: &Path) -> Result<()> {
    std::fs::write(path, b"1")?;
    Ok(())
}

fn stop_requested(stop_file: &Path, sent: u64, check_every: u64) -> bool {
    sent.is_multiple_of(check_every) && stop_file.exists()
}

fn check_every(size: usize) -> u64 {
    u64::try_from((1024 * 1024 / size.max(1)).clamp(1, 1024)).unwrap()
}

fn ticks_per_second() -> f64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as f64 } else { 100.0 }
}

fn process_cpu_secs(pid: u32) -> Result<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat.rfind(") ").context("bad proc stat")? + 2;
    let fields: Vec<&str> = stat[end..].split_whitespace().collect();
    let utime: f64 = fields.get(11).context("missing utime")?.parse()?;
    let stime: f64 = fields.get(12).context("missing stime")?.parse()?;
    Ok((utime + stime) / ticks_per_second())
}

fn self_cpu_secs() -> Result<f64> {
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

fn broker_pid(impl_name: &str) -> Option<u32> {
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

fn spawn_producer(
    args: &Args,
    impl_name: &str,
    size: usize,
    token: &str,
    start_file: &Path,
    stop_file: &Path,
    result_file: &Path,
) -> Result<std::process::Child> {
    let exe = std::env::current_exe()?;
    Command::new(exe)
        .arg("--role")
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
        .arg(start_file)
        .arg("--stop-file")
        .arg(stop_file)
        .arg("--result-file")
        .arg(result_file)
        .arg("--nats-url")
        .arg(&args.nats_url)
        .arg("--rabbitmq-url")
        .arg(&args.rabbitmq_url)
        .arg("--redis-url")
        .arg(&args.redis_url)
        .arg("--kafka-url")
        .arg(&args.kafka_url)
        .stdout(Stdio::null())
        .spawn()
        .context("spawn producer")
}

async fn run_producer(args: &Args) -> Result<()> {
    let impl_name = args.impls.first().context("impl missing")?;
    let size = args.sizes.parse::<usize>()?;
    let token = args.token.as_deref().context("token missing")?;
    let start_file = args.start_file.as_deref().context("start file missing")?;
    let stop_file = args.stop_file.as_deref().context("stop file missing")?;
    let result_file = args.result_file.as_deref().context("result file missing")?;
    wait_for_file(start_file);
    let warmup = Duration::from_secs_f64(args.warmup);
    let cpu_time = match impl_name.as_str() {
        "nats" => producer_nats(&args.nats_url, token, size, warmup, stop_file).await?,
        "rabbitmq" => producer_rabbit(&args.rabbitmq_url, token, size, warmup, stop_file).await?,
        "kafka" => producer_kafka(&args.kafka_url, token, size, warmup, stop_file)?,
        "redis-streams" => producer_redis(&args.redis_url, token, size, warmup, stop_file)?,
        other => bail!("unknown impl {other}"),
    };
    write_push_cpu(result_file, cpu_time)
}

async fn producer_nats(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    let subject = format!("omq.bench.rust.{token}.{size}");
    let payload = Bytes::from(vec![b'x'; size]);
    let client = async_nats::connect(url).await?;
    let check_every = check_every(size);
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        client.publish(subject.clone(), payload.clone()).await?;
        sent += 1;
        if sent.is_multiple_of(check_every) {
            client.flush().await?;
            cpu.sample_start()?;
            if stop_requested(stop_file, sent, check_every) {
                break;
            }
        }
    }
    client.flush().await?;
    cpu.finish()
}

async fn producer_rabbit(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    use lapin::{BasicProperties, Connection, ConnectionProperties, options::BasicPublishOptions};

    let queue = format!("omq-bench-rust-{token}-{size}");
    let payload = vec![b'x'; size];
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        let confirm = ch
            .basic_publish(
                "",
                &queue,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(1),
            )
            .await?;
        confirm.await?;
        sent += 1;
        cpu.sample_start()?;
        if stop_requested(stop_file, sent, 1) {
            break;
        }
    }
    conn.close(0, "done").await?;
    cpu.finish()
}

fn producer_kafka(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    use rdkafka::{
        ClientConfig,
        producer::{FutureProducer, FutureRecord, Producer},
    };

    let topic = format!("omq-bench-rust-{token}-{size}");
    let payload = vec![b'x'; size];
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "65536")
        .set("message.max.bytes", "16777216")
        .set("batch.size", "1048576")
        .set("linger.ms", "0")
        .create()?;
    let check_every = check_every(size);
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        let record = FutureRecord::<(), _>::to(&topic).payload(payload.as_slice());
        match producer.send_result(record) {
            Ok(_) => sent += 1,
            Err((_err, _record)) => producer.poll(Duration::from_millis(1)),
        }
        if sent.is_multiple_of(check_every) {
            producer.poll(Duration::ZERO);
            cpu.sample_start()?;
            if stop_requested(stop_file, sent, check_every) {
                break;
            }
        }
    }
    producer.flush(Duration::from_secs(10))?;
    cpu.finish()
}

fn producer_redis(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    let key = format!("omq:bench:rust:{token}:{size}");
    let payload = vec![b'x'; size];
    let client = redis::Client::open(url)?;
    let mut conn = client.get_connection()?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("MAXLEN")
            .arg("~")
            .arg(100_000)
            .arg("*")
            .arg("d")
            .arg(payload.as_slice())
            .query(&mut conn)?;
        sent += 1;
        cpu.sample_start()?;
        if stop_requested(stop_file, sent, 1) {
            break;
        }
    }
    cpu.finish()
}

async fn bench_nats(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let subject = format!("omq.bench.rust.{token}.{size}");
    let client = async_nats::connect(&args.nats_url).await?;
    let mut sub = client.subscribe(subject).await?;
    client.flush().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(args, "nats", size, token, &paths.0, &paths.1, &paths.2)?;
    write_marker(&paths.0)?;

    let warmup = Duration::from_secs_f64(args.warmup);
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, sub.next()).await {
            Ok(Some(msg)) => {
                if msg.payload.len() != size {
                    bail!("bad NATS payload size");
                }
            }
            Ok(None) | Err(_) => break,
        }
    }

    measure_receive(
        args,
        &paths.1,
        &paths.2,
        &mut producer,
        "nats",
        |deadline| async move {
            let mut count = 0_u64;
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, sub.next()).await {
                    Ok(Some(msg)) => {
                        if msg.payload.len() != size {
                            bail!("bad NATS payload size");
                        }
                        count += 1;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            Ok(count)
        },
    )
    .await
}

async fn bench_rabbit(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    use lapin::{
        Connection, ConnectionProperties,
        options::{BasicConsumeOptions, QueueDeclareOptions, QueueDeleteOptions},
        types::FieldTable,
    };

    let queue = format!("omq-bench-rust-{token}-{size}");
    let conn = Connection::connect(&args.rabbitmq_url, ConnectionProperties::default()).await?;
    let consume_ch = conn.create_channel().await?;
    consume_ch
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                durable: false,
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;
    let mut consumer = consume_ch
        .basic_consume(
            &queue,
            "omq-bench-rust",
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(args, "rabbitmq", size, token, &paths.0, &paths.1, &paths.2)?;
    write_marker(&paths.0)?;

    let warmup = Duration::from_secs_f64(args.warmup);
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.next()).await {
            Ok(Some(delivery)) => {
                let delivery = delivery?;
                if delivery.data.len() != size {
                    bail!("bad RabbitMQ payload size");
                }
            }
            Ok(None) | Err(_) => break,
        }
    }

    let result = measure_receive(
        args,
        &paths.1,
        &paths.2,
        &mut producer,
        "rabbitmq",
        |deadline| async move {
            let mut count = 0_u64;
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, consumer.next()).await {
                    Ok(Some(delivery)) => {
                        let delivery = delivery?;
                        if delivery.data.len() != size {
                            bail!("bad RabbitMQ payload size");
                        }
                        count += 1;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            Ok(count)
        },
    )
    .await?;
    let _ = consume_ch
        .queue_delete(&queue, QueueDeleteOptions::default())
        .await;
    conn.close(0, "done").await?;
    Ok(result)
}

async fn bench_kafka(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    use rdkafka::{
        ClientConfig, Message,
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        consumer::{Consumer, StreamConsumer},
    };

    let topic = format!("omq-bench-rust-{token}-{size}");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_url)
        .create()?;
    let new_topic = NewTopic::new(&topic, 1, TopicReplication::Fixed(1))
        .set("cleanup.policy", "delete")
        .set("retention.ms", "30000")
        .set("retention.bytes", "67108864")
        .set("segment.bytes", "16777216")
        .set("max.message.bytes", "16777216");
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .context("create Kafka topic")?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_url)
        .set("group.id", format!("omq-bench-rust-{token}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.message.max.bytes", "16777216")
        .set("max.partition.fetch.bytes", "16777216")
        .create()?;
    consumer.subscribe(&[&topic])?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(args, "kafka", size, token, &paths.0, &paths.1, &paths.2)?;
    write_marker(&paths.0)?;

    let warmup = Duration::from_secs_f64(args.warmup);
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let payload = msg.payload().context("Kafka message payload")?;
                if payload.len() != size {
                    bail!("bad Kafka payload size");
                }
            }
            Ok(Err(err)) => bail!(err),
            Err(_) => break,
        }
    }

    let result = measure_receive(
        args,
        &paths.1,
        &paths.2,
        &mut producer,
        "kafka",
        |deadline| async move {
            let mut count = 0_u64;
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, consumer.recv()).await {
                    Ok(Ok(msg)) => {
                        let payload = msg.payload().context("Kafka message payload")?;
                        if payload.len() != size {
                            bail!("bad Kafka payload size");
                        }
                        count += 1;
                    }
                    Ok(Err(err)) => bail!(err),
                    Err(_) => break,
                }
            }
            Ok(count)
        },
    )
    .await?;
    let _ = admin.delete_topics(&[&topic], &AdminOptions::new()).await;
    Ok(result)
}

async fn bench_redis(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let key = format!("omq:bench:rust:{token}:{size}");
    let client = redis::Client::open(args.redis_url.as_str())?;
    let mut conn = client.get_connection()?;
    let _: () = redis::cmd("DEL").arg(&key).query(&mut conn)?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(
        args,
        "redis-streams",
        size,
        token,
        &paths.0,
        &paths.1,
        &paths.2,
    )?;
    write_marker(&paths.0)?;

    let warmup = Duration::from_secs_f64(args.warmup);
    let duration = Duration::from_secs_f64(args.duration);
    let broker_pid_for_measure = broker_pid("redis-streams");
    let (count, elapsed, pull_cpu_time, broker_cpu_time) =
        tokio::task::spawn_blocking(move || -> Result<_> {
            let mut last_id = "$".to_string();
            let warmup_deadline = Instant::now() + warmup;
            while Instant::now() < warmup_deadline {
                let value: redis::Value = redis::cmd("XREAD")
                    .arg("COUNT")
                    .arg(1024)
                    .arg("BLOCK")
                    .arg(50)
                    .arg("STREAMS")
                    .arg(&key)
                    .arg(&last_id)
                    .query(&mut conn)?;
                for (entry_id, body) in parse_redis_xread(value)? {
                    if body.len() != size {
                        bail!("bad Redis payload size");
                    }
                    last_id = entry_id;
                }
            }

            let broker_start = broker_pid_for_measure.and_then(|pid| process_cpu_secs(pid).ok());
            let pull_start = self_cpu_secs()?;
            let start = Instant::now();
            let deadline = start + duration;
            let mut count = 0_u64;
            while Instant::now() < deadline {
                let value: redis::Value = redis::cmd("XREAD")
                    .arg("COUNT")
                    .arg(1024)
                    .arg("BLOCK")
                    .arg(50)
                    .arg("STREAMS")
                    .arg(&key)
                    .arg(&last_id)
                    .query(&mut conn)?;
                for (entry_id, body) in parse_redis_xread(value)? {
                    if body.len() != size {
                        bail!("bad Redis payload size");
                    }
                    last_id = entry_id;
                    count += 1;
                }
            }
            let elapsed = start.elapsed().as_secs_f64();
            let pull_cpu_time = self_cpu_secs()? - pull_start;
            let broker_cpu_time = broker_pid_for_measure
                .zip(broker_start)
                .and_then(|(pid, start)| process_cpu_secs(pid).ok().map(|end| end - start));
            let _: () = redis::cmd("DEL").arg(&key).query(&mut conn)?;
            Ok((count, elapsed, pull_cpu_time, broker_cpu_time))
        })
        .await??;
    write_marker(&paths.1)?;
    let status = producer.wait()?;
    if !status.success() {
        bail!("producer failed: {status}");
    }
    let push_cpu_time = read_push_cpu(&paths.2)?;
    Ok(BenchResult {
        count,
        elapsed,
        pull_cpu_time,
        push_cpu_time,
        broker_cpu_time,
    })
}

async fn measure_receive<F, Fut>(
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

fn run_paths(token: &str, size: usize) -> (PathBuf, PathBuf, PathBuf) {
    let base = std::env::temp_dir();
    (
        base.join(format!("omq-mom-{token}-{size}.start")),
        base.join(format!("omq-mom-{token}-{size}.stop")),
        base.join(format!("omq-mom-{token}-{size}.json")),
    )
}

fn clean_paths(paths: &(PathBuf, PathBuf, PathBuf)) -> Result<()> {
    for path in [&paths.0, &paths.1, &paths.2] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

fn parse_redis_xread(value: redis::Value) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let redis::Value::Array(streams) = value else {
        return Ok(out);
    };
    for stream in streams {
        let redis::Value::Array(stream) = stream else {
            continue;
        };
        if stream.len() != 2 {
            continue;
        }
        let redis::Value::Array(entries) = &stream[1] else {
            continue;
        };
        for entry in entries {
            let redis::Value::Array(entry) = entry else {
                continue;
            };
            if entry.len() != 2 {
                continue;
            }
            let entry_id = match &entry[0] {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone())?,
                redis::Value::SimpleString(s) => s.clone(),
                _ => continue,
            };
            let redis::Value::Array(fields) = &entry[1] else {
                continue;
            };
            let mut i = 0;
            while i + 1 < fields.len() {
                let is_data = match &fields[i] {
                    redis::Value::BulkString(bytes) => bytes == b"d",
                    redis::Value::SimpleString(s) => s == "d",
                    _ => false,
                };
                if is_data && let redis::Value::BulkString(bytes) = &fields[i + 1] {
                    out.push((entry_id.clone(), bytes.clone()));
                }
                i += 2;
            }
        }
    }
    Ok(out)
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
                "nats" => bench_nats(&args, &token, size).await?,
                "rabbitmq" => bench_rabbit(&args, &token, size).await?,
                "kafka" => bench_kafka(&args, &token, size).await?,
                "redis-streams" => bench_redis(&args, &token, size).await?,
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
