use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, ProducerRun,
    ResponderRun, check_every, clean_paths, measure_receive, run_paths, spawn_producer,
    spawn_responder, stop_requested, wait_for_marker, write_marker,
};

fn rpc_topics(token: &str, size: usize) -> (String, String) {
    (
        format!("omq-bench-rpc-request-{token}-{size}"),
        format!("omq-bench-rpc-response-{token}-{size}"),
    )
}

/// One-partition topic sized for 16 MiB messages and short retention.
fn topic_spec(name: &str) -> NewTopic<'_> {
    NewTopic::new(name, 1, TopicReplication::Fixed(1))
        .set("cleanup.policy", "delete")
        .set("retention.ms", "30000")
        .set("retention.bytes", "67108864")
        .set("segment.bytes", "16777216")
        .set("max.message.bytes", "16777216")
}

fn admin_client(url: &str) -> Result<AdminClient<DefaultClientContext>> {
    Ok(ClientConfig::new().set("bootstrap.servers", url).create()?)
}

/// Consumer that starts at the earliest offset and never commits.
fn stream_consumer(url: &str, group: String) -> Result<StreamConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.message.max.bytes", "16777216")
        .set("max.partition.fetch.bytes", "16777216")
        .create()?)
}

/// Unbatched producer for request/reply traffic.
fn rpc_producer(url: &str) -> Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("message.max.bytes", "16777216")
        .set("linger.ms", "0")
        .create()?)
}

pub(crate) fn producer(url: &str, run: &ProducerRun<'_>) -> Result<f64> {
    use rdkafka::producer::Producer;

    let topic = format!("omq-bench-rust-{}-{}", run.token, run.size);
    let payload = vec![b'x'; run.size];
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", url)
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "65536")
        .set("message.max.bytes", "16777216")
        .set("batch.size", "1048576")
        .set("linger.ms", "0")
        .create()?;
    let check_every = check_every(run.size);
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(run.warmup);
    loop {
        let record = FutureRecord::<(), _>::to(&topic).payload(payload.as_slice());
        match producer.send_result(record) {
            Ok(_) => sent += 1,
            Err((_err, _record)) => producer.poll(Duration::from_millis(1)),
        }
        if sent.is_multiple_of(check_every) {
            producer.poll(Duration::ZERO);
            cpu.sample_start()?;
            if stop_requested(run.stop_file, sent, check_every) {
                break;
            }
        }
    }
    producer.flush(Duration::from_secs(10))?;
    cpu.finish()
}

/// Receives until `deadline`, checking payload sizes. Returns the count.
async fn receive_until(consumer: &StreamConsumer, deadline: Instant, size: usize) -> Result<u64> {
    use rdkafka::Message;

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
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let topic = format!("omq-bench-rust-{token}-{size}");
    let admin = admin_client(&args.kafka_url)?;
    admin
        .create_topics(&[topic_spec(&topic)], &AdminOptions::new())
        .await
        .context("create Kafka topic")?;

    let consumer = stream_consumer(&args.kafka_url, format!("omq-bench-rust-{token}"))?;
    consumer.subscribe(&[&topic])?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let mut producer = spawn_producer(args, "kafka", size, token, files)?;
    write_marker(files.start)?;

    // Warmup deliveries are checked, not counted.
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    receive_until(&consumer, warmup_deadline, size).await?;

    let result = measure_receive(args, files, &mut producer, "kafka", |deadline| async move {
        receive_until(&consumer, deadline, size).await
    })
    .await?;
    let _ = admin.delete_topics(&[&topic], &AdminOptions::new()).await;
    Ok(result)
}

pub(crate) async fn responder(url: &str, run: &ResponderRun<'_>) -> Result<()> {
    use rdkafka::Message;

    let (request_topic, response_topic) = rpc_topics(run.token, run.size);
    let consumer = stream_consumer(url, format!("omq-bench-rpc-responder-{}", run.token))?;
    consumer.subscribe(&[&request_topic])?;
    let producer = rpc_producer(url)?;
    write_marker(run.ready_file)?;

    loop {
        let message = consumer.recv().await?;
        let payload = message.payload().context("Kafka request payload")?;
        if payload.len() != run.size {
            bail!("bad Kafka request payload size");
        }
        producer
            .send_result(FutureRecord::<(), _>::to(&response_topic).payload(payload))
            .map_err(|(error, _)| error)?;
    }
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let (request_topic, response_topic) = rpc_topics(token, size);
    let admin = admin_client(&args.kafka_url)?;
    admin
        .create_topics(
            &[topic_spec(&request_topic), topic_spec(&response_topic)],
            &AdminOptions::new(),
        )
        .await
        .context("create Kafka RPC topics")?;

    let producer = rpc_producer(&args.kafka_url)?;
    let consumer = stream_consumer(&args.kafka_url, format!("omq-bench-rpc-requester-{token}"))?;
    consumer.subscribe(&[&response_topic])?;

    let files = ProducerFiles::new(&paths);
    let responder = spawn_responder(args, "kafka", size, token, files)?;
    let mut meter = LatencyMeter::new("kafka", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let payload = vec![b'x'; size];

    for _ in 0..args.latency_warmup {
        kafka_roundtrip(&producer, &consumer, &request_topic, size, &payload).await?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        kafka_roundtrip(&producer, &consumer, &request_topic, size, &payload).await?;
        meter.record(start.elapsed())?;
    }
    let result = meter.finish()?;
    let _ = admin
        .delete_topics(
            &[request_topic.as_str(), response_topic.as_str()],
            &AdminOptions::new(),
        )
        .await;
    Ok(result)
}

async fn kafka_roundtrip(
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    request_topic: &str,
    size: usize,
    payload: &[u8],
) -> Result<()> {
    use rdkafka::Message;

    producer
        .send_result(FutureRecord::<(), _>::to(request_topic).payload(payload))
        .map_err(|(error, _)| error)?;
    let message = tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await??;
    let response = message.payload().context("Kafka response payload")?;
    if response.len() != size {
        bail!("bad Kafka response payload size");
    }
    Ok(())
}
