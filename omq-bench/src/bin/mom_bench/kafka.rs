use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::{
    Args, BenchResult, CpuWindow, ProducerFiles, check_every, clean_paths, measure_receive,
    run_paths, spawn_producer, stop_requested, write_marker,
};

pub(crate) fn producer(
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

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
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
    let mut producer = spawn_producer(
        args,
        "kafka",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: None,
        },
    )?;
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
