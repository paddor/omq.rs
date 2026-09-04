use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, DirectConfig, Identifier, IggyClient, IggyExpiry, IggyMessage,
    MaxTopicSize, Partitioning, PollingStrategy, StreamClient,
};

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, clean_paths,
    measure_receive, run_paths, spawn_producer, spawn_responder, stop_requested, wait_for_marker,
    write_marker,
};

const TOPIC: &str = "messages";
const REQUEST_TOPIC: &str = "requests";
const RESPONSE_TOPIC: &str = "responses";
const PARTITION_ID: u32 = 0;
const MAX_BATCH_LENGTH: usize = 1000;
const MAX_BATCH_BYTES: usize = 1024 * 1024;

fn stream_name(token: &str, size: usize) -> String {
    format!("omq-bench-rust-{token}-{size}")
}

fn batch_length(size: usize) -> usize {
    (MAX_BATCH_BYTES / size.max(1)).clamp(1, MAX_BATCH_LENGTH)
}

fn make_batch(payload: &Bytes, length: usize) -> Result<Vec<IggyMessage>> {
    (0..length)
        .map(|_| {
            IggyMessage::builder()
                .payload(payload.clone())
                .build()
                .map_err(Into::into)
        })
        .collect()
}

async fn connect(url: &str) -> Result<IggyClient> {
    let client = IggyClient::from_connection_string(url)?;
    client.connect().await?;
    Ok(client)
}

pub(crate) async fn producer(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    let stream = stream_name(token, size);
    let client = connect(url).await?;
    let length = batch_length(size);
    let producer = client
        .producer(&stream, TOPIC)?
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(
            DirectConfig::builder()
                .batch_length(u32::try_from(length)?)
                .build(),
        )
        .build();
    producer.init().await?;

    let payload = Bytes::from(vec![b'x'; size]);
    let check_every = u64::try_from(length)?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        producer.send(make_batch(&payload, length)?).await?;
        sent += check_every;
        cpu.sample_start()?;
        if stop_requested(stop_file, sent, check_every) {
            break;
        }
    }
    producer.shutdown().await;
    client.shutdown().await?;
    cpu.finish()
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let stream = stream_name(token, size);
    let client = connect(&args.iggy_url).await?;
    let length = batch_length(size);

    let setup = client
        .producer(&stream, TOPIC)?
        .create_stream_if_not_exists()
        .create_topic_if_not_exists(
            1,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(
            DirectConfig::builder()
                .batch_length(u32::try_from(length)?)
                .build(),
        )
        .build();
    setup.init().await?;
    setup.shutdown().await;

    let mut consumer = client
        .consumer(token, &stream, TOPIC, PARTITION_ID)?
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(u32::try_from(length)?)
        .auto_commit(AutoCommit::Disabled)
        .without_poll_interval()
        .build();
    consumer.init().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(
        args,
        "iggy",
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

    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.next()).await {
            Ok(Some(Ok(message))) => {
                if message.message.payload.len() != size {
                    bail!("bad Iggy payload size");
                }
            }
            Ok(Some(Err(err))) => bail!(err),
            Ok(None) | Err(_) => break,
        }
    }

    let result = measure_receive(
        args,
        &paths.1,
        &paths.2,
        &mut producer,
        "iggy",
        |deadline| async move {
            let mut count = 0_u64;
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, consumer.next()).await {
                    Ok(Some(Ok(message))) => {
                        if message.message.payload.len() != size {
                            bail!("bad Iggy payload size");
                        }
                        count += 1;
                    }
                    Ok(Some(Err(err))) => bail!(err),
                    Ok(None) | Err(_) => break,
                }
            }
            Ok(count)
        },
    )
    .await?;

    client.delete_stream(&Identifier::try_from(stream)?).await?;
    client.shutdown().await?;
    Ok(result)
}

pub(crate) async fn responder(
    url: &str,
    token: &str,
    size: usize,
    ready_file: &Path,
) -> Result<()> {
    let stream = stream_name(token, size);
    let client = connect(url).await?;
    let producer = client
        .producer(&stream, RESPONSE_TOPIC)?
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    producer.init().await?;
    let mut consumer = client
        .consumer(token, &stream, REQUEST_TOPIC, PARTITION_ID)?
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(1)
        .auto_commit(AutoCommit::Disabled)
        .without_poll_interval()
        .build();
    consumer.init().await?;
    write_marker(ready_file)?;

    while let Some(message) = consumer.next().await {
        let message = message?;
        if message.message.payload.len() != size {
            bail!("bad Iggy request payload size");
        }
        producer
            .send(make_batch(&message.message.payload, 1)?)
            .await?;
    }
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let stream = stream_name(token, size);
    let client = connect(&args.iggy_url).await?;

    let request_setup = client
        .producer(&stream, REQUEST_TOPIC)?
        .create_stream_if_not_exists()
        .create_topic_if_not_exists(
            1,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    request_setup.init().await?;
    request_setup.shutdown().await;
    let response_setup = client
        .producer(&stream, RESPONSE_TOPIC)?
        .do_not_create_stream_if_not_exists()
        .create_topic_if_not_exists(
            1,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    response_setup.init().await?;
    response_setup.shutdown().await;

    let producer = client
        .producer(&stream, REQUEST_TOPIC)?
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    producer.init().await?;
    let mut consumer = client
        .consumer(token, &stream, RESPONSE_TOPIC, PARTITION_ID)?
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(1)
        .auto_commit(AutoCommit::Disabled)
        .without_poll_interval()
        .build();
    consumer.init().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let responder = spawn_responder(
        args,
        "iggy",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: None,
        },
    )?;
    let mut meter = LatencyMeter::new("iggy", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let payload = Bytes::from(vec![b'x'; size]);

    for _ in 0..args.latency_warmup {
        producer.send(make_batch(&payload, 1)?).await?;
        let response = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy response consumer closed"))??;
        if response.message.payload.len() != size {
            bail!("bad Iggy response payload size");
        }
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        producer.send(make_batch(&payload, 1)?).await?;
        let response = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy response consumer closed"))??;
        if response.message.payload.len() != size {
            bail!("bad Iggy response payload size");
        }
        meter.record(start.elapsed())?;
    }

    let result = meter.finish()?;
    producer.shutdown().await;
    client.delete_stream(&Identifier::try_from(stream)?).await?;
    client.shutdown().await?;
    Ok(result)
}
