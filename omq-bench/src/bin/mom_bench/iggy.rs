use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, DirectConfig, Identifier, IggyClient, IggyExpiry, IggyMessage,
    MaxTopicSize, MessageClient, Partitioning, PollingStrategy, StreamClient,
};

use super::{
    Args, BenchResult, CpuWindow, IggyCommit, LatencyMeter, LatencyResult, ProducerFiles,
    clean_paths, measure_receive, run_paths, spawn_producer, spawn_responder, stop_requested,
    wait_for_marker, workload, write_marker,
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

fn make_message(payload: Bytes) -> Result<IggyMessage> {
    IggyMessage::builder()
        .payload(payload)
        .build()
        .map_err(Into::into)
}

fn make_batch(size: usize, length: usize, first_sequence: u64) -> Vec<Bytes> {
    (0..length)
        .map(|index| {
            let sequence = first_sequence + index as u64;
            workload::record(size, sequence)
        })
        .collect()
}

fn make_messages(payloads: &[Bytes]) -> Result<Vec<IggyMessage>> {
    payloads
        .iter()
        .cloned()
        .map(make_message)
        .collect::<Result<Vec<_>>>()
}

fn check_record(message: &IggyMessage, size: usize, next_offset: &mut u64) -> Result<()> {
    if message.payload.len() != size {
        bail!("bad Iggy payload size");
    }
    if message.header.offset != *next_offset {
        bail!(
            "non-contiguous Iggy delivery: expected {}, got {}",
            next_offset,
            message.header.offset
        );
    }
    *next_offset = next_offset
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("Iggy delivery offset exhausted"))?;
    Ok(())
}

fn make_corpus(size: usize, length: usize) -> Result<Vec<Vec<Bytes>>> {
    (0..workload::THROUGHPUT_CORPUS_BATCHES)
        .map(|batch_index| {
            let first_sequence = u64::try_from(batch_index.saturating_mul(length))?;
            Ok(make_batch(size, length, first_sequence))
        })
        .collect()
}

async fn commit_if_requested(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    commit: IggyCommit,
) -> Result<()> {
    match commit {
        IggyCommit::Accepted | IggyCommit::BufferedInline | IggyCommit::FsyncInline => Ok(()),
        IggyCommit::Buffered | IggyCommit::Fsync => client
            .flush_unsaved_buffer(stream, topic, PARTITION_ID, commit == IggyCommit::Fsync)
            .await
            .map_err(Into::into),
    }
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
    commit: IggyCommit,
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

    let batches = make_corpus(size, length)?;
    let stream_id = Identifier::try_from(stream.as_str())?;
    let topic_id = Identifier::try_from(TOPIC)?;
    let mut next_batch = 0;
    let check_every = u64::try_from(length)?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(warmup);
    loop {
        producer.send(make_messages(&batches[next_batch])?).await?;
        commit_if_requested(&client, &stream_id, &topic_id, commit).await?;
        next_batch = (next_batch + 1) % batches.len();
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
    let mut next_offset = 0;
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.next()).await {
            Ok(Some(Ok(message))) => {
                check_record(&message.message, size, &mut next_offset)?;
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
                        check_record(&message.message, size, &mut next_offset)?;
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
    stop_file: &Path,
    iterations: u64,
    commit: IggyCommit,
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
    let stream_id = Identifier::try_from(stream.as_str())?;
    let response_topic_id = Identifier::try_from(RESPONSE_TOPIC)?;
    write_marker(ready_file)?;

    for _ in 0..iterations {
        let message = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy request consumer closed"))??;
        if message.message.payload.len() != size {
            bail!("bad Iggy request payload size");
        }
        producer
            .send(vec![make_message(message.message.payload.clone())?])
            .await?;
        commit_if_requested(&client, &stream_id, &response_topic_id, commit).await?;
    }
    // Keep the process alive for the final CPU snapshot, without issuing a
    // poll that would be canceled while shutting down its TCP connection.
    while !stop_file.exists() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    producer.shutdown().await;
    client.shutdown().await?;
    Ok(())
}

async fn setup_latency_topics(client: &IggyClient, stream: &str) -> Result<()> {
    let request = client
        .producer(stream, REQUEST_TOPIC)?
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
    request.init().await?;
    request.shutdown().await;
    let response = client
        .producer(stream, RESPONSE_TOPIC)?
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
    response.init().await?;
    response.shutdown().await;
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let stream = stream_name(token, size);
    let client = connect(&args.iggy_url).await?;
    setup_latency_topics(&client, &stream).await?;

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
    let requests = (0..workload::LATENCY_CORPUS_RECORDS)
        .map(|sequence| workload::record(size, sequence as u64))
        .collect::<Vec<_>>();
    let stream_id = Identifier::try_from(stream.as_str())?;
    let request_topic_id = Identifier::try_from(REQUEST_TOPIC)?;
    let mut next_request = 0;

    for _ in 0..args.latency_warmup {
        producer
            .send(vec![make_message(requests[next_request].clone())?])
            .await?;
        commit_if_requested(&client, &stream_id, &request_topic_id, args.iggy_commit).await?;
        next_request = (next_request + 1) % requests.len();
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
        producer
            .send(vec![make_message(requests[next_request].clone())?])
            .await?;
        commit_if_requested(&client, &stream_id, &request_topic_id, args.iggy_commit).await?;
        next_request = (next_request + 1) % requests.len();
        let response = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy response consumer closed"))??;
        if response.message.payload.len() != size {
            bail!("bad Iggy response payload size");
        }
        meter.record(start.elapsed())?;
    }

    let result = meter.finish_gracefully(&paths.1).await?;
    producer.shutdown().await;
    client.delete_stream(&Identifier::try_from(stream)?).await?;
    client.shutdown().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_validation_rejects_gaps_duplicates_and_wrong_size() {
        let mut message = make_message(Bytes::from_static(b"record")).unwrap();
        let mut next_offset = 0;
        message.header.offset = 0;
        check_record(&message, 6, &mut next_offset).unwrap();
        assert_eq!(next_offset, 1);
        assert!(check_record(&message, 6, &mut next_offset).is_err());
        message.header.offset = 2;
        assert!(check_record(&message, 6, &mut next_offset).is_err());
        message.header.offset = 1;
        assert!(check_record(&message, 7, &mut next_offset).is_err());
        assert_eq!(next_offset, 1);
        check_record(&message, 6, &mut next_offset).unwrap();
        assert_eq!(next_offset, 2);
    }
}
