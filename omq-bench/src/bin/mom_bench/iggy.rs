use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, DirectConfig, Identifier, IggyClient, IggyConsumer, IggyExpiry,
    IggyMessage, IggyProducer, MaxTopicSize, MessageClient, Partitioning, PollingStrategy,
    StreamClient,
};

use super::{
    Args, BenchResult, CpuWindow, IggyCommit, LatencyMeter, LatencyResult, ProducerFiles,
    ProducerRun, ResponderRun, clean_paths, measure_receive, run_paths, spawn_producer,
    spawn_responder, stop_requested, wait_for_marker, workload, write_marker,
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

/// Consumer of one partition from offset 0, without auto commit or a poll
/// interval.
fn offset_consumer(
    client: &IggyClient,
    group: &str,
    stream: &str,
    topic: &str,
    length: usize,
) -> Result<IggyConsumer> {
    Ok(client
        .consumer(group, stream, topic, PARTITION_ID)?
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(u32::try_from(length)?)
        .auto_commit(AutoCommit::Disabled)
        .without_poll_interval()
        .build())
}

/// Direct producer of single-message batches to an existing topic.
fn single_producer(client: &IggyClient, stream: &str, topic: &str) -> Result<IggyProducer> {
    Ok(client
        .producer(stream, topic)?
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .build())
}

pub(crate) async fn producer(url: &str, run: &ProducerRun<'_>, commit: IggyCommit) -> Result<f64> {
    let stream = stream_name(run.token, run.size);
    let client = connect(url).await?;
    let length = batch_length(run.size);
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

    let batches = make_corpus(run.size, length)?;
    let stream_id = Identifier::try_from(stream.as_str())?;
    let topic_id = Identifier::try_from(TOPIC)?;
    let mut next_batch = 0;
    let check_every = u64::try_from(length)?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(run.warmup);
    loop {
        producer.send(make_messages(&batches[next_batch])?).await?;
        commit_if_requested(&client, &stream_id, &topic_id, commit).await?;
        next_batch = (next_batch + 1) % batches.len();
        sent += check_every;
        cpu.sample_start()?;
        if stop_requested(run.stop_file, sent, check_every) {
            break;
        }
    }
    producer.shutdown().await;
    client.shutdown().await?;
    cpu.finish()
}

/// Creates the throughput stream and topic through a short-lived producer.
async fn create_stream(client: &IggyClient, stream: &str, length: usize) -> Result<()> {
    let setup = client
        .producer(stream, TOPIC)?
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
    Ok(())
}

/// Receives until `deadline`, checking sizes and contiguous offsets.
/// Returns the count.
async fn receive_until(
    consumer: &mut IggyConsumer,
    deadline: Instant,
    size: usize,
    next_offset: &mut u64,
) -> Result<u64> {
    let mut count = 0_u64;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.next()).await {
            Ok(Some(Ok(message))) => {
                check_record(&message.message, size, next_offset)?;
                count += 1;
            }
            Ok(Some(Err(err))) => bail!(err),
            Ok(None) | Err(_) => break,
        }
    }
    Ok(count)
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let stream = stream_name(token, size);
    let client = connect(&args.iggy_url).await?;
    let length = batch_length(size);
    create_stream(&client, &stream, length).await?;
    let mut consumer = offset_consumer(&client, token, &stream, TOPIC, length)?;
    consumer.init().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let mut producer = spawn_producer(args, "iggy", size, token, files)?;
    write_marker(files.start)?;

    // Warmup deliveries are checked, not counted; offsets carry over.
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    let mut next_offset = 0;
    receive_until(&mut consumer, warmup_deadline, size, &mut next_offset).await?;

    let result = measure_receive(args, files, &mut producer, "iggy", |deadline| async move {
        receive_until(&mut consumer, deadline, size, &mut next_offset).await
    })
    .await?;

    client.delete_stream(&Identifier::try_from(stream)?).await?;
    client.shutdown().await?;
    Ok(result)
}

pub(crate) async fn responder(url: &str, run: &ResponderRun<'_>, commit: IggyCommit) -> Result<()> {
    let stream = stream_name(run.token, run.size);
    let client = connect(url).await?;
    let producer = single_producer(&client, &stream, RESPONSE_TOPIC)?;
    producer.init().await?;
    let mut consumer = offset_consumer(&client, run.token, &stream, REQUEST_TOPIC, 1)?;
    consumer.init().await?;
    let stream_id = Identifier::try_from(stream.as_str())?;
    let response_topic_id = Identifier::try_from(RESPONSE_TOPIC)?;
    write_marker(run.ready_file)?;

    for _ in 0..run.iterations {
        let message = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy request consumer closed"))??;
        if message.message.payload.len() != run.size {
            bail!("bad Iggy request payload size");
        }
        producer
            .send(vec![make_message(message.message.payload.clone())?])
            .await?;
        commit_if_requested(&client, &stream_id, &response_topic_id, commit).await?;
    }
    // Keep the process alive for the final CPU snapshot, without issuing a
    // poll that would be canceled while shutting down its TCP connection.
    while !run.stop_file.exists() {
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

/// Requester side of the request/response topic pair.
struct Rpc<'a> {
    client: &'a IggyClient,
    producer: IggyProducer,
    consumer: IggyConsumer,
    stream_id: Identifier,
    request_topic_id: Identifier,
    commit: IggyCommit,
    size: usize,
}

impl Rpc<'_> {
    async fn roundtrip(&mut self, request: Bytes) -> Result<()> {
        self.producer.send(vec![make_message(request)?]).await?;
        commit_if_requested(
            self.client,
            &self.stream_id,
            &self.request_topic_id,
            self.commit,
        )
        .await?;
        let response = tokio::time::timeout(Duration::from_secs(5), self.consumer.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Iggy response consumer closed"))??;
        if response.message.payload.len() != self.size {
            bail!("bad Iggy response payload size");
        }
        Ok(())
    }
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let stream = stream_name(token, size);
    let client = connect(&args.iggy_url).await?;
    setup_latency_topics(&client, &stream).await?;

    let producer = single_producer(&client, &stream, REQUEST_TOPIC)?;
    producer.init().await?;
    let mut consumer = offset_consumer(&client, token, &stream, RESPONSE_TOPIC, 1)?;
    consumer.init().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let responder = spawn_responder(args, "iggy", size, token, files)?;
    let mut meter = LatencyMeter::new("iggy", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let requests = (0..workload::LATENCY_CORPUS_RECORDS)
        .map(|sequence| workload::record(size, sequence as u64))
        .collect::<Vec<_>>();
    let mut rpc = Rpc {
        client: &client,
        producer,
        consumer,
        stream_id: Identifier::try_from(stream.as_str())?,
        request_topic_id: Identifier::try_from(REQUEST_TOPIC)?,
        commit: args.iggy_commit,
        size,
    };
    let mut next_request = 0;

    for _ in 0..args.latency_warmup {
        rpc.roundtrip(requests[next_request].clone()).await?;
        next_request = (next_request + 1) % requests.len();
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        rpc.roundtrip(requests[next_request].clone()).await?;
        next_request = (next_request + 1) % requests.len();
        meter.record(start.elapsed())?;
    }

    let result = meter.finish_gracefully(&paths.1).await?;
    rpc.producer.shutdown().await;
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
