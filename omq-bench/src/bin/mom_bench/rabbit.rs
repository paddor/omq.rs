use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use futures_util::StreamExt;
use lapin::options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer, Queue};

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, ProducerRun,
    ResponderRun, clean_paths, measure_receive, run_paths, spawn_producer, spawn_responder,
    stop_requested, wait_for_marker, write_marker,
};

fn queue_name(token: &str, size: usize) -> String {
    format!("omq-bench-rust-{token}-{size}")
}

/// Declares a transient queue that the broker drops with this connection.
/// An empty name lets the broker pick one.
async fn declare_queue(channel: &Channel, name: &str) -> Result<Queue> {
    let options = QueueDeclareOptions {
        durable: false,
        exclusive: true,
        auto_delete: true,
        ..QueueDeclareOptions::default()
    };
    Ok(channel
        .queue_declare(name.into(), options, FieldTable::default())
        .await?)
}

/// Starts a consumer without acknowledgements.
async fn consume(channel: &Channel, queue: &str, tag: &str) -> Result<Consumer> {
    let options = BasicConsumeOptions {
        no_ack: true,
        ..BasicConsumeOptions::default()
    };
    Ok(channel
        .basic_consume(queue.into(), tag.into(), options, FieldTable::default())
        .await?)
}

pub(crate) async fn producer(url: &str, run: &ProducerRun<'_>) -> Result<f64> {
    let queue = ShortString::from(queue_name(run.token, run.size));
    let payload = vec![b'x'; run.size];
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(run.warmup);
    loop {
        let confirm = ch
            .basic_publish(
                ShortString::default(),
                queue.clone(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(1),
            )
            .await?;
        confirm.await?;
        sent += 1;
        cpu.sample_start()?;
        if stop_requested(run.stop_file, sent, 1) {
            break;
        }
    }
    conn.close(0, "done".into()).await?;
    cpu.finish()
}

/// Receives until `deadline`, checking payload sizes. Returns the count.
async fn receive_until(consumer: &mut Consumer, deadline: Instant, size: usize) -> Result<u64> {
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
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    use lapin::options::QueueDeleteOptions;

    let queue = queue_name(token, size);
    let conn = Connection::connect(&args.rabbitmq_url, ConnectionProperties::default()).await?;
    let consume_ch = conn.create_channel().await?;
    declare_queue(&consume_ch, &queue).await?;
    let mut consumer = consume(&consume_ch, &queue, "omq-bench-rust").await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let mut producer = spawn_producer(args, "rabbitmq", size, token, files)?;
    write_marker(files.start)?;

    // Warmup deliveries are checked, not counted.
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    receive_until(&mut consumer, warmup_deadline, size).await?;

    let result = measure_receive(
        args,
        files,
        &mut producer,
        "rabbitmq",
        |deadline| async move { receive_until(&mut consumer, deadline, size).await },
    )
    .await?;
    let _ = consume_ch
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    conn.close(0, "done".into()).await?;
    Ok(result)
}

/// Echoes `run.iterations` requests to their reply-to queues.
async fn echo_requests(
    channel: &Channel,
    consumer: &mut Consumer,
    run: &ResponderRun<'_>,
) -> Result<()> {
    for _ in 0..run.iterations {
        let delivery = consumer
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("RabbitMQ request consumer closed"))??;
        if delivery.data.len() != run.size {
            bail!("bad RabbitMQ request payload size");
        }
        let reply_to = delivery
            .properties
            .reply_to()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("RabbitMQ request missing reply-to"))?;
        let properties = delivery
            .properties
            .correlation_id()
            .as_ref()
            .map_or_else(BasicProperties::default, |id| {
                BasicProperties::default().with_correlation_id(id.clone())
            });
        let _confirm = channel
            .basic_publish(
                ShortString::default(),
                reply_to,
                BasicPublishOptions::default(),
                &delivery.data,
                properties,
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn responder(url: &str, run: &ResponderRun<'_>) -> Result<()> {
    let queue = queue_name(run.token, run.size);
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    declare_queue(&channel, &queue).await?;
    let mut consumer = consume(&channel, &queue, "omq-bench-rpc-responder").await?;
    write_marker(run.ready_file)?;

    echo_requests(&channel, &mut consumer, run).await?;
    while !run.stop_file.exists() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    conn.close(0, "done".into()).await?;
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let conn = Connection::connect(&args.rabbitmq_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    let reply_queue = declare_queue(&channel, "").await?.name().clone();
    let replies = consume(&channel, reply_queue.as_str(), "omq-bench-rpc-requester").await?;
    let mut rpc = Rpc {
        channel: &channel,
        replies,
        queue: queue_name(token, size).into(),
        reply_queue,
        correlation_id: token,
    };

    let files = ProducerFiles::new(&paths);
    let responder = spawn_responder(args, "rabbitmq", size, token, files)?;
    let mut meter = LatencyMeter::new("rabbitmq", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let payload = vec![b'x'; size];

    for _ in 0..args.latency_warmup {
        rpc.roundtrip(&payload).await?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        rpc.roundtrip(&payload).await?;
        meter.record(start.elapsed())?;
    }
    let result = meter.finish_gracefully(&paths.1).await?;
    conn.close(0, "done".into()).await?;
    Ok(result)
}

/// Requester side of one RPC queue pair.
struct Rpc<'a> {
    channel: &'a Channel,
    replies: Consumer,
    queue: ShortString,
    reply_queue: ShortString,
    correlation_id: &'a str,
}

impl Rpc<'_> {
    async fn roundtrip(&mut self, payload: &[u8]) -> Result<()> {
        let properties = BasicProperties::default()
            .with_reply_to(self.reply_queue.clone())
            .with_correlation_id(self.correlation_id.into());
        let _confirm = self
            .channel
            .basic_publish(
                ShortString::default(),
                self.queue.clone(),
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await?;

        let delivery = tokio::time::timeout(Duration::from_secs(5), self.replies.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("RabbitMQ reply consumer closed"))??;
        if delivery.data.len() != payload.len() {
            bail!("bad RabbitMQ response payload size");
        }
        if delivery
            .properties
            .correlation_id()
            .as_ref()
            .map(ShortString::as_str)
            != Some(self.correlation_id)
        {
            bail!("bad RabbitMQ response correlation ID");
        }
        Ok(())
    }
}
