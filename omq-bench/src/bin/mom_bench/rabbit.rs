use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use futures_util::StreamExt;

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, clean_paths,
    measure_receive, run_paths, spawn_producer, spawn_responder, stop_requested, wait_for_marker,
    write_marker,
};

fn queue_name(token: &str, size: usize) -> String {
    format!("omq-bench-rust-{token}-{size}")
}

pub(crate) async fn producer(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    use lapin::{BasicProperties, Connection, ConnectionProperties, options::BasicPublishOptions};

    let queue = queue_name(token, size);
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

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    use lapin::{
        Connection, ConnectionProperties,
        options::{BasicConsumeOptions, QueueDeclareOptions, QueueDeleteOptions},
        types::FieldTable,
    };

    let queue = queue_name(token, size);
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
    let mut producer = spawn_producer(
        args,
        "rabbitmq",
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

pub(crate) async fn responder(
    url: &str,
    token: &str,
    size: usize,
    ready_file: &Path,
) -> Result<()> {
    use lapin::{
        BasicProperties, Connection, ConnectionProperties,
        options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
        types::FieldTable,
    };

    let queue = queue_name(token, size);
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    channel
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
    let mut consumer = channel
        .basic_consume(
            &queue,
            "omq-bench-rpc-responder",
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await?;
    write_marker(ready_file)?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        if delivery.data.len() != size {
            bail!("bad RabbitMQ request payload size");
        }
        let reply_to = delivery
            .properties
            .reply_to()
            .as_ref()
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
                "",
                reply_to.as_str(),
                BasicPublishOptions::default(),
                &delivery.data,
                properties,
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    use lapin::{
        Connection, ConnectionProperties,
        options::{BasicConsumeOptions, QueueDeclareOptions},
        types::FieldTable,
    };

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let queue = queue_name(token, size);
    let conn = Connection::connect(&args.rabbitmq_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    let reply_queue = channel
        .queue_declare(
            "",
            QueueDeclareOptions {
                durable: false,
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?
        .name()
        .to_string();
    let mut replies = channel
        .basic_consume(
            &reply_queue,
            "omq-bench-rpc-requester",
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    let responder = spawn_responder(
        args,
        "rabbitmq",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: None,
        },
    )?;
    let mut meter = LatencyMeter::new("rabbitmq", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let payload = vec![b'x'; size];

    for _ in 0..args.latency_warmup {
        rabbit_roundtrip(
            &channel,
            &mut replies,
            &queue,
            &reply_queue,
            token,
            &payload,
        )
        .await?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        rabbit_roundtrip(
            &channel,
            &mut replies,
            &queue,
            &reply_queue,
            token,
            &payload,
        )
        .await?;
        meter.record(start.elapsed())?;
    }
    let result = meter.finish()?;
    conn.close(0, "done").await?;
    Ok(result)
}

async fn rabbit_roundtrip(
    channel: &lapin::Channel,
    replies: &mut lapin::Consumer,
    queue: &str,
    reply_queue: &str,
    correlation_id: &str,
    payload: &[u8],
) -> Result<()> {
    use lapin::{BasicProperties, options::BasicPublishOptions};

    let properties = BasicProperties::default()
        .with_reply_to(reply_queue.into())
        .with_correlation_id(correlation_id.into());
    let _confirm = channel
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            payload,
            properties,
        )
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(5), replies.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("RabbitMQ reply consumer closed"))??;
    if delivery.data.len() != payload.len() {
        bail!("bad RabbitMQ response payload size");
    }
    if delivery
        .properties
        .correlation_id()
        .as_ref()
        .map(lapin::types::ShortString::as_str)
        != Some(correlation_id)
    {
        bail!("bad RabbitMQ response correlation ID");
    }
    Ok(())
}
