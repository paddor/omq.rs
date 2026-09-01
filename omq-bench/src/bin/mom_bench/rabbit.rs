use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use futures_util::StreamExt;

use super::{
    Args, BenchResult, CpuWindow, ProducerFiles, clean_paths, measure_receive, run_paths,
    spawn_producer, stop_requested, write_marker,
};

pub(crate) async fn producer(
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

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
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
