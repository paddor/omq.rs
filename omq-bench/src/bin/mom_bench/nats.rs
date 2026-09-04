use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, check_every,
    clean_paths, measure_receive, run_paths, spawn_producer, spawn_responder, stop_requested,
    wait_for_marker, write_marker,
};

fn subject(token: &str, size: usize) -> String {
    format!("omq.bench.rust.{token}.{size}")
}

pub(crate) async fn producer(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    let subject = subject(token, size);
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

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let subject = subject(token, size);
    let client = async_nats::connect(&args.nats_url).await?;
    let mut sub = client.subscribe(subject).await?;
    client.flush().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let mut producer = spawn_producer(
        args,
        "nats",
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

pub(crate) async fn responder(
    url: &str,
    token: &str,
    size: usize,
    ready_file: &Path,
) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let mut subscription = client.subscribe(subject(token, size)).await?;
    client.flush().await?;
    write_marker(ready_file)?;

    while let Some(message) = subscription.next().await {
        if message.payload.len() != size {
            bail!("bad NATS request payload size");
        }
        let reply = message
            .reply
            .context("NATS request missing reply subject")?;
        client.publish(reply, message.payload).await?;
    }
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let client = async_nats::connect(&args.nats_url).await?;
    let subject = subject(token, size);
    let payload = Bytes::from(vec![b'x'; size]);
    let responder = spawn_responder(
        args,
        "nats",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: None,
        },
    )?;
    let mut meter = LatencyMeter::new("nats", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;

    for _ in 0..args.latency_warmup {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.request(subject.clone(), payload.clone()),
        )
        .await??;
        if response.payload.len() != size {
            bail!("bad NATS response payload size");
        }
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.request(subject.clone(), payload.clone()),
        )
        .await??;
        if response.payload.len() != size {
            bail!("bad NATS response payload size");
        }
        meter.record(start.elapsed())?;
    }
    meter.finish()
}
