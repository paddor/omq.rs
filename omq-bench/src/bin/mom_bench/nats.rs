use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, ProducerRun,
    ResponderRun, check_every, clean_paths, measure_receive, run_paths, spawn_producer,
    spawn_responder, stop_requested, wait_for_marker, write_marker,
};

fn subject(token: &str, size: usize) -> String {
    format!("omq.bench.rust.{token}.{size}")
}

pub(crate) async fn producer(url: &str, run: &ProducerRun<'_>) -> Result<f64> {
    let subject = subject(run.token, run.size);
    let payload = Bytes::from(vec![b'x'; run.size]);
    let client = async_nats::connect(url).await?;
    let check_every = check_every(run.size);
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(run.warmup);
    loop {
        client.publish(subject.clone(), payload.clone()).await?;
        sent += 1;
        if sent.is_multiple_of(check_every) {
            client.flush().await?;
            cpu.sample_start()?;
            if stop_requested(run.stop_file, sent, check_every) {
                break;
            }
        }
    }
    client.flush().await?;
    cpu.finish()
}

/// Receives until `deadline`, checking payload sizes. Returns the count.
async fn receive_until(
    sub: &mut async_nats::Subscriber,
    deadline: Instant,
    size: usize,
) -> Result<u64> {
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
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let subject = subject(token, size);
    let client = async_nats::connect(&args.nats_url).await?;
    let mut sub = client.subscribe(subject).await?;
    client.flush().await?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let mut producer = spawn_producer(args, "nats", size, token, files)?;
    write_marker(files.start)?;

    // Warmup deliveries are checked, not counted.
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    receive_until(&mut sub, warmup_deadline, size).await?;

    measure_receive(args, files, &mut producer, "nats", |deadline| async move {
        receive_until(&mut sub, deadline, size).await
    })
    .await
}

pub(crate) async fn responder(url: &str, run: &ResponderRun<'_>) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let mut subscription = client.subscribe(subject(run.token, run.size)).await?;
    client.flush().await?;
    write_marker(run.ready_file)?;

    while let Some(message) = subscription.next().await {
        if message.payload.len() != run.size {
            bail!("bad NATS request payload size");
        }
        let reply = message
            .reply
            .context("NATS request missing reply subject")?;
        client.publish(reply, message.payload).await?;
    }
    Ok(())
}

async fn roundtrip(
    client: &async_nats::Client,
    subject: &str,
    payload: &Bytes,
    size: usize,
) -> Result<()> {
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request(subject.to_owned(), payload.clone()),
    )
    .await??;
    if response.payload.len() != size {
        bail!("bad NATS response payload size");
    }
    Ok(())
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let client = async_nats::connect(&args.nats_url).await?;
    let subject = subject(token, size);
    let payload = Bytes::from(vec![b'x'; size]);
    let responder = spawn_responder(args, "nats", size, token, ProducerFiles::new(&paths))?;
    let mut meter = LatencyMeter::new("nats", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;

    for _ in 0..args.latency_warmup {
        roundtrip(&client, &subject, &payload, size).await?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        roundtrip(&client, &subject, &payload, size).await?;
        meter.record(start.elapsed())?;
    }
    meter.finish()
}
