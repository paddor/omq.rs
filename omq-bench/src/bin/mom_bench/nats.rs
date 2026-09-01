use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use futures_util::StreamExt;

use super::{
    Args, BenchResult, CpuWindow, ProducerFiles, check_every, clean_paths, measure_receive,
    run_paths, spawn_producer, stop_requested, write_marker,
};

pub(crate) async fn producer(
    url: &str,
    token: &str,
    size: usize,
    warmup: Duration,
    stop_file: &Path,
) -> Result<f64> {
    let subject = format!("omq.bench.rust.{token}.{size}");
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
    let subject = format!("omq.bench.rust.{token}.{size}");
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
