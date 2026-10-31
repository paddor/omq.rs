use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::{
    Args, BenchResult, CpuWindow, LatencyMeter, LatencyResult, ProducerFiles, ProducerRun,
    ResponderRun, broker_pid, clean_paths, process_cpu_secs, run_paths, self_cpu_secs,
    spawn_producer, spawn_responder, stop_producer, stop_requested, wait_for_marker, write_marker,
};

fn rpc_keys(token: &str, size: usize) -> (String, String) {
    (
        format!("omq:bench:rpc:request:{token}:{size}"),
        format!("omq:bench:rpc:response:{token}:{size}"),
    )
}

pub(crate) fn producer(url: &str, run: &ProducerRun<'_>) -> Result<f64> {
    let key = format!("omq:bench:rust:{}:{}", run.token, run.size);
    let payload = vec![b'x'; run.size];
    let client = redis::Client::open(url)?;
    let mut conn = client.get_connection()?;
    let mut sent = 0_u64;
    let mut cpu = CpuWindow::new(run.warmup);
    loop {
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("MAXLEN")
            .arg("~")
            .arg(100_000)
            .arg("*")
            .arg("d")
            .arg(payload.as_slice())
            .query(&mut conn)?;
        sent += 1;
        cpu.sample_start()?;
        if stop_requested(run.stop_file, sent, 1) {
            break;
        }
    }
    cpu.finish()
}

/// Blocking stream reader of one throughput run.
struct StreamReader {
    conn: redis::Connection,
    key: String,
    size: usize,
    last_id: String,
    /// Looked up before the warmup, as `podman inspect` takes a while.
    broker_pid: Option<u32>,
}

/// Counts and CPU times of one measured receive window.
struct ReceiveWindow {
    count: u64,
    elapsed: f64,
    pull_cpu_time: f64,
    broker_cpu_time: Option<f64>,
}

impl StreamReader {
    /// Reads one batch of up to 1024 entries, checking payload sizes.
    /// Returns the number of entries read.
    fn read_batch(&mut self) -> Result<u64> {
        let value: redis::Value = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(1024)
            .arg("BLOCK")
            .arg(50)
            .arg("STREAMS")
            .arg(&self.key)
            .arg(&self.last_id)
            .query(&mut self.conn)?;
        let mut count = 0_u64;
        for (entry_id, body) in parse_xread(value)? {
            if body.len() != self.size {
                bail!("bad Redis payload size");
            }
            self.last_id = entry_id;
            count += 1;
        }
        Ok(count)
    }

    /// Drains the warmup, then measures one window of `duration`.
    fn run(mut self, warmup: Duration, duration: Duration) -> Result<ReceiveWindow> {
        let warmup_deadline = Instant::now() + warmup;
        while Instant::now() < warmup_deadline {
            self.read_batch()?;
        }

        let broker_pid = self.broker_pid;
        let broker_start = broker_pid.and_then(|pid| process_cpu_secs(pid).ok());
        let pull_start = self_cpu_secs()?;
        let start = Instant::now();
        let deadline = start + duration;
        let mut count = 0_u64;
        while Instant::now() < deadline {
            count += self.read_batch()?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let pull_cpu_time = self_cpu_secs()? - pull_start;
        let broker_cpu_time = broker_pid
            .zip(broker_start)
            .and_then(|(pid, start)| process_cpu_secs(pid).ok().map(|end| end - start));

        let _: () = redis::cmd("DEL").arg(&self.key).query(&mut self.conn)?;
        Ok(ReceiveWindow {
            count,
            elapsed,
            pull_cpu_time,
            broker_cpu_time,
        })
    }
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let key = format!("omq:bench:rust:{token}:{size}");
    let client = redis::Client::open(args.redis_url.as_str())?;
    let mut conn = client.get_connection()?;
    let _: () = redis::cmd("DEL").arg(&key).query(&mut conn)?;

    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let files = ProducerFiles::new(&paths);
    let mut producer = spawn_producer(args, "redis-streams", size, token, files)?;
    write_marker(files.start)?;

    let warmup = Duration::from_secs_f64(args.warmup);
    let duration = Duration::from_secs_f64(args.duration);
    let reader = StreamReader {
        conn,
        key,
        size,
        last_id: "$".to_string(),
        broker_pid: broker_pid("redis-streams"),
    };
    let window = tokio::task::spawn_blocking(move || reader.run(warmup, duration)).await??;
    let push_cpu_time = stop_producer(files, &mut producer)?;
    Ok(BenchResult {
        count: window.count,
        elapsed: window.elapsed,
        pull_cpu_time: window.pull_cpu_time,
        push_cpu_time,
        broker_cpu_time: window.broker_cpu_time,
    })
}

fn parse_xread(value: redis::Value) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let redis::Value::Array(streams) = value else {
        return Ok(out);
    };
    for stream in streams {
        let redis::Value::Array(stream) = stream else {
            continue;
        };
        if stream.len() != 2 {
            continue;
        }
        let redis::Value::Array(entries) = &stream[1] else {
            continue;
        };
        for entry in entries {
            let redis::Value::Array(entry) = entry else {
                continue;
            };
            if entry.len() != 2 {
                continue;
            }
            let entry_id = match &entry[0] {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone())?,
                redis::Value::SimpleString(s) => s.clone(),
                _ => continue,
            };
            let redis::Value::Array(fields) = &entry[1] else {
                continue;
            };
            let mut i = 0;
            while i + 1 < fields.len() {
                let is_data = match &fields[i] {
                    redis::Value::BulkString(bytes) => bytes == b"d",
                    redis::Value::SimpleString(s) => s == "d",
                    _ => false,
                };
                if is_data && let redis::Value::BulkString(bytes) = &fields[i + 1] {
                    out.push((entry_id.clone(), bytes.clone()));
                }
                i += 2;
            }
        }
    }
    Ok(out)
}

pub(crate) fn responder(url: &str, run: &ResponderRun<'_>) -> Result<()> {
    let (request_key, response_key) = rpc_keys(run.token, run.size);
    let client = redis::Client::open(url)?;
    let mut conn = client.get_connection()?;
    let mut last_id = "0-0".to_string();
    write_marker(run.ready_file)?;

    loop {
        let (entry_id, payload) = xread_one(&mut conn, &request_key, &last_id)?;
        if payload.len() != run.size {
            bail!("bad Redis request payload size");
        }
        last_id = entry_id;
        let _: String = redis::cmd("XADD")
            .arg(&response_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(10_000)
            .arg("*")
            .arg("d")
            .arg(payload)
            .query(&mut conn)?;
    }
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let (request_key, response_key) = rpc_keys(token, size);
    let client = redis::Client::open(args.redis_url.as_str())?;
    let mut conn = client.get_connection()?;
    let _: () = redis::cmd("DEL")
        .arg(&request_key)
        .arg(&response_key)
        .query(&mut conn)?;
    let files = ProducerFiles::new(&paths);
    let responder = spawn_responder(args, "redis-streams", size, token, files)?;
    let mut meter = LatencyMeter::new("redis-streams", args.latency_iterations, responder)?;
    wait_for_marker(&paths.0, meter.responder_mut()).await?;
    let payload = vec![b'x'; size];
    let mut response_id = "0-0".to_string();

    for _ in 0..args.latency_warmup {
        response_id = redis_roundtrip(
            &mut conn,
            &request_key,
            &response_key,
            &response_id,
            &payload,
        )?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        response_id = redis_roundtrip(
            &mut conn,
            &request_key,
            &response_key,
            &response_id,
            &payload,
        )?;
        meter.record(start.elapsed())?;
    }
    let result = meter.finish()?;
    let _: () = redis::cmd("DEL")
        .arg(&request_key)
        .arg(&response_key)
        .query(&mut conn)?;
    Ok(result)
}

fn redis_roundtrip(
    conn: &mut redis::Connection,
    request_key: &str,
    response_key: &str,
    response_id: &str,
    payload: &[u8],
) -> Result<String> {
    let _: String = redis::cmd("XADD")
        .arg(request_key)
        .arg("MAXLEN")
        .arg("~")
        .arg(10_000)
        .arg("*")
        .arg("d")
        .arg(payload)
        .query(conn)?;
    let (entry_id, response) = xread_one(conn, response_key, response_id)?;
    if response.len() != payload.len() {
        bail!("bad Redis response payload size");
    }
    Ok(entry_id)
}

fn xread_one(conn: &mut redis::Connection, key: &str, last_id: &str) -> Result<(String, Vec<u8>)> {
    let value: redis::Value = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(1)
        .arg("BLOCK")
        .arg(5000)
        .arg("STREAMS")
        .arg(key)
        .arg(last_id)
        .query(conn)?;
    parse_xread(value)?
        .into_iter()
        .next()
        .context("Redis response timeout")
}
