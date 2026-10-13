//! Verified single-stream throughput and request/reply latency.

use super::{
    BENCH_HWM, Config, DEADLINE, Error, PROBE, SIZES, connect, patterned_message, prepare_message,
    send_slab,
};
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::{Duration, Instant};

use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Context, Message, Options, PayloadPool, SocketType};
use serde_json::json;

const ROUNDS: usize = 3;
const WARMUP: usize = 20_000;
const SAMPLES: usize = 100_000;

struct PointConfig {
    base: Config,
    latency: bool,
    spin_us: u64,
}

impl PointConfig {
    fn parse() -> Result<Self, Error> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 7 {
            return Err("usage: pair transport pub|sub size recv_spin_us directory".into());
        }
        let config = Self {
            base: Config {
                implementation: args[2].clone(),
                publisher: match args[3].as_str() {
                    "pub" => true,
                    "sub" => false,
                    _ => return Err("invalid role".into()),
                },
                size: args[4].parse()?,
                readers: 1,
                id: 0,
                directory: args[6].clone().into(),
            },
            latency: args[1] == "client-server",
            spin_us: args[5].parse()?,
        };
        if !["inproc", "ipc", "shm", "shm-pool", "tcp"]
            .contains(&config.base.implementation.as_str())
            || !SIZES.contains(&config.base.size)
            || ![0, 30, 50].contains(&config.spin_us)
        {
            return Err("invalid point-to-point configuration".into());
        }
        Ok(config)
    }

    fn pool(&self) -> Result<Option<PayloadPool>, Error> {
        self.base.pool()
    }

    fn socket(&self, context: &Context, primary: bool) -> omq_tokio::blocking::Socket {
        let kind = match (self.latency, primary) {
            (true, true) => SocketType::Client,
            (true, false) => SocketType::Server,
            (false, true) => SocketType::Scatter,
            (false, false) => SocketType::Gather,
        };
        context.blocking_socket(
            kind,
            Options {
                send_hwm: BENCH_HWM,
                recv_hwm: BENCH_HWM,
                recv_batching: !self.latency,
                recv_spin: Duration::from_micros(self.spin_us),
                workload_profile: Some(if self.latency {
                    WorkloadProfile::Latency
                } else {
                    WorkloadProfile::Throughput
                }),
                ..Options::default()
            },
        )
    }
}

fn verify(message: &Message, size: usize, sequence: u64) -> Result<&[u8], Error> {
    let body = message.part_slice(0).ok_or("missing body")?;
    if message.len() != 1
        || body.len() != size
        || body[..8] != sequence.to_le_bytes()
        || body[size - 1] != 0xa5
    {
        return Err("wrong message length, sequence, or sentinel".into());
    }
    Ok(body)
}

fn serve(config: &PointConfig, context: &Context) -> Result<(), Error> {
    let socket = config.socket(context, false);
    let endpoint = socket.bind(config.base.endpoint())?;
    if config.base.implementation == "tcp" {
        std::fs::write(config.base.directory.join("endpoint"), endpoint.to_string())?;
    }
    let listener = UnixListener::bind(config.base.control())?;
    std::fs::write(config.base.directory.join("ready"), [])?;
    let (mut control, _) = listener.accept()?;
    control.set_read_timeout(Some(DEADLINE))?;
    let probe = socket.recv_timeout(DEADLINE)?;
    if probe.part_slice(0) != Some(PROBE.to_le_bytes().as_slice()) {
        return Err("invalid readiness probe".into());
    }
    if config.latency {
        socket.send(probe)?;
    }
    control.write_all(&[1])?;
    if config.latency {
        serve_latency(config, &socket)?;
    } else {
        serve_throughput(config, &socket, &mut control)?;
    }
    control.read_exact(&mut [0])?;
    socket.close()?;
    Ok(())
}

fn serve_latency(config: &PointConfig, socket: &omq_tokio::blocking::Socket) -> Result<(), Error> {
    let pool = config.pool()?;
    for _ in 0..ROUNDS {
        for sequence in 0..WARMUP + SAMPLES {
            let request = socket.recv_timeout(DEADLINE)?;
            let body = verify(&request, config.base.size, sequence as u64)?;
            let reply = prepare_message(body, pool.as_ref())?
                .with_routing_id(request.routing_id().ok_or("missing reply route")?);
            socket.send_timeout(reply, DEADLINE)?;
        }
    }
    Ok(())
}

fn serve_throughput(
    config: &PointConfig,
    socket: &omq_tokio::blocking::Socket,
    control: &mut UnixStream,
) -> Result<(), Error> {
    let mut messages = Vec::with_capacity(256);
    let batch = (1024 * 1024 / config.base.size).clamp(1, 256);
    for round in 0..=ROUNDS {
        let mut count = 0_u64;
        let mut started = None;
        let mut ended = false;
        while !ended {
            socket.recv_many_timeout_into(batch, DEADLINE, &mut messages)?;
            for message in &messages {
                if message.part_slice(0)
                    == Some((PROBE - 1 - round as u64).to_le_bytes().as_slice())
                {
                    ended = true;
                } else {
                    if ended {
                        return Err("data after round marker".into());
                    }
                    verify(message, config.base.size, count)?;
                    started.get_or_insert_with(Instant::now);
                    count += 1;
                }
            }
            messages.clear();
        }
        let elapsed = started
            .ok_or("empty throughput round")?
            .elapsed()
            .as_secs_f64();
        control.write_all(&count.to_le_bytes())?;
        control.write_all(&elapsed.to_le_bytes())?;
    }
    Ok(())
}

fn primary(config: &PointConfig, context: &Context) -> Result<(), Error> {
    let mut control = connect(&config.base.control())?;
    let socket = config.socket(context, true);
    socket.connect(config.base.endpoint())?;
    socket.wait_connected(1, DEADLINE)?;
    socket.send_timeout(Message::from_slice(&PROBE.to_le_bytes()), DEADLINE)?;
    control.read_exact(&mut [0])?;
    if config.latency {
        assert_eq!(
            socket.recv_timeout(DEADLINE)?.part_slice(0),
            Some(PROBE.to_le_bytes().as_slice())
        );
        measure_latency(config, &socket)?;
    } else {
        measure_throughput(config, &socket, &mut control)?;
    }
    control.write_all(&[1])?;
    socket.close()?;
    Ok(())
}

fn metadata(config: &PointConfig) -> serde_json::Value {
    let mut row = json!({
        "implementation":config.base.implementation, "msg_size":config.base.size,
        "readers":1, "send_hwm":BENCH_HWM, "recv_hwm":BENCH_HWM,
        "shm_max_memory":if config.base.implementation.starts_with("shm") { Some(64 * 1024 * 1024) } else { None },
        "recv_spin_us":config.spin_us, "verified":true,
        "socket_pair":if config.latency { "client-server" } else { "scatter-gather" },
        "workload_profile":if config.latency { "latency" } else { "throughput" },
    });
    config.base.add_pool_metadata(&mut row, !config.latency);
    row
}

fn measure_throughput(
    config: &PointConfig,
    socket: &omq_tokio::blocking::Socket,
    control: &mut UnixStream,
) -> Result<(), Error> {
    let pool = config.pool()?;
    for round in 0..=ROUNDS {
        let duration = Duration::from_secs(if round == 0 { 1 } else { 3 });
        let start = Instant::now();
        let mut count = 0_u64;
        while start.elapsed() < duration {
            if config.base.uses_slabs() {
                count += send_slab(pool.as_ref().unwrap(), config.base.size, count, |message| {
                    Ok(socket.send_timeout(message, DEADLINE)?)
                })?;
                continue;
            }
            socket.send_timeout(
                patterned_message(config.base.size, count, pool.as_ref())?,
                DEADLINE,
            )?;
            count += 1;
        }
        let sent_elapsed = start.elapsed().as_secs_f64();
        socket.send_timeout(
            Message::from_slice(&(PROBE - 1 - round as u64).to_le_bytes()),
            DEADLINE,
        )?;
        let mut result = [0; 16];
        control.read_exact(&mut result)?;
        let received = u64::from_le_bytes(result[..8].try_into()?);
        let recv_elapsed = f64::from_le_bytes(result[8..].try_into()?);
        if received != count {
            return Err("throughput delivery mismatch".into());
        }
        if round != 0 {
            let elapsed = sent_elapsed.max(recv_elapsed);
            let mut row = metadata(config);
            row.as_object_mut().unwrap().extend(json!({
                "round":round, "offered":count, "reader_counts":[received],
                "reader_elapsed":[recv_elapsed], "sender_elapsed":sent_elapsed, "elapsed":elapsed,
                "msgs_s":count as f64 / elapsed, "mbps":count as f64 * config.base.size as f64 / elapsed / 1e6,
                "missing":0, "duplicates":0, "gaps":0, "corrupt":0,
            }).as_object().unwrap().clone());
            println!("{row}");
        }
    }
    Ok(())
}

fn measure_latency(
    config: &PointConfig,
    socket: &omq_tokio::blocking::Socket,
) -> Result<(), Error> {
    let pool = config.pool()?;
    let mut bytes = vec![0xa5; config.base.size];
    let mut samples = Vec::with_capacity(SAMPLES);
    for round in 1..=ROUNDS {
        samples.clear();
        for sequence in 0..WARMUP + SAMPLES {
            bytes[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
            let start = Instant::now();
            socket.send_timeout(prepare_message(&bytes, pool.as_ref())?, DEADLINE)?;
            let reply = socket.recv_timeout(DEADLINE)?;
            verify(&reply, config.base.size, sequence as u64)?;
            if reply.part_slice(0) != Some(bytes.as_slice()) {
                return Err("corrupt reply".into());
            }
            if sequence >= WARMUP {
                samples.push(u64::try_from(start.elapsed().as_nanos())?);
            }
        }
        samples.sort_unstable();
        let quantile = |numerator: usize, denominator: usize| {
            samples[SAMPLES * numerator / denominator - 1] as f64 / 1000.0
        };
        let mut row = metadata(config);
        row.as_object_mut().unwrap().extend(
            json!({
                "round":round, "samples":SAMPLES, "warmup_iterations":WARMUP,
                "p50_us":quantile(50, 100), "p99_us":quantile(99, 100),
                "p999_us":quantile(999, 1000), "max_us":samples[SAMPLES - 1] as f64 / 1000.0,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        println!("{row}");
    }
    Ok(())
}

pub(super) fn run() -> Result<(), Error> {
    let config = PointConfig::parse()?;
    let context = Context::new();
    if config.base.publisher {
        if config.base.implementation == "inproc" {
            std::thread::scope(|scope| {
                let server = scope.spawn(|| serve(&config, &context));
                let result = primary(&config, &context);
                server.join().map_err(|_| "receiver thread panicked")??;
                result
            })
        } else {
            primary(&config, &context)
        }
    } else {
        serve(&config, &context)
    }
}
