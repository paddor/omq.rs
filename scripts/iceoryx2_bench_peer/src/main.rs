//! Two-process SHM byte messages, copy-in and borrowed receive. No async runtime.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::{Duration, Instant};

use iceoryx2::port::{BackpressureAction, publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::sample::Sample;

const WARMUP: Duration = Duration::from_secs(1);
const MEASURE: Duration = Duration::from_secs(3);
const DEADLINE: Duration = Duration::from_secs(30);
const WARMUP_RTT: usize = 2_000;
const SAMPLES: usize = 10_000;
const IMPL: &str = "iceoryx2-shm-2proc";
type Error = Box<dyn std::error::Error>;
type Sender = Publisher<ipc::Service, [u8], ()>;
type Receiver = Subscriber<ipc::Service, [u8], ()>;
type Received = Sample<ipc::Service, [u8], ()>;

fn duration(round: usize) -> Duration {
    if round == 0 { WARMUP } else { MEASURE }
}

fn send(sender: &Sender, bytes: &[u8]) -> Result<(), Error> {
    let recipients = sender
        .loan_slice_uninit(bytes.len())?
        .write_from_slice(bytes)
        .send()?;
    if recipients != 1 {
        return Err("expected exactly one recipient".into());
    }
    Ok(())
}

fn receive(receiver: &Receiver) -> Result<Received, Error> {
    let mut idle_since = None;
    loop {
        // Bound clock reads on empty polling as well as data drains.
        for _ in 0..256 {
            if let Some(sample) = receiver.receive()? {
                return Ok(sample);
            }
            std::hint::spin_loop();
        }
        if idle_since.get_or_insert_with(Instant::now).elapsed() >= DEADLINE {
            return Err("receive timeout".into());
        }
    }
}

fn wait_for(path: &Path) -> Result<(), Error> {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() >= DEADLINE {
            return Err("readiness timeout".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn server_throughput(sender: &Sender, receiver: &Receiver, size: usize) -> Result<(), Error> {
    let batch_limit = (1024 * 1024 / size).clamp(1, 256);
    for round in 0..4 {
        let mut total = 0_u64;
        let mut measured = 0_u64;
        let mut measured_elapsed = Duration::ZERO;
        let mut started = None;
        let mut ended = false;
        while !ended {
            let mut batch = 0;
            for _ in 0..batch_limit {
                let sample = receive(receiver)?;
                let bytes = sample.payload();
                if bytes.len() == 1 {
                    if bytes != [round as u8] {
                        return Err("wrong end marker".into());
                    }
                    ended = true;
                    break;
                }
                let expected = total.to_le_bytes();
                if bytes.len() != size || bytes[..8] != expected || bytes[size - 1] != 0xa5 {
                    return Err("wrong size, sequence, or payload".into());
                }
                started.get_or_insert_with(Instant::now);
                total += 1;
                batch += 1;
            }
            // Exclude batches crossing the receiver's timed window.
            let elapsed = started.map_or(Duration::ZERO, |start| start.elapsed());
            if !elapsed.is_zero() && elapsed < duration(round) {
                measured += batch;
                measured_elapsed = elapsed;
            }
        }
        if total == 0 || measured == 0 {
            return Err("empty round".into());
        }
        let mut ack = [0_u8; 25];
        ack[0] = round as u8;
        ack[1..9].copy_from_slice(&measured.to_le_bytes());
        ack[9..17].copy_from_slice(&total.to_le_bytes());
        ack[17..].copy_from_slice(&(measured_elapsed.as_nanos() as u64).to_le_bytes());
        send(sender, &ack)?;
    }
    Ok(())
}

fn client_throughput(sender: &Sender, receiver: &Receiver, size: usize) -> Result<(), Error> {
    let mut bytes = vec![0xa5; size];
    let batch_limit = (1024 * 1024 / size).clamp(1, 256);
    for round in 0..4 {
        let started = Instant::now();
        let mut sent = 0_u64;
        while started.elapsed() < duration(round) {
            for _ in 0..batch_limit {
                bytes[..8].copy_from_slice(&sent.to_le_bytes());
                send(sender, &bytes)?;
                sent += 1;
            }
        }
        send(sender, &[round as u8])?;
        let ack = receive(receiver)?;
        let bytes = ack.payload();
        if bytes.len() != 25 || bytes[0] != round as u8 {
            return Err("wrong acknowledgment".into());
        }
        let measured = u64::from_le_bytes(bytes[1..9].try_into()?);
        let total = u64::from_le_bytes(bytes[9..17].try_into()?);
        let elapsed = Duration::from_nanos(u64::from_le_bytes(bytes[17..].try_into()?));
        if total != sent || measured > total || elapsed.is_zero() || elapsed >= duration(round) {
            return Err("wrong receiver count".into());
        }
        if round > 0 {
            let elapsed = elapsed.as_secs_f64();
            let msgs_s = measured as f64 / elapsed;
            let mbps = msgs_s * size as f64 / 1_000_000.0;
            println!(
                "impl={IMPL} kind=throughput size={size} round={round} msgs_s={msgs_s:.6} mbps={mbps:.6} elapsed={elapsed:.6}"
            );
        }
    }
    Ok(())
}

fn latency(sender: &Sender, receiver: &Receiver, size: usize, server: bool) -> Result<(), Error> {
    let mut bytes = vec![0xa5; size];
    let mut samples = Vec::with_capacity(SAMPLES);
    for index in 0..WARMUP_RTT + SAMPLES {
        if server {
            let sample = receive(receiver)?;
            if sample.payload().len() != size {
                return Err("wrong message size".into());
            }
            send(sender, sample.payload())?;
        } else {
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            let started = Instant::now();
            send(sender, &bytes)?;
            let sample = receive(receiver)?;
            if sample.payload() != bytes {
                return Err("wrong echo".into());
            }
            if index >= WARMUP_RTT {
                samples.push(started.elapsed().as_nanos() as u64);
            }
        }
    }
    if !server {
        samples.sort_unstable();
        println!(
            "impl={IMPL} kind=latency size={size} round=1 p50_us={:.6} p99_us={:.6} p999_us={:.6}",
            samples[SAMPLES / 2] as f64 / 1_000.0,
            samples[SAMPLES * 99 / 100] as f64 / 1_000.0,
            samples[SAMPLES * 999 / 1000] as f64 / 1_000.0,
        );
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        return Err("usage: omq_iceoryx2_bench_peer throughput|latency server|client size port control_file".into());
    }
    let mode = args[1].as_str();
    let server = match args[2].as_str() {
        "server" => true,
        "client" => false,
        _ => return Err("unknown role".into()),
    };
    let size: usize = args[3].parse()?;
    if !(16..=8_388_608).contains(&size) || !matches!(mode, "throughput" | "latency") {
        return Err("unsupported mode or message size".into());
    }
    let control = Path::new(&args[5]);
    let mut hash = DefaultHasher::new();
    control.hash(&mut hash);
    args[4].hash(&mut hash);
    let prefix = format!("omq/rivals/iceoryx2/{:x}/{size}", hash.finish());
    // Cleanup can fall back to the global config even when a node has a local one.
    let config_path = control.with_extension(if server { "server.toml" } else { "client.toml" });
    std::fs::write(&config_path, b"# Use the library defaults.\n")?;
    let config_path = FilePath::new(config_path.as_os_str().as_encoded_bytes())?;
    let config = Config::setup_global_config_from_file(&config_path)?;
    let node = NodeBuilder::new().config(config).create::<ipc::Service>()?;
    let capacity = (8 * 1024 * 1024 / size).clamp(2, 256);
    let make_service = |suffix| {
        node.service_builder(&format!("{prefix}/{suffix}").as_str().try_into()?)
            .publish_subscribe::<[u8]>()
            .max_publishers(1)
            .max_subscribers(1)
            .subscriber_max_buffer_size(capacity)
            .subscriber_max_borrowed_samples(1)
            .history_size(0)
            .enable_safe_overflow(false)
            .open_or_create()
            .map_err(Error::from)
    };
    let data = make_service("data")?;
    let reply = make_service("reply")?;
    let (outgoing, incoming) = if server {
        (&reply, &data)
    } else {
        (&data, &reply)
    };
    let receiver = incoming
        .subscriber_builder()
        .buffer_size(capacity)
        .create()?;
    let sender = outgoing
        .publisher_builder()
        .initial_max_slice_len(size.max(25))
        .max_loaned_samples(1)
        .set_backpressure_handler(|info| {
            if info.elapsed_time >= DEADLINE {
                BackpressureAction::DiscardDataAndFail
            } else {
                std::hint::spin_loop();
                BackpressureAction::Retry
            }
        })
        .create()?;
    let client_ready = control.with_extension("client-ready");
    if server {
        std::fs::write(control, b"ready")?;
        wait_for(&client_ready)?;
        send(&sender, &[255])?;
    } else {
        wait_for(control)?;
        std::fs::write(&client_ready, b"ready")?;
        if receive(&receiver)?.payload() != [255] {
            return Err("wrong ready acknowledgment".into());
        }
    }
    match (mode, server) {
        ("throughput", true) => server_throughput(&sender, &receiver, size),
        ("throughput", false) => client_throughput(&sender, &receiver, size),
        ("latency", _) => latency(&sender, &receiver, size, server),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_sample_survives_other_reader_release_and_port_drop() -> Result<(), Error> {
        let node = NodeBuilder::new()
            .config(&Config::default())
            .create::<ipc::Service>()?;
        let name: ServiceName = format!("omq/iceoryx2/lifetime/{}", node.id())
            .as_str()
            .try_into()?;
        let service = node
            .service_builder(&name)
            .publish_subscribe::<[u8]>()
            .max_publishers(1)
            .max_subscribers(2)
            .subscriber_max_buffer_size(1)
            .subscriber_max_borrowed_samples(2)
            .history_size(0)
            .enable_safe_overflow(false)
            .open_or_create()?;
        let reader_a = service.subscriber_builder().buffer_size(1).create()?;
        let reader_b = service.subscriber_builder().buffer_size(1).create()?;
        let sender = service
            .publisher_builder()
            .initial_max_slice_len(64)
            .max_loaned_samples(1)
            .create()?;
        let original = [0x42_u8; 64];
        assert_eq!(
            sender
                .loan_slice_uninit(64)?
                .write_from_slice(&original)
                .send()?,
            2
        );
        let retained = receive(&reader_b)?;
        drop(receive(&reader_a)?);
        // Exercise pool reuse while B still holds the original allocation.
        for value in 0..100_u8 {
            assert_eq!(
                sender
                    .loan_slice_uninit(64)?
                    .write_from_slice(&[value; 64])
                    .send()?,
                2
            );
            drop(receive(&reader_a)?);
            drop(receive(&reader_b)?);
            assert_eq!(retained.payload(), original);
        }
        drop(reader_a);
        drop(reader_b);
        drop(sender);
        drop(service);
        drop(node);
        assert_eq!(retained.payload(), original);
        drop(retained);
        Ok(())
    }
}
