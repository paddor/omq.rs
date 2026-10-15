//! Two-process loopback comparisons. Each invocation owns one peer.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::{Duration, Instant};

const ALPN: &[u8] = b"omq-rivals/1";
const WARMUP_RTT: usize = 2_000;
const SAMPLES: usize = 10_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURE: Duration = Duration::from_secs(3);

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Throughput,
    Latency,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

fn round_duration(round: usize) -> Duration {
    if round == 0 { WARMUP } else { MEASURE }
}

fn report_throughput(transport: &str, size: usize, count: u64, round: usize) {
    let impl_name = match transport {
        "iroh" => "iroh-quic-2proc",
        _ => "zenoh-tcp-2proc",
    };
    let elapsed = MEASURE.as_secs_f64();
    let msgs_s = count as f64 / elapsed;
    let mbps = msgs_s * size as f64 / 1_000_000.0;
    println!(
        "impl={impl_name} kind=throughput size={size} round={round} msgs_s={msgs_s:.6} mbps={mbps:.6} elapsed={elapsed:.6}"
    );
}

fn report_latency(transport: &str, size: usize, round: usize, samples: &mut [u64]) {
    let impl_name = match transport {
        "iroh" => "iroh-quic-2proc",
        _ => "zenoh-tcp-2proc",
    };
    samples.sort_unstable();
    println!(
        "impl={impl_name} kind=latency size={size} round={round} p50_us={:.6} p99_us={:.6} p999_us={:.6}",
        samples[SAMPLES / 2] as f64 / 1_000.0,
        samples[SAMPLES * 99 / 100] as f64 / 1_000.0,
        samples[SAMPLES * 999 / 1000] as f64 / 1_000.0,
    );
}

async fn wait_for(mut ready: impl FnMut() -> bool) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn iroh_server(mode: Mode, size: usize, port: u16, control_file: &Path) -> Result<(), Error> {
    use iroh::{Endpoint, endpoint::presets};

    let endpoint = Endpoint::builder(presets::Minimal)
        .bind_addr((Ipv4Addr::LOCALHOST, port))?
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    std::fs::write(control_file, endpoint.id().to_string())?;
    let conn = endpoint
        .accept()
        .await
        .ok_or("endpoint closed before connect")?
        .await?;

    match mode {
        Mode::Throughput => {
            for round in 0..4 {
                let mut recv = conn.accept_uni().await?;
                let mut received = 0_u64;
                let mut measured = 0_u64;
                let mut started = None;
                while let Some(chunk) = recv.read_chunk(64 * 1024).await? {
                    let now = Instant::now();
                    let start = *started.get_or_insert(now);
                    received += chunk.len() as u64;
                    if now.duration_since(start) < round_duration(round) {
                        measured += chunk.len() as u64;
                    }
                }
                if received == 0 || !received.is_multiple_of(size as u64) {
                    return Err("incomplete stream".into());
                }
                let mut result = [0_u8; 17];
                result[0] = round as u8;
                result[1..9].copy_from_slice(&(measured / size as u64).to_le_bytes());
                result[9..17].copy_from_slice(&(received / size as u64).to_le_bytes());
                let mut ack = conn.open_uni().await?;
                ack.write_all(&result).await?;
                ack.finish()?;
            }
        }
        Mode::Latency => {
            let (mut send, mut recv) = conn.accept_bi().await?;
            let mut payload = vec![0u8; size];
            for _ in 0..WARMUP_RTT + SAMPLES {
                recv.read_exact(&mut payload).await?;
                send.write_all(&payload).await?;
            }
            send.finish()?;
        }
    }
    let _ = conn.closed().await;
    endpoint.close().await;
    Ok(())
}

async fn iroh_client(mode: Mode, size: usize, port: u16, control_file: &Path) -> Result<(), Error> {
    use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::presets};

    wait_for(|| control_file.exists()).await?;
    let id: EndpointId = std::fs::read_to_string(control_file)?.trim().parse()?;
    let addr = EndpointAddr::new(id).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
    let endpoint = Endpoint::builder(presets::Minimal)
        .bind_addr((Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    let conn = endpoint.connect(addr, ALPN).await?;

    match mode {
        Mode::Throughput => {
            let payload = vec![0u8; size];
            for round in 0..4 {
                let started = Instant::now();
                let mut send = conn.open_uni().await?;
                let mut sent = 0_u64;
                while started.elapsed() < round_duration(round) {
                    send.write_all(&payload).await?;
                    sent += 1;
                }
                send.finish()?;
                let mut ack = conn.accept_uni().await?;
                let mut got = [0u8; 17];
                ack.read_exact(&mut got).await?;
                if got[0] != round as u8 {
                    return Err("wrong acknowledgment".into());
                }
                let measured = u64::from_le_bytes(got[1..9].try_into()?);
                let received = u64::from_le_bytes(got[9..17].try_into()?);
                if received != sent || measured > received {
                    return Err("wrong receiver count".into());
                }
                if round > 0 {
                    report_throughput("iroh", size, measured, round);
                }
            }
        }
        Mode::Latency => {
            let (mut send, mut recv) = conn.open_bi().await?;
            let mut sent = vec![0u8; size];
            let mut received = vec![0u8; size];
            let mut samples = Vec::with_capacity(SAMPLES);
            for sample in 0..WARMUP_RTT + SAMPLES {
                sent[..8].copy_from_slice(&(sample as u64).to_le_bytes());
                let started = Instant::now();
                send.write_all(&sent).await?;
                recv.read_exact(&mut received).await?;
                if sent != received {
                    return Err("wrong echo".into());
                }
                if sample >= WARMUP_RTT {
                    samples.push(started.elapsed().as_nanos() as u64);
                }
            }
            report_latency("iroh", size, 1, &mut samples);
            send.finish()?;
        }
    }
    endpoint.close().await;
    Ok(())
}

fn zenoh_config(role: Role, port: u16) -> Result<zenoh::Config, Error> {
    let address = format!("tcp/127.0.0.1:{port}");
    let mut config = zenoh::Config::default();
    config.insert_json5("mode", "\"peer\"")?;
    config.insert_json5("scouting/multicast/enabled", "false")?;
    config.insert_json5("scouting/gossip/enabled", "false")?;
    if role == Role::Server {
        config.insert_json5("listen/endpoints", &format!("[\"{address}\"]"))?;
    } else {
        config.insert_json5("listen/endpoints", "[]")?;
        config.insert_json5("connect/endpoints", &format!("[\"{address}\"]"))?;
    }
    Ok(config)
}

async fn zenoh_peer(mode: Mode, role: Role, size: usize, port: u16) -> Result<(), Error> {
    use zenoh::{Wait, bytes::ZBytes, qos::CongestionControl};

    let session = zenoh::open(zenoh_config(role, port)?).await?;
    let data_key = format!("omq/rivals/{port}/{size}/data");
    let reply_key = format!("omq/rivals/{port}/{size}/reply");
    if role == Role::Server {
        let incoming = session.declare_subscriber(data_key.as_str()).await?;
        let outgoing = session
            .declare_publisher(reply_key.as_str())
            .congestion_control(CongestionControl::Block)
            .await?;
        wait_for(|| {
            outgoing
                .matching_status()
                .wait()
                .is_ok_and(|s| s.matching())
        })
        .await?;
        if mode == Mode::Throughput {
            for round in 0..4 {
                let mut received = 0_u64;
                let mut measured = 0_u64;
                let mut started = None;
                loop {
                    let sample = incoming.recv_async().await?;
                    let length = sample.payload().len();
                    if length == 1 {
                        if sample.payload().to_bytes().as_ref() != [round as u8] {
                            return Err("wrong end marker".into());
                        }
                        break;
                    }
                    if length != size {
                        return Err("wrong message size".into());
                    }
                    let now = Instant::now();
                    let start = *started.get_or_insert(now);
                    received += 1;
                    if now.duration_since(start) < round_duration(round) {
                        measured += 1;
                    }
                }
                if received == 0 {
                    return Err("empty round".into());
                }
                let mut result = [0_u8; 17];
                result[0] = round as u8;
                result[1..9].copy_from_slice(&measured.to_le_bytes());
                result[9..17].copy_from_slice(&received.to_le_bytes());
                outgoing.put(result).await?;
            }
        } else {
            for _ in 0..WARMUP_RTT + SAMPLES {
                let sample = incoming.recv_async().await?;
                if sample.payload().len() != size {
                    return Err("wrong request size".into());
                }
                outgoing.put(sample.payload().clone()).await?;
            }
        }
    } else {
        let incoming = session.declare_subscriber(reply_key.as_str()).await?;
        let outgoing = session
            .declare_publisher(data_key.as_str())
            .congestion_control(CongestionControl::Block)
            .await?;
        wait_for(|| {
            outgoing
                .matching_status()
                .wait()
                .is_ok_and(|s| s.matching())
        })
        .await?;
        if mode == Mode::Throughput {
            let payload = ZBytes::from(vec![0u8; size]);
            for round in 0..4 {
                let started = Instant::now();
                let mut sent = 0_u64;
                while started.elapsed() < round_duration(round) {
                    outgoing.put(payload.clone()).await?;
                    sent += 1;
                }
                outgoing.put([round as u8]).await?;
                let ack = incoming.recv_async().await?;
                let bytes = ack.payload().to_bytes();
                let bytes = bytes.as_ref();
                if bytes.len() != 17 || bytes[0] != round as u8 {
                    return Err("wrong acknowledgment".into());
                }
                let measured = u64::from_le_bytes(bytes[1..9].try_into()?);
                let received = u64::from_le_bytes(bytes[9..17].try_into()?);
                if received != sent || measured > received {
                    return Err("wrong receiver count".into());
                }
                if round > 0 {
                    report_throughput("zenoh", size, measured, round);
                }
            }
        } else {
            let mut sent = vec![0u8; size];
            let mut samples = Vec::with_capacity(SAMPLES);
            for sample in 0..WARMUP_RTT + SAMPLES {
                sent[..8].copy_from_slice(&(sample as u64).to_le_bytes());
                let started = Instant::now();
                outgoing.put(ZBytes::from(sent.clone())).await?;
                let reply = incoming.recv_async().await?;
                if reply.payload().to_bytes().as_ref() != sent {
                    return Err("wrong echo".into());
                }
                if sample >= WARMUP_RTT {
                    samples.push(started.elapsed().as_nanos() as u64);
                }
            }
            report_latency("zenoh", size, 1, &mut samples);
        }
    }
    session.close().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        return Err(
            "usage: omq_rivals iroh|zenoh throughput|latency server|client size port control_file"
                .into(),
        );
    }
    let mode = match args[2].as_str() {
        "throughput" => Mode::Throughput,
        "latency" => Mode::Latency,
        _ => return Err("unknown mode".into()),
    };
    let role = match args[3].as_str() {
        "server" => Role::Server,
        "client" => Role::Client,
        _ => return Err("unknown role".into()),
    };
    let size: usize = args[4].parse()?;
    let port: u16 = args[5].parse()?;
    const THROUGHPUT_SIZES: &[usize] = &[
        16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768, 262_144, 4_194_304,
        8_388_608,
    ];
    const LATENCY_SIZES: &[usize] = &[16, 32, 64, 256, 1024, 4096];
    let sizes = if mode == Mode::Latency {
        LATENCY_SIZES
    } else {
        THROUGHPUT_SIZES
    };
    if !sizes.contains(&size) {
        return Err("unsupported message size".into());
    }
    let control_file = Path::new(&args[6]);
    match (args[1].as_str(), role) {
        ("iroh", Role::Server) => iroh_server(mode, size, port, control_file).await,
        ("iroh", Role::Client) => iroh_client(mode, size, port, control_file).await,
        ("zenoh", _) => zenoh_peer(mode, role, size, port).await,
        _ => Err("unknown transport".into()),
    }
}
