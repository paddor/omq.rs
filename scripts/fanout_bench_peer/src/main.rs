//! One publisher and one subscriber process; subscriber ports share a context.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use iceoryx2::port::{BackpressureAction, publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use omq_tokio::{Context, Message, Options, PayloadPool, SocketType};
use serde_json::json;
use zenoh::Wait;

const DEADLINE: Duration = Duration::from_secs(30);
const PROBE: u64 = u64::MAX;
const MAX_SIZE: usize = 8 * 1024 * 1024;
const QUEUE_BYTES: usize = 32 * 1024 * 1024;
const POOL_BYTES: usize = 64 * 1024 * 1024;
const HWM: usize = 256;
const OMQ_HWM: usize = 1000;
const POOL_SLOTS: usize = 1024;
type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
struct Config {
    implementation: String,
    publisher: bool,
    size: usize,
    readers: usize,
    id: usize,
    port: u16,
    directory: PathBuf,
    recv_spin_us: u64,
}

#[derive(Clone)]
struct Resources {
    omq: Option<Context>,
    zmq: Option<zmq::Context>,
    zenoh: Option<zenoh::Session>,
    rabbit: Option<Arc<RabbitResources>>,
}

struct RabbitResources {
    runtime: tokio::runtime::Runtime,
    connection: Connection,
}

impl Resources {
    fn new(config: &Config) -> Result<Self, Error> {
        let zenoh = if config.implementation.starts_with("zenoh-") && config.publisher {
            Some(open_zenoh(config)?)
        } else {
            None
        };
        let rabbit = if config.implementation == "rabbitmq-tcp" {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?;
            let url = std::env::var("OMQ_FANOUT_RABBITMQ_URL")
                .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());
            let connection =
                runtime.block_on(Connection::connect(&url, ConnectionProperties::default()))?;
            Some(Arc::new(RabbitResources {
                runtime,
                connection,
            }))
        } else {
            None
        };
        Ok(Self {
            omq: config.implementation.starts_with("omq-").then(Context::new),
            zmq: config
                .implementation
                .starts_with("libzmq-")
                .then(zmq::Context::new),
            zenoh,
            rabbit,
        })
    }
}

fn open_zenoh(config: &Config) -> Result<zenoh::Session, Error> {
    let mut options = zenoh::Config::default();
    options.insert_json5("mode", "\"peer\"")?;
    options.insert_json5("scouting/multicast/enabled", "false")?;
    options.insert_json5("scouting/gossip/enabled", "false")?;
    let address = if config.implementation == "zenoh-unix" {
        format!(
            "unixsock-stream/{}",
            config.directory.join("data.sock").display()
        )
    } else {
        format!("tcp/127.0.0.1:{}", config.port)
    };
    if config.publisher {
        options.insert_json5("listen/endpoints", &format!("[\"{address}\"]"))?;
    } else {
        options.insert_json5("listen/endpoints", "[]")?;
        options.insert_json5("connect/endpoints", &format!("[\"{address}\"]"))?;
    }
    zenoh::open(options).wait()
}

impl Config {
    fn parse() -> Result<Self, Error> {
        let args: Vec<_> = std::env::args().collect();
        if ![8, 9].contains(&args.len()) {
            return Err("usage: impl pub|sub size readers id port directory [recv_spin_us]".into());
        }
        let config = Self {
            implementation: args[1].clone(),
            publisher: match args[2].as_str() {
                "pub" => true,
                "sub" => false,
                _ => return Err("unknown role".into()),
            },
            size: args[3].parse()?,
            readers: args[4].parse()?,
            id: args[5].parse()?,
            port: args[6].parse()?,
            directory: PathBuf::from(&args[7]),
            recv_spin_us: args.get(8).map_or(Ok(0), |value| value.parse())?,
        };
        if !(16..=MAX_SIZE).contains(&config.size)
            || ![1, 4, 32].contains(&config.readers)
            || config.id >= config.readers
            || ![0, 50].contains(&config.recv_spin_us)
        {
            return Err("invalid size or reader count".into());
        }
        Ok(config)
    }

    fn queue_capacity(&self) -> usize {
        if self.implementation.starts_with("omq-") {
            OMQ_HWM
        } else {
            (QUEUE_BYTES / self.size).clamp(4, HWM)
        }
    }

    fn pool_slots(&self) -> usize {
        (POOL_BYTES / self.size).clamp(4, POOL_SLOTS)
    }

    fn endpoint(&self) -> String {
        if matches!(self.implementation.as_str(), "omq-shm" | "omq-shm-pool") {
            format!("shm://{}", self.directory.join("data.sock").display())
        } else if self.implementation.ends_with("ipc") {
            format!("ipc://{}", self.directory.join("data.sock").display())
        } else {
            let transport = self.implementation.split('-').nth(1).unwrap();
            format!("{transport}://127.0.0.1:{}", self.port)
        }
    }

    fn control(&self) -> PathBuf {
        self.directory.join("control.sock")
    }

    fn drain_deadline(&self) -> Duration {
        if self.implementation == "rabbitmq-tcp" && self.readers == 32 {
            Duration::from_secs(300)
        } else {
            DEADLINE
        }
    }

    fn send_batch(&self) -> usize {
        if self.implementation == "rabbitmq-tcp" && self.readers == 32 {
            1
        } else {
            (1024 * 1024 / self.size).clamp(1, 256)
        }
    }
}

enum Port {
    Omq {
        socket: omq_tokio::blocking::Socket,
        grouped: bool,
        pool: Option<PayloadPool>,
        messages: Vec<Message>,
        next: usize,
        batch: usize,
    },
    Zmq(zmq::Socket),
    IcePublisher(Publisher<ipc::Service, [u8], ()>, usize),
    IceSubscriber(Subscriber<ipc::Service, [u8], ()>),
    ZenohPublisher(zenoh::pubsub::Publisher<'static>, zenoh::Session),
    ZenohSubscriber(
        zenoh::pubsub::Subscriber<zenoh::handlers::FifoChannelHandler<zenoh::sample::Sample>>,
        zenoh::Session,
    ),
    RabbitPublisher(
        lapin::Channel,
        Arc<RabbitResources>,
        lapin::types::ShortString,
    ),
    RabbitSubscriber(lapin::Consumer, lapin::Channel, Arc<RabbitResources>),
}

impl Port {
    #[expect(clippy::too_many_lines)]
    fn open(config: &Config, resources: &Resources) -> Result<Self, Error> {
        let publisher = config.publisher;
        match config.implementation.as_str() {
            "omq-tcp" | "omq-ipc" | "omq-quic" | "omq-dart" | "omq-shm" | "omq-shm-pool" => {
                let grouped = matches!(
                    config.implementation.as_str(),
                    "omq-dart" | "omq-shm" | "omq-shm-pool"
                );
                let hwm = u32::try_from(config.queue_capacity())?;
                let mut options = Options::default();
                options.xpub_nodrop = true;
                options.send_hwm = hwm;
                options.recv_hwm = hwm;
                options.recv_batching = true;
                options.recv_spin = Duration::from_micros(config.recv_spin_us);
                options.workload_profile = Some(omq_tokio::options::WorkloadProfile::Throughput);
                options.dart.congestion = omq_tokio::DartCongestion::Adaptive;
                options.dart.window_messages = 512;
                if config.implementation == "omq-quic" {
                    let cert = std::fs::read(config.directory.join("cert.pem"))?;
                    options.quic.server_cert_pem = Some(cert.clone());
                    options.quic.server_key_pem =
                        Some(std::fs::read(config.directory.join("key.pem"))?);
                    options.quic.trust_pem = Some(cert);
                    options.quic.trust_system = false;
                    options.quic.server_name = Some("localhost".into());
                    options.send_buffer_size = Some(4 * 1024 * 1024);
                    options.recv_buffer_size = Some(4 * 1024 * 1024);
                }
                let kind = match (publisher, grouped) {
                    (true, true) => SocketType::Radio,
                    (false, true) => SocketType::Dish,
                    (true, false) => SocketType::XPub,
                    (false, false) => SocketType::Sub,
                };
                let socket = resources
                    .omq
                    .as_ref()
                    .unwrap()
                    .blocking_socket(kind, options);
                if publisher {
                    socket.bind(config.endpoint())?;
                } else {
                    if grouped {
                        socket.join("fanout")?;
                    } else {
                        socket.subscribe("")?;
                    }
                    socket.connect(config.endpoint())?;
                }
                Ok(Self::Omq {
                    socket,
                    grouped,
                    pool: if publisher && config.implementation == "omq-shm-pool" {
                        Some(omq_tokio::shm_payload_pool([(
                            config.size,
                            config.pool_slots(),
                        )])?)
                    } else {
                        None
                    },
                    messages: Vec::with_capacity(256),
                    next: 0,
                    batch: (1024 * 1024 / config.size).clamp(1, 256),
                })
            }
            "libzmq-tcp" | "libzmq-ipc" => {
                let mut socket = resources.zmq.as_ref().unwrap().socket(if publisher {
                    zmq::XPUB
                } else {
                    zmq::SUB
                })?;
                socket.set_sndhwm(256)?;
                socket.set_rcvhwm(256)?;
                socket.set_linger(0)?;
                socket.set_rcvtimeo(30_000)?;
                socket.set_sndtimeo(30_000)?;
                if publisher {
                    let enabled: i32 = 1;
                    // SAFETY: XPUB socket and a live, correctly sized integer option.
                    let result = unsafe {
                        zmq_sys::zmq_setsockopt(
                            socket.as_mut_ptr(),
                            zmq_sys::ZMQ_XPUB_NODROP.cast_signed(),
                            std::ptr::from_ref(&enabled).cast(),
                            std::mem::size_of_val(&enabled),
                        )
                    };
                    if result != 0 {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    socket.bind(&config.endpoint())?;
                } else {
                    socket.set_subscribe(b"")?;
                    socket.connect(&config.endpoint())?;
                }
                Ok(Self::Zmq(socket))
            }
            "iceoryx2-shm" => {
                let global = iceoryx2::config::Config::global_config();
                let node = NodeBuilder::new().config(global).create::<ipc::Service>()?;
                let name = format!("omq/fanout/{}", config.port);
                let capacity = config.queue_capacity();
                let service = node
                    .service_builder(&name.as_str().try_into()?)
                    .publish_subscribe::<[u8]>()
                    .max_nodes(64)
                    .max_publishers(1)
                    .max_subscribers(config.readers)
                    .subscriber_max_buffer_size(capacity)
                    .subscriber_max_borrowed_samples(1)
                    .history_size(0)
                    .enable_safe_overflow(false)
                    .open_or_create()?;
                if publisher {
                    let port = service
                        .publisher_builder()
                        .initial_max_slice_len(config.size)
                        .max_loaned_samples(1)
                        .set_backpressure_handler(|info| {
                            if info.elapsed_time >= DEADLINE {
                                BackpressureAction::DiscardDataAndFail
                            } else {
                                std::thread::yield_now();
                                BackpressureAction::Retry
                            }
                        })
                        .create()?;
                    Ok(Self::IcePublisher(port, config.readers))
                } else {
                    Ok(Self::IceSubscriber(
                        service
                            .subscriber_builder()
                            .buffer_size(capacity)
                            .create()?,
                    ))
                }
            }
            "zenoh-tcp" | "zenoh-unix" => {
                let session = if publisher {
                    resources.zenoh.as_ref().unwrap().clone()
                } else {
                    open_zenoh(config)?
                };
                let key = format!("omq/fanout/{}", config.port);
                if publisher {
                    Ok(Self::ZenohPublisher(
                        session
                            .declare_publisher(key)
                            .congestion_control(zenoh::qos::CongestionControl::Block)
                            .wait()?,
                        session,
                    ))
                } else {
                    Ok(Self::ZenohSubscriber(
                        session
                            .declare_subscriber(key)
                            .with(zenoh::handlers::FifoChannel::new(256))
                            .wait()?,
                        session,
                    ))
                }
            }
            "rabbitmq-tcp" => {
                let resources = resources.rabbit.as_ref().unwrap().clone();
                let exchange = format!("omq.fanout.{}", config.port);
                let channel = resources.runtime.block_on(async {
                    let channel = resources.connection.create_channel().await?;
                    channel
                        .exchange_declare(
                            exchange.as_str().into(),
                            lapin::ExchangeKind::Fanout,
                            ExchangeDeclareOptions {
                                auto_delete: true,
                                ..ExchangeDeclareOptions::default()
                            },
                            FieldTable::default(),
                        )
                        .await?;
                    Ok::<_, lapin::Error>(channel)
                })?;
                if publisher {
                    Ok(Self::RabbitPublisher(channel, resources, exchange.into()))
                } else {
                    let consumer = resources.runtime.block_on(async {
                        let queue = channel
                            .queue_declare(
                                "".into(),
                                QueueDeclareOptions {
                                    exclusive: true,
                                    auto_delete: true,
                                    ..QueueDeclareOptions::default()
                                },
                                FieldTable::default(),
                            )
                            .await?;
                        channel
                            .queue_bind(
                                queue.name().clone(),
                                exchange.as_str().into(),
                                "".into(),
                                QueueBindOptions::default(),
                                FieldTable::default(),
                            )
                            .await?;
                        channel
                            .basic_consume(
                                queue.name().clone(),
                                "".into(),
                                BasicConsumeOptions {
                                    no_ack: true,
                                    ..BasicConsumeOptions::default()
                                },
                                FieldTable::default(),
                            )
                            .await
                    })?;
                    Ok(Self::RabbitSubscriber(consumer, channel, resources))
                }
            }
            _ => Err("unknown implementation".into()),
        }
    }

    fn send(&self, bytes: &[u8], checked: bool) -> Result<(), Error> {
        match self {
            Self::Omq {
                socket,
                grouped,
                pool,
                ..
            } => {
                let message = prepare_message(bytes, pool.as_ref())?;
                if *grouped {
                    socket.send_group("fanout", message)?;
                } else {
                    socket.send(message)?;
                }
            }
            Self::Zmq(socket) => socket.send(bytes, 0)?,
            Self::IcePublisher(port, readers) => {
                let got = port
                    .loan_slice_uninit(bytes.len())?
                    .write_from_slice(bytes)
                    .send()?;
                if checked && got != *readers {
                    return Err(format!("expected {readers} readers, got {got}").into());
                }
            }
            Self::ZenohPublisher(port, _session) => port.put(bytes.to_vec()).wait()?,
            Self::RabbitPublisher(channel, resources, exchange) => {
                resources.runtime.block_on(channel.basic_publish(
                    exchange.clone(),
                    "".into(),
                    BasicPublishOptions::default(),
                    bytes,
                    BasicProperties::default(),
                ))?;
            }
            _ => return Err("not a publisher".into()),
        }
        Ok(())
    }

    fn receive(&mut self, check: &mut dyn FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        match self {
            Self::Omq {
                socket,
                messages,
                next,
                batch,
                ..
            } => {
                if *next == messages.len() {
                    messages.clear();
                    socket.recv_many_timeout_into(*batch, DEADLINE, messages)?;
                    *next = 0;
                }
                let message = &messages[*next];
                check(
                    message
                        .len()
                        .checked_sub(1)
                        .and_then(|index| message.part_slice(index))
                        .ok_or("empty message")?,
                )?;
                *next += 1;
            }
            Self::Zmq(socket) => check(socket.recv_msg(0)?.as_ref())?,
            Self::IceSubscriber(port) => {
                let started = Instant::now();
                loop {
                    for _ in 0..256 {
                        if let Some(sample) = port.receive()? {
                            return check(sample.payload());
                        }
                        std::hint::spin_loop();
                    }
                    if started.elapsed() >= DEADLINE {
                        return Err("receive timeout".into());
                    }
                    std::thread::yield_now();
                }
            }
            Self::ZenohSubscriber(port, _session) => {
                let sample = port
                    .recv_deadline(Instant::now() + DEADLINE)?
                    .ok_or("receive timeout")?;
                check(sample.payload().to_bytes().as_ref())?;
            }
            Self::RabbitSubscriber(consumer, _channel, resources) => {
                let delivery = resources
                    .runtime
                    .block_on(async { tokio::time::timeout(DEADLINE, consumer.next()).await })?
                    .ok_or("consumer closed")??;
                check(&delivery.data)?;
            }
            _ => return Err("not a subscriber".into()),
        }
        Ok(())
    }
}

fn prepare_message(bytes: &[u8], pool: Option<&PayloadPool>) -> Result<Message, Error> {
    let Some(pool) = pool else {
        return Ok(Message::from_slice(bytes));
    };
    let started = Instant::now();
    loop {
        if let Some(message) = pool.try_message(bytes.len(), |body| body.copy_from_slice(bytes))? {
            return Ok(message);
        }
        if started.elapsed() >= DEADLINE {
            return Err("pool credit timeout".into());
        }
        std::thread::yield_now();
    }
}

fn connect(path: &Path) -> Result<UnixStream, Error> {
    let started = Instant::now();
    loop {
        match UnixStream::connect(path) {
            Ok(stream) => {
                stream.set_read_timeout(Some(DEADLINE))?;
                return Ok(stream);
            }
            Err(error) if started.elapsed() >= DEADLINE => return Err(error.into()),
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

fn subscribe(config: &Config, mut port: Port) -> Result<(), Error> {
    let mut control = connect(&config.control())?;
    let mut ready = false;
    for round in 0..4_u64 {
        let mut received = 0_u64;
        let mut started = None;
        let mut ended = false;
        let batch = (1024 * 1024 / config.size).clamp(1, 256);
        while !ended {
            for _ in 0..batch {
                port.receive(&mut |bytes| {
                    if bytes.len() == 8 {
                        let marker = u64::from_le_bytes(bytes.try_into()?);
                        if marker == PROBE {
                            if !ready {
                                control.write_all(&[1])?;
                                ready = true;
                            }
                        } else if marker == PROBE - 1 - round {
                            ended = true;
                        } else {
                            return Err("unexpected round marker".into());
                        }
                    } else {
                        if !ready
                            || ended
                            || bytes.len() != config.size
                            || bytes[..8] != received.to_le_bytes()
                            || bytes[config.size - 1] != 0xa5
                        {
                            return Err(format!(
                                "reader {} wrong length/sequence/sentinel at {received}",
                                config.id
                            )
                            .into());
                        }
                        started.get_or_insert_with(Instant::now);
                        received += 1;
                    }
                    Ok(())
                })?;
                if ended {
                    break;
                }
            }
        }
        let elapsed = started.ok_or("empty round")?.elapsed().as_secs_f64();
        if std::env::var_os("OMQ_FANOUT_DEBUG").is_some() {
            eprintln!(
                "reader {} round {round}: {received} messages in {elapsed}s",
                config.id
            );
        }
        control.write_all(&received.to_le_bytes())?;
        control.write_all(&elapsed.to_le_bytes())?;
    }
    // Keep the data port alive until the publisher has verified every reader.
    let mut done = [0];
    control.read_exact(&mut done)?;
    Ok(())
}

/// Accepts every reader's control connection.
fn accept_readers(
    config: &Config,
    listener: &UnixListener,
    started: Instant,
) -> Result<Vec<(UnixStream, bool)>, Error> {
    let mut readers = Vec::with_capacity(config.readers);
    while readers.len() < config.readers {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true)?;
                readers.push((stream, false));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
        if started.elapsed() >= DEADLINE {
            return Err("connection timeout".into());
        }
    }
    Ok(readers)
}

/// Publishes probes until every reader reports that it receives them.
fn await_readiness(
    port: &Port,
    readers: &mut [(UnixStream, bool)],
    started: Instant,
) -> Result<(), Error> {
    while readers.iter().any(|(_, ready)| !ready) {
        port.send(&PROBE.to_le_bytes(), false)?;
        for (stream, ready) in readers.iter_mut() {
            if !*ready {
                match stream.read(&mut [0]) {
                    Ok(1) => *ready = true,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    _ => return Err("readiness connection closed".into()),
                }
            }
        }
        if started.elapsed() >= DEADLINE {
            return Err("subscription timeout".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// Reads each reader's count and receive interval for one round. Every
/// reader must have received all `sent` messages.
fn collect_acks(
    readers: &mut [(UnixStream, bool)],
    round: u64,
    sent: u64,
) -> Result<(Vec<u64>, Vec<f64>), Error> {
    let mut counts = Vec::with_capacity(readers.len());
    let mut elapsed = Vec::with_capacity(readers.len());
    for (id, (stream, _)) in readers.iter_mut().enumerate() {
        let mut ack = [0; 16];
        stream
            .read_exact(&mut ack)
            .map_err(|error| format!("reader {id} round {round} acknowledgment failed: {error}"))?;
        let count = u64::from_le_bytes(ack[..8].try_into()?);
        let seconds = f64::from_le_bytes(ack[8..].try_into()?);
        if count != sent || !seconds.is_finite() || seconds <= 0.0 {
            return Err(format!(
                "delivery mismatch: sent {sent}, reader {count}, elapsed {seconds}"
            )
            .into());
        }
        counts.push(count);
        elapsed.push(seconds);
    }
    Ok((counts, elapsed))
}

/// One verified measurement round.
struct RoundResult {
    round: u64,
    sent: u64,
    sender_elapsed: f64,
    counts: Vec<u64>,
    elapsed: Vec<f64>,
}

/// Prints one round as a JSON result line. The rate includes the slowest
/// reader's drain.
fn print_round(config: &Config, result: &RoundResult) {
    let RoundResult {
        round,
        sent,
        sender_elapsed,
        ref counts,
        ref elapsed,
    } = *result;
    let total_elapsed = elapsed.iter().copied().fold(sender_elapsed, f64::max);
    let rate = sent as f64 / total_elapsed;
    println!(
        "{}",
        json!({"event":"result", "implementation":config.implementation,
        "msg_size":config.size,"readers":config.readers,"round":round,
        "recv_mode":if config.implementation.starts_with("omq-") || config.implementation.starts_with("zenoh-") || config.implementation == "rabbitmq-tcp" { Some("blocking") } else { None },
        "recv_spin_us":if config.implementation.starts_with("omq-") { Some(config.recv_spin_us) } else { None },
        "io_spin_us":if config.implementation.starts_with("omq-") { Some(0) } else { None },
        "offered":sent,"reader_counts":counts,"reader_elapsed":elapsed,
        "sender_elapsed":sender_elapsed,"elapsed":total_elapsed,"msgs_s":rate,
        "aggregate_msgs_s":rate * config.readers as f64,"mbps":rate * config.size as f64 / 1e6,
        "missing":0,"duplicates":0,"gaps":0,"corrupt":0})
    );
}

fn publish(config: &Config, port: &Port) -> Result<(), Error> {
    let listener = UnixListener::bind(config.control())?;
    listener.set_nonblocking(true)?;
    std::fs::write(config.directory.join("ready"), b"ready")?;
    let started = Instant::now();
    let mut readers = accept_readers(config, &listener, started)?;
    await_readiness(port, &mut readers, started)?;
    for (stream, _) in &readers {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(config.drain_deadline()))?;
    }

    // Round 0 is the warmup and is not reported.
    let mut bytes = vec![0xa5; config.size];
    let batch = config.send_batch();
    for round in 0..4_u64 {
        let duration = Duration::from_secs(if round == 0 { 1 } else { 3 });
        let started = Instant::now();
        let mut sent = 0_u64;
        while started.elapsed() < duration {
            for _ in 0..batch {
                bytes[..8].copy_from_slice(&sent.to_le_bytes());
                port.send(&bytes, true)?;
                sent += 1;
            }
        }
        let sender_elapsed = started.elapsed().as_secs_f64();
        port.send(&(PROBE - 1 - round).to_le_bytes(), true)?;
        if std::env::var_os("OMQ_FANOUT_DEBUG").is_some() {
            eprintln!("publisher round {round}: {sent} messages in {sender_elapsed}s; marker sent");
        }

        let (counts, elapsed) = collect_acks(&mut readers, round, sent)?;
        if round > 0 {
            let result = RoundResult {
                round,
                sent,
                sender_elapsed,
                counts,
                elapsed,
            };
            print_round(config, &result);
        }
    }
    for (stream, _) in &mut readers {
        stream.write_all(&[1])?;
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let config = Config::parse()?;
    let resources = Resources::new(&config)?;
    if config.implementation == "iceoryx2-shm" {
        let path = config.directory.join("ice.toml");
        std::fs::write(&path, b"# Library defaults.\n")?;
        let path = FilePath::new(path.as_os_str().as_encoded_bytes())?;
        iceoryx2::config::Config::setup_global_config_from_file(&path)?;
    }
    if config.publisher {
        let port = Port::open(&config, &resources)?;
        publish(&config, &port)
    } else {
        let mut workers = Vec::with_capacity(config.readers);
        for id in 0..config.readers {
            let mut config = config.clone();
            config.id = id;
            let resources = resources.clone();
            workers.push(std::thread::spawn(move || {
                let port = Port::open(&config, &resources)?;
                subscribe(&config, port)
            }));
        }
        for worker in workers {
            worker.join().map_err(|_| "subscriber thread panicked")??;
        }
        Ok(())
    }
}
