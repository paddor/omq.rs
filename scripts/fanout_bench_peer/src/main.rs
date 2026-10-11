//! Two processes; N subscriber sockets share a receiver context and IO thread.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use iceoryx2::port::{BackpressureAction, publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use omq_tokio::{Context, Message, Options, SocketType};
use serde_json::json;
use zenoh::Wait;

const DEADLINE: Duration = Duration::from_secs(30);
const PROBE: u64 = u64::MAX;
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
}

#[derive(Clone)]
struct Resources {
    omq: Option<Context>,
    zmq: Option<zmq::Context>,
    zenoh: Option<zenoh::Session>,
}

impl Resources {
    fn new(config: &Config) -> Result<Self, Error> {
        let zenoh = if config.implementation == "zenoh-tcp" {
            let mut options = zenoh::Config::default();
            options.insert_json5("mode", "\"peer\"")?;
            options.insert_json5("scouting/multicast/enabled", "false")?;
            options.insert_json5("scouting/gossip/enabled", "false")?;
            let address = format!("tcp/127.0.0.1:{}", config.port);
            let listen = if config.publisher {
                format!("[\"{address}\"]")
            } else {
                "[]".into()
            };
            options.insert_json5("listen/endpoints", &listen)?;
            if !config.publisher {
                options.insert_json5("connect/endpoints", &format!("[\"{address}\"]"))?;
            }
            Some(zenoh::open(options).wait()?)
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
        })
    }
}

impl Config {
    fn parse() -> Result<Self, Error> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 8 {
            return Err("usage: impl pub|sub size readers id port directory".into());
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
        };
        if !(16..=262_144).contains(&config.size)
            || ![1, 4, 32].contains(&config.readers)
            || config.id >= config.readers
        {
            return Err("invalid size or reader count".into());
        }
        Ok(config)
    }

    fn endpoint(&self) -> String {
        if self.implementation.ends_with("ipc") {
            format!("ipc://{}", self.directory.join("data.sock").display())
        } else {
            let transport = self.implementation.split('-').nth(1).unwrap();
            format!("{transport}://127.0.0.1:{}", self.port)
        }
    }

    fn control(&self) -> PathBuf {
        self.directory.join("control.sock")
    }
}

enum Port {
    Omq {
        socket: omq_tokio::blocking::Socket,
        dart: bool,
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
}

impl Port {
    #[expect(clippy::too_many_lines)]
    fn open(config: &Config, resources: &Resources) -> Result<Self, Error> {
        let publisher = config.publisher;
        match config.implementation.as_str() {
            "omq-tcp" | "omq-ipc" | "omq-quic" | "omq-dart" => {
                let dart = config.implementation == "omq-dart";
                let mut options = Options::default();
                options.xpub_nodrop = true;
                options.send_hwm = 256;
                options.recv_hwm = 256;
                options.recv_batching = true;
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
                let kind = match (publisher, dart) {
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
                    if dart {
                        socket.join("fanout")?;
                    } else {
                        socket.subscribe("")?;
                    }
                    socket.connect(config.endpoint())?;
                }
                Ok(Self::Omq {
                    socket,
                    dart,
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
                let capacity = (8 * 1024 * 1024 / config.size).clamp(2, 256);
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
            "zenoh-tcp" => {
                let session = resources.zenoh.as_ref().unwrap().clone();
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
            _ => Err("unknown implementation".into()),
        }
    }

    fn send(&self, bytes: &[u8], checked: bool) -> Result<(), Error> {
        match self {
            Self::Omq { socket, dart, .. } => {
                let message = if *dart {
                    Message::with_group("fanout", bytes.to_vec())
                } else {
                    Message::from_slice(bytes)
                };
                socket.send(message)?;
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
                    let started = Instant::now();
                    loop {
                        match socket.try_recv_many_into(*batch, messages) {
                            Ok(count) if count > 0 => break,
                            Ok(_) | Err(omq_tokio::Error::WouldBlock) => {}
                            Err(error) => return Err(error.into()),
                        }
                        if started.elapsed() >= DEADLINE {
                            return Err("receive timeout".into());
                        }
                        std::thread::yield_now();
                    }
                    *next = 0;
                }
                let message = &messages[*next];
                check(message.iter().last().ok_or("empty message")?.as_ref())?;
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
                let started = Instant::now();
                loop {
                    if let Some(sample) = port.try_recv()? {
                        return check(sample.payload().to_bytes().as_ref());
                    }
                    if started.elapsed() >= DEADLINE {
                        return Err("receive timeout".into());
                    }
                    std::thread::yield_now();
                }
            }
            _ => return Err("not a subscriber".into()),
        }
        Ok(())
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

fn publish(config: &Config, port: &Port) -> Result<(), Error> {
    let listener = UnixListener::bind(config.control())?;
    listener.set_nonblocking(true)?;
    std::fs::write(config.directory.join("ready"), b"ready")?;
    let started = Instant::now();
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
    while readers.iter().any(|(_, ready)| !ready) {
        port.send(&PROBE.to_le_bytes(), false)?;
        for (stream, ready) in &mut readers {
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
    for (stream, _) in &readers {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(DEADLINE))?;
    }
    let mut bytes = vec![0xa5; config.size];
    let batch = (1024 * 1024 / config.size).clamp(1, 256);
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
        let mut counts = Vec::with_capacity(config.readers);
        let mut elapsed = Vec::with_capacity(config.readers);
        for (id, (stream, _)) in readers.iter_mut().enumerate() {
            let mut ack = [0; 16];
            stream.read_exact(&mut ack).map_err(|error| {
                format!("reader {id} round {round} acknowledgment failed: {error}")
            })?;
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
        if round > 0 {
            let total_elapsed = elapsed.iter().copied().fold(sender_elapsed, f64::max);
            let rate = sent as f64 / total_elapsed;
            println!(
                "{}",
                json!({"event":"result", "implementation":config.implementation,
                "msg_size":config.size,"readers":config.readers,"round":round,
                "offered":sent,"reader_counts":counts,"reader_elapsed":elapsed,
                "sender_elapsed":sender_elapsed,"elapsed":total_elapsed,"msgs_s":rate,
                "aggregate_msgs_s":rate * config.readers as f64,"mbps":rate * config.size as f64 / 1e6,
                "missing":0,"duplicates":0,"gaps":0,"corrupt":0})
            );
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
