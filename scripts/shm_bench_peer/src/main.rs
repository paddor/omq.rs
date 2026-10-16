//! Verified RADIO/DISH fan-out over local transports, TCP, and DART.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use omq_tokio::{Context, Message, Options, PayloadPool, SocketType};
use serde_json::json;

mod point_to_point;

const DEADLINE: Duration = Duration::from_secs(30);
const PROBE: u64 = u64::MAX;
const BENCH_HWM: u32 = 1000;
const SLAB_BYTES: usize = 4096;
const BATCH_MESSAGES: usize = 256;
const BATCH_BYTES: usize = 1024 * 1024;
const POOL_BYTES: usize = 64 * 1024 * 1024;
const LARGE_POOL_BYTES: usize = 256 * 1024 * 1024;
const LARGE_POOL_MIN_BODY: usize = 1024 * 1024;
const RECEIVE_POOL_SLOTS: usize = 1024;
const SIZES: &[usize] = &[
    16, 64, 256, 1024, 4096, 16384, 131_072, 1_048_576, 8_388_608,
];
type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
struct Config {
    implementation: String,
    publisher: bool,
    size: usize,
    readers: usize,
    id: usize,
    directory: PathBuf,
    source_pool: bool,
    receive_pool: bool,
}

impl Config {
    fn parse() -> Result<Self, Error> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 7 {
            return Err("usage: transport pub|sub size readers id directory".into());
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
            directory: PathBuf::from(&args[6]),
            source_pool: std::env::var_os("OMQ_SHM_BENCH_NO_POOL").is_none(),
            receive_pool: std::env::var_os("OMQ_SHM_BENCH_RECV_POOL").is_some(),
        };
        if ![
            "inproc",
            "ipc",
            "shm",
            "shm-pool",
            "tcp",
            "dart-lan",
            "dart-adaptive",
        ]
        .contains(&config.implementation.as_str())
            || !SIZES.contains(&config.size)
            || ![1, 4, 32].contains(&config.readers)
            || config.id >= config.readers
        {
            return Err("invalid transport, size, or reader count".into());
        }
        Ok(config)
    }

    fn endpoint(&self) -> String {
        match self.implementation.as_str() {
            "inproc" => "inproc://shm-comparison".into(),
            "ipc" => format!("ipc://{}", self.directory.join("data.sock").display()),
            "tcp" => std::fs::read_to_string(self.directory.join("endpoint"))
                .unwrap_or_else(|_| "tcp://127.0.0.1:0".into()),
            "dart-lan" | "dart-adaptive" => {
                std::fs::read_to_string(self.directory.join("endpoint"))
                    .unwrap_or_else(|_| "dart://127.0.0.1:0".into())
            }
            _ => format!("shm://{}", self.directory.join("data.sock").display()),
        }
    }

    fn control(&self) -> PathBuf {
        self.directory.join("control.sock")
    }

    fn pool_slot_size(&self) -> usize {
        self.size.max(SLAB_BYTES)
    }

    fn pool_slots(&self) -> usize {
        let bytes = if self.size >= LARGE_POOL_MIN_BODY {
            LARGE_POOL_BYTES
        } else {
            POOL_BYTES
        };
        (bytes / self.pool_slot_size()).max(1)
    }

    fn pool(&self) -> Result<Option<PayloadPool>, Error> {
        if !self.source_pool {
            return Ok(None);
        }
        let classes = [(self.pool_slot_size(), self.pool_slots())];
        Ok(Some(if self.implementation == "shm-pool" {
            omq_tokio::shm_payload_pool(classes)?
        } else {
            PayloadPool::new(classes)?
        }))
    }

    fn uses_slabs(&self) -> bool {
        self.source_pool
            && (omq_tokio::message::MAX_INLINE_MESSAGE + 1..SLAB_BYTES).contains(&self.size)
    }

    fn batch_size(&self) -> usize {
        (BATCH_BYTES / self.size).clamp(1, BATCH_MESSAGES)
    }

    fn add_pool_metadata(&self, row: &mut serde_json::Value, throughput: bool) {
        if throughput {
            row["sender_clock_batch"] = json!(if self.uses_slabs() {
                SLAB_BYTES / self.size
            } else {
                self.batch_size()
            });
        }
        row["payload_fill"] = json!(if throughput { "memset" } else { "memcpy" });
        row["pooled_batch_copy_threshold"] = json!(
            self.implementation
                .starts_with("shm")
                .then_some(if self.readers > 1 {
                    omq_shm::FANOUT_POOLED_BATCH_COPY_THRESHOLD
                } else {
                    omq_shm::POOLED_BATCH_COPY_THRESHOLD
                })
        );
        row["source_pool"] = json!(self.source_pool);
        row["receive_pool"] = json!(self.receive_pool);
        row["receive_pool_slots"] = json!(if self.receive_pool {
            RECEIVE_POOL_SLOTS
        } else {
            0
        });
        row["receive_pool_slot_bytes"] = json!(if self.receive_pool {
            self.size.max(64)
        } else {
            0
        });
        row["receive_pool_batch_recycle"] = json!(self.receive_pool);
        row["pool_slots"] = json!(if self.source_pool {
            self.pool_slots()
        } else {
            0
        });
        row["pool_slot_bytes"] = json!(if self.source_pool {
            self.pool_slot_size()
        } else {
            0
        });
        row["payload_pool_bytes"] = json!(if self.source_pool {
            self.pool_slots() * self.pool_slot_size()
        } else {
            0
        });
        row["slab_bytes"] = json!(if throughput && self.uses_slabs() {
            SLAB_BYTES
        } else {
            0
        });
    }
}

struct Port {
    socket: omq_tokio::blocking::Socket,
    pool: Option<PayloadPool>,
    messages: Vec<Message>,
    next: usize,
    batch: usize,
}

impl Port {
    fn open(config: &Config, context: &Context) -> Result<Self, Error> {
        let hwm = BENCH_HWM;
        let receive_pool = if config.receive_pool && !config.publisher {
            Some(PayloadPool::new([(
                config.size.max(64),
                RECEIVE_POOL_SLOTS,
            )])?)
        } else {
            None
        };
        let options = Options {
            xpub_nodrop: true,
            send_hwm: hwm,
            recv_hwm: hwm,
            recv_batching: true,
            recv_payload_pool: receive_pool.clone(),
            workload_profile: Some(omq_tokio::options::WorkloadProfile::Throughput),
            ..Options::default()
        };
        #[cfg(feature = "dart")]
        let options = Options {
            dart: omq_tokio::DartOptions {
                congestion: if config.implementation == "dart-lan" {
                    omq_tokio::DartCongestion::Lan
                } else {
                    omq_tokio::DartCongestion::Adaptive
                },
                ..options.dart
            },
            ..options
        };
        let socket = context.blocking_socket(
            if config.publisher {
                SocketType::Radio
            } else {
                SocketType::Dish
            },
            options,
        );
        if config.publisher {
            let endpoint = socket.bind(config.endpoint().parse::<omq_tokio::Endpoint>()?)?;
            std::fs::write(config.directory.join("endpoint"), endpoint.to_string())?;
        } else {
            socket.join("fanout")?;
            socket.connect(config.endpoint().parse::<omq_tokio::Endpoint>()?)?;
        }
        let pool = if config.publisher {
            config.pool()?
        } else {
            receive_pool
        };
        Ok(Self {
            socket,
            pool,
            messages: Vec::with_capacity(BATCH_MESSAGES),
            next: 0,
            batch: config.batch_size(),
        })
    }

    fn send(&self, bytes: &[u8], _checked: bool) -> Result<(), Error> {
        let sequence = u64::from_le_bytes(bytes[..8].try_into()?);
        let body = patterned_message(bytes.len(), sequence, self.pool.as_ref())?;
        let group = bytes::Bytes::from_static(b"fanout");
        self.socket.send_group(group, body)?;
        Ok(())
    }

    fn receive(&mut self, check: &mut dyn FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        if self.next == self.messages.len() {
            if let Some(pool) = &self.pool {
                pool.with_recycling_batch(|| self.messages.clear());
            } else {
                self.messages.clear();
            }
            let started = Instant::now();
            loop {
                match self
                    .socket
                    .try_recv_many_into(self.batch, &mut self.messages)
                {
                    Ok(count) if count > 0 => break,
                    Ok(_) | Err(omq_tokio::Error::WouldBlock) => {}
                    Err(error) => return Err(error.into()),
                }
                if started.elapsed() >= DEADLINE {
                    return Err("receive timeout".into());
                }
                std::thread::yield_now();
            }
            self.next = 0;
        }
        let message = &self.messages[self.next];
        check(
            message
                .len()
                .checked_sub(1)
                .and_then(|index| message.part_slice(index))
                .ok_or("empty message")?,
        )?;
        self.next += 1;
        Ok(())
    }
}

fn prepare_message(bytes: &[u8], pool: Option<&PayloadPool>) -> Result<Message, Error> {
    prepare_with(bytes.len(), pool, |body| body.copy_from_slice(bytes))
}

fn patterned_message(
    size: usize,
    sequence: u64,
    pool: Option<&PayloadPool>,
) -> Result<Message, Error> {
    prepare_with(size, pool, |body| {
        body.fill(0xa5);
        body[..8].copy_from_slice(&sequence.to_le_bytes());
    })
}

fn prepare_with(
    size: usize,
    pool: Option<&PayloadPool>,
    mut fill: impl FnMut(&mut [u8]),
) -> Result<Message, Error> {
    Ok(if let Some(pool) = pool {
        let mut started = None;
        loop {
            if let Some(message) = pool.try_message(size, &mut fill)? {
                break message;
            }
            if started.get_or_insert_with(Instant::now).elapsed() >= DEADLINE {
                return Err("pool credit timeout".into());
            }
            std::thread::yield_now();
        }
    } else {
        let mut body = vec![0; size];
        fill(&mut body);
        Message::from(bytes::Bytes::from(body))
    })
}

fn send_slab(
    pool: &PayloadPool,
    size: usize,
    sequence: u64,
    mut send: impl FnMut(Message) -> Result<(), Error>,
) -> Result<u64, Error> {
    let Some(mut buffer) = pool.try_buffer(SLAB_BYTES) else {
        std::thread::yield_now();
        return Ok(0);
    };
    buffer.writable().fill(0xa5);
    for (index, body) in buffer.writable().chunks_exact_mut(size).enumerate() {
        body[..8].copy_from_slice(&(sequence + index as u64).to_le_bytes());
    }
    buffer.set_len(SLAB_BYTES)?;
    let mut bytes: bytes::Bytes = buffer.into_payload().into();
    let mut count = 0;
    while !bytes.is_empty() {
        send(Message::from(bytes.split_to(size)))?;
        count += 1;
    }
    Ok(count)
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
        let batch = config.batch_size();
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
        if std::env::var_os("OMQ_SHM_BENCH_DEBUG").is_some() {
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

fn accept_readers(config: &Config, port: &Port) -> Result<Vec<(UnixStream, bool)>, Error> {
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
    Ok(readers)
}

fn publish(config: &Config, port: &Port) -> Result<(), Error> {
    let mut readers = accept_readers(config, port)?;
    let mut bytes = vec![0xa5; config.size];
    let batch = config.batch_size();
    for round in 0..4_u64 {
        let duration = Duration::from_secs(if round == 0 { 1 } else { 3 });
        let started = Instant::now();
        let mut sent = 0_u64;
        while started.elapsed() < duration {
            if config.uses_slabs() {
                sent += send_slab(port.pool.as_ref().unwrap(), config.size, sent, |message| {
                    Ok(port
                        .socket
                        .send_group(bytes::Bytes::from_static(b"fanout"), message)?)
                })?;
                continue;
            }
            for _ in 0..batch {
                bytes[..8].copy_from_slice(&sent.to_le_bytes());
                port.send(&bytes, true)?;
                sent += 1;
            }
        }
        let sender_elapsed = started.elapsed().as_secs_f64();
        port.send(&(PROBE - 1 - round).to_le_bytes(), true)?;
        if std::env::var_os("OMQ_SHM_BENCH_DEBUG").is_some() {
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
            let mut row = json!({"event":"result", "implementation":config.implementation,
                "msg_size":config.size,"readers":config.readers,"round":round,
                "send_hwm":BENCH_HWM,"recv_hwm":BENCH_HWM,
                "shm_max_memory":if matches!(config.implementation.as_str(), "shm" | "shm-pool") {
                    Some(omq_tokio::options::DEFAULT_SHM_MAX_MEMORY)
                } else { None },
                "offered":sent,"reader_counts":counts,"reader_elapsed":elapsed,
                "sender_elapsed":sender_elapsed,"elapsed":total_elapsed,"msgs_s":rate,
                "aggregate_msgs_s":rate * config.readers as f64,"mbps":rate * config.size as f64 / 1e6,
                "missing":0,"duplicates":0,"gaps":0,"corrupt":0});
            config.add_pool_metadata(&mut row, true);
            #[cfg(feature = "dart")]
            if config.implementation.starts_with("dart-") {
                let stats = omq_tokio::diagnostics::dart_stats(&port.socket);
                row["dart_wire_version"] = json!(omq_proto::dart::VERSION);
                row["dart_window_messages"] = json!(Options::default().dart.window_messages);
                row["dart_congestion"] = json!(config.implementation.trim_start_matches("dart-"));
                row["dart_retransmitted"] = json!(stats.retransmitted);
                row["dart_credit_stalls"] = json!(stats.credit_stalls);
                row["dart_congestion_stalls"] = json!(stats.congestion_stalls);
                row["dart_send_failures"] = json!(stats.send_failures);
            }
            println!("{row}");
        }
    }
    for (stream, _) in &mut readers {
        stream.write_all(&[1])?;
    }
    Ok(())
}

fn subscribers(config: &Config, context: &Context) -> Result<(), Error> {
    let mut workers = Vec::with_capacity(config.readers);
    for id in 0..config.readers {
        let mut config = config.clone();
        config.id = id;
        config.publisher = false;
        let context = context.clone();
        workers.push(std::thread::spawn(move || {
            let port = Port::open(&config, &context)?;
            subscribe(&config, port)
        }));
    }
    for worker in workers {
        worker.join().map_err(|_| "subscriber thread panicked")??;
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    if std::env::args()
        .nth(1)
        .is_some_and(|arg| matches!(arg.as_str(), "scatter-gather" | "client-server"))
    {
        return point_to_point::run();
    }
    let config = Config::parse()?;
    let context = Context::new();
    if config.publisher {
        let port = Port::open(&config, &context)?;
        if config.implementation == "inproc" {
            std::thread::scope(|scope| {
                let receiver = scope.spawn(|| subscribers(&config, &context));
                let result = publish(&config, &port);
                receiver
                    .join()
                    .map_err(|_| "subscriber process panicked")??;
                result
            })
        } else {
            publish(&config, &port)
        }
    } else {
        subscribers(&config, &context)
    }
}
