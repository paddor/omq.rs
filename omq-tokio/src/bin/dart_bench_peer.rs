//! Two-process Dart/TCP/QUIC peers. Only socket APIs carry measured messages.
//! Stdin coordinates the measurement window; stdout contains JSON events.

use std::collections::VecDeque;
use std::io::{BufRead, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use omq_tokio::blocking::{BlockingRecvCancel, Socket};
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{
    Context, DartCongestion, DartPool, DartStats, Message, Options, SocketType, TrySendError,
};

#[path = "perf_verify/affinity.rs"]
mod affinity;

#[path = "dart_bench/current.rs"]
mod current;

#[cfg(feature = "quic")]
mod ws_bench_config;

const BATCH: usize = 64;
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeMode {
    Owned,
    Current,
    CurrentPoll,
}

struct Config {
    role: String,
    endpoint: omq_tokio::Endpoint,
    size: usize,
    duration: Duration,
    iterations: usize,
    warmup: usize,
    throughput_warmup: Duration,
    drain: Duration,
    spin: Duration,
    io_spin: Duration,
    window_messages: usize,
    congestion: DartCongestion,
    runtime: RuntimeMode,
}

impl Config {
    fn parse() -> Self {
        let args: Vec<_> = std::env::args().collect();
        assert!(
            (10..=11).contains(&args.len()),
            "role endpoint size seconds iterations warmup spin_us io_spin_us lan|adaptive [current|current-poll]"
        );
        let runtime = match args.get(10).map(String::as_str) {
            None => RuntimeMode::Owned,
            Some("current") => RuntimeMode::Current,
            Some("current-poll") => RuntimeMode::CurrentPoll,
            Some(_) => panic!("unknown runtime mode"),
        };
        let size = args[3].parse().expect("body size");
        let spin = spin_budget(&args[7]);
        let io_spin = spin_budget(&args[8]);
        assert!((16..=8_388_608).contains(&size));
        #[cfg(feature = "quic")]
        ws_bench_config::set_endpoint(Some(&args[2]));
        assert!(spin <= Duration::from_micros(50) || spin == Duration::MAX);
        assert!(io_spin <= Duration::from_micros(50) || io_spin == Duration::MAX);
        assert!(runtime == RuntimeMode::Owned || (spin.is_zero() && io_spin.is_zero()));
        Self {
            role: args[1].clone(),
            endpoint: args[2].parse().expect("endpoint"),
            size,
            duration: Duration::from_secs_f64(args[4].parse().expect("seconds")),
            iterations: args[5].parse().expect("iterations"),
            warmup: args[6].parse().expect("warmup"),
            throughput_warmup: Duration::from_secs_f64(
                std::env::var("OMQ_DART_WARMUP_SECS")
                    .map_or(0.2, |value| value.parse().expect("warmup seconds")),
            ),
            drain: Duration::from_secs_f64(
                std::env::var("OMQ_DART_DRAIN_SECS")
                    .map_or(2.0, |value| value.parse().expect("drain seconds")),
            ),
            spin,
            io_spin,
            window_messages: std::env::var("OMQ_DART_WINDOW_MESSAGES")
                .map_or(256, |value| value.parse().expect("window_messages")),
            runtime,
            congestion: match args[9].as_str() {
                "lan" => DartCongestion::Lan,
                "adaptive" => DartCongestion::Adaptive,
                _ => panic!("unknown congestion mode"),
            },
        }
    }

    fn socket_type(&self) -> SocketType {
        match self.role.as_str() {
            "scatter" => SocketType::Scatter,
            "gather" => SocketType::Gather,
            "client" => SocketType::Client,
            "server" => SocketType::Server,
            _ => panic!("unknown role"),
        }
    }

    fn receiving(&self) -> bool {
        matches!(self.role.as_str(), "gather" | "server")
    }

    fn options(&self) -> Options {
        let mut options = Options::default().recv_spin(self.spin);
        options.workload_profile = Some(if matches!(self.role.as_str(), "client" | "server") {
            WorkloadProfile::Latency
        } else {
            WorkloadProfile::Throughput
        });
        options.recv_batching = matches!(self.role.as_str(), "scatter" | "gather");
        options.dart.io_spin = self.io_spin;
        options.dart.window_messages = self.window_messages;
        options.dart.congestion = self.congestion;
        #[cfg(feature = "quic")]
        ws_bench_config::configure(&mut options);
        options
    }
}

fn spin_budget(argument: &str) -> Duration {
    if argument == "continuous" {
        Duration::MAX
    } else {
        Duration::from_micros(argument.parse().expect("spin_us"))
    }
}

fn emit(event: &str) {
    println!("{event}");
    std::io::stdout().flush().unwrap();
}

fn offloads(socket: &Socket) -> String {
    format_offloads(socket.dart_capabilities())
}

fn format_offloads(capabilities: Option<omq_tokio::DartCapabilities>) -> String {
    let Some(capabilities) = capabilities else {
        return "null".into();
    };
    let ecn = |available: Option<bool>| match available {
        Some(true) => "true",
        Some(false) => "false",
        None => "null",
    };
    format!(
        "{{\"gso_segments\":{},\"gro_segments\":{},\"ecn_ipv4\":{},\"ecn_ipv6\":{},\"may_fragment\":{}}}",
        capabilities.max_gso_segments,
        capabilities.max_gro_segments,
        ecn(capabilities.ecn_ipv4),
        ecn(capabilities.ecn_ipv6),
        capabilities.may_fragment,
    )
}

fn start() -> Instant {
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    let epoch = UNIX_EPOCH + Duration::from_nanos(line.trim().parse().expect("start_ns"));
    let delay = epoch.duration_since(SystemTime::now()).expect("late start");
    let at = Instant::now() + delay;
    std::thread::sleep(delay);
    at
}

fn timed_cancel(at: Instant) -> Arc<BlockingRecvCancel> {
    let cancel = Arc::new(BlockingRecvCancel::new());
    cancel.register_current_thread_once();
    let timer = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(at.saturating_duration_since(Instant::now()));
        timer.cancel();
    });
    cancel
}

fn make_body(pool: &DartPool, size: usize, tag: u64) -> Option<Message> {
    if size > omq_proto::dart::MAX_BODY {
        let mut body = vec![7; size];
        body[..8].copy_from_slice(&tag.to_le_bytes());
        return Some(Message::single(body));
    }
    let mut body = pool.try_take()?;
    let writable = body.writable();
    writable[..size].fill(7);
    writable[..8].copy_from_slice(&tag.to_le_bytes());
    body.set_len(size).unwrap();
    Some(body.into_message())
}

// A failed attempt keeps polling for one bounded window before yielding.
// Any progress starts a fresh window; zero spin yields immediately.
struct CapacityWait {
    spin: Duration,
    deadline: Option<Instant>,
}

impl CapacityWait {
    fn stalled(&mut self, now: Instant) {
        if self.spin == Duration::MAX {
            std::hint::spin_loop();
            return;
        }
        let deadline = *self.deadline.get_or_insert_with(|| now + self.spin);
        if now >= deadline {
            std::thread::yield_now();
            self.deadline = None;
        } else {
            std::hint::spin_loop();
        }
    }

    fn progressed(&mut self) {
        self.deadline = None;
    }
}

fn scatter(socket: &Socket, config: &Config, native: bool, at: Instant) {
    let pool = if native {
        socket.dart_pool().unwrap().clone()
    } else {
        DartPool::new(1024)
    };
    let pooled = config.size < omq_proto::dart::MAX_BODY
        || (native && config.size == omq_proto::dart::MAX_BODY);
    let mut cache = (!pooled).then(|| BodyCache::new(config.size));
    let measure_at = at + config.throughput_warmup;
    let until = measure_at + config.duration;
    let mut queued = VecDeque::with_capacity(BATCH);
    let mut buffers = Vec::with_capacity(BATCH);
    let mut offered = 0u64;
    let mut warmup_offered = 0u64;
    let mut pool_empty = 0u64;
    let mut measured = false;
    let mut sequence = 0u64;
    let mut wait = CapacityWait {
        spin: config.spin,
        deadline: None,
    };
    let mut now = at;
    let mut clock_stride = 0;
    loop {
        // Successful batches keep an exact phase/deadline check. Empty pool
        // and capacity probes share one timestamp for up to eight attempts.
        if clock_stride == 0 {
            now = Instant::now();
        }
        clock_stride = (clock_stride + 1) % 8;
        if now >= until {
            break;
        }
        if queued.is_empty() {
            measured = now >= measure_at;
            // Each batch has one phase, including retries across the boundary.
            // Warmup bodies stay distinguishable after reordering or delays.
            if pooled {
                pool.try_take_many_into(BATCH, &mut buffers);
                pool_empty += u64::from(buffers.is_empty());
            }
            for mut body in buffers.drain(..) {
                let tag = sequence | (u64::from(measured) << 63);
                prepare_pooled_body(&mut body, config.size, tag);
                sequence += 1;
                queued.push_back(body.into_message());
            }
            if let Some(cache) = &mut cache {
                prepare_cached_batch(cache, &mut queued, &mut sequence, measured);
            }
        }
        if queued.is_empty() {
            wait.stalled(now);
            continue;
        }
        let count = match socket.try_send_many(&mut queued, BATCH) {
            Ok(count) => count,
            Err(TrySendError::Full(body)) => {
                queued.push_front(body);
                0
            }
            Err(error) => panic!("send failed: {error}"),
        };
        if measured {
            offered += count as u64;
        } else {
            warmup_offered += count as u64;
        }
        if count == 0 {
            wait.stalled(now);
        } else {
            wait.progressed();
            clock_stride = 0;
        }
    }
    drop(queued);
    std::thread::sleep((config.drain / 2).min(Duration::from_secs(1)));
    if native {
        wait_acknowledged(
            socket,
            offered + warmup_offered,
            until + config.drain,
            &mut wait,
        );
    }
    scatter_result(socket, config, native, offered, warmup_offered, pool_empty);
}

fn scatter_result(
    socket: &Socket,
    config: &Config,
    native: bool,
    offered: u64,
    warmup_offered: u64,
    pool_empty: u64,
) {
    let stats = socket.dart_stats();
    let unacknowledged = if native {
        (offered + warmup_offered).saturating_sub(stats.acknowledged)
    } else {
        0
    };
    let sent = if native {
        stats
            .sent_messages
            .saturating_sub(warmup_offered)
            .to_string()
    } else {
        "null".into()
    };
    emit(&format!(
        "{{\"event\":\"result\",\"offered\":{offered},\"warmup_offered\":{warmup_offered},\"unacknowledged\":{unacknowledged},\"seconds\":{},\"sent\":{sent},\"pool_empty\":{pool_empty},\"send_failures\":{},\"retransmitted\":{},\"credit_stalls\":{},\"congestion_stalls\":{},\"ecn_failures\":{},\"offloads\":{}}}",
        config.duration.as_secs_f64(),
        stats.send_failures,
        stats.retransmitted,
        stats.credit_stalls,
        stats.congestion_stalls,
        stats.ecn_failures,
        offloads(socket),
    ));
}

fn prepare_pooled_body(body: &mut omq_tokio::DartBuffer, size: usize, tag: u64) {
    let writable = body.writable();
    writable[..size].fill(7);
    writable[..8].copy_from_slice(&tag.to_le_bytes());
    writable[8..16].copy_from_slice(&(!tag).to_le_bytes());
    body.set_len(size).unwrap();
}

fn prepare_cached_batch(
    cache: &mut BodyCache,
    queued: &mut VecDeque<Message>,
    sequence: &mut u64,
    measured: bool,
) {
    let size = cache.bodies.front().expect("bounded body cache").len();
    for _ in 0..BATCH.min((64_000 / size).max(1)) {
        let bytes = cache.bodies.pop_front().expect("bounded body cache");
        // The cache keeps one immutable view. Reuse is allowed only after
        // every transport/repair reference has gone; no bytes change in flight.
        let mut body = match bytes.try_into_mut() {
            Ok(body) => body,
            Err(bytes) => {
                cache.bodies.push_front(bytes);
                break;
            }
        };
        let tag = *sequence | (u64::from(measured) << 63);
        body[..8].copy_from_slice(&tag.to_le_bytes());
        body[8..16].copy_from_slice(&(!tag).to_le_bytes());
        let body = body.freeze();
        *sequence += 1;
        queued.push_back(Message::single(body.clone()));
        cache.bodies.push_back(body);
    }
}

struct BodyCache {
    bodies: VecDeque<bytes::Bytes>,
}

impl BodyCache {
    fn new(size: usize) -> Self {
        let count = (1024 * 1024 / size).clamp(BATCH, 1024);
        Self {
            bodies: (0..count)
                .map(|_| bytes::Bytes::from(vec![7; size]))
                .collect(),
        }
    }
}

fn wait_acknowledged(socket: &Socket, count: u64, deadline: Instant, wait: &mut CapacityWait) {
    while socket.dart_stats().acknowledged < count {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        wait.stalled(now);
    }
}

fn gather(socket: &Socket, config: &Config, at: Instant) {
    let until = at + config.throughput_warmup + config.duration;
    let cancel = timed_cancel(until + config.drain);
    let mut batch = Vec::with_capacity(256);
    let pool = socket.dart_pool().ok().cloned();
    let mut received = 0u64;
    let mut total = 0u64;
    let mut expected = 0u64;
    let mut duplicates = 0u64;
    let mut gaps = 0u64;
    let mut corrupt = 0u64;
    while socket
        .recv_many_registered_cancelable_into(256, &cancel, &mut batch)
        .unwrap()
        .is_some()
    {
        let mut measured = 0u64;
        for message in &batch {
            let body = message.part_slice(0).expect("single body");
            if body.len() != config.size {
                corrupt += 1;
                continue;
            }
            let tag = u64::from_le_bytes(body[..8].try_into().unwrap());
            let check = u64::from_le_bytes(body[8..16].try_into().unwrap());
            if check != !tag || !valid_padding(&body[16..]) {
                corrupt += 1;
            }
            let sequence = tag & !(1 << 63);
            if sequence < expected {
                duplicates += 1;
            } else {
                gaps += sequence - expected;
                expected = sequence + 1;
            }
            measured += tag >> 63;
        }
        total += measured;
        if Instant::now() <= until {
            received += measured;
        }
        if let Some(pool) = &pool {
            while !batch.is_empty() {
                pool.recycle_many(&mut batch, BATCH);
            }
        } else {
            batch.clear();
        }
    }
    let stats = socket.dart_stats();
    emit(&format!(
        "{{\"event\":\"result\",\"received\":{received},\"received_total\":{total},\"duplicates\":{duplicates},\"gaps\":{gaps},\"corrupt\":{corrupt},\"seconds\":{},\"received_datagrams\":{},\"pool_exhausted\":{},\"receive_overflow\":{},\"invalid_datagrams\":{},\"receive_failures\":{},\"ect0\":{},\"ect1\":{},\"ce\":{},\"ecn_unavailable\":{},\"offloads\":{}}}",
        config.duration.as_secs_f64(),
        stats.received_datagrams,
        stats.pool_exhausted,
        stats.receive_overflow,
        stats.invalid_datagrams,
        stats.receive_failures,
        stats.ect0,
        stats.ect1,
        stats.ce,
        stats.ecn_unavailable,
        offloads(socket),
    ));
}

fn valid_padding(bytes: &[u8]) -> bool {
    // Check every byte without a branch per byte. The reduction can be
    // vectorized; scalar early-exit iteration capped verified TCP at 2 GB/s.
    let (words, tail) = bytes.as_chunks::<8>();
    let difference = words.iter().fold(0, |difference, word| {
        difference | (u64::from_ne_bytes(*word) ^ 0x0707_0707_0707_0707)
    });
    difference == 0 && tail.iter().all(|byte| *byte == 7)
}

fn server(socket: &Socket) {
    let cancel = Arc::new(BlockingRecvCancel::new());
    cancel.register_current_thread_once();
    let stop = cancel.clone();
    std::thread::spawn(move || {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        stop.cancel();
    });
    let mut replies = Vec::with_capacity(BATCH);
    while socket
        .recv_many_registered_cancelable_into(BATCH, &cancel, &mut replies)
        .unwrap()
        .is_some()
    {
        for reply in replies.drain(..) {
            socket.send(reply).unwrap();
        }
    }
    emit(&format!(
        "{{\"event\":\"result\",\"offloads\":{},{} }}",
        offloads(socket),
        latency_counters(socket.dart_stats()),
    ));
}

fn client(socket: &Socket, config: &Config, native: bool) {
    let pool = if native {
        socket.dart_pool().unwrap().clone()
    } else {
        DartPool::new(1024)
    };
    let mut samples = Vec::with_capacity(config.iterations);
    for index in 0..config.warmup + config.iterations {
        let tag = u64::try_from(index).unwrap();
        let body = make_body(&pool, config.size, tag).expect("send pool exhausted");
        let at = Instant::now();
        socket.send(body).unwrap();
        let reply = socket
            .recv_timeout(Duration::from_secs(1))
            .expect("RTT timeout");
        let elapsed = at.elapsed().as_secs_f64() * 1e6;
        validate_reply(&reply, config.size, tag);
        if index >= config.warmup {
            samples.push(elapsed);
        }
    }
    latency_result(&mut samples, config, &offloads(socket), socket.dart_stats());
}

fn validate_reply(reply: &Message, size: usize, tag: u64) {
    assert_eq!(reply.byte_len(), size);
    assert_eq!(&reply.part_slice(0).unwrap()[..8], &tag.to_le_bytes());
    assert!(
        reply.part_slice(0).unwrap()[8..]
            .iter()
            .all(|byte| *byte == 7)
    );
}

fn save_latency_samples(samples: &[f64]) {
    let Some(path) = std::env::var_os("OMQ_DART_RTT_SAMPLES") else {
        return;
    };
    // Preserve chronological order. File I/O happens after every timed RTT.
    let mut output = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    writeln!(output, "sample,rtt_us").unwrap();
    for (index, sample) in samples.iter().enumerate() {
        writeln!(output, "{index},{sample:.6}").unwrap();
    }
    output.flush().unwrap();
}

fn latency_counters(stats: DartStats) -> String {
    format!(
        "\"retransmitted\":{},\"credit_stalls\":{},\"congestion_stalls\":{},\"send_failures\":{},\"receive_failures\":{},\"invalid_datagrams\":{},\"ecn_failures\":{}",
        stats.retransmitted,
        stats.credit_stalls,
        stats.congestion_stalls,
        stats.send_failures,
        stats.receive_failures,
        stats.invalid_datagrams,
        stats.ecn_failures,
    )
}

fn latency_result(samples: &mut [f64], config: &Config, offloads: &str, stats: DartStats) {
    save_latency_samples(samples);
    samples.sort_by(f64::total_cmp);
    let quantile = |numerator: usize, denominator: usize| {
        samples[((samples.len() - 1) * numerator).div_ceil(denominator)]
    };
    emit(&format!(
        "{{\"event\":\"result\",\"iterations\":{},\"p50_us\":{},\"p99_us\":{},\"p999_us\":{},\"max_us\":{},\"timeouts\":0,\"offloads\":{},{} }}",
        config.iterations,
        quantile(50, 100),
        quantile(99, 100),
        quantile(999, 1000),
        samples.last().unwrap(),
        offloads,
        latency_counters(stats),
    ));
}

fn main() {
    let config = Config::parse();
    let affinity = affinity::Affinity::from_env();
    if config.runtime != RuntimeMode::Owned {
        assert!(matches!(config.role.as_str(), "client" | "server"));
        affinity.pin(if config.receiving() { 5 } else { 0 });
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .event_interval(if config.runtime == RuntimeMode::CurrentPoll {
                1
            } else {
                61
            })
            .build()
            .unwrap()
            .block_on(current::run(&config, &affinity));
        return;
    }
    let placement = if config.receiving() {
        affinity::Side::Receiver
    } else {
        affinity::Side::Sender
    };
    let context = Context::with_name("dartbench");
    affinity.pin_context("dartbench", 1, placement);
    affinity.pin(if config.receiving() { 5 } else { 0 });
    let native = matches!(config.endpoint, omq_tokio::Endpoint::Dart { .. });
    let socket = context.blocking_socket(config.socket_type(), config.options());
    if config.receiving() {
        let endpoint = socket.bind(config.endpoint.clone()).unwrap();
        emit(&format!(
            "{{\"event\":\"bound\",\"endpoint\":\"{endpoint}\"}}"
        ));
    } else {
        socket.connect(config.endpoint.clone()).unwrap();
    }
    socket.wait_connected(1, SETUP_TIMEOUT).unwrap();
    let wire_version = if native {
        omq_proto::dart::VERSION.to_string()
    } else {
        "null".into()
    };
    emit(&format!(
        "{{\"event\":\"ready\",\"affinity\":\"{}\",\"offloads\":{},\"dart_wire_version\":{wire_version},\"dart_window_messages\":{}}}",
        affinity.description(),
        offloads(&socket),
        config.window_messages,
    ));
    let at = start();
    match config.role.as_str() {
        "scatter" => scatter(&socket, &config, native, at),
        "gather" => gather(&socket, &config, at),
        "client" => client(&socket, &config, native),
        "server" => server(&socket),
        _ => unreachable!(),
    }
    socket.close().unwrap();
    context.term();
}
