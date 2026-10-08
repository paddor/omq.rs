//! Raw Quinn DATAGRAM benchmark peer: no OMQ sockets, framing, or queues.
//! Use `omq-bench run quinn-datagram` for the two-process coordinator.

use std::{
    error::Error,
    future::{Future, poll_fn},
    io::{self, BufRead, IoSliceMut, Write},
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use quinn::{AsyncUdpSocket, Connection, Runtime, UdpPoller, udp};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::sync::{mpsc, oneshot};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const WARMUP_TIME: Duration = Duration::from_millis(200);
const TAIL: Duration = Duration::from_millis(200);
const QUEUE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct Config {
    role: String,
    address: SocketAddr,
    size: usize,
    seconds: f64,
    iterations: usize,
    warmup: usize,
    layout: String,
    app_spin: Duration,
    io_spin: Duration,
    directory: PathBuf,
    app_cpu: usize,
    io_cpu: usize,
    batch: usize,
    event_interval: u32,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self> {
        if args.len() != 15 {
            return Err("role address size seconds iterations warmup layout app_spin_us io_spin_us cert_dir app_cpu io_cpu batch event_interval".into());
        }
        let config = Self {
            role: args[1].clone(),
            address: args[2].parse()?,
            size: args[3].parse()?,
            seconds: args[4].parse()?,
            iterations: args[5].parse()?,
            warmup: args[6].parse()?,
            layout: args[7].clone(),
            app_spin: Duration::from_micros(args[8].parse()?),
            io_spin: Duration::from_micros(args[9].parse()?),
            directory: PathBuf::from(&args[10]),
            app_cpu: args[11].parse()?,
            io_cpu: args[12].parse()?,
            batch: args[13].parse()?,
            event_interval: args[14].parse()?,
        };
        if !(16..=1024).contains(&config.size)
            || config.seconds <= 0.0
            || !config.seconds.is_finite()
            || config.iterations == 0
            || !(1..=4096).contains(&config.batch)
            || config.event_interval == 0
            || !matches!(config.layout.as_str(), "inline" | "split" | "multi")
            || !matches!(
                config.role.as_str(),
                "send" | "receive" | "client" | "server"
            )
            || config.app_spin > Duration::from_micros(50)
            || config.io_spin > Duration::from_micros(50)
            || (config.layout != "split" && !config.app_spin.is_zero())
        {
            return Err(
                "invalid benchmark configuration (only split can spin the application)".into(),
            );
        }
        Ok(config)
    }

    fn receiving(&self) -> bool {
        matches!(self.role.as_str(), "receive" | "server")
    }
}

fn emit(line: &str) -> io::Result<()> {
    println!("{line}");
    io::stdout().flush()
}

fn pin(cpu: usize) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(io::Error::other("CPU ID exceeds CPU_SETSIZE"));
        }
        // SAFETY: set is initialized, cpu is in range, and the supplied size is correct.
        unsafe {
            let mut set = std::mem::zeroed::<libc::cpu_set_t>();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);
            if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &raw const set) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpu;
        Err(io::Error::other(
            "explicit benchmark CPU placement requires Linux",
        ))
    }
}

// Probe the UDP receive queue during a bounded active window. A failed probe
// schedules another endpoint turn, allowing connection tasks to run in between.
// Once idle, use Quinn's ordinary Tokio readiness registration. Packet
// protection, congestion control, pacing, GSO, and GRO remain Quinn's own.
#[derive(Debug)]
struct Activity {
    epoch: Instant,
    last_ns: AtomicU64,
}

impl Activity {
    fn mark(&self) {
        self.last_ns.store(
            self.epoch.elapsed().as_nanos() as u64 + 1,
            Ordering::Relaxed,
        );
    }

    fn active(&self, spin: Duration) -> bool {
        (self.epoch.elapsed().as_nanos() as u64)
            < self.last_ns.load(Ordering::Relaxed) + spin.as_nanos() as u64
    }
}

#[derive(Debug)]
struct ActiveSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    activity: Arc<Activity>,
    probe: UdpSocket,
    probe_state: udp::UdpSocketState,
    spin: Duration,
    latency: bool,
}

impl AsyncUdpSocket for ActiveSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        self.inner.try_send(transmit)?;
        self.activity.mark();
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let count = if self.latency { 1 } else { bufs.len() };
        let result = match self.probe_state.recv(
            (&self.probe).into(),
            &mut bufs[..count],
            &mut meta[..count],
        ) {
            Ok(count) => Poll::Ready(Ok(count)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if self.activity.active(self.spin) {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    self.inner.poll_recv(cx, bufs, meta)
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        };
        if matches!(result, Poll::Ready(Ok(count)) if count > 0) {
            self.activity.mark();
        }
        result
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

fn transport() -> Arc<quinn::TransportConfig> {
    let mut config = quinn::TransportConfig::default();
    config
        .max_concurrent_bidi_streams(0_u8.into())
        .max_concurrent_uni_streams(0_u8.into())
        .datagram_receive_buffer_size(Some(QUEUE_BYTES))
        .datagram_send_buffer_size(QUEUE_BYTES)
        .max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
    Arc::new(config)
}

fn tls_configs(config: &Config) -> Result<(quinn::ServerConfig, quinn::ClientConfig)> {
    let cert = CertificateDer::from(std::fs::read(config.directory.join("cert.der"))?);
    let key = PrivatePkcs8KeyDer::from(std::fs::read(config.directory.join("key.der"))?);
    let mut provider = rustls::crypto::ring::default_provider();
    provider.cipher_suites = vec![rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256];
    let provider = Arc::new(provider);
    let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key.into())?;
    server.alpn_protocols = vec![b"quinn-datagram-bench".to_vec()];
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols.clone_from(&server.alpn_protocols);
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server)?,
    ));
    server.transport = transport();
    let mut client = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client)?,
    ));
    client.transport_config(transport());
    Ok((server, client))
}

async fn connect(config: &Config) -> Result<(quinn::Endpoint, Connection)> {
    let (server, client) = tls_configs(config)?;
    let address = if config.receiving() {
        config.address
    } else {
        "127.0.0.1:0".parse()?
    };
    let udp = UdpSocket::bind(address)?;
    udp.set_nonblocking(true)?;
    let sock = socket2::SockRef::from(&udp);
    sock.set_recv_buffer_size(QUEUE_BYTES)?;
    sock.set_send_buffer_size(QUEUE_BYTES)?;
    let recv_buffer = sock.recv_buffer_size()?;
    let send_buffer = sock.send_buffer_size()?;
    let runtime = Arc::new(quinn::TokioRuntime);
    let probe = udp.try_clone()?;
    let probe_state = udp::UdpSocketState::new((&probe).into())?;
    let mut socket = runtime.wrap_udp_socket(udp)?;
    let gso = socket.max_transmit_segments();
    let gro = socket.max_receive_segments();
    if !config.io_spin.is_zero() {
        let activity = Arc::new(Activity {
            epoch: Instant::now(),
            last_ns: AtomicU64::new(0),
        });
        socket = Arc::new(ActiveSocket {
            inner: socket,
            activity,
            probe,
            probe_state,
            spin: config.io_spin,
            latency: matches!(config.role.as_str(), "client" | "server"),
        });
    }
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        config.receiving().then_some(server),
        socket,
        runtime,
    )?;
    endpoint.set_default_client_config(client);
    if config.receiving() {
        emit(&format!(
            "{{\"event\":\"bound\",\"endpoint\":\"{}\"}}",
            endpoint.local_addr()?
        ))?;
    }
    let connection = tokio::time::timeout(Duration::from_secs(5), async {
        if config.receiving() {
            Ok::<_, Box<dyn Error + Send + Sync>>(
                endpoint.accept().await.ok_or("listener closed")?.await?,
            )
        } else {
            Ok(endpoint.connect(config.address, "localhost")?.await?)
        }
    })
    .await??;
    if connection
        .max_datagram_size()
        .is_none_or(|size| size < config.size)
    {
        return Err("negotiated DATAGRAM size is too small".into());
    }
    emit(&format!(
        "{{\"event\":\"ready\",\"gso_segments\":{gso},\"gro_segments\":{gro},\"send_buffer\":{send_buffer},\"recv_buffer\":{recv_buffer},\"max_datagram_size\":{}}}",
        connection.max_datagram_size().unwrap()
    ))?;
    Ok((endpoint, connection))
}

async fn receive(
    connection: &Connection,
    spin: Duration,
) -> std::result::Result<Bytes, quinn::ConnectionError> {
    if spin.is_zero() {
        return connection.read_datagram().await;
    }
    let future = connection.read_datagram();
    tokio::pin!(future);
    poll_fn(|cx| {
        let until = Instant::now() + spin;
        loop {
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => return Poll::Ready(result),
                Poll::Pending if Instant::now() < until => std::hint::spin_loop(),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await
}

fn stats(connection: &Connection) -> String {
    let s = connection.stats();
    format!(
        "{{\"udp_tx\":{},\"udp_rx\":{},\"udp_tx_ios\":{},\"udp_rx_ios\":{},\"udp_tx_bytes\":{},\"udp_rx_bytes\":{},\"datagram_tx\":{},\"datagram_rx\":{},\"acks_tx\":{},\"acks_rx\":{},\"lost_packets\":{},\"congestion_events\":{},\"cwnd\":{},\"mtu\":{},\"path_rtt_us\":{}}}",
        s.udp_tx.datagrams,
        s.udp_rx.datagrams,
        s.udp_tx.ios,
        s.udp_rx.ios,
        s.udp_tx.bytes,
        s.udp_rx.bytes,
        s.frame_tx.datagram,
        s.frame_rx.datagram,
        s.frame_tx.acks,
        s.frame_rx.acks,
        s.path.lost_packets,
        s.path.congestion_events,
        s.path.cwnd,
        s.path.current_mtu,
        s.path.rtt.as_secs_f64() * 1e6,
    )
}

async fn start(commands: &mut mpsc::Receiver<String>) -> Result<Instant> {
    let epoch_ns: u64 = commands
        .recv()
        .await
        .ok_or("stdin closed")?
        .trim()
        .parse()?;
    let delay = (UNIX_EPOCH + Duration::from_nanos(epoch_ns)).duration_since(SystemTime::now())?;
    let at = Instant::now() + delay;
    tokio::time::sleep_until(at.into()).await;
    Ok(at)
}

async fn send_throughput(config: &Config, connection: &Connection, at: Instant) -> Result<()> {
    // Immutable, reusable bodies deliberately exclude per-message allocation
    // from this transport ceiling. The first byte separates warmup traffic.
    let warm = Bytes::from(vec![0; config.size]);
    let measured = Bytes::from(vec![1; config.size]);
    let begin = at + WARMUP_TIME;
    let end = begin + Duration::from_secs_f64(config.seconds);
    let batch = config.batch.min(65536 / config.size);
    let mut offered = 0_u64;
    let mut warmup = 0_u64;
    let sending = async {
        while Instant::now() < end {
            let measurement = Instant::now() >= begin;
            let payload = if measurement { &measured } else { &warm };
            for _ in 0..batch {
                // Unlike send_datagram(), this does not evict older queued data.
                connection.send_datagram_wait(payload.clone()).await?;
                if measurement {
                    offered += 1;
                } else {
                    warmup += 1;
                }
            }
            tokio::task::yield_now().await;
        }
        Ok::<(), quinn::SendDatagramError>(())
    };
    tokio::select! {
        result = sending => result?,
        () = tokio::time::sleep_until(end.into()) => {},
    }
    // Let both the protocol driver and receiver drain the final queued data.
    tokio::time::sleep(TAIL).await;
    emit(&format!(
        "{{\"event\":\"result\",\"offered\":{offered},\"warmup\":{warmup},\"stats\":{}}}",
        stats(connection)
    ))?;
    Ok(())
}

async fn receive_throughput(config: &Config, connection: &Connection, at: Instant) -> Result<()> {
    let begin = at + WARMUP_TIME;
    let end = begin + Duration::from_secs_f64(config.seconds);
    let tail = end + TAIL + Duration::from_millis(100);
    let batch = config.batch.min(65536 / config.size);
    let mut received = 0_u64;
    let mut received_total = 0_u64;
    let receiving = async {
        loop {
            for _ in 0..batch {
                let body = receive(connection, config.app_spin).await?;
                if body.len() != config.size || body[0] > 1 {
                    return Err::<(), Box<dyn Error + Send + Sync>>(
                        "invalid throughput body".into(),
                    );
                }
                if body[0] == 1 {
                    received_total += 1;
                    if Instant::now() < end {
                        received += 1;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    };
    // One timer for the whole measurement, avoiding timer registration and
    // cancellation on every immediately available application datagram.
    tokio::select! {
        result = receiving => result?,
        () = tokio::time::sleep_until(tail.into()) => {},
    }
    emit(&format!(
        "{{\"event\":\"result\",\"received\":{received},\"received_total\":{received_total},\"stats\":{}}}",
        stats(connection)
    ))?;
    Ok(())
}

async fn latency_client(config: &Config, connection: &Connection) -> Result<()> {
    let mut samples = Vec::with_capacity(config.iterations);
    for index in 0..config.warmup + config.iterations {
        let mut body = vec![7; config.size];
        body[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let body = Bytes::from(body);
        let started = Instant::now();
        connection.send_datagram(body.clone())?;
        let reply =
            tokio::time::timeout(Duration::from_secs(1), receive(connection, config.app_spin))
                .await??;
        let elapsed = started.elapsed().as_nanos() as u64;
        if reply != body {
            return Err("reply changed payload or sequence".into());
        }
        if index >= config.warmup {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    let percentile = |n: usize| {
        samples[((samples.len() * n).div_ceil(1000) - 1).min(samples.len() - 1)] as f64 / 1000.0
    };
    emit(&format!(
        "{{\"event\":\"result\",\"p50_us\":{},\"p99_us\":{},\"p999_us\":{},\"max_us\":{},\"timeouts\":0,\"stats\":{}}}",
        percentile(500),
        percentile(990),
        percentile(999),
        samples[samples.len() - 1] as f64 / 1000.0,
        stats(connection),
    ))?;
    Ok(())
}

async fn latency_server(
    config: &Config,
    connection: &Connection,
    commands: &mut mpsc::Receiver<String>,
) -> Result<()> {
    let mut echoed = 0_u64;
    loop {
        let body = tokio::select! {
            command = commands.recv() => {
                if command.as_deref().map(str::trim) != Some("STOP") { return Err("expected STOP".into()); }
                emit(&format!("{{\"event\":\"result\",\"echoed\":{echoed},\"stats\":{}}}", stats(connection)))?;
                return Ok(());
            }
            body = receive(connection, config.app_spin) => body?,
        };
        if body.len() != config.size {
            return Err("invalid request size".into());
        }
        connection.send_datagram(body)?;
        echoed += 1;
        if echoed.is_multiple_of(64) {
            tokio::task::yield_now().await;
        }
    }
}

async fn run(config: Config) -> Result<()> {
    let (sender, mut commands) = mpsc::channel(4);
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if sender.blocking_send(line).is_err() {
                break;
            }
        }
    });
    let (_endpoint, connection) = connect(&config).await?;
    let at = start(&mut commands).await?;
    match config.role.as_str() {
        "send" => {
            send_throughput(&config, &connection, at).await?;
            commands.recv().await.ok_or("stdin closed before STOP")?;
            Ok(())
        }
        "receive" => receive_throughput(&config, &connection, at).await,
        "client" => {
            latency_client(&config, &connection).await?;
            // Keep the connection alive until the server reports its result.
            commands.recv().await.ok_or("stdin closed before STOP")?;
            Ok(())
        }
        "server" => latency_server(&config, &connection, &mut commands).await,
        _ => unreachable!(),
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if let [_, command, directory] = args.as_slice()
        && command == "certs"
    {
        let tls = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        std::fs::write(PathBuf::from(directory).join("cert.der"), tls.cert.der())?;
        std::fs::write(
            PathBuf::from(directory).join("key.der"),
            tls.signing_key.serialize_der(),
        )?;
        return Ok(());
    }
    let config = Config::parse(&args)?;
    let mut builder = if config.layout == "multi" {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        let cpus = [config.io_cpu, config.app_cpu];
        let index = AtomicUsize::new(0);
        builder.worker_threads(2).on_thread_start(move || {
            pin(cpus[index.fetch_add(1, Ordering::Relaxed) % 2]).expect("IO affinity");
        });
        builder
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().event_interval(config.event_interval);
    let runtime = builder.build()?;
    if config.layout == "split" {
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let cpu = config.io_cpu;
        let thread = std::thread::spawn(move || -> Result<()> {
            pin(cpu)?;
            runtime.block_on(async {
                let _ = stopped.await;
            });
            Ok(())
        });
        pin(config.app_cpu)?;
        let result = handle.block_on(run(config));
        let _ = stop.send(());
        thread.join().map_err(|_| "IO thread panicked")??;
        return result;
    }
    pin(if config.layout == "multi" {
        config.app_cpu
    } else {
        config.io_cpu
    })?;
    // Give the application the same scheduler queue as Quinn's drivers.
    // A block_on root future can be revisited less often under IO load.
    runtime.block_on(async {
        tokio::spawn(run(config))
            .await
            .map_err(|_| "application task panicked")?
    })
}
