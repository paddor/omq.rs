//! Remoc TCP peer for `omq_bench run comparisons --impl remoc`.
//!
//! MPSC throughput and generated echo RPC use Serde `Bytes` with default Postbag.
//! Each process runs current-thread Tokio with `TCP_NODELAY`. Local/remote MPSC
//! queues hold 1000 items; byte buffers use Remoc defaults. No compression.
//! Rates count complete received messages and application bytes. Timings include
//! serialization but exclude setup; RPC keeps one request in flight.

use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail, ensure};
use bytes::Bytes;
use omq_tokio::{Context, Message, Options, Socket, SocketType};
use remoc::prelude::*;
use tokio::net::{TcpListener, TcpStream};

use rpc::{Echo, EchoClient, EchoServer, EchoService};

const BUFFER: usize = 1_000;
const MAX_ITEM_SIZE: usize = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(5);
type Receiver = rch::mpsc::Receiver<Bytes, remoc::codec::Default, BUFFER, MAX_ITEM_SIZE>;

mod rpc {
    #![expect(
        clippy::unsafe_derive_deserialize,
        reason = "Remoc's generated client contains an unsafe method and derives Deserialize"
    )]

    use bytes::Bytes;
    use remoc::rtc::{self, CallError};

    #[rtc::remote]
    pub(super) trait Echo {
        async fn echo(&self, payload: Bytes) -> Result<Bytes, CallError>;
    }

    #[derive(Debug)]
    pub(super) struct EchoService;

    impl Echo for EchoService {
        fn echo(&self, payload: Bytes) -> impl Future<Output = Result<Bytes, CallError>> {
            std::future::ready(Ok(payload))
        }
    }
}

fn cpu_secs() -> Result<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the valid output pointer on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: getrusage succeeded and initialized usage.
    let usage = unsafe { usage.assume_init() };
    Ok(usage.ru_utime.tv_sec as f64
        + usage.ru_stime.tv_sec as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6)
}

fn address(value: &str) -> String {
    if value.bytes().all(|c| c.is_ascii_digit()) {
        format!("127.0.0.1:{value}")
    } else {
        value.strip_prefix("tcp://").unwrap_or(value).to_owned()
    }
}

// OMQ carries only the harness readiness message, before any measurement.
// Keep the socket alive until the benchmark ends so its queued message flushes.
async fn report_port(port: u16) -> Result<Option<Socket>> {
    let Ok(endpoint) = std::env::var("OMQ_BENCH_COORD") else {
        println!("PORT {port}");
        return Ok(None);
    };
    let socket = Context::current().socket(SocketType::Push, Options::default());
    socket.connect(endpoint.as_str()).await?;
    socket.wait_connected(1, TIMEOUT).await?;
    socket
        .send(Message::from_slice(format!("READY {port}").as_bytes()))
        .await?;
    Ok(Some(socket))
}

async fn serve(role: &str, addr: &str, size: usize) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let _coord = report_port(listener.local_addr()?.port()).await?;
    let (socket, _) = tokio::time::timeout(TIMEOUT, listener.accept()).await??;
    socket.set_nodelay(true)?;
    let (read, write) = socket.into_split();
    if role == "push" {
        let (sender, receiver) =
            rch::mpsc::with_local_buffer::<Bytes, remoc::codec::Default>(BUFFER)
                .with_buffer::<BUFFER>()
                .with_max_item_size::<MAX_ITEM_SIZE>();
        tokio::time::timeout(
            TIMEOUT,
            remoc::Connect::io(remoc::Cfg::default(), read, write).provide(receiver),
        )
        .await??;
        let payload = Bytes::from(vec![b'x'; size]);
        loop {
            match sender.send(payload.clone()).await {
                // Queue for transfer, without a per-message delivery round trip.
                Ok(sending) => drop(sending),
                Err(error) if error.is_closed() => break,
                Err(error) => return Err(error.into()),
            }
        }
    } else {
        let (server, client) = EchoServer::<_, remoc::codec::Default>::new(EchoService);
        tokio::time::timeout(
            TIMEOUT,
            remoc::Connect::io(remoc::Cfg::default(), read, write).provide(client),
        )
        .await??;
        let (_, result) = server.serve().await;
        result.map_err(|error| anyhow::anyhow!("Remoc RPC server: {error}"))?;
    }
    // The harness samples server CPU after the client exits, then terminates us.
    std::future::pending().await
}

async fn connect(addr: &str) -> Result<TcpStream> {
    let socket = tokio::time::timeout(TIMEOUT, TcpStream::connect(addr)).await??;
    socket.set_nodelay(true)?;
    Ok(socket)
}

async fn receive_for(receiver: &mut Receiver, size: usize, duration: Duration) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + duration;
    let mut count = 0;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, receiver.recv()).await {
            Ok(result) => {
                let payload = result?.context("Remoc channel closed during measurement")?;
                ensure!(payload.len() == size, "bad Remoc payload size");
                count += 1;
            }
            Err(_) => break,
        }
    }
    ensure!(count > 0, "Remoc channel delivered no messages");
    Ok(count)
}

async fn pull(addr: &str, size: usize, duration: Duration) -> Result<()> {
    let (read, write) = connect(addr).await?.into_split();
    let mut receiver: Receiver = tokio::time::timeout(
        TIMEOUT,
        remoc::Connect::io(remoc::Cfg::default(), read, write).consume(),
    )
    .await??;
    receive_for(&mut receiver, size, Duration::from_millis(500)).await?;
    let cpu_start = cpu_secs()?;
    let start = Instant::now();
    let count = receive_for(&mut receiver, size, duration).await?;
    let elapsed = start.elapsed().as_secs_f64();
    let cpu = cpu_secs()? - cpu_start;
    println!("{count} {elapsed:.6} {size} {cpu:.6}");
    Ok(())
}

async fn request(addr: &str, size: usize, iterations: usize, warmup: usize) -> Result<()> {
    ensure!(iterations > 0, "iterations must be positive");
    let (read, write) = connect(addr).await?.into_split();
    let client: EchoClient = tokio::time::timeout(
        TIMEOUT,
        remoc::Connect::io(remoc::Cfg::default(), read, write).consume(),
    )
    .await??;
    let payload = Bytes::from(vec![b'x'; size]);
    for _ in 0..warmup {
        let reply = tokio::time::timeout(TIMEOUT, client.echo(payload.clone())).await??;
        ensure!(reply == payload, "bad Remoc echo payload");
    }
    let mut rtts = Vec::with_capacity(iterations);
    let cpu_start = cpu_secs()?;
    let start = Instant::now();
    for _ in 0..iterations {
        let sent = Instant::now();
        let reply = tokio::time::timeout(TIMEOUT, client.echo(payload.clone())).await??;
        rtts.push(u64::try_from(sent.elapsed().as_nanos())?);
        ensure!(reply == payload, "bad Remoc echo payload");
    }
    let elapsed = start.elapsed().as_secs_f64();
    let cpu = cpu_secs()? - cpu_start;
    rtts.sort_unstable();
    let percentile = |numerator: usize, denominator: usize| {
        rtts[(rtts.len() * numerator / denominator).min(rtts.len() - 1)] as f64 / 1_000.0
    };
    println!(
        "{:.3} {:.3} {:.3} {:.3} {iterations} {cpu:.6} {elapsed:.6}",
        percentile(50, 100),
        percentile(99, 100),
        percentile(999, 1_000),
        percentile(1, 1),
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() >= 3,
        "usage: remoc_bench_peer push|pull|rep|req <addr> <size> [duration|iterations warmup]"
    );
    let addr = address(&args[1]);
    let size = args[2].parse()?;
    ensure!(size <= MAX_ITEM_SIZE / 2, "payload exceeds 8 MiB");
    match args[0].as_str() {
        "push" | "rep" => serve(&args[0], &addr, size).await,
        "pull" => {
            let seconds: f64 = args.get(3).context("duration missing")?.parse()?;
            ensure!(seconds.is_finite() && seconds > 0.0, "invalid duration");
            pull(&addr, size, Duration::try_from_secs_f64(seconds)?).await
        }
        "req" => {
            let iterations = args.get(3).context("iterations missing")?.parse()?;
            let warmup = args.get(4).context("warmup missing")?.parse()?;
            request(&addr, size, iterations, warmup).await
        }
        _ => bail!("unknown role {}", args[0]),
    }
}
