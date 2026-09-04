use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Status};

use super::{
    Args, BenchResult, LatencyMeter, LatencyResult, ProducerFiles, clean_paths, process_cpu_secs,
    run_paths, self_cpu_secs, spawn_producer, spawn_responder, write_marker,
};

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::result_large_err,
    clippy::too_many_lines
)]
pub mod bench {
    tonic::include_proto!("bench");
}

use bench::Blob;
use bench::blob_service_client::BlobServiceClient;
use bench::blob_service_server::{BlobService, BlobServiceServer};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
struct ServiceImpl {
    size: usize,
    start_file: std::path::PathBuf,
}

#[tonic::async_trait]
impl BlobService for ServiceImpl {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<Blob, Status>>;

    async fn stream(
        &self,
        _request: Request<Blob>,
    ) -> Result<tonic::Response<Self::StreamStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let this = self.clone();
        tokio::spawn(async move {
            wait_for_file_async(&this.start_file).await;
            let blob = Blob {
                data: vec![b'x'; this.size],
            };
            loop {
                if tx.send(Ok(blob.clone())).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    async fn echo(&self, request: Request<Blob>) -> Result<tonic::Response<Blob>, Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}

pub(crate) async fn producer(size: usize, start_file: &Path, port_file: &Path) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    std::fs::write(port_file, port.to_string()).context("write gRPC port")?;
    let incoming = TcpListenerStream::new(listener).inspect(|stream| {
        if let Ok(stream) = stream {
            stream.set_nodelay(true).expect("set gRPC TCP_NODELAY");
        }
    });
    let service = BlobServiceServer::new(ServiceImpl {
        size,
        start_file: start_file.to_owned(),
    })
    .max_decoding_message_size(MAX_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_MESSAGE_SIZE);

    Server::builder()
        .add_service(service)
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

pub(crate) async fn bench(args: &Args, token: &str, size: usize) -> Result<BenchResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let port_file = std::env::temp_dir().join(format!("omq-mom-{token}-{size}.grpc-port"));
    remove_if_exists(&port_file)?;

    let mut producer = spawn_producer(
        args,
        "grpc-rust",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: Some(&port_file),
        },
    )?;
    let port = wait_port(&port_file).await?;
    let mut grpc = client(&format!("127.0.0.1:{port}")).await?;
    let mut stream = grpc.stream(Blob { data: Vec::new() }).await?.into_inner();

    write_marker(&paths.0)?;
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(args.warmup);
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, stream.message()).await {
            Ok(Ok(Some(blob))) => check_payload(&blob, size)?,
            Ok(Ok(None)) | Err(_) => break,
            Ok(Err(err)) => return Err(err.into()),
        }
    }

    let duration = Duration::from_secs_f64(args.duration);
    let pull_start = self_cpu_secs()?;
    let push_start = process_cpu_secs(producer.id())?;
    let start = Instant::now();
    let deadline = start + duration;
    let mut count = 0_u64;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, stream.message()).await {
            Ok(Ok(Some(blob))) => {
                check_payload(&blob, size)?;
                count += 1;
            }
            Ok(Ok(None)) | Err(_) => break,
            Ok(Err(err)) => return Err(err.into()),
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let pull_cpu_time = self_cpu_secs()? - pull_start;
    let push_cpu_time = process_cpu_secs(producer.id())? - push_start;
    producer.kill().ok();
    producer.wait().ok();

    Ok(BenchResult {
        count,
        elapsed,
        pull_cpu_time,
        push_cpu_time,
        broker_cpu_time: None,
    })
}

pub(crate) async fn latency(args: &Args, token: &str, size: usize) -> Result<LatencyResult> {
    let paths = run_paths(token, size);
    clean_paths(&paths)?;
    let port_file = std::env::temp_dir().join(format!("omq-mom-{token}-{size}.grpc-port"));
    remove_if_exists(&port_file)?;

    let responder = spawn_responder(
        args,
        "grpc-rust",
        size,
        token,
        ProducerFiles {
            start: &paths.0,
            stop: &paths.1,
            result: &paths.2,
            grpc_port: Some(&port_file),
        },
    )?;
    let mut meter = LatencyMeter::new("grpc-rust", args.latency_iterations, responder)?;
    let port = wait_port(&port_file).await?;
    let mut grpc = client(&format!("127.0.0.1:{port}")).await?;
    let payload = Blob {
        data: vec![b'x'; size],
    };

    for _ in 0..args.latency_warmup {
        let response = tokio::time::timeout(Duration::from_secs(5), grpc.echo(payload.clone()))
            .await??
            .into_inner();
        check_payload(&response, size)?;
    }

    meter.begin()?;
    for _ in 0..args.latency_iterations {
        let start = Instant::now();
        let response = tokio::time::timeout(Duration::from_secs(5), grpc.echo(payload.clone()))
            .await??
            .into_inner();
        check_payload(&response, size)?;
        meter.record(start.elapsed())?;
    }
    meter.finish()
}

async fn client(addr: &str) -> Result<BlobServiceClient<Channel>> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?.tcp_nodelay(true);
    Ok(BlobServiceClient::connect(endpoint)
        .await?
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE))
}

fn check_payload(blob: &Blob, size: usize) -> Result<()> {
    if blob.data.len() != size {
        bail!("bad gRPC payload size");
    }
    Ok(())
}

async fn wait_for_file_async(path: &Path) {
    while !path.exists() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn wait_port(path: &Path) -> Result<u16> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(path)
            && let Ok(port) = text.trim().parse()
        {
            remove_if_exists(path)?;
            return Ok(port);
        }
        if Instant::now() >= deadline {
            bail!("gRPC: no port file from producer");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}
