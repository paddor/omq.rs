use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Status};

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::too_many_lines
)]
pub mod bench {
    tonic::include_proto!("bench");
}

use bench::Blob;
use bench::blob_service_client::BlobServiceClient;
use bench::blob_service_server::{BlobService, BlobServiceServer};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug)]
struct ServiceImpl {
    size: usize,
}

#[tonic::async_trait]
impl BlobService for ServiceImpl {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<Blob, Status>>;

    async fn stream(
        &self,
        _request: Request<Blob>,
    ) -> Result<tonic::Response<Self::StreamStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let size = self.size;
        tokio::spawn(async move {
            let blob = Blob {
                data: vec![b'x'; size],
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

async fn bind_server(
    size: usize,
) -> (
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    u16,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gRPC");
    let port = listener.local_addr().expect("local addr").port();
    report_port(port);
    let incoming = TcpListenerStream::new(listener).inspect(|stream| {
        if let Ok(stream) = stream {
            stream.set_nodelay(true).expect("set gRPC TCP_NODELAY");
        }
    });
    let service = BlobServiceServer::new(ServiceImpl { size })
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);
    let server = Server::builder().add_service(service);
    let task = tokio::spawn(server.serve_with_incoming(incoming));
    (task, port)
}

fn report_port(port: u16) {
    let path = std::env::var("OMQ_BENCH_PORT_FILE").expect("OMQ_BENCH_PORT_FILE");
    std::fs::write(path, port.to_string()).expect("write gRPC port");
}

async fn client(addr: &str) -> BlobServiceClient<Channel> {
    let addr = addr.strip_prefix("tcp://").unwrap_or(addr);
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .expect("gRPC endpoint")
        .tcp_nodelay(true);
    BlobServiceClient::connect(endpoint)
        .await
        .expect("connect gRPC")
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE)
}

async fn run_push(size: usize) {
    let (task, _) = bind_server(size).await;
    task.await.expect("gRPC server task").expect("gRPC server");
}

async fn run_pull(addr: &str, size: usize, duration: Duration) {
    let mut grpc = client(addr).await;
    let mut stream = grpc
        .stream(Blob { data: Vec::new() })
        .await
        .expect("start gRPC stream")
        .into_inner();
    stream.message().await.expect("warmup gRPC message");
    let start = Instant::now();
    let deadline = start + duration;
    let mut count = 0_u64;
    while Instant::now() < deadline {
        let Some(blob) = stream.message().await.expect("receive gRPC blob") else {
            break;
        };
        assert_eq!(blob.data.len(), size, "gRPC payload size");
        count += 1;
    }
    println!("{count} {:.9} {size}", start.elapsed().as_secs_f64());
}

async fn run_rep(size: usize) {
    let (task, _) = bind_server(size).await;
    task.await.expect("gRPC server task").expect("gRPC server");
}

async fn run_req(addr: &str, size: usize, iterations: usize, warmup: usize) {
    let mut grpc = client(addr).await;
    let request = Blob {
        data: vec![b'x'; size],
    };
    for _ in 0..warmup {
        grpc.echo(request.clone()).await.expect("gRPC warmup");
    }
    let mut samples = Vec::with_capacity(iterations);
    let start = Instant::now();
    for _ in 0..iterations {
        let before = Instant::now();
        grpc.echo(request.clone()).await.expect("gRPC echo");
        samples.push(before.elapsed().as_secs_f64() * 1_000_000.0);
    }
    samples.sort_by(f64::total_cmp);
    let quantile = |p: f64| samples[((samples.len() - 1) as f64 * p).round() as usize];
    println!(
        "{:.3} {:.3} {:.3} {:.3} {iterations} {:.9}",
        quantile(0.50),
        quantile(0.99),
        quantile(0.999),
        samples[iterations - 1],
        start.elapsed().as_secs_f64()
    );
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let size = args[3].parse().expect("message size");
    match args[1].as_str() {
        "push" => run_push(size).await,
        "pull" => {
            run_pull(
                &args[2],
                size,
                Duration::from_secs_f64(args[4].parse().unwrap()),
            )
            .await;
        }
        "rep" => run_rep(size).await,
        "req" => {
            run_req(
                &args[2],
                size,
                args[4].parse().unwrap(),
                args[5].parse().unwrap(),
            )
            .await;
        }
        other => panic!("unknown command: {other}"),
    }
}
