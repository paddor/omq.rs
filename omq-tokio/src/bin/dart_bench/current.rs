//! Same-thread latency comparison through the ordinary async Socket API.

use std::io::BufRead;
use std::time::{Duration, Instant};

use omq_tokio::{BufferPool, Context, Socket};
use tokio_util::sync::CancellationToken;

use super::{Config, RuntimeMode, SETUP_TIMEOUT, affinity, emit, format_offloads, make_body};

pub(super) async fn run(config: &Config, affinity: &affinity::Affinity) {
    let context = Context::current();
    let socket = context.socket(config.socket_type(), config.options());
    let native = matches!(config.endpoint, omq_tokio::Endpoint::Dart { .. });
    let pool = (!config.receiving()).then(|| {
        if native {
            BufferPool::new(2048, 8192)
        } else {
            BufferPool::new(1024, 1024)
        }
    });
    if config.receiving() {
        let endpoint = socket.bind(config.endpoint.clone()).await.unwrap();
        emit(&format!(
            "{{\"event\":\"bound\",\"endpoint\":\"{endpoint}\"}}"
        ));
    } else {
        socket.connect(config.endpoint.clone()).await.unwrap();
    }
    socket.wait_connected(1, SETUP_TIMEOUT).await.unwrap();
    let version = if native {
        omq_proto::dart::VERSION.to_string()
    } else {
        "null".into()
    };
    emit(&format!(
        "{{\"event\":\"ready\",\"affinity\":\"{}\",\"offloads\":{},\"dart_wire_version\":{version},\"dart_window_messages\":{},\"dart_pool_buffers\":{},\"dart_buffer_capacity\":{}}}",
        affinity.description(),
        format_offloads(socket.dart_capabilities()),
        config.window_messages,
        config.options().dart.pool_buffers,
        omq_tokio::transport::dart::BUFFER_CAPACITY,
    ));
    // Keep driving connection setup and maintenance while awaiting the
    // runner's barrier. Stdin never blocks the current-thread runtime.
    tokio::task::spawn_blocking(super::start).await.unwrap();
    let stop = CancellationToken::new();
    let poller = (config.runtime == RuntimeMode::CurrentPoll).then(|| {
        let stop = stop.clone();
        // Explicit benchmark polling: keep this one runtime thread ready so
        // the reactor never sleeps while app and transport tasks alternate.
        tokio::spawn(async move {
            while !stop.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
    });
    if config.receiving() {
        server(&socket).await;
    } else {
        client(&socket, config, pool.as_ref().unwrap()).await;
    }
    stop.cancel();
    if let Some(poller) = poller {
        poller.await.unwrap();
    }
    socket.close().await.unwrap();
    context.term();
}

async fn server(socket: &Socket) {
    let stop = CancellationToken::new();
    let cancel = stop.clone();
    std::thread::spawn(move || {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        cancel.cancel();
    });
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            reply = socket.recv() => socket.send(reply.unwrap()).await.unwrap(),
        }
    }
    emit(&format!(
        "{{\"event\":\"result\",\"offloads\":{},{} }}",
        format_offloads(socket.dart_capabilities()),
        super::latency_counters(socket.dart_stats()),
    ));
}

async fn client(socket: &Socket, config: &Config, pool: &BufferPool) {
    let mut samples = Vec::with_capacity(config.iterations);
    for index in 0..config.warmup + config.iterations {
        let tag = u64::try_from(index).unwrap();
        let body = make_body(pool, config.size, tag).expect("send pool exhausted");
        let at = Instant::now();
        socket.send(body).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(1), socket.recv())
            .await
            .expect("RTT timeout")
            .unwrap();
        let elapsed = at.elapsed().as_secs_f64() * 1e6;
        super::validate_reply(&reply, config.size, tag);
        if index >= config.warmup {
            samples.push(elapsed);
        }
    }
    super::latency_result(
        &mut samples,
        config,
        &format_offloads(socket.dart_capabilities()),
        socket.dart_stats(),
    );
}
