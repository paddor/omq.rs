use std::time::{Duration, Instant};

use omq_tokio::{
    ByteRateLimit, Context, ContextConfig, Message, NetworkRateLimits, Options, SocketType,
};

#[tokio::test]
async fn configuration_is_shared_frozen_and_owned_only() {
    let limits = NetworkRateLimits {
        ingress: Some(ByteRateLimit::new(1000, 100)),
        egress: None,
    };
    assert!(Context::current().set_network_rate_limits(limits).is_err());
    assert!(
        Context::with_config(ContextConfig { io_threads: 0 })
            .set_network_rate_limits(limits)
            .is_err()
    );
    let context = Context::new();
    for invalid in [ByteRateLimit::new(0, 1), ByteRateLimit::new(1, 0)] {
        assert!(
            context
                .set_network_rate_limits(NetworkRateLimits {
                    ingress: Some(invalid),
                    egress: None
                })
                .is_err()
        );
    }
    context.set_network_rate_limits(limits).unwrap();
    let imported = Context::from_share_key(context.share_key()).unwrap();
    assert_eq!(imported.network_rate_limits(), limits);
    let socket = imported.socket(SocketType::Pair, Options::default());
    assert!(
        context
            .set_network_rate_limits(NetworkRateLimits::default())
            .is_err()
    );
    socket.close().await.unwrap();
}

async fn aggregate(threads: usize, ingress: bool, scheme: &str) {
    let context = Context::with_config(ContextConfig {
        io_threads: threads,
    });
    let limit = Some(ByteRateLimit::new(64 * 1024, 1500));
    context
        .set_network_rate_limits(NetworkRateLimits {
            ingress: ingress.then_some(limit).flatten(),
            egress: (!ingress).then_some(limit).flatten(),
        })
        .unwrap();
    let other = Context::current();
    // DART admits initial receive credit while endpoints are established.
    // Its ingress envelope starts there, rather than at the first data send.
    let admission_started = Instant::now();
    let mut pairs = Vec::new();
    for _ in 0..4 {
        let pull =
            if ingress { &context } else { &other }.socket(SocketType::Gather, Options::default());
        let push =
            if ingress { &other } else { &context }.socket(SocketType::Scatter, Options::default());
        let endpoint = pull
            .bind(
                format!("{scheme}://127.0.0.1:0")
                    .parse::<omq_tokio::Endpoint>()
                    .unwrap(),
            )
            .await
            .unwrap();
        push.connect(endpoint).await.unwrap();
        pull.wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        push.wait_connected(1, Duration::from_secs(2))
            .await
            .unwrap();
        pairs.push((push, pull));
    }
    let started = Instant::now();
    for (push, _) in &pairs {
        push.send(Message::from_slice(&vec![7; 8192]))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for (_, pull) in &pairs {
            assert_eq!(pull.recv().await.unwrap().byte_len(), 8192);
        }
    })
    .await
    .unwrap();
    let elapsed = if scheme == "dart" && ingress {
        admission_started.elapsed()
    } else {
        started.elapsed()
    };
    assert!(
        elapsed >= Duration::from_millis(450),
        "{scheme} ingress={ingress} escaped the budget: {elapsed:?}"
    );
    for (push, pull) in pairs {
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[tokio::test]
async fn tcp_aggregate_across_sockets_and_io_threads() {
    for threads in [1, 4] {
        for ingress in [false, true] {
            aggregate(threads, ingress, "tcp").await;
        }
    }
}

#[cfg(feature = "dart")]
#[tokio::test]
async fn dart_datagrams_share_a_small_burst_across_io_lanes() {
    for ingress in [false, true] {
        aggregate(4, ingress, "dart").await;
    }
}

#[tokio::test]
async fn raw_stream_runs_and_is_paced_on_owned_io_lanes() {
    use tokio::io::AsyncReadExt;
    let context = Context::with_config(ContextConfig { io_threads: 3 });
    context
        .set_network_rate_limits(NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(32 * 1024, 512)),
        })
        .unwrap();
    let stream = context.socket(SocketType::Stream, Options::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    stream
        .connect(
            format!("tcp://{}", listener.local_addr().unwrap())
                .parse::<omq_tokio::Endpoint>()
                .unwrap(),
        )
        .await
        .unwrap();
    let (mut remote, _) = listener.accept().await.unwrap();
    let identity = stream.recv().await.unwrap().part_bytes(0).unwrap();
    let start = Instant::now();
    stream
        .send(Message::multipart([
            identity,
            bytes::Bytes::from(vec![3; 8192]),
        ]))
        .await
        .unwrap();
    let mut received = vec![0; 8192];
    tokio::time::timeout(Duration::from_secs(3), remote.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, vec![3; 8192]);
    assert!(start.elapsed() >= Duration::from_millis(220));
    stream.close().await.unwrap();
}

async fn transfer(
    scheme: &str,
    ingress: bool,
    body: Vec<u8>,
    count: usize,
    minimum: Duration,
    maximum: Duration,
) {
    let limited = Context::with_config(ContextConfig { io_threads: 3 });
    let limit = Some(ByteRateLimit::new(64 * 1024, 1500));
    limited
        .set_network_rate_limits(NetworkRateLimits {
            ingress: if ingress { limit } else { None },
            egress: if ingress { None } else { limit },
        })
        .unwrap();
    let unlimited = Context::current();
    let mut server = Options::default();
    let mut client = Options::default();
    server.heartbeat_interval = Some(Duration::from_millis(50));
    server.heartbeat_timeout = Some(Duration::from_millis(150));
    client.heartbeat_interval = server.heartbeat_interval;
    client.heartbeat_timeout = server.heartbeat_timeout;
    #[cfg(feature = "quic")]
    if scheme == "quic" {
        let tls = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        server.quic.server_cert_pem = Some(tls.cert.pem().into_bytes());
        server.quic.server_key_pem = Some(tls.signing_key.serialize_pem().into_bytes());
        client.quic.trust_system = false;
        client.quic.trust_pem = Some(tls.cert.pem().into_bytes());
    }
    let pull = if ingress { &limited } else { &unlimited }.socket(SocketType::Gather, server);
    let push = if ingress { &unlimited } else { &limited }.socket(SocketType::Scatter, client);
    let endpoint = pull
        .bind(
            format!("{scheme}://127.0.0.1:0")
                .parse::<omq_tokio::Endpoint>()
                .unwrap(),
        )
        .await
        .unwrap();
    push.connect(endpoint).await.unwrap();
    pull.wait_connected(1, Duration::from_secs(3))
        .await
        .unwrap();
    push.wait_connected(1, Duration::from_secs(3))
        .await
        .unwrap();
    let start = Instant::now();
    tokio::time::timeout(maximum, async {
        for _ in 0..count {
            push.send(Message::from_slice(&body)).await.unwrap();
        }
        for _ in 0..count {
            assert_eq!(
                pull.recv().await.unwrap().part_bytes(0).unwrap().as_ref(),
                &body
            );
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{scheme} ingress={ingress} stalled"));
    assert!(
        start.elapsed() >= minimum,
        "{scheme} escaped limit in {:?}",
        start.elapsed()
    );
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test]
async fn pacing_preserves_heartbeat_liveness() {
    transfer(
        "tcp",
        true,
        vec![9; 32 * 1024],
        1,
        Duration::from_millis(450),
        Duration::from_secs(5),
    )
    .await;
}

#[tokio::test]
async fn compression_is_charged_after_encoding() {
    let schemes: &[&str] = &[
        #[cfg(feature = "lz4")]
        "lz4+tcp",
        #[cfg(feature = "zstd")]
        "zstd+tcp",
    ];
    for scheme in schemes {
        for ingress in [false, true] {
            transfer(
                scheme,
                ingress,
                vec![7; 256 * 1024],
                1,
                Duration::ZERO,
                Duration::from_secs(2),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn phase_two_transports_use_the_context_budget() {
    let schemes: &[&str] = &[
        #[cfg(feature = "ws")]
        "ws",
        #[cfg(feature = "quic")]
        "quic",
        #[cfg(feature = "dart")]
        "dart",
    ];
    for scheme in schemes {
        for ingress in [false, true] {
            transfer(
                scheme,
                ingress,
                vec![7; 1024],
                32,
                Duration::from_millis(430),
                Duration::from_secs(5),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn udp_paces_complete_datagrams_in_both_directions() {
    for ingress in [false, true] {
        let limited = Context::with_config(ContextConfig { io_threads: 3 });
        let limit = Some(ByteRateLimit::new(128 * 1024, 65_507));
        limited
            .set_network_rate_limits(NetworkRateLimits {
                ingress: if ingress { limit } else { None },
                egress: if ingress { None } else { limit },
            })
            .unwrap();
        let unlimited = Context::current();
        let dish = if ingress { &limited } else { &unlimited }
            .socket(SocketType::Dish, Options::default());
        let radio = if ingress { &unlimited } else { &limited }
            .socket(SocketType::Radio, Options::default());
        dish.join("g").await.unwrap();
        let endpoint = dish
            .bind("udp://127.0.0.1:0".parse::<omq_tokio::Endpoint>().unwrap())
            .await
            .unwrap();
        radio.connect(endpoint).await.unwrap();
        let body = bytes::Bytes::from(vec![7; 16 * 1024]);
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..8 {
                radio
                    .send(Message::multipart([
                        bytes::Bytes::from_static(b"g"),
                        body.clone(),
                    ]))
                    .await
                    .unwrap();
            }
            for _ in 0..8 {
                assert_eq!(dish.recv().await.unwrap().part_bytes(1).unwrap(), body);
            }
        })
        .await
        .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(450));
        radio.close().await.unwrap();
        dish.close().await.unwrap();
    }
}

#[tokio::test]
async fn undersized_datagram_bursts_fail_at_endpoint_setup() {
    let context = Context::new();
    context
        .set_network_rate_limits(NetworkRateLimits {
            ingress: Some(ByteRateLimit::new(1000, 100)),
            egress: None,
        })
        .unwrap();
    let dish = context.socket(SocketType::Dish, Options::default());
    assert!(
        dish.bind("udp://127.0.0.1:0".parse::<omq_tokio::Endpoint>().unwrap())
            .await
            .is_err()
    );
    #[cfg(feature = "dart")]
    assert!(
        dish.bind("dart://127.0.0.1:0".parse::<omq_tokio::Endpoint>().unwrap())
            .await
            .is_err()
    );
    dish.close().await.unwrap();
}

#[tokio::test]
async fn fanout_charges_each_wire_copy() {
    let limited = Context::with_config(ContextConfig { io_threads: 3 });
    limited
        .set_network_rate_limits(NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(64 * 1024, 1024)),
        })
        .unwrap();
    let publisher = limited.socket(SocketType::Pub, Options::default());
    let endpoint = publisher
        .bind("tcp://127.0.0.1:0".parse::<omq_tokio::Endpoint>().unwrap())
        .await
        .unwrap();
    let unlimited = Context::current();
    let mut subscribers = Vec::new();
    for _ in 0..4 {
        let subscriber = unlimited.socket(SocketType::Sub, Options::default());
        subscriber.subscribe("").await.unwrap();
        subscriber.connect(endpoint.clone()).await.unwrap();
        subscribers.push(subscriber);
    }
    publisher
        .wait_subscribed(4, Duration::from_secs(3))
        .await
        .unwrap();
    let start = Instant::now();
    publisher
        .send(Message::from_slice(&vec![7; 8192]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        for subscriber in &subscribers {
            assert_eq!(subscriber.recv().await.unwrap().byte_len(), 8192);
        }
    })
    .await
    .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(450));
    publisher.close().await.unwrap();
    for subscriber in subscribers {
        subscriber.close().await.unwrap();
    }
}

#[tokio::test]
async fn latency_direct_writer_cannot_bypass_egress() {
    let limited = Context::with_config(ContextConfig { io_threads: 2 });
    limited
        .set_network_rate_limits(NetworkRateLimits {
            ingress: None,
            egress: Some(ByteRateLimit::new(64 * 1024, 1024)),
        })
        .unwrap();
    let sender = limited.socket(
        SocketType::Channel,
        Options {
            workload_profile: Some(omq_proto::WorkloadProfile::Latency),
            ..Options::default()
        },
    );
    let receiver = Context::current().socket(SocketType::Channel, Options::default());
    let endpoint = receiver
        .bind("tcp://127.0.0.1:0".parse::<omq_tokio::Endpoint>().unwrap())
        .await
        .unwrap();
    sender.connect(endpoint).await.unwrap();
    sender
        .wait_connected(1, Duration::from_secs(3))
        .await
        .unwrap();
    let start = Instant::now();
    sender
        .send(Message::from_slice(&vec![7; 16 * 1024]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(220));
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn inproc_bypasses_the_network_budget() {
    let context = Context::new();
    let limit = Some(ByteRateLimit::new(1, 1));
    context
        .set_network_rate_limits(NetworkRateLimits {
            ingress: limit,
            egress: limit,
        })
        .unwrap();
    let sender = context.socket(SocketType::Push, Options::default());
    let receiver = context.socket(SocketType::Pull, Options::default());
    let endpoint = receiver
        .bind("inproc://unpaced".parse::<omq_tokio::Endpoint>().unwrap())
        .await
        .unwrap();
    sender.connect(endpoint).await.unwrap();
    sender
        .send(Message::from_slice(&vec![7; 8192]))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .byte_len(),
        8192
    );
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
}
