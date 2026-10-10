#![cfg(feature = "quic")]
//! QUIC peers through an impairing UDP proxy: loss, duplication, reordering,
//! and address rebinding. The proxy is deterministic (seeded) per test.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::{Options, QuicOptions};
use omq_proto::proto::SocketType;
use omq_tokio::Socket;
use tokio::net::UdpSocket;

struct Tls {
    cert: Vec<u8>,
    key: Vec<u8>,
}

fn tls() -> Tls {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    Tls {
        cert: certified.cert.pem().into_bytes(),
        key: certified.signing_key.serialize_pem().into_bytes(),
    }
}

fn server_options(tls: &Tls) -> Options {
    Options {
        quic: QuicOptions {
            server_cert_pem: Some(tls.cert.clone()),
            server_key_pem: Some(tls.key.clone()),
            ..QuicOptions::default()
        },
        ..Options::default()
    }
}

fn client_options(tls: &Tls) -> Options {
    Options {
        quic: QuicOptions {
            trust_pem: Some(tls.cert.clone()),
            trust_system: false,
            ..QuicOptions::default()
        },
        ..Options::default()
    }
}

#[derive(Clone, Copy)]
struct Impairment {
    /// Per-mille probabilities.
    drop: u64,
    duplicate: u64,
    reorder: u64,
}

/// Small deterministic generator; test impairment only.
struct XorShift(u64);

impl XorShift {
    fn per_mille(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % 1000
    }
}

struct Proxy {
    addr: SocketAddr,
    /// Rebind the upstream-facing socket; the server sees a new client port.
    rebind: Arc<AtomicBool>,
    forwarded: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn proxy(server: SocketAddr, impairment: Impairment, seed: u64) -> Proxy {
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = front.local_addr().unwrap();
    let rebind = Arc::new(AtomicBool::new(false));
    let forwarded = Arc::new(AtomicU64::new(0));
    let task = tokio::spawn(run_proxy(
        front,
        server,
        impairment,
        seed,
        rebind.clone(),
        forwarded.clone(),
    ));
    Proxy {
        addr,
        rebind,
        forwarded,
        task,
    }
}

async fn run_proxy(
    front: Arc<UdpSocket>,
    server: SocketAddr,
    impairment: Impairment,
    seed: u64,
    rebind: Arc<AtomicBool>,
    forwarded: Arc<AtomicU64>,
) {
    let mut back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let mut rng = XorShift(seed | 1);
    let mut client: Option<SocketAddr> = None;
    let mut up = vec![0u8; 65536];
    let mut down = vec![0u8; 65536];
    loop {
        if rebind.swap(false, Ordering::AcqRel) {
            back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        }
        let (bytes, to_server, target) = tokio::select! {
            result = front.recv_from(&mut up) => {
                let (n, from) = result.unwrap();
                client = Some(from);
                (up[..n].to_vec(), true, server)
            }
            result = back.recv_from(&mut down) => {
                let (n, _) = result.unwrap();
                let Some(client) = client else { continue };
                (down[..n].to_vec(), false, client)
            }
        };
        if rng.per_mille() < impairment.drop {
            continue;
        }
        let copies = if rng.per_mille() < impairment.duplicate {
            2
        } else {
            1
        };
        let delay = if rng.per_mille() < impairment.reorder {
            Duration::from_millis(1 + rng.per_mille() % 8)
        } else {
            Duration::ZERO
        };
        let socket = if to_server {
            back.clone()
        } else {
            front.clone()
        };
        forwarded.fetch_add(1, Ordering::Relaxed);
        if delay.is_zero() {
            for _ in 0..copies {
                let _ = socket.send_to(&bytes, target).await;
            }
        } else {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                for _ in 0..copies {
                    let _ = socket.send_to(&bytes, target).await;
                }
            });
        }
    }
}

fn port_of(endpoint: &Endpoint) -> u16 {
    match endpoint {
        Endpoint::Quic { port, .. } => *port,
        other => panic!("expected quic endpoint, got {other}"),
    }
}

async fn recv(socket: &Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(30), socket.recv())
        .await
        .expect("recv timed out")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_and_atomic_multipart_survive_loss_duplication_and_reordering() {
    let tls = tls();
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let port = port_of(&pull.bind("quic://127.0.0.1:0").await.unwrap());
    let proxy = proxy(
        format!("127.0.0.1:{port}").parse().unwrap(),
        Impairment {
            drop: 30,
            duplicate: 20,
            reorder: 50,
        },
        0x5eed,
    )
    .await;
    let push = Socket::new(SocketType::Push, client_options(&tls));
    push.connect(format!("quic://{}", proxy.addr))
        .await
        .unwrap();

    let large = Bytes::from((0..300_000).map(|i| (i % 253) as u8).collect::<Vec<_>>());
    let sender = tokio::spawn({
        let large = large.clone();
        async move {
            for i in 0u32..2_000 {
                let head = Bytes::from(i.to_be_bytes().to_vec());
                let msg = if i % 100 == 0 {
                    Message::multipart([head, Bytes::new(), large.clone()])
                } else {
                    Message::single(head)
                };
                push.send(msg).await.unwrap();
            }
            push
        }
    });
    for i in 0u32..2_000 {
        let msg = recv(&pull).await;
        assert_eq!(msg.part_bytes(0).unwrap(), &i.to_be_bytes()[..], "FIFO");
        if i % 100 == 0 {
            assert_eq!(msg.len(), 3, "multipart atomicity");
            assert!(msg.part_bytes(1).unwrap().is_empty());
            assert_eq!(msg.part_bytes(2).unwrap(), &large[..]);
        } else {
            assert_eq!(msg.len(), 1);
        }
    }
    drop(sender.await.unwrap());
    assert!(proxy.forwarded.load(Ordering::Relaxed) > 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_holds_under_loss_and_bidirectional_saturation() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.heartbeat_interval = Some(Duration::from_millis(100));
    server.heartbeat_timeout = Some(Duration::from_secs(2));
    server.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    server.recv_hwm = 8;
    server.send_hwm = 8;
    let a = Socket::new(SocketType::Pair, server);
    let port = port_of(&a.bind("quic://127.0.0.1:0").await.unwrap());
    let proxy = proxy(
        format!("127.0.0.1:{port}").parse().unwrap(),
        Impairment {
            drop: 20,
            duplicate: 0,
            reorder: 20,
        },
        0xbeef,
    )
    .await;
    let mut client = client_options(&tls);
    client.heartbeat_interval = Some(Duration::from_millis(100));
    client.heartbeat_timeout = Some(Duration::from_secs(2));
    client.quic.stream_window = QuicOptions::MIN_STREAM_WINDOW;
    client.recv_hwm = 8;
    client.send_hwm = 8;
    let b = Socket::new(SocketType::Pair, client);
    let mut monitor = b.monitor();
    b.connect(format!("quic://{}", proxy.addr)).await.unwrap();

    // Both sides send while neither reads: every queue and window fills.
    let body = Bytes::from(vec![5u8; 8 * 1024]);
    let send_all = |socket: Socket, body: Bytes| {
        tokio::spawn(async move {
            for i in 0u32..300 {
                let mut data = body.to_vec();
                data[..4].copy_from_slice(&i.to_be_bytes());
                socket.send(Message::single(data)).await.unwrap();
            }
            socket
        })
    };
    let a_sender = send_all(a.clone(), body.clone());
    let b_sender = send_all(b.clone(), body.clone());
    tokio::time::sleep(Duration::from_secs(4)).await;
    for (socket, name) in [(&a, "a"), (&b, "b")] {
        for i in 0u32..300 {
            let msg = recv(socket).await;
            assert_eq!(
                &msg.part_bytes(0).unwrap()[..4],
                &i.to_be_bytes()[..],
                "{name}"
            );
        }
    }
    a_sender.await.unwrap();
    b_sender.await.unwrap();
    while let Ok(event) = monitor.try_recv() {
        assert!(
            !matches!(event, omq_proto::MonitorEvent::Disconnected { .. }),
            "saturation caused a disconnect: {event:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn address_rebinding_recovers_through_reconnect() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.quic.idle_timeout = Duration::from_secs(2);
    server.quic.keep_alive_interval = Duration::from_millis(500);
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind("quic://127.0.0.1:0").await.unwrap());
    let proxy = proxy(
        format!("127.0.0.1:{port}").parse().unwrap(),
        Impairment {
            drop: 0,
            duplicate: 0,
            reorder: 0,
        },
        1,
    )
    .await;
    let mut client = client_options(&tls);
    client.quic.idle_timeout = Duration::from_secs(2);
    client.quic.keep_alive_interval = Duration::from_millis(500);
    let push = Socket::new(SocketType::Push, client);
    push.connect(format!("quic://{}", proxy.addr))
        .await
        .unwrap();
    push.send(Message::single("before")).await.unwrap();
    assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &b"before"[..]);

    // The server now sees the client from a new port. Migration is disabled,
    // so the old connection idles out and the socket reconnects silently.
    proxy.rebind.store(true, Ordering::Release);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut n = 0u32;
    loop {
        let _ = push.try_send(Message::single(format!("after-{n}")));
        n += 1;
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(100), pull.recv()).await
            && msg.part_bytes(0).unwrap().starts_with(b"after-")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no recovery after rebinding"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_linger_completes_through_a_lossy_path() {
    let tls = tls();
    let mut server = server_options(&tls);
    server.recv_hwm = 10_000;
    let pull = Socket::new(SocketType::Pull, server);
    let port = port_of(&pull.bind("quic://127.0.0.1:0").await.unwrap());
    let proxy = proxy(
        format!("127.0.0.1:{port}").parse().unwrap(),
        Impairment {
            drop: 50,
            duplicate: 10,
            reorder: 30,
        },
        0x11_4e,
    )
    .await;
    let mut client = client_options(&tls);
    client.linger = Some(Duration::from_secs(20));
    client.send_hwm = 10_000;
    let push = Socket::new(SocketType::Push, client);
    push.connect(format!("quic://{}", proxy.addr))
        .await
        .unwrap();
    let body = Bytes::from(vec![0x42; 4096]);
    for _ in 0..1_000 {
        push.send(Message::single(body.clone())).await.unwrap();
    }
    // Close while most data is unacknowledged; lost FIN/ACK packets are
    // retransmitted within the linger deadline.
    push.close().await.unwrap();
    for _ in 0..1_000 {
        assert_eq!(recv(&pull).await.part_bytes(0).unwrap(), &body[..]);
    }
}
