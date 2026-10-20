#![cfg(feature = "lz4")]

//! End-to-end integration of the `lz4+tcp://` transport scheme.
//!
//! Verifies the per-part transform pipeline runs over a real TCP listener:
//! small parts pass through plaintext, larger compressible parts use the
//! `LZ4B` envelope, multipart messages survive intact.

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use rand::Rng;

#[tokio::test]
async fn arenas_resume_through_receive_hwm_and_preserve_multipart_limits() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let pull = Socket::new(
            SocketType::Pull,
            Options::default().recv_hwm(4).max_message_size(256),
        );
        let endpoint = pull.bind("lz4+tcp://127.0.0.1:0").await.unwrap();
        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .send_hwm(2048)
                .compression_auto_train(true),
        );
        push.connect(endpoint).await.unwrap();
        let expected: Vec<_> = (0..1024u32)
            .map(|index| {
                Message::multipart([
                    Bytes::new(),
                    Bytes::copy_from_slice(&index.to_le_bytes()),
                    Bytes::new(),
                ])
            })
            .collect();
        for message in &expected {
            push.send(message.clone()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        for message in expected {
            assert_eq!(pull.recv().await.unwrap(), message);
        }
    })
    .await
    .expect("arena receive stalled");
}

#[tokio::test]
async fn arenas_keep_order_across_large_compression_offloads() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (pull, endpoint) = pull_on_loopback().await;
        let push = Socket::new(
            SocketType::Push,
            Options::default().compression_offload_threshold(Some(4096)),
        );
        push.connect(endpoint).await.unwrap();
        let expected: Vec<_> = (0..300u32)
            .map(|index| {
                // Runs of large messages hold and offload in turn around arenas.
                let size = match index % 31 {
                    0..=2 => 16 * 1024,
                    3 => 128 * 1024,
                    _ => 128,
                };
                let mut bytes = vec![b'x'; size];
                bytes[..4].copy_from_slice(&index.to_le_bytes());
                Message::single(bytes)
            })
            .collect();
        for message in &expected {
            push.send(message.clone()).await.unwrap();
        }
        for message in expected {
            assert_eq!(pull.recv().await.unwrap(), message);
        }
    })
    .await
    .expect("ordered arena/offload stream stalled");
}

/// Bind an `lz4+tcp://` Pull socket to an ephemeral port and return both
/// the socket and the discovered loopback endpoint.
async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    })
    .await
    .expect("handshake did not arrive within 5s");
}

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("expected Lz4Tcp Listening, got {other:?}"),
    };
    let connect_ep = Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };
    (pull, connect_ep)
}

#[tokio::test]
async fn small_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hello"));
}

#[tokio::test]
async fn large_compressible_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let plain = vec![b'Z'; 16 * 1024];
    push.send(Message::single(plain.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
}

#[tokio::test]
async fn multipart_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let big = vec![b'q'; 4096];
    let msg = Message::multipart::<_, Bytes>([
        Bytes::from_static(b"hdr"),
        Bytes::from(big.clone()),
        Bytes::from_static(b"tail"),
    ]);
    push.send(msg).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"hdr"[..]);
    assert_eq!(m.part_bytes(1).unwrap().to_vec(), big);
    assert_eq!(m.part_bytes(2).unwrap(), &b"tail"[..]);
}

#[tokio::test]
async fn dict_roundtrip_small_payload() {
    // 64 B payloads - below the no-dict threshold (512), above the
    // with-dict threshold (32). Dict-aware compression must succeed and
    // the wire path must ship + use the dict on both ends.
    let dict = Bytes::from_static(b"omq-omq-omq-omq-omq-omq-omq-omq-shared-prefix");
    let opts = || Options::default().compression_dict(dict.clone());

    let pull = Socket::new(SocketType::Pull, opts());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };

    let push = Socket::new(SocketType::Push, opts());
    push.connect(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    })
    .await
    .unwrap();

    // Repeating, dict-friendly payload above the with-dict threshold (32 B)
    // and below the no-dict threshold (512 B), so dict participation is the
    // only thing that lets it compress.
    let plain = b"omq-".repeat(20); // 80 bytes

    for _ in 0..3 {
        push.send(Message::single(plain.clone())).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
    }
}

#[tokio::test]
async fn empty_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single(Bytes::new())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(m.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn single_byte_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single(Bytes::from_static(&[0x42])))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &[0x42][..]);
}

#[tokio::test]
async fn incompressible_data_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let mut random = vec![0u8; 8192];
    rand::rng().fill_bytes(&mut random);
    push.send(Message::single(random.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), random);
}

#[tokio::test]
async fn req_rep_over_lz4() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut mon = rep.monitor();
    rep.bind(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let port = match tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };
    let ep = Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("question")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("question"));

    rep.send(Message::single("answer")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("answer"));
}

#[tokio::test]
async fn many_messages_in_a_row() {
    const N: usize = 200;
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    wait_for_handshake(&pull).await;

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }
    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), format!("m-{i}").as_bytes());
    }
}
