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

/// Bind an `lz4+tcp://` Pull socket to an ephemeral port and return both
/// the socket and the discovered loopback endpoint.
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
    assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
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
    assert_eq!(m.parts()[0].coalesce().to_vec(), plain);
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
    assert_eq!(m.parts()[0].coalesce(), &b"hdr"[..]);
    assert_eq!(m.parts()[1].coalesce().to_vec(), big);
    assert_eq!(m.parts()[2].coalesce(), &b"tail"[..]);
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
        assert_eq!(m.parts()[0].coalesce().to_vec(), plain);
    }
}

#[tokio::test]
async fn many_messages_in_a_row() {
    const N: usize = 200;
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    for i in 0..N {
        push.send(Message::single(format!("m-{i}"))).await.unwrap();
    }
    for i in 0..N {
        let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.parts()[0].coalesce(), format!("m-{i}").as_bytes());
    }
}
