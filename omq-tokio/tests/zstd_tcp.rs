#![cfg(feature = "zstd")]

//! End-to-end integration of the `zstd+tcp://` transport scheme.

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
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
        MonitorEvent::Listening { endpoint: Endpoint::ZstdTcp { port, .. } } => port,
        other => panic!("unexpected {other:?}"),
    };
    let connect_ep = Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    };
    (pull, connect_ep)
}

#[tokio::test]
async fn small_plaintext_roundtrip() {
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
async fn large_compressible_roundtrip() {
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
async fn multipart_roundtrip() {
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
    let dict = Bytes::from_static(b"omq-omq-omq-omq-omq-omq-omq-omq-shared-prefix");

    let opts = || Options::default().compression_dict(dict.clone());
    let pull = Socket::new(SocketType::Pull, opts());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
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
        MonitorEvent::Listening { endpoint: Endpoint::ZstdTcp { port, .. } } => port,
        other => panic!("unexpected {other:?}"),
    };

    let push = Socket::new(SocketType::Push, opts());
    push.connect(Endpoint::ZstdTcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    })
    .await
    .unwrap();

    let plain = b"omq-".repeat(20); // 80 bytes - above with-dict threshold (64).
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
