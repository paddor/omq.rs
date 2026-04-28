#![cfg(feature = "lz4")]

//! `lz4+tcp://` integration test for omq-compio.

use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port: 0,
    })
    .await
    .unwrap();
    let ev = compio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } => port,
        other => panic!("expected Lz4Tcp Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::Lz4Tcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

#[compio::test]
async fn lz4_small_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over lz4")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over lz4"[..]);
}

#[compio::test]
async fn lz4_large_compressible_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    // 16 KiB of repeating bytes - well above the LZ4 threshold and
    // highly compressible.
    let payload = Bytes::from(vec![b'A'; 16 * 1024]);
    push.send(Message::single(payload.clone())).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().len(), payload.len());
    assert_eq!(&m.parts()[0].coalesce()[..], &payload[..]);
}

#[compio::test]
async fn lz4_multipart_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.parts()[0].coalesce(), &b"a"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"bb"[..]);
    assert_eq!(m.parts()[2].coalesce(), &b"ccc"[..]);
}
