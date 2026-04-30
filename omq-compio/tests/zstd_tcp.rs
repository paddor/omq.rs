#![cfg(feature = "zstd")]

//! `zstd+tcp://` integration test for omq-compio.

use std::time::Duration;

use bytes::Bytes;
use omq_compio::endpoint::Host;
use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

async fn pull_on_loopback() -> (Socket, Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::ZstdTcp {
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
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("expected ZstdTcp Listening, got {other:?}"),
    };
    (
        pull,
        Endpoint::ZstdTcp {
            host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            port,
        },
    )
}

#[compio::test]
async fn zstd_small_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over zstd")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over zstd"[..]);
}

#[compio::test]
async fn zstd_large_compressible_message_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    let payload = Bytes::from(vec![b'A'; 16 * 1024]);
    push.send(Message::single(payload.clone())).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(1), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&m.parts()[0].coalesce()[..], &payload[..]);
}

#[compio::test]
async fn zstd_multipart_message_roundtrip() {
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

#[compio::test]
async fn zstd_auto_train_end_to_end() {
    // Pull side leaves auto-train off; push side opts in. Once the
    // training threshold fires (1000 messages or 100 KiB), the next
    // outbound message ships a trained dictionary as a single-part
    // ZMTP message before the regular encoded payload - the pull
    // side decodes it transparently and continues delivering
    // plaintext to recv().
    let (pull, ep) = pull_on_loopback().await;
    // Default Options has auto-train enabled. Bump linger so close()
    // drains the post-training dict shipment + the last few sends
    // before the runtime tears down.
    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(1)),
    );
    push.connect(ep).await.unwrap();

    let sample =
        br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
    for _ in 0..1500 {
        push.send(Message::single(sample.as_slice())).await.unwrap();
    }
    push.close().await.unwrap();

    let mut got = 0usize;
    while let Ok(Ok(m)) = compio::time::timeout(Duration::from_millis(200), pull.recv()).await {
        assert_eq!(m.parts()[0].coalesce(), &sample[..]);
        got += 1;
    }
    assert!(got >= 1000, "auto-train flow lost too many messages: got {got}");
}
