#![cfg(feature = "zstd")]

//! End-to-end integration of `zstd+tcp://`.

use std::time::Duration;

use bytes::Bytes;
use omq_proto::proto::transform::train_zdict;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use rand::Rng;

fn zstd_loopback(port: u16) -> Endpoint {
    Endpoint::ZstdTcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn make_test_dict(seed: &[u8]) -> Bytes {
    let samples: Vec<&[u8]> = (0..200).map(|_| seed).collect();
    train_zdict(&samples, 8 * 1024).expect("train_zdict")
}

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
    pull.bind(zstd_loopback(0)).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap();
    let port = match ev {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("expected ZstdTcp Listening, got {other:?}"),
    };
    (pull, zstd_loopback(port))
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
    assert_eq!(m, Message::single("hello"));
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
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
}

#[tokio::test]
async fn custom_level_roundtrip() {
    let (pull, ep) = pull_on_loopback().await;
    let push = Socket::new(SocketType::Push, Options::default().compression_level(1));
    push.connect(ep).await.unwrap();

    let plain = vec![b'L'; 16 * 1024];
    push.send(Message::single(plain.clone())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), plain);
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
    assert_eq!(m.part_bytes(0).unwrap(), &b"hdr"[..]);
    assert_eq!(m.part_bytes(1).unwrap().to_vec(), big);
    assert_eq!(m.part_bytes(2).unwrap(), &b"tail"[..]);
}

#[tokio::test]
async fn dict_roundtrip_small_payload() {
    let dict = make_test_dict(b"omq-omq-omq-omq-omq-omq-shared-prefix\n");
    let opts = || Options::default().compression_dict(dict.clone());
    let pull = Socket::new(SocketType::Pull, opts());
    let mut mon = pull.monitor();
    pull.bind(zstd_loopback(0)).await.unwrap();
    let port = match tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };

    let push = Socket::new(SocketType::Push, opts());
    push.connect(zstd_loopback(port)).await.unwrap();
    let plain = b"omq-".repeat(20);
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
async fn req_rep_over_zstd() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let mut mon = rep.monitor();
    rep.bind(zstd_loopback(0)).await.unwrap();
    let port = match tokio::time::timeout(Duration::from_millis(500), mon.recv())
        .await
        .unwrap()
        .unwrap()
    {
        MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } => port,
        other => panic!("unexpected {other:?}"),
    };

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(zstd_loopback(port)).await.unwrap();
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
async fn auto_train_survives_reconnect() {
    const FIRST: usize = 120;
    const SECOND: usize = 20;

    let (pull, ep) = pull_on_loopback().await;
    let make_payload = |i: usize| -> Vec<u8> {
        let prefix = format!("{i:05}|");
        let mut v = prefix.into_bytes();
        v.extend(
            b"omq-zstd-auto-train-reconnect-test-payload-"
                .iter()
                .cycle()
                .take(1000 - v.len()),
        );
        v
    };

    {
        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .compression_auto_train(true)
                .linger(Duration::from_secs(4)),
        );
        push.connect(ep.clone()).await.unwrap();
        wait_for_handshake(&pull).await;
        for i in 0..FIRST {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    {
        let push = Socket::new(
            SocketType::Push,
            Options::default()
                .compression_auto_train(true)
                .linger(Duration::from_secs(2)),
        );
        push.connect(ep.clone()).await.unwrap();
        wait_for_handshake(&pull).await;
        for i in 0..SECOND {
            push.send(Message::single(make_payload(i))).await.unwrap();
        }
        push.close().await.unwrap();
    }

    let mut got = 0;
    while let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), pull.recv()).await {
        got += 1;
        if got == FIRST + SECOND {
            break;
        }
    }
    assert_eq!(got, FIRST + SECOND);
}
