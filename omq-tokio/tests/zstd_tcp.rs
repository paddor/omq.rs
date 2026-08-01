#![cfg(feature = "zstd")]

//! End-to-end integration of `zstd+tcp://`.

use std::time::Duration;

use bytes::Bytes;
use omq_proto::proto::transform::train_zdict;
use omq_tokio::endpoint::Host;
use omq_tokio::{
    Context, ContextConfig, Endpoint, Message, MonitorEvent, Options, Socket, SocketType,
};
use rand::Rng;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

fn zstd_loopback(port: u16) -> Endpoint {
    Endpoint::ZstdTcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn tcp_from_zstd(ep: &Endpoint) -> Endpoint {
    match ep {
        Endpoint::ZstdTcp { host, port } => Endpoint::Tcp {
            host: host.clone(),
            port: *port,
        },
        other => panic!("expected zstd+tcp endpoint, got {other:?}"),
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

async fn expect_connected(sock: &Socket, min_peers: usize, label: &str) {
    tokio::time::timeout(
        Duration::from_secs(5),
        sock.wait_connected(min_peers, Duration::from_secs(5)),
    )
    .await
    .unwrap_or_else(|_| panic!("{label} wait_connected hung"))
    .unwrap_or_else(|e| panic!("{label} did not connect: {e:?}"));
}

async fn expect_subscribed(sock: &Socket, min_subscriptions: u64, label: &str) {
    tokio::time::timeout(
        Duration::from_secs(5),
        sock.wait_subscribed(min_subscriptions, Duration::from_secs(5)),
    )
    .await
    .unwrap_or_else(|_| panic!("{label} wait_subscribed hung"))
    .unwrap_or_else(|e| panic!("{label} subscription did not arrive: {e:?}"));
}

async fn expect_payload(sock: &Socket, expected: &Bytes, label: &str) {
    let got = tokio::time::timeout(Duration::from_secs(5), sock.recv())
        .await
        .unwrap_or_else(|_| panic!("{label} missed payload"))
        .unwrap();
    assert_eq!(got.part_bytes(0).unwrap(), &expected[..]);
}

#[derive(Clone, Copy)]
enum FanoutSendMode {
    Send,
    TrySend,
}

async fn send_fanout(publisher: &Socket, msg: Message, mode: FanoutSendMode) {
    match mode {
        FanoutSendMode::Send => publisher.send(msg).await.unwrap(),
        FanoutSendMode::TrySend => publisher.try_send(msg).unwrap(),
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_zstd_io_lane_send_auto_train_dict_for_late_subscriber() {
    run_pub_sub_zstd_io_lane_auto_train_dict_for_late_subscriber(FanoutSendMode::Send).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_zstd_io_lane_try_send_auto_train_dict_for_late_subscriber() {
    run_pub_sub_zstd_io_lane_auto_train_dict_for_late_subscriber(FanoutSendMode::TrySend).await;
}

async fn run_pub_sub_zstd_io_lane_auto_train_dict_for_late_subscriber(mode: FanoutSendMode) {
    let ctx = Context::with_config(ContextConfig { io_threads: 4 });
    let mut opts = Options::default()
        .compression_auto_train(true)
        .send_hwm(2048)
        .recv_hwm(2048);
    opts.xpub_nodrop = true;
    let publisher = ctx.socket(SocketType::Pub, opts.clone());
    let mut mon = publisher.monitor();
    let decoded_subs: Vec<_> = (0..4)
        .map(|_| ctx.socket(SocketType::Sub, opts.clone()))
        .collect();
    let raw = ctx.socket(SocketType::Sub, Options::default().recv_hwm(64));

    publisher.bind(zstd_loopback(0)).await.unwrap();
    let ep = loop {
        if let MonitorEvent::Listening {
            endpoint: Endpoint::ZstdTcp { port, .. },
        } = tokio::time::timeout(Duration::from_secs(5), mon.recv())
            .await
            .expect("publisher did not listen")
            .unwrap()
        {
            break zstd_loopback(port);
        }
    };

    for sub in &decoded_subs {
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe(Bytes::new()).await.unwrap();
    }

    expect_subscribed(&publisher, decoded_subs.len() as u64, "decoded subscribers").await;

    let payload = |seq: u64| {
        Bytes::from(format!(
            "{{\"kind\":\"quote\",\"venue\":\"XNAS\",\"symbol\":\"OMQ\",\"seq\":{seq},\"pad\":\"{}\"}}",
            "A".repeat(256)
        ))
    };
    for seq in 0..128 {
        let expected = payload(seq);
        send_fanout(&publisher, Message::single(expected.clone()), mode).await;
        for (idx, sub) in decoded_subs.iter().enumerate() {
            expect_payload(
                sub,
                &expected,
                &format!("decoded sub {idx} training seq {seq}"),
            )
            .await;
        }
    }

    raw.connect(tcp_from_zstd(&ep)).await.unwrap();
    expect_connected(&raw, 1, "raw subscriber").await;
    expect_connected(&publisher, decoded_subs.len() + 1, "publisher").await;
    raw.subscribe(Bytes::new()).await.unwrap();
    expect_subscribed(&publisher, decoded_subs.len() as u64 + 1, "raw subscriber").await;

    for seq in 128..132 {
        send_fanout(&publisher, Message::single(payload(seq)), mode).await;
    }

    let dict = tokio::time::timeout(Duration::from_secs(5), raw.recv())
        .await
        .expect("raw subscriber did not receive dictionary")
        .unwrap();
    let dict_part = dict.part_bytes(0).unwrap();
    assert_eq!(&dict_part[..4], &ZDICT_MAGIC);

    let compressed = tokio::time::timeout(Duration::from_secs(5), raw.recv())
        .await
        .expect("raw subscriber did not receive compressed payload")
        .unwrap();
    let compressed_part = compressed.part_bytes(0).unwrap();
    assert_eq!(&compressed_part[..4], &ZSTD_MAGIC);

    for seq in 128..132 {
        let expected = payload(seq);
        for (idx, sub) in decoded_subs.iter().enumerate() {
            expect_payload(sub, &expected, &format!("decoded sub {idx} late seq {seq}")).await;
        }
    }

    raw.close_with_linger(Some(Duration::ZERO)).await.unwrap();
    for sub in decoded_subs {
        sub.close_with_linger(Some(Duration::ZERO)).await.unwrap();
    }
    publisher
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();
}
