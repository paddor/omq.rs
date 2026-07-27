#![cfg(feature = "lz4")]

//! Pub/sub over `lz4+tcp://`.

mod test_support;

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

const FAN_OUT_READY_TIMEOUT: Duration = Duration::from_secs(5);

fn lz4_loopback() -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
    }
}

fn tcp_from_lz4(ep: &Endpoint) -> Endpoint {
    match ep {
        Endpoint::Lz4Tcp { host, port } => Endpoint::Tcp {
            host: host.clone(),
            port: *port,
        },
        other => panic!("expected lz4+tcp endpoint, got {other:?}"),
    }
}

async fn bind_lz4_loopback(sock: &Socket) -> Endpoint {
    let mut mon = sock.monitor();
    sock.bind(lz4_loopback()).await.unwrap();
    let port = loop {
        if let MonitorEvent::Listening {
            endpoint: Endpoint::Lz4Tcp { port, .. },
        } = mon.recv().await.unwrap()
        {
            break port;
        }
    };
    Endpoint::Lz4Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[tokio::test]
async fn sub_duplicate_lz4_tcp_connect_is_ignored() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let ep = bind_lz4_loopback(&publisher).await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep.clone()).await.unwrap();
    subscriber.connect(ep).await.unwrap();

    publisher
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("publisher did not see subscriber");
    subscriber
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("subscriber did not connect");
    test_support::assert_no_second_connection(&publisher, "publisher").await;
    test_support::assert_no_second_connection(&subscriber, "subscriber").await;

    subscriber.subscribe(bytes::Bytes::new()).await.unwrap();
    publisher
        .wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");
    let second_subscribe = publisher
        .wait_subscribed(2, Duration::from_millis(250))
        .await;
    assert!(
        second_subscribe.is_err(),
        "duplicate lz4+tcp connect replayed subscription twice"
    );

    publisher.send(Message::single("hello")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .expect("subscriber did not receive message")
        .unwrap();
    assert_eq!(msg, Message::single("hello"));

    let duplicate = tokio::time::timeout(Duration::from_millis(250), subscriber.recv()).await;
    assert!(duplicate.is_err(), "subscriber received duplicate message");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_sub_lz4_io_lane_fan_out_ships_dict_to_late_subscriber() {
    use omq_proto::proto::transform::lz4::DictTrainer;

    const N_DECODED_SUBS: usize = 4;

    let sample = r#"{"kind":"quote","venue":"XNAS","symbol":"OMQ","bid":101.25,"ask":101.27,"depth":[10125,10126,10127],"flags":"regular"}"#;
    let mut trainer = DictTrainer::new(2048);
    for _ in 0..100 {
        trainer.add_sample(sample.as_bytes());
    }
    let dict = bytes::Bytes::from(trainer.train());
    let opts = Options::default().compression_dict(dict);

    let publisher = Socket::new(SocketType::Pub, opts.clone());
    let ep = bind_lz4_loopback(&publisher).await;

    let mut decoded_subs = Vec::with_capacity(N_DECODED_SUBS);
    for _ in 0..N_DECODED_SUBS {
        let sub = Socket::new(SocketType::Sub, opts.clone());
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        decoded_subs.push(sub);
    }
    publisher
        .wait_subscribed(N_DECODED_SUBS as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("subscriptions did not arrive");

    let raw_sub = Socket::new(SocketType::Sub, Options::default());
    raw_sub.connect(tcp_from_lz4(&ep)).await.unwrap();
    raw_sub.subscribe(bytes::Bytes::new()).await.unwrap();
    publisher
        .wait_subscribed((N_DECODED_SUBS + 1) as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("raw subscriber subscription did not arrive");

    let payload1 = bytes::Bytes::from(format!("{sample}{}", " ".repeat(256)));
    let payload2 = bytes::Bytes::from(format!("{sample}{}", " ".repeat(300)));
    publisher
        .send(Message::single(payload1.clone()))
        .await
        .unwrap();
    publisher
        .send(Message::single(payload2.clone()))
        .await
        .unwrap();

    let dict_msg = tokio::time::timeout(Duration::from_secs(5), raw_sub.recv())
        .await
        .expect("raw subscriber did not receive dictionary shipment")
        .unwrap();
    let dict_part = dict_msg.part_bytes(0).unwrap();
    assert!(
        dict_part.starts_with(b"LZ4D"),
        "first raw message must be the LZ4 dictionary shipment, got prefix {:?}",
        &dict_part[..dict_part.len().min(4)]
    );

    let raw_payload1 = tokio::time::timeout(Duration::from_secs(5), raw_sub.recv())
        .await
        .expect("raw subscriber did not receive first compressed payload")
        .unwrap();
    let raw_part1 = raw_payload1.part_bytes(0).unwrap();
    assert!(
        raw_part1.starts_with(b"LZ4B"),
        "first payload should use the shipped dictionary, got prefix {:?}",
        &raw_part1[..raw_part1.len().min(4)]
    );

    let raw_payload2 = tokio::time::timeout(Duration::from_secs(5), raw_sub.recv())
        .await
        .expect("raw subscriber did not receive second compressed payload")
        .unwrap();
    let raw_part2 = raw_payload2.part_bytes(0).unwrap();
    assert!(
        raw_part2.starts_with(b"LZ4B"),
        "second payload should remain dictionary-compressed, got prefix {:?}",
        &raw_part2[..raw_part2.len().min(4)]
    );

    for (idx, sub) in decoded_subs.iter().enumerate() {
        let got1 = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("decoded sub {idx} missed first payload"))
            .unwrap();
        assert_eq!(got1.part_bytes(0).unwrap(), payload1);

        let got2 = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("decoded sub {idx} missed second payload"))
            .unwrap();
        assert_eq!(got2.part_bytes(0).unwrap(), payload2);
    }
}

#[tokio::test]
async fn pub_sub_lz4_io_lane_fan_out_auto_trains_dict_for_late_subscriber() {
    const N_DECODED_SUBS: usize = 4;
    const N_TRAINING_MSGS: usize = 100;

    let opts = Options::default().compression_auto_train(true);
    let publisher = Socket::new(SocketType::Pub, opts.clone());
    let ep = bind_lz4_loopback(&publisher).await;

    let mut decoded_subs = Vec::with_capacity(N_DECODED_SUBS);
    for _ in 0..N_DECODED_SUBS {
        let sub = Socket::new(SocketType::Sub, opts.clone());
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        decoded_subs.push(sub);
    }
    publisher
        .wait_subscribed(N_DECODED_SUBS as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("subscriptions did not arrive");

    for i in 0..N_TRAINING_MSGS {
        let payload = bytes::Bytes::from(format!(
            "{{\"kind\":\"quote\",\"venue\":\"XNAS\",\"symbol\":\"OMQ\",\"seq\":{i},\"bid\":101.25,\"ask\":101.27,\"depth\":[10125,10126,10127],\"pad\":\"{}\"}}",
            "A".repeat(160)
        ));
        tokio::time::timeout(
            Duration::from_secs(5),
            publisher.send(Message::single(payload)),
        )
        .await
        .unwrap_or_else(|_| panic!("publisher send timed out for training payload {i}"))
        .unwrap();

        for (idx, sub) in decoded_subs.iter().enumerate() {
            tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("decoded sub {idx} missed training payload {i}"))
                .unwrap();
        }
    }
    let raw_sub = Socket::new(SocketType::Sub, Options::default());
    raw_sub.connect(tcp_from_lz4(&ep)).await.unwrap();
    raw_sub.subscribe(bytes::Bytes::new()).await.unwrap();
    publisher
        .wait_subscribed((N_DECODED_SUBS + 1) as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("raw subscriber subscription did not arrive");

    let late_payload = bytes::Bytes::from(format!(
        "{{\"kind\":\"quote\",\"venue\":\"XNAS\",\"symbol\":\"OMQ\",\"seq\":1000,\"bid\":101.25,\"ask\":101.27,\"depth\":[10125,10126,10127],\"pad\":\"{}\"}}",
        "A".repeat(192)
    ));
    tokio::time::timeout(
        Duration::from_secs(5),
        publisher.send(Message::single(late_payload.clone())),
    )
    .await
    .expect("publisher send timed out for late payload")
    .unwrap();

    let dict_msg = tokio::time::timeout(Duration::from_secs(5), raw_sub.recv())
        .await
        .expect("raw subscriber did not receive auto-trained dictionary shipment")
        .unwrap();
    let dict_part = dict_msg.part_bytes(0).unwrap();
    assert!(
        dict_part.starts_with(b"LZ4D"),
        "first raw late message must be the auto-trained LZ4 dictionary, got prefix {:?}",
        &dict_part[..dict_part.len().min(4)]
    );

    let compressed = tokio::time::timeout(Duration::from_secs(5), raw_sub.recv())
        .await
        .expect("raw subscriber did not receive dictionary-compressed late payload")
        .unwrap();
    let compressed_part = compressed.part_bytes(0).unwrap();
    assert!(
        compressed_part.starts_with(b"LZ4B"),
        "late payload should use the auto-trained dictionary, got prefix {:?}",
        &compressed_part[..compressed_part.len().min(4)]
    );

    for (idx, sub) in decoded_subs.iter().enumerate() {
        let got = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("decoded sub {idx} missed late payload"))
            .unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), late_payload);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_sub_lz4_io_lane_fan_out_deferred_large_message_preserves_order() {
    use omq_proto::proto::transform::lz4::DictTrainer;

    const N_SUBS: usize = 4;
    const N_MSGS: usize = 12;

    let sample = r#"{"kind":"quote","venue":"XNAS","symbol":"OMQ","bid":101.25,"ask":101.27,"depth":[10125,10126,10127],"flags":"regular"}"#;
    let mut trainer = DictTrainer::new(2048);
    for _ in 0..100 {
        trainer.add_sample(sample.as_bytes());
    }
    let opts = Options::default().compression_dict(bytes::Bytes::from(trainer.train()));

    let publisher = Socket::new(SocketType::Pub, opts.clone());
    let ep = bind_lz4_loopback(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let sub = Socket::new(SocketType::Sub, opts.clone());
        sub.connect(ep.clone()).await.unwrap();
        sub.subscribe(bytes::Bytes::new()).await.unwrap();
        subs.push(sub);
    }
    publisher
        .wait_subscribed(N_SUBS as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("subscriptions did not arrive");

    let mut expected = Vec::with_capacity(N_MSGS);
    let first = bytes::Bytes::from(vec![b'A'; 2 * 1024 * 1024]);
    expected.push(first.clone());
    publisher.send(Message::single(first)).await.unwrap();

    for i in 1..N_MSGS {
        let payload = bytes::Bytes::from(format!("{sample} seq={i} {}", "B".repeat(256)));
        expected.push(payload.clone());
        publisher.send(Message::single(payload)).await.unwrap();
    }

    for (sub_idx, sub) in subs.iter().enumerate() {
        for (msg_idx, expected_payload) in expected.iter().enumerate() {
            let got = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {sub_idx} timed out at msg {msg_idx}"))
                .unwrap();
            assert_eq!(
                got.part_bytes(0).unwrap(),
                expected_payload,
                "sub {sub_idx} msg {msg_idx}"
            );
        }
    }
}

#[tokio::test]
async fn pub_sub_prefix_filter() {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    let ep = bind_lz4_loopback(&publisher).await;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber.connect(ep).await.unwrap();
    subscriber.subscribe("news.").await.unwrap();

    publisher
        .wait_subscribed(1, Duration::from_secs(1))
        .await
        .expect("subscription did not arrive");

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["news.tech", "rust 2.0"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.part_bytes(0).unwrap(), &b"news.sports"[..]);
    assert_eq!(m1.part_bytes(1).unwrap(), &b"ball scores"[..]);

    let m2 = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m2.part_bytes(0).unwrap(), &b"news.tech"[..]);
    assert_eq!(m2.part_bytes(1).unwrap(), &b"rust 2.0"[..]);

    let nothing = tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await;
    assert!(
        nothing.is_err(),
        "non-matching message must not be delivered"
    );
}

/// Fan-out to multiple subscribers: exercises the multi-target
/// `dispatch_to_targets` path that encodes once and pushes
/// pre-encoded compressed bytes to all peers.
#[tokio::test]
async fn pub_sub_lz4_fan_out() {
    const N_SUBS: usize = 4;
    const N_MSGS: usize = 200;

    let publisher = Socket::new(SocketType::Pub, Options::default());
    let ep = bind_lz4_loopback(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, Options::default());
        s.connect(ep.clone()).await.unwrap();
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        subs.push(s);
    }
    publisher
        .wait_subscribed(N_SUBS as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("subscriptions did not arrive");

    for i in 0..N_MSGS {
        let body = format!("msg-{i:04}");
        publisher
            .send(Message::single(bytes::Bytes::from(body)))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!("msg-{i:04}");
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}

/// Fan-out with a compression dictionary configured. The fan-out
/// encoder ships the socket dictionary once per connection, then uses
/// dictionary-compressed payloads for subsequent messages.
#[tokio::test]
async fn pub_sub_lz4_fan_out_with_dict() {
    use omq_proto::proto::transform::lz4::DictTrainer;

    const N_SUBS: usize = 4;
    const N_MSGS: usize = 50;

    let mut trainer = DictTrainer::new(2048);
    for i in 0..100 {
        let s = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        trainer.add_sample(s.as_bytes());
    }
    let dict = bytes::Bytes::from(trainer.train());
    let opts = Options::default().compression_dict(dict);

    let publisher = Socket::new(SocketType::Pub, opts.clone());
    let ep = bind_lz4_loopback(&publisher).await;

    let mut subs = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let s = Socket::new(SocketType::Sub, opts.clone());
        s.connect(ep.clone()).await.unwrap();
        s.subscribe(bytes::Bytes::new()).await.unwrap();
        subs.push(s);
    }
    publisher
        .wait_subscribed(N_SUBS as u64, FAN_OUT_READY_TIMEOUT)
        .await
        .expect("subscriptions did not arrive");

    for i in 0..N_MSGS {
        let body = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
        publisher
            .send(Message::single(bytes::Bytes::from(body)))
            .await
            .unwrap();
    }

    for (si, sub) in subs.iter().enumerate() {
        for i in 0..N_MSGS {
            let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                .await
                .unwrap_or_else(|_| panic!("sub {si} timed out at msg {i}"))
                .unwrap();
            let expected = format!(r#"{{"seq":{i},"msg":"hello world","tag":"bench"}}"#);
            assert_eq!(
                m.part_bytes(0).unwrap(),
                expected.as_bytes(),
                "sub {si} msg {i}"
            );
        }
    }
}
