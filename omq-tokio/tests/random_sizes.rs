//! Randomized message sizes — verify framing works for non-power-of-2 payloads.
//! Uses TCP to exercise the full wire codec (inproc bypasses framing).

use std::time::Duration;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xxhash_rust::xxh3::xxh3_128;

use omq_tokio::{Message, Options, Socket, SocketType};

#[tokio::test]
async fn random_message_sizes() {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind("tcp://127.0.0.1:0").await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sizes: Vec<usize> = (0..30).map(|_| rng.random_range(1..=512 * 1024)).collect();

    let payloads: Vec<Vec<u8>> = sizes
        .iter()
        .map(|&size| (0..size).map(|_| rng.random()).collect())
        .collect();
    let hashes: Vec<u128> = payloads.iter().map(|p| xxh3_128(p)).collect();

    let push_sender = push.clone();
    let send_handle = tokio::spawn(async move {
        for payload in payloads {
            push_sender.send(Message::single(payload)).await.unwrap();
        }
    });

    for (i, (expected, &size)) in hashes.iter().zip(&sizes).enumerate() {
        let m = tokio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out for message {i} ({size} B)"))
            .unwrap();
        let got = m.part_bytes(0).unwrap();
        assert_eq!(got.len(), size, "length mismatch on message {i}");
        assert_eq!(
            xxh3_128(&got),
            *expected,
            "xxh3-128 mismatch on message {i} ({size} B)"
        );
    }

    send_handle.await.unwrap();
}

#[tokio::test]
async fn random_multipart_sizes() {
    let mut rng = StdRng::seed_from_u64(0xCAFE_BABE);

    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind("tcp://127.0.0.1:0").await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for round in 0..10 {
        let n_parts = rng.random_range(1..=5);
        let parts: Vec<Vec<u8>> = (0..n_parts)
            .map(|_| {
                let size = rng.random_range(1..=256 * 1024);
                (0..size).map(|_| rng.random()).collect()
            })
            .collect();
        let hashes: Vec<u128> = parts.iter().map(|p| xxh3_128(p)).collect();

        req.send(Message::multipart(parts.clone())).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(10), rep.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out on round {round}"))
            .unwrap();
        assert_eq!(m.len(), n_parts, "part count mismatch on round {round}");
        for (j, expected) in hashes.iter().enumerate() {
            let got = m.part_bytes(j).unwrap();
            assert_eq!(
                xxh3_128(&got),
                *expected,
                "xxh3-128 mismatch on round {round} part {j}"
            );
        }

        rep.send(Message::single(b"ack".to_vec())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .unwrap()
            .unwrap();
    }
}
