//! Large messages over NULL mechanism TCP.
//!
//! Exercises multi-chunk Payload / scatter-gather framing at payload sizes
//! that span many TCP segments. Encryption masks framing bugs in CURVE
//! suites; this file tests plain framing directly.

mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

async fn push_pull_large(size_bytes: usize) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let payload: Vec<u8> = (0..size_bytes).map(|i| (i & 0xFF) as u8).collect();
    push.send(Message::single(payload.clone())).await.unwrap();

    let m = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("large message recv timed out")
        .unwrap();
    let got = m.part_bytes(0).unwrap();
    assert_eq!(
        got.len(),
        size_bytes,
        "payload length mismatch at {size_bytes} B"
    );
    assert_eq!(
        &*got,
        &payload[..],
        "payload data corrupted at {size_bytes} B"
    );
}

fn patterned_payload(len: usize, seq: u64) -> Vec<u8> {
    let mut payload = vec![0u8; len];
    payload[..8].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
    payload[8..16].copy_from_slice(&seq.to_le_bytes());
    let mask = (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3);
    for (i, byte) in payload.iter_mut().enumerate().skip(16) {
        let mut expected = (i as u8).wrapping_mul(31);
        expected ^= ((i >> 8) as u8).wrapping_mul(17);
        expected ^= ((i >> 16) as u8).wrapping_mul(13);
        *byte = expected ^ mask;
    }
    payload
}

fn assert_patterned_payload(got: &[u8], seq: u64) {
    let magic = u64::from_le_bytes(got[..8].try_into().unwrap());
    let got_seq = u64::from_le_bytes(got[8..16].try_into().unwrap());
    assert_eq!(magic, 0xDEAD_BEEF_CAFE_F00D, "canary corrupt");
    assert_eq!(got_seq, seq, "payload sequence mismatch");

    let mask = (seq as u8).wrapping_mul(7) ^ ((seq >> 8) as u8).wrapping_mul(3);
    for (i, &byte) in got.iter().enumerate().skip(16) {
        let mut expected = (i as u8).wrapping_mul(31);
        expected ^= ((i >> 8) as u8).wrapping_mul(17);
        expected ^= ((i >> 16) as u8).wrapping_mul(13);
        expected ^= mask;
        assert_eq!(
            byte, expected,
            "payload byte corruption at offset {i}: seq={seq}"
        );
    }
}

#[tokio::test]
async fn large_message_64kib() {
    push_pull_large(64 * 1024).await;
}

#[tokio::test]
async fn large_message_256kib() {
    push_pull_large(256 * 1024).await;
}

#[tokio::test]
async fn large_message_1mib() {
    push_pull_large(1024 * 1024).await;
}

#[tokio::test]
async fn large_multipart_over_tcp() {
    let part_size = 256 * 1024;
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();
    // Keep the sender buffer small enough to force partial direct writes on
    // loopback. The IO driver must flush the queued remainder.
    let req = Socket::new(SocketType::Req, Options::default().send_buffer_size(4096));
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;
    test_support::wait_for_handshake(&rep).await;

    let part_a: Vec<u8> = vec![0xAA; part_size];
    let part_b: Vec<u8> = vec![0xBB; part_size];

    req.send(Message::multipart([part_a, part_b]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(10), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 2, "expected 2-part message");
    assert_eq!(m.part_bytes(0).unwrap().len(), part_size);
    assert_eq!(*m.part_bytes(0).unwrap().first().unwrap(), 0xAA);
    assert_eq!(m.part_bytes(1).unwrap().len(), part_size);
    assert_eq!(*m.part_bytes(1).unwrap().first().unwrap(), 0xBB);
}

#[tokio::test]
async fn huge_messages_xxhash() {
    use xxhash_rust::xxh3::xxh3_128;

    const SIZES: [usize; 3] = [4 * 1024 * 1024, 8 * 1024 * 1024, 100 * 1024 * 1024];

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let mut hashes = [0u128; 3];
    for (i, &size) in SIZES.iter().enumerate() {
        let seed = (i as u8).wrapping_mul(0xAB).wrapping_add(0x37);
        let payload = vec![seed; size];
        hashes[i] = xxh3_128(&payload);
        push.send(Message::single(payload)).await.unwrap();
    }

    for (i, expected) in hashes.iter().enumerate() {
        let m = tokio::time::timeout(Duration::from_mins(2), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out for message {i}"))
            .unwrap();
        let got = m.part_bytes(0).unwrap();
        assert_eq!(got.len(), SIZES[i], "length mismatch on message {i}");
        assert_eq!(
            xxh3_128(&got),
            *expected,
            "xxh3-128 mismatch on message {i} — payload corrupted in transit"
        );
    }
}

#[tokio::test]
async fn large_message_back_to_back() {
    // Two large messages sent without waiting; verifies framing continuity.
    let size = 128 * 1024;
    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let p1: Vec<u8> = vec![0x11; size];
    let p2: Vec<u8> = vec![0x22; size];
    push.send(Message::single(p1.clone())).await.unwrap();
    push.send(Message::single(p2.clone())).await.unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*m1.part_bytes(0).unwrap(), &p1[..]);
    assert_eq!(&*m2.part_bytes(0).unwrap(), &p2[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_large_messages_preserve_pattern_under_hwm_backpressure() {
    const COUNT: u64 = 32;
    const SIZE: usize = 1024 * 1024;

    let opts = Options::default()
        .send_hwm(4)
        .recv_hwm(4)
        .send_buffer_size(64 * 1024)
        .recv_buffer_size(64 * 1024);
    let pull = Socket::new(SocketType::Pull, opts.clone());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let sender = tokio::spawn(async move {
        for seq in 0..COUNT {
            push.send(Message::single(patterned_payload(SIZE, seq)))
                .await
                .unwrap();
        }
        push
    });

    for seq in 0..COUNT {
        let m = tokio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out receiving seq {seq}"))
            .unwrap();
        let got = m.part_bytes(0).unwrap();
        assert_eq!(got.len(), SIZE, "payload length mismatch for seq {seq}");
        assert_patterned_payload(&got, seq);
    }

    let push = sender.await.unwrap();
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_large_messages_survive_recv_bypass_ring_pressure() {
    const COUNT: u64 = 16;
    const HOLD_COUNT: u64 = 4;
    const SIZE: usize = 4 * 1024 * 1024;

    let opts = Options::default()
        .send_hwm(4)
        .recv_hwm(4)
        .send_buffer_size(64 * 1024)
        .recv_buffer_size(64 * 1024);
    let pull = Socket::new(SocketType::Pull, opts.clone());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let sender = tokio::spawn(async move {
        for seq in 0..COUNT {
            push.send(Message::single(patterned_payload(SIZE, seq)))
                .await
                .unwrap();
        }
        push
    });

    let mut held = Vec::new();
    for seq in 0..COUNT {
        let m = tokio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out receiving seq {seq}"))
            .unwrap();
        let got = m.part_bytes(0).unwrap();
        assert_eq!(got.len(), SIZE, "payload length mismatch for seq {seq}");
        assert_patterned_payload(&got, seq);

        if seq < HOLD_COUNT {
            held.push((seq, m));
        }
        for (held_seq, held_msg) in &held {
            let held_payload = held_msg.part_bytes(0).unwrap();
            assert_patterned_payload(&held_payload, *held_seq);
        }
    }

    let push = sender.await.unwrap();
    push.close().await.unwrap();
    pull.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_message_preserves_pattern_with_tiny_tcp_buffers() {
    const COUNT: u64 = 1;
    const SIZE: usize = 1024 * 1024;

    let opts = Options::default()
        .send_hwm(4)
        .recv_hwm(4)
        .send_buffer_size(16 * 1024)
        .recv_buffer_size(16 * 1024);
    let pull = Socket::new(SocketType::Pull, opts.clone());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    let push = Socket::new(SocketType::Push, opts);
    push.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&push).await;
    test_support::wait_for_handshake(&pull).await;

    let sender = tokio::spawn(async move {
        for seq in 0..COUNT {
            push.send(Message::single(patterned_payload(SIZE, seq)))
                .await
                .unwrap();
        }
        push
    });

    for seq in 0..COUNT {
        let m = tokio::time::timeout(Duration::from_secs(15), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out receiving seq {seq}"))
            .unwrap();
        let got = m.part_bytes(0).unwrap();
        assert_eq!(got.len(), SIZE, "payload length mismatch for seq {seq}");
        assert_patterned_payload(&got, seq);
    }

    let push = sender.await.unwrap();
    push.close().await.unwrap();
    pull.close().await.unwrap();
}
