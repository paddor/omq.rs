//! Raw peers exercise wire rules independently of OMQ's encoder.

use std::time::Duration;

use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEADLINE: Duration = Duration::from_secs(5);

fn greeting(version: (u8, u8)) -> [u8; 64] {
    let mut wire = [0; 64];
    wire[0] = 255;
    wire[9] = 127;
    wire[10] = version.0;
    wire[11] = version.1;
    wire[12..16].copy_from_slice(b"NULL");
    wire
}

fn frame(flags: u8, body: &[u8]) -> Vec<u8> {
    let mut wire = vec![flags, u8::try_from(body.len()).unwrap()];
    wire.extend_from_slice(body);
    wire
}

fn command(name: &[u8], body: &[u8]) -> Vec<u8> {
    let mut payload = vec![u8::try_from(name.len()).unwrap()];
    payload.extend_from_slice(name);
    payload.extend_from_slice(body);
    frame(4, &payload)
}

async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(DEADLINE, async {
        let flags = stream.read_u8().await.unwrap();
        let len = if flags & 2 == 0 {
            usize::from(stream.read_u8().await.unwrap())
        } else {
            usize::try_from(stream.read_u64().await.unwrap()).unwrap()
        };
        assert!(len < 4096, "unexpected frame size");
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await.unwrap();
        (flags, body)
    })
    .await
    .unwrap()
}

async fn handshake(stream: &mut TcpStream, peer_type: &[u8], version: (u8, u8)) {
    let mut props = b"\x0bSocket-Type".to_vec();
    props.extend_from_slice(&u32::try_from(peer_type.len()).unwrap().to_be_bytes());
    props.extend_from_slice(peer_type);
    let mut wire = greeting(version).to_vec();
    wire.extend_from_slice(&command(b"READY", &props));
    stream.write_all(&wire).await.unwrap();
    let mut response = [0; 64];
    tokio::time::timeout(DEADLINE, stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response[10..12], [3, 1]);
    assert_eq!(read_frame(stream).await.1[..6], *b"\x05READY");
}

async fn raw_peer(local: &Socket, peer_type: &[u8], version: (u8, u8)) -> TcpStream {
    let endpoint = local
        .bind("tcp://127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let Endpoint::Tcp { port, .. } = endpoint else {
        unreachable!()
    };
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    handshake(&mut stream, peer_type, version).await;
    local.wait_connected(1, DEADLINE).await.unwrap();
    stream
}

async fn ping(stream: &mut TcpStream, ttl: u16) {
    let mut body = ttl.to_be_bytes().to_vec();
    body.extend_from_slice(b"ctx");
    stream.write_all(&command(b"PING", &body)).await.unwrap();
    assert_eq!(read_frame(stream).await, (4, b"\x04PONGctx".to_vec()));
}

async fn wait_disconnected(monitor: &mut omq_tokio::MonitorStream) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if matches!(
                monitor.recv().await.unwrap(),
                MonitorEvent::Disconnected { .. }
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn single_frame_sockets_discard_multipart_without_losing_connection() {
    for (kind, peer) in [
        (SocketType::Client, &b"SERVER"[..]),
        (SocketType::Server, b"CLIENT"),
        (SocketType::Gather, b"SCATTER"),
        (SocketType::Channel, b"CHANNEL"),
    ] {
        let local = Socket::new(kind, Options::default().linger(Duration::ZERO));
        let mut raw = raw_peer(&local, peer, (3, 1)).await;
        let mut wire = frame(1, b"first");
        wire.extend_from_slice(&frame(0, b"last"));
        wire.extend_from_slice(&frame(0, b"valid"));
        raw.write_all(&wire).await.unwrap();
        let message = tokio::time::timeout(DEADLINE, local.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.len(), 1);
        assert_eq!(message.part_bytes(0).unwrap().as_ref(), b"valid");
        assert!(local.try_recv().is_err());
        raw.write_all(&frame(0, b"still connected")).await.unwrap();
        assert_eq!(
            tokio::time::timeout(DEADLINE, local.recv())
                .await
                .unwrap()
                .unwrap()
                .part_bytes(0)
                .unwrap()
                .as_ref(),
            b"still connected"
        );
    }
}

#[tokio::test]
async fn subscription_commands_follow_peer_version() {
    for kind in [SocketType::Sub, SocketType::XSub] {
        for version in [(3, 0), (3, 1), (4, 0)] {
            let local = Socket::new(kind, Options::default().linger(Duration::ZERO));
            let mut raw = raw_peer(&local, b"PUB", version).await;
            local.subscribe("topic").await.unwrap();
            let sub = read_frame(&mut raw).await;
            local.unsubscribe("topic").await.unwrap();
            let cancel = read_frame(&mut raw).await;
            if version == (3, 0) {
                assert_eq!(sub, (0, b"\x01topic".to_vec()));
                assert_eq!(cancel, (0, b"\x00topic".to_vec()));
            } else {
                assert_eq!(sub, (4, b"\x09SUBSCRIBEtopic".to_vec()));
                assert_eq!(cancel, (4, b"\x06CANCELtopic".to_vec()));
            }
        }
    }
}

#[tokio::test]
async fn duplicate_subscriptions_survive_one_cancel_and_replay() {
    for transport in ["tcp", "inproc"] {
        for kind in [SocketType::Sub, SocketType::XSub] {
            for prefix in ["", "topic"] {
                let publisher =
                    Socket::new(SocketType::XPub, Options::default().linger(Duration::ZERO));
                let subscriber = Socket::new(kind, Options::default().linger(Duration::ZERO));
                subscriber.subscribe(prefix).await.unwrap();
                subscriber.subscribe(prefix).await.unwrap();
                let endpoint = if transport == "tcp" {
                    "tcp://127.0.0.1:0".to_owned()
                } else {
                    format!("inproc://duplicate-{kind:?}-{}", prefix.len())
                };
                let endpoint = publisher.bind(endpoint.parse().unwrap()).await.unwrap();
                for attempt in 0..2 {
                    subscriber.connect(endpoint.clone()).await.unwrap();
                    // XPUB notifications provide a control-plane barrier for both replayed copies.
                    for _ in 0..2 {
                        let notification = tokio::time::timeout(DEADLINE, publisher.recv())
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            notification.part_bytes(0).unwrap().as_ref(),
                            [b"\x01".as_slice(), prefix.as_bytes()].concat()
                        );
                    }
                    subscriber.unsubscribe(prefix).await.unwrap();
                    let cancel = tokio::time::timeout(DEADLINE, publisher.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        cancel.part_bytes(0).unwrap().as_ref(),
                        [b"\x00".as_slice(), prefix.as_bytes()].concat()
                    );
                    publisher.send(Message::single("topic/body")).await.unwrap();
                    assert_eq!(
                        tokio::time::timeout(DEADLINE, subscriber.recv())
                            .await
                            .unwrap()
                            .unwrap()
                            .part_bytes(0)
                            .unwrap()
                            .as_ref(),
                        b"topic/body"
                    );
                    if attempt == 0 {
                        subscriber.disconnect(endpoint.clone()).await.unwrap();
                        subscriber.subscribe(prefix).await.unwrap();
                    } else {
                        subscriber.unsubscribe(prefix).await.unwrap();
                        let _ = tokio::time::timeout(DEADLINE, publisher.recv())
                            .await
                            .unwrap()
                            .unwrap();
                        publisher
                            .send(Message::single("topic/dropped"))
                            .await
                            .unwrap();
                        assert!(
                            tokio::time::timeout(Duration::from_millis(50), subscriber.recv())
                                .await
                                .is_err()
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn received_ttl_expires_without_local_heartbeats_and_reconnects() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let local = Socket::new(SocketType::Pair, Options::default().linger(Duration::ZERO));
    let mut monitor = local.monitor();
    local.connect(endpoint).await.unwrap();
    let (mut raw, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    handshake(&mut raw, b"PAIR", (3, 1)).await;
    ping(&mut raw, 1).await;
    wait_disconnected(&mut monitor).await;
    let (mut next, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    handshake(&mut next, b"PAIR", (3, 1)).await;
    next.write_all(&frame(0, b"reconnected")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, local.recv())
            .await
            .unwrap()
            .unwrap()
            .part_bytes(0)
            .unwrap()
            .as_ref(),
        b"reconnected"
    );
}

#[tokio::test]
async fn incoming_traffic_and_zero_ttl_cancel_peer_timeout() {
    let local = Socket::new(SocketType::Pair, Options::default().linger(Duration::ZERO));
    let mut raw = raw_peer(&local, b"PAIR", (3, 1)).await;
    for cancel_with_data in [true, false] {
        ping(&mut raw, 3).await;
        if cancel_with_data {
            raw.write_all(&frame(0, b"activity")).await.unwrap();
            let _ = tokio::time::timeout(DEADLINE, local.recv())
                .await
                .unwrap()
                .unwrap();
        } else {
            ping(&mut raw, 0).await;
        }
        // Twice the advertised TTL; any surviving timer would close the stream.
        assert!(
            tokio::time::timeout(Duration::from_millis(600), raw.read_u8())
                .await
                .is_err()
        );
        raw.write_all(&frame(0, b"still alive")).await.unwrap();
        assert_eq!(
            tokio::time::timeout(DEADLINE, local.recv())
                .await
                .unwrap()
                .unwrap()
                .part_bytes(0)
                .unwrap()
                .as_ref(),
            b"still alive"
        );
    }
}

#[tokio::test]
async fn peer_ttl_suspends_during_receive_backpressure_then_expires() {
    let local = Socket::new(
        SocketType::Pull,
        Options::default().recv_hwm(16).linger(Duration::ZERO),
    );
    let mut raw = raw_peer(&local, b"PUSH", (3, 1)).await;
    let mut wire = Vec::new();
    for _ in 0..17 {
        wire.extend_from_slice(&frame(0, b"queued"));
    }
    wire.extend_from_slice(&command(b"PING", b"\x00\x03ctx"));
    raw.write_all(&wire).await.unwrap();
    assert_eq!(read_frame(&mut raw).await.1, b"\x04PONGctx");
    assert!(
        tokio::time::timeout(Duration::from_millis(600), raw.read_u8())
            .await
            .is_err(),
        "local backpressure must suspend TTL"
    );
    for _ in 0..17 {
        assert_eq!(
            tokio::time::timeout(DEADLINE, local.recv())
                .await
                .unwrap()
                .unwrap()
                .part_bytes(0)
                .unwrap()
                .as_ref(),
            b"queued"
        );
    }
    // The timer gets a fresh TTL after the application resumes draining.
    assert_eq!(
        tokio::time::timeout(DEADLINE, raw.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn heartbeat_interval_does_not_send_ping_to_zmtp_30_peer() {
    let local = Socket::new(
        SocketType::Pair,
        Options::default()
            .heartbeat_interval(Duration::from_millis(50))
            .linger(Duration::ZERO),
    );
    let mut raw = raw_peer(&local, b"PAIR", (3, 0)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), raw.read_u8())
            .await
            .is_err()
    );
    raw.write_all(&frame(0, b"legacy alive")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, local.recv())
            .await
            .unwrap()
            .unwrap()
            .part_bytes(0)
            .unwrap()
            .as_ref(),
        b"legacy alive"
    );
}

#[tokio::test]
async fn incompatible_socket_types_send_error_before_closing() {
    let local = Socket::new(SocketType::Pull, Options::default().linger(Duration::ZERO));
    let endpoint = local
        .bind("tcp://127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let Endpoint::Tcp { port, .. } = endpoint else {
        unreachable!()
    };
    let mut raw = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    handshake(&mut raw, b"PUB", (3, 1)).await;
    assert_eq!(
        read_frame(&mut raw).await,
        (4, b"\x05ERROR\x19Incompatible socket types".to_vec())
    );
    assert!(
        tokio::time::timeout(DEADLINE, raw.read_u8())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn exclusive_receive_honors_received_ttl_without_local_heartbeats() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut raw, _) = listener.accept().await.unwrap();
        handshake(&mut raw, b"PAIR", (3, 1)).await;
        ping(&mut raw, 1).await;
        assert!(
            tokio::time::timeout(DEADLINE, raw.read_u8())
                .await
                .unwrap()
                .is_err()
        );
    });
    let mut local = omq_tokio::exclusive::Socket::connect(
        SocketType::Pair,
        endpoint,
        omq_tokio::exclusive::Options::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, local.recv()).await.unwrap(),
        Err(omq_tokio::Error::Timeout)
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn peer_ttl_expires_during_continuous_outbound_traffic() {
    let local = Socket::new(SocketType::Pair, Options::default().linger(Duration::ZERO));
    let mut raw = raw_peer(&local, b"PAIR", (3, 1)).await;
    ping(&mut raw, 3).await;
    let sender = local.clone();
    let traffic = tokio::spawn(async move {
        loop {
            sender
                .send(Message::single("outbound does not prove peer activity"))
                .await
                .unwrap();
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut total = 0;
        let mut buf = [0; 8192];
        loop {
            let n = raw.read(&mut buf).await.unwrap();
            if n == 0 {
                return total;
            }
            total += n;
        }
    })
    .await;
    traffic.abort();
    let _ = traffic.await;
    assert!(result.unwrap() > 0, "outbound traffic did not run");
}
