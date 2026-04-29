//! BLAKE3ZMQ end-to-end integration tests: handshake + per-frame AEAD
//! between two omq.rs sockets.

#![cfg(feature = "blake3zmq")]

use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_tokio::{Blake3ZmqKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType};

// Encrypted mechanisms aren't valid on inproc (no wire to protect;
// the inproc fast path skips the codec entirely). Use IPC instead
// - same in-process testing convenience, real byte-stream
// transport, codec runs.
fn inproc_ep(name: &str) -> Endpoint {
    let mut p = std::env::temp_dir();
    p.push(format!("omq-blake3-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

#[tokio::test]
async fn blake3zmq_push_pull_roundtrip() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-pp");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hello over blake3zmq")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over blake3zmq"[..]);
}

#[tokio::test]
async fn blake3zmq_multiple_messages_keep_counter_synced() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-many");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    for i in 0..50u32 {
        let msg = format!("m-{i}");
        client.send(Message::single(msg.clone())).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.parts()[0].coalesce(), msg.as_bytes());
    }
}

#[tokio::test]
async fn blake3zmq_long_payload_uses_long_frame_aad() {
    // Payloads > 255 bytes set the LONG flag bit on the wire frame
    // header. The flag byte is part of the BLAKE3ZMQ AAD, so both
    // peers must compute the same flags for AEAD to verify. Send a
    // mix of short and long payloads to exercise both paths.
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-long");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    // Short.
    client.send(Message::single("short")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"short"[..]);

    // Long (1 KiB).
    let plaintext = vec![b'L'; 1024];
    client.send(Message::single(plaintext.clone())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().to_vec(), plaintext);

    // Mix again.
    client.send(Message::single("short again")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"short again"[..]);
}

#[tokio::test]
async fn blake3zmq_multipart_message_delivered_atomically() {
    // RFC §10.6: a 3-part message arrives atomically. The two
    // intermediate parts (MORE=1) are buffered inside the codec; the
    // application sees the message only when the last (MORE=0) part
    // verifies. The test indirectly proves no MORE-flagged part
    // reaches the application by checking parts.len() and the
    // contents of every part - if intermediates leaked early as
    // separate messages, recv would surface a different shape.
    use bytes::Bytes;
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-multipart-atomic");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    let msg = Message::multipart::<_, Bytes>([
        Bytes::from_static(b"part1"),
        Bytes::from_static(b"middle"),
        Bytes::from_static(b"final"),
    ]);
    client.send(msg).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got.parts()[0].coalesce(), &b"part1"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"middle"[..]);
    assert_eq!(got.parts()[2].coalesce(), &b"final"[..]);

    // No second message is queued - recv should time out.
    let extra = tokio::time::timeout(Duration::from_millis(150), server.recv()).await;
    assert!(extra.is_err(), "no extra messages: {extra:?}");
}

#[tokio::test]
async fn blake3zmq_encrypts_subscribe_command() {
    // RFC §10.3: every post-handshake frame is AEAD-encrypted, data
    // and commands alike. Exercise the command path with PUB/SUB:
    // SUB sends SUBSCRIBE("topic"), PUB receives it (decrypted),
    // matches against published messages, and only delivers what
    // matches. If commands were unencrypted, SUBSCRIBE would still
    // cross the wire correctly (it'd just leak the topic prefix in
    // plaintext) - so this test verifies the *functional* command
    // round-trip; the privacy property is a wire-format invariant
    // covered by the unit tests.
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-pub-sub");

    let pub_socket = Socket::new(
        SocketType::Pub,
        Options::default().blake3zmq_server(server_kp),
    );
    pub_socket.bind(ep.clone()).await.unwrap();

    let sub_socket = Socket::new(
        SocketType::Sub,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    sub_socket.connect(ep).await.unwrap();
    // SUBSCRIBE flows as an encrypted COMMAND frame to PUB; PUB
    // decrypts it, parses the prefix, and stores it in the
    // subscriptions table for that peer.
    sub_socket.subscribe("weather").await.unwrap();

    // Wait for the SUBSCRIBE to land. Without it, PUB would drop
    // every message because no peer subscribes to anything.
    tokio::time::sleep(Duration::from_millis(50)).await;

    pub_socket
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    pub_socket
        .send(Message::multipart(["news", "ignored"]))
        .await
        .unwrap();
    pub_socket
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(1), sub_socket.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(1), sub_socket.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1.parts()[1].coalesce(), &b"sunny"[..]);
    assert_eq!(m2.parts()[1].coalesce(), &b"rain"[..]);

    // No third message - `news` was filtered by the subscription.
    let extra = tokio::time::timeout(Duration::from_millis(150), sub_socket.recv()).await;
    assert!(extra.is_err(), "news leaked: {extra:?}");
}

#[tokio::test]
async fn blake3zmq_rejects_wrong_server_key() {
    // Client targets a different public key than the server holds.
    // The HELLO box is encrypted to the wrong key; the server's
    // X25519(s, C') yields a different dh1, so AEAD decrypt fails
    // and the handshake aborts.
    let real_server = Blake3ZmqKeypair::generate();
    let unrelated = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let ep = inproc_ep("blake3-bad-key");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(real_server),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, unrelated.public),
    );
    client.connect(ep).await.unwrap();

    // The handshake will fail; no message will be received. PUSH send
    // blocks waiting for a routable peer that will never arrive, so
    // bound it with a timeout - we only care that recv stays empty.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("nope")),
    )
    .await;
    let res = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(res.is_err(), "handshake should have failed; got {res:?}");
}

#[tokio::test]
async fn blake3zmq_authenticator_admits_known_client() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let allowed = client_kp.public.0;
    let ep = inproc_ep("blake3-auth-allow");

    let saw_callback = Arc::new(AtomicBool::new(false));
    let saw_callback_cb = saw_callback.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default()
            .blake3zmq_server(server_kp)
            .authenticator(move |peer| {
                saw_callback_cb.store(true, Ordering::SeqCst);
                peer.public_key == allowed
            }),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("authed")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"authed"[..]);
    assert!(saw_callback.load(Ordering::SeqCst), "authenticator must run");
}

#[tokio::test]
async fn blake3zmq_authenticator_rejects_unknown_client() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-auth-deny");

    let server = Socket::new(
        SocketType::Pull,
        Options::default()
            .blake3zmq_server(server_kp)
            .authenticator(|_peer| false),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // PUSH send blocks indefinitely without a routable peer; bound it.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("denied")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "rejected client must not deliver any frame");
}

// =====================================================================
// Strategy-bucket coverage: REQ/REP, DEALER/ROUTER, PUB/SUB over BLAKE3ZMQ.
// =====================================================================

#[tokio::test]
async fn blake3zmq_req_rep() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-req-rep");

    let rep = Socket::new(SocketType::Rep, Options::default().blake3zmq_server(server_kp));
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv()).await.unwrap().unwrap();
    assert_eq!(q.parts()[0].coalesce(), &b"q"[..]);
    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv()).await.unwrap().unwrap();
    assert_eq!(a.parts()[0].coalesce(), &b"a"[..]);
}

#[tokio::test]
async fn blake3zmq_dealer_router() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-dr");

    let router = Socket::new(
        SocketType::Router,
        Options::default().blake3zmq_server(server_kp),
    );
    router.bind(ep.clone()).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .blake3zmq_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv()).await.unwrap().unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"d1"[..]);
    assert_eq!(m.parts()[1].coalesce(), &b"hi"[..]);
}

#[tokio::test]
async fn blake3zmq_pub_sub() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = inproc_ep("blake3-ps");

    let p = Socket::new(SocketType::Pub, Options::default().blake3zmq_server(server_kp));
    p.bind(ep.clone()).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.parts()[0].coalesce(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over BLAKE3ZMQ");
}
