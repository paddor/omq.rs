//! PLAIN end-to-end integration tests: username/password handshake
//! between two omq-tokio sockets.

#![cfg(feature = "plain")]

mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

// Auth tests need a real transport (inproc bypasses the wire codec).
// IPC on Unix, TCP :0 on Windows.
#[cfg(unix)]
fn auth_ep(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("plain-{name}"))
}

#[cfg(not(unix))]
fn auth_ep(_name: &str) -> Endpoint {
    "tcp://127.0.0.1:0".parse().unwrap()
}

fn accept_alice(peer: &omq_tokio::MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

#[tokio::test]
async fn plain_refusal_stops_automatic_reconnect() {
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    let server = Socket::new(
        SocketType::Router,
        Options::default().plain_server(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            false
        }),
    );
    let endpoint = server.bind(auth_ep("refusal-reconnect")).await.unwrap();
    let client = Socket::new(
        SocketType::Dealer,
        Options::default()
            .plain_client("alice", "wrong")
            .reconnect(omq_tokio::ReconnectPolicy::Fixed(Duration::from_millis(10))),
    );
    let mut monitor = client.monitor();
    client.connect(endpoint).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(
            monitor.recv().await.unwrap(),
            omq_tokio::MonitorEvent::HandshakeFailed { .. }
        ) {}
    })
    .await
    .unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(1), monitor.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        stopped,
        omq_tokio::MonitorEvent::ConnectStopped {
            reason: omq_tokio::DisconnectReason::HandshakeRefused(refusal), ..
        } if refusal.mechanism == omq_tokio::proto::greeting::MechanismName::PLAIN
            && refusal.status_code() == Some(400)
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "refused credentials retried"
    );
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn plain_temporary_auth_failure_reconnects_and_delivers() {
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    let server = Socket::new(
        SocketType::Pull,
        Options {
            mechanism: omq_tokio::MechanismSetup::PlainServer {
                authenticator: omq_tokio::Authenticator::new_with_result(move |_| {
                    if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                        omq_tokio::AuthenticationResult {
                            status: omq_tokio::AuthenticationStatus::TemporaryFailure,
                            ..omq_tokio::AuthenticationResult::allow()
                        }
                    } else {
                        omq_tokio::AuthenticationResult::allow()
                    }
                }),
            },
            ..Options::default()
        },
    );
    let endpoint = server.bind(auth_ep("temporary-reconnect")).await.unwrap();
    let client = Socket::new(
        SocketType::Push,
        Options::default()
            .plain_client("alice", "secret")
            .reconnect(omq_tokio::ReconnectPolicy::Fixed(Duration::from_millis(10))),
    );
    client.connect(endpoint).await.unwrap();
    client
        .wait_connected(1, Duration::from_secs(2))
        .await
        .expect("temporary authentication failure must reconnect");
    client.send(Message::single("recovered")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .unwrap(),
        Message::single("recovered")
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn plain_push_pull_roundtrip() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let ep = server.bind(auth_ep("push-pull")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over plain"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over plain"[..]);
}

#[tokio::test]
async fn plain_multipart_roundtrip() {
    let pair_a = Socket::new(
        SocketType::Pair,
        Options::default().plain_server(accept_alice),
    );
    let ep = pair_a.bind(auth_ep("multipart")).await.unwrap();

    let pair_b = Socket::new(
        SocketType::Pair,
        Options::default().plain_client("alice", "secret"),
    );
    pair_b.connect(ep).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(2), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#[tokio::test]
async fn plain_wrong_credentials_rejected() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let ep = server.bind(auth_ep("wrong-creds")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "wrong"),
    );
    client.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("ghost")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "wrong credentials must prevent delivery");
}

#[tokio::test]
async fn plain_authenticator_callback_runs() {
    let saw = Arc::new(AtomicBool::new(false));
    let saw_cb = saw.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(move |peer| {
            saw_cb.store(true, Ordering::SeqCst);
            accept_alice(peer)
        }),
    );
    let ep = server.bind(auth_ep("auth-callback")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().as_ref(), b"hi");
    assert!(saw.load(Ordering::SeqCst), "authenticator must run");
}

#[tokio::test]
async fn plain_req_rep() {
    let rep = Socket::new(
        SocketType::Rep,
        Options::default().plain_server(accept_alice),
    );
    let ep = rep.bind(auth_ep("req-rep")).await.unwrap();

    let req = Socket::new(
        SocketType::Req,
        Options::default().plain_client("alice", "secret"),
    );
    req.connect(ep).await.unwrap();

    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.part_bytes(0).unwrap(), &b"q"[..]);

    rep.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.part_bytes(0).unwrap(), &b"a"[..]);
}

#[tokio::test]
async fn plain_dealer_router() {
    let saw_auth = Arc::new(AtomicBool::new(false));
    let saw_auth_cb = saw_auth.clone();
    let router = Socket::new(
        SocketType::Router,
        Options::default().plain_server(move |peer| {
            saw_auth_cb.store(true, Ordering::SeqCst);
            accept_alice(peer)
        }),
    );
    let ep = router.bind(auth_ep("dealer-router")).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .plain_client("alice", "secret"),
    );
    dealer.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&dealer).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 2, "DEALER/ROUTER must not add REQ delimiter");
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);
    assert!(saw_auth.load(Ordering::SeqCst), "authenticator must run");

    router
        .send(Message::multipart(["d1", "plain-reply"]))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("plain-reply"));
}

#[tokio::test]
async fn plain_pub_sub() {
    let p = Socket::new(
        SocketType::Pub,
        Options::default().plain_server(accept_alice),
    );
    let ep = p.bind(auth_ep("pub-sub")).await.unwrap();

    let s = Socket::new(
        SocketType::Sub,
        Options::default().plain_client("alice", "secret"),
    );
    s.subscribe("").await.unwrap();
    s.connect(ep).await.unwrap();

    for _ in 0..30 {
        let _ = p.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            return;
        }
    }
    panic!("SUB never received over PLAIN");
}

#[tokio::test]
async fn plain_empty_message() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let ep = server.bind(auth_ep("empty-msg")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single(bytes::Bytes::new()))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(m.part_bytes(0).unwrap().is_empty());
}

#[tokio::test]
async fn plain_large_message() {
    let server = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let ep = server.bind(auth_ep("large-msg")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "secret"),
    );
    client.connect(ep).await.unwrap();

    let data = vec![0xAB_u8; 256 * 1024];
    client.send(Message::single(data.clone())).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap().to_vec(), data);
}

#[tokio::test]
async fn plain_reconnect_after_server_restart() {
    use omq_tokio::options::ReconnectPolicy;

    let server1 = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let ep = server1.bind(auth_ep("reconnect")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default()
            .plain_client("alice", "secret")
            .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50))),
    );
    client.connect(ep.clone()).await.unwrap();

    client.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server1.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"before"[..]);

    server1.close().await.unwrap();

    let server2 = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(accept_alice),
    );
    let mut bound = false;
    for _ in 0..20 {
        if server2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound);

    client.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), server2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"after"[..]);
}
