//! CURVE end-to-end integration tests: handshake + per-frame encryption
//! between two omq.rs sockets.

#![cfg(feature = "curve")]

mod test_support;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use omq_tokio::endpoint::Host;
use omq_tokio::{CurveKeypair, CurveServerOptions, Endpoint, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    use std::net::{IpAddr, Ipv4Addr};
    Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    }
}

fn url_of(ep: &Endpoint) -> String {
    match ep {
        Endpoint::Tcp { host, port } => format!("{host}:{port}"),
        other => panic!("expected tcp endpoint, got {other:?}"),
    }
}

// Auth tests need a real transport (inproc bypasses the wire codec).
// IPC on Unix, TCP :0 on Windows.
#[cfg(unix)]
fn auth_ep(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("curve-{name}"))
}

#[cfg(not(unix))]
fn auth_ep(_name: &str) -> Endpoint {
    "tcp://127.0.0.1:0".parse().unwrap()
}

fn handshake_prefix(stream: &[u8]) -> Option<Vec<u8>> {
    let mut off = 64usize;
    for _ in 0..2 {
        let flags = *stream.get(off)?;
        let (hdr, len) = if flags & 0x02 != 0 {
            let raw = stream.get(off + 1..off + 9)?;
            (9usize, u64::from_be_bytes(raw.try_into().ok()?) as usize)
        } else {
            (2usize, *stream.get(off + 1)? as usize)
        };
        off = off.checked_add(hdr)?.checked_add(len)?;
        if stream.len() < off {
            return None;
        }
    }
    Some(stream[..off].to_vec())
}

fn recording_proxy(upstream: String) -> (String, Arc<Mutex<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let cap = Arc::new(Mutex::new(Vec::new()));
    let cap_thread = cap.clone();
    std::thread::spawn(move || {
        let (inbound, _) = listener.accept().unwrap();
        let outbound = TcpStream::connect(&upstream).unwrap();
        let (mut c_in, mut s_out) = (inbound.try_clone().unwrap(), outbound.try_clone().unwrap());
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = c_in.read(&mut buf) {
                if n == 0 {
                    break;
                }
                cap_thread.lock().unwrap().extend_from_slice(&buf[..n]);
                if s_out.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
        });
        let (mut s_in, mut c_out) = (outbound, inbound);
        let mut buf = [0u8; 8192];
        while let Ok(n) = s_in.read(&mut buf) {
            if n == 0 || c_out.write_all(&buf[..n]).is_err() {
                break;
            }
        }
    });
    (addr, cap)
}

fn replay(addr: &str, prefix: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(1500)))
        .unwrap();
    s.write_all(prefix).unwrap();
    let (mut got, mut buf) = (Vec::new(), [0u8; 8192]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && got.len() <= 4096 {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
    got
}

fn handshake_completed(resp: &[u8]) -> bool {
    resp.len() > 64 + 172 && resp.windows(8).skip(64 + 170).any(|w| w == b"\x07MESSAGE")
}

#[test]
fn curve_rejects_captured_hello_initiate_replay() {
    let server_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let client_kp = CurveKeypair::generate();

    let (url_tx, url_rx) = mpsc::channel::<String>();
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let push =
                    Socket::new(SocketType::Push, Options::default().curve_server(server_kp));
                let ep = push.bind(tcp_ep(0)).await.unwrap();
                url_tx.send(url_of(&ep)).unwrap();
                while stop_rx.try_recv().is_err() {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(100),
                        push.send(Message::single(vec![0xAB; 256])),
                    )
                    .await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            });
    });
    let server_addr = url_rx.recv().unwrap();

    let (proxy_addr, cap) = recording_proxy(server_addr.clone());
    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pull = Socket::new(
                SocketType::Pull,
                Options::default().curve_client(client_kp, server_pub),
            );
            let ep: Endpoint = format!("tcp://{proxy_addr}").parse().unwrap();
            pull.connect(ep).await.unwrap();
            tokio::time::timeout(Duration::from_secs(8), pull.recv())
                .await
                .expect("legit handshake timed out")
                .expect("legit handshake failed");
        });
    }
    std::thread::sleep(Duration::from_millis(200));

    let captured = cap.lock().unwrap().clone();
    let prefix = handshake_prefix(&captured).expect("could not split handshake prefix");

    let greeting_only = replay(&server_addr, &prefix[..64]);
    assert!(
        !handshake_completed(&greeting_only),
        "control failed: greeting alone completed handshake"
    );

    let mut bad_box = prefix.clone();
    let off = bad_box.len() - 100;
    bad_box[off] ^= 0x01;
    assert!(
        !handshake_completed(&replay(&server_addr, &bad_box)),
        "control failed: corrupted INITIATE box was accepted"
    );

    let mut bad_cookie = prefix.clone();
    bad_cookie[268 + 20] ^= 0x01;
    assert!(
        !handshake_completed(&replay(&server_addr, &bad_cookie)),
        "control failed: corrupted cookie was accepted"
    );

    let resp = replay(&server_addr, &prefix);
    let accepted = handshake_completed(&resp);

    let _ = stop_tx.send(());
    let _ = server.join();

    assert!(
        !accepted,
        "verbatim HELLO+INITIATE replay accepted by peer with no key material \
         ({} bytes returned)",
        resp.len()
    );
}

#[tokio::test]
async fn curve_push_pull_roundtrip_over_ipc() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let server = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    let ep = server.bind(auth_ep("push-pull")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client
        .send(Message::single("hello over curve"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"hello over curve"[..]);
}

#[tokio::test]
async fn curve_multipart_roundtrip() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let pair_a = Socket::new(SocketType::Pair, Options::default().curve_server(server_kp));
    let ep = pair_a.bind(auth_ep("multipart")).await.unwrap();

    let pair_b = Socket::new(
        SocketType::Pair,
        Options::default().curve_client(client_kp, server_pub),
    );
    pair_b.connect(ep).await.unwrap();

    pair_b
        .send(Message::multipart(["a", "bb", "ccc"]))
        .await
        .unwrap();

    let m = tokio::time::timeout(Duration::from_secs(1), pair_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"bb"[..]);
    assert_eq!(m.part_bytes(2).unwrap(), &b"ccc"[..]);
}

#[tokio::test]
async fn curve_wrong_server_key_fails_handshake() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    // Client expects a different server long-term key than what the
    // server actually has -- handshake should fail.
    let wrong_pub = CurveKeypair::generate().public;

    let server = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    let ep = server.bind(auth_ep("wrong-key")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, wrong_pub),
    );
    client.connect(ep).await.unwrap();

    // Give the doomed handshake a moment.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // PUSH send blocks waiting for a routable peer that will never
    // arrive (handshake failed); bound it.
    let _ = tokio::time::timeout(
        Duration::from_millis(50),
        client.send(Message::single("ghost")),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_millis(200), server.recv()).await;
    assert!(r.is_err(), "wrong server key must prevent delivery");
}

#[tokio::test]
async fn curve_emits_handshake_succeeded_with_curve_mechanism() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let server = Socket::new(SocketType::Pair, Options::default().curve_server(server_kp));
    let mut mon = server.monitor();
    let ep = server.bind(auth_ep("monitor")).await.unwrap();

    let client = Socket::new(
        SocketType::Pair,
        Options::default().curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    let mut saw_handshake = false;
    for _ in 0..6 {
        match tokio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(omq_tokio::MonitorEvent::HandshakeSucceeded { peer, .. })) => {
                assert_eq!(peer.zmtp_version, (3, 1));
                saw_handshake = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saw_handshake, "CURVE handshake must complete");
}

#[tokio::test]
async fn curve_authenticator_admits_known_client() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let allowed = client_kp.public.0;

    let saw_callback = Arc::new(AtomicBool::new(false));
    let saw_callback_cb = saw_callback.clone();

    let server = Socket::new(
        SocketType::Pull,
        Options::default().curve_server_with_options(
            server_kp,
            CurveServerOptions::default().authenticator(move |peer| {
                saw_callback_cb.store(true, Ordering::SeqCst);
                peer.public_key == allowed
            }),
        ),
    );
    let ep = server.bind(auth_ep("auth-allow")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default()
            .identity(bytes::Bytes::from_static(b"client-id"))
            .curve_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("authed")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(1), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"authed"[..]);
    assert!(
        saw_callback.load(Ordering::SeqCst),
        "authenticator must run"
    );
}

#[tokio::test]
async fn curve_authenticator_identity_matches_router_message() {
    let server_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let auth_identities = Arc::new(Mutex::new(Vec::new()));
    let auth_identities_cb = auth_identities.clone();

    let router = Socket::new(
        SocketType::Router,
        Options::default().curve_server_with_options(
            server_kp,
            CurveServerOptions::default().authenticator(move |peer| {
                if let Some(identity) = &peer.identity {
                    auth_identities_cb.lock().unwrap().push(identity.clone());
                }
                true
            }),
        ),
    );
    let ep = router.bind(auth_ep("auth-router-identity")).await.unwrap();

    let dealer_one = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"client-one"))
            .curve_client(CurveKeypair::generate(), server_pub),
    );
    dealer_one.connect(ep.clone()).await.unwrap();

    let dealer_two = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"client-two"))
            .curve_client(CurveKeypair::generate(), server_pub),
    );
    dealer_two.connect(ep).await.unwrap();

    dealer_one.send(Message::single("from-one")).await.unwrap();
    dealer_two.send(Message::single("from-two")).await.unwrap();

    let mut identities_by_message = Vec::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(1), router.recv())
            .await
            .unwrap()
            .unwrap();
        identities_by_message.push((
            message.part_bytes(1).unwrap().to_vec(),
            message.part_bytes(0).unwrap().to_vec(),
        ));
    }
    identities_by_message.sort_unstable();
    assert_eq!(
        identities_by_message,
        vec![
            (b"from-one".to_vec(), b"client-one".to_vec()),
            (b"from-two".to_vec(), b"client-two".to_vec()),
        ]
    );

    let mut stored_identities = auth_identities.lock().unwrap().clone();
    stored_identities.sort_unstable();
    assert_eq!(
        stored_identities,
        vec![
            bytes::Bytes::from_static(b"client-one"),
            bytes::Bytes::from_static(b"client-two"),
        ]
    );
}

#[tokio::test]
async fn curve_authenticator_rejects_unknown_client() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let server = Socket::new(
        SocketType::Pull,
        Options::default().curve_server_with_options(
            server_kp,
            CurveServerOptions::default().authenticator(|_peer| false),
        ),
    );
    let ep = server.bind(auth_ep("auth-deny")).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub),
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
// Strategy-bucket coverage: every send strategy must route through a
// CURVE-encrypted connection without surprises. PUSH/PULL covers the
// round-robin bucket above; here: REQ/REP, DEALER/ROUTER (identity),
// PUB/SUB (fan-out subscription-filtered).
// =====================================================================

#[tokio::test]
async fn curve_req_rep() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let rep = Socket::new(SocketType::Rep, Options::default().curve_server(server_kp));
    let ep = rep.bind(auth_ep("req-rep")).await.unwrap();
    let req = Socket::new(
        SocketType::Req,
        Options::default().curve_client(client_kp, server_pub),
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
async fn curve_dealer_router() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let router = Socket::new(
        SocketType::Router,
        Options::default().curve_server(server_kp),
    );
    let ep = router.bind(auth_ep("dealer-router")).await.unwrap();
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default()
            .identity(bytes::Bytes::from_static(b"d1"))
            .curve_client(client_kp, server_pub),
    );
    dealer.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), router.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"d1"[..]);
    assert_eq!(m.part_bytes(1).unwrap(), &b"hi"[..]);
}

#[tokio::test]
async fn curve_pub_sub() {
    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;
    let p = Socket::new(SocketType::Pub, Options::default().curve_server(server_kp));
    let ep = p.bind(auth_ep("pub-sub")).await.unwrap();
    let s = Socket::new(
        SocketType::Sub,
        Options::default().curve_client(client_kp, server_pub),
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
    panic!("SUB never received over CURVE");
}

#[tokio::test]
async fn curve_reconnects_after_server_restart() {
    // Client holds the server's public key. After the server restarts with
    // the same keypair, the client must re-handshake successfully and
    // resume message delivery.
    use omq_tokio::options::ReconnectPolicy;

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub = server_kp.public;

    let server1 = Socket::new(
        SocketType::Pull,
        Options::default().curve_server(server_kp.clone()),
    );
    let ep = server1.bind(tcp_ep(0)).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(50)),
            ..Options::default().curve_client(client_kp, server_pub)
        },
    );
    let mut client_mon = client.monitor();
    client.connect(ep.clone()).await.unwrap();
    test_support::wait_for_handshake_on(&mut client_mon).await;

    client.send(Message::single("before")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server1.recv())
        .await
        .expect("first recv timed out")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"before");

    // Server restarts with same keypair.
    server1.close().await.unwrap();

    let server2 = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    let mut bound = false;
    for _ in 0..20 {
        if server2.bind(ep.clone()).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "server2 failed to bind after server1 closed");

    test_support::wait_for_handshake_on(&mut client_mon).await;
    client.send(Message::single("after")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(5), server2.recv())
        .await
        .expect("second recv timed out — CURVE reconnect failed")
        .unwrap();
    assert_eq!(&*m.part_bytes(0).unwrap(), b"after");
}
