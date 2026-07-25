//! Broker pattern: ROUTER (frontend) ↔ DEALER (backend) ↔ REP (worker).
//!
//! A canonical ZMQ intermediary that exercises identity routing through a
//! proxy. The broker forwards request envelopes intact so that REP's
//! save-restore round-trip delivers replies back to the correct REQ client.
//!
//! Message envelope at the ROUTER level: [`client_id` | "" | body]
//! Message envelope at the DEALER/REP level: [`client_id` | "" | body]
//! REP delivers [body] to the application and re-wraps on reply.

mod test_support;

use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn inproc(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.local_addr().unwrap().port()
}

fn tcp(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(feature = "lz4")]
fn lz4_tcp(port: u16) -> Endpoint {
    Endpoint::Lz4Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[cfg(unix)]
fn ipc(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("broker-{name}"))
}

#[tokio::test]
async fn router_dealer_rep_single_cycle() {
    let frontend = inproc("broker-fe-tok");
    let backend = inproc("broker-be-tok");

    router_dealer_rep_single_cycle_with(frontend, backend).await;
}

#[tokio::test]
async fn router_dealer_rep_single_cycle_tcp() {
    let frontend = tcp(free_tcp_port());
    let backend = tcp(free_tcp_port());

    router_dealer_rep_single_cycle_with(frontend, backend).await;
}

#[tokio::test]
async fn router_dealer_rep_multi_hop_envelope_tcp() {
    let frontend = tcp(free_tcp_port());
    let backend = tcp(free_tcp_port());

    router_dealer_rep_multi_hop_envelope_with(frontend, backend).await;
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn router_dealer_rep_single_cycle_lz4_tcp() {
    let frontend = lz4_tcp(free_tcp_port());
    let backend = lz4_tcp(free_tcp_port());

    router_dealer_rep_single_cycle_with(frontend, backend).await;
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn router_dealer_rep_multi_hop_envelope_lz4_tcp() {
    let frontend = lz4_tcp(free_tcp_port());
    let backend = lz4_tcp(free_tcp_port());

    router_dealer_rep_multi_hop_envelope_with(frontend, backend).await;
}

#[tokio::test]
#[cfg(unix)]
async fn router_dealer_rep_single_cycle_ipc() {
    let frontend = ipc("fe");
    let backend = ipc("be");

    router_dealer_rep_single_cycle_with(frontend, backend).await;
}

async fn router_dealer_rep_single_cycle_with(frontend: Endpoint, backend: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    test_support::wait_for_handshake(&router).await;
    test_support::wait_for_handshake(&dealer).await;
    test_support::wait_for_handshake(&req).await;
    test_support::wait_for_handshake(&rep).await;

    // Broker: transparently forward one request and one reply.
    let router_c = router.clone();
    let dealer_c = dealer.clone();
    let broker = tokio::spawn(async move {
        let request = tokio::time::timeout(Duration::from_secs(2), router_c.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer_c.send(request).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(2), dealer_c.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router_c.send(reply).await.unwrap();
    });

    req.send(Message::single("work")).await.unwrap();

    let work = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work, Message::single("work"));
    rep.send(Message::single("done")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("done"));

    broker.await.unwrap();
}

async fn router_dealer_rep_multi_hop_envelope_with(frontend: Endpoint, backend: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"hop-a")),
    );
    client.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    test_support::wait_for_handshake(&router).await;
    test_support::wait_for_handshake(&dealer).await;
    test_support::wait_for_handshake(&client).await;
    test_support::wait_for_handshake(&rep).await;

    let router_c = router.clone();
    let dealer_c = dealer.clone();
    let broker = tokio::spawn(async move {
        let request = tokio::time::timeout(Duration::from_secs(2), router_c.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer_c.send(request).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(2), dealer_c.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router_c.send(reply).await.unwrap();
    });

    client
        .send(Message::multipart(["hop-b", "", "work"]))
        .await
        .unwrap();

    let work = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work, Message::single("work"));
    rep.send(Message::single("done")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 3);
    assert_eq!(reply.part_bytes(0).unwrap(), &b"hop-b"[..]);
    assert!(reply.part_bytes(1).unwrap().is_empty());
    assert_eq!(reply.part_bytes(2).unwrap(), &b"done"[..]);

    broker.await.unwrap();
}

#[tokio::test]
async fn router_dealer_rep_multiple_rounds() {
    const ROUNDS: usize = 5;

    let frontend = inproc("broker-rounds-fe-tok");
    let backend = inproc("broker-rounds-be-tok");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    // Each round is fully sequential: REQ→ROUTER→DEALER→REP→DEALER→ROUTER→REQ.
    for i in 0..ROUNDS {
        req.send(Message::single(format!("job-{i}"))).await.unwrap();

        // Broker: forward request ROUTER→DEALER.
        let request = tokio::time::timeout(Duration::from_secs(2), router.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer.send(request).await.unwrap();

        // Worker: receive request and reply.
        let m = tokio::time::timeout(Duration::from_secs(2), rep.recv())
            .await
            .expect("rep recv timed out")
            .unwrap();
        let body = m.part_bytes(0).unwrap().to_vec();
        let mut ack = b"ack:".to_vec();
        ack.extend_from_slice(&body);
        rep.send(Message::single(ack)).await.unwrap();

        // Broker: forward reply DEALER→ROUTER.
        let reply = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router.send(reply).await.unwrap();

        // Client: receive reply.
        let r = tokio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .expect("req recv timed out")
            .unwrap();
        let got = r.part_bytes(0).unwrap();
        let expected = format!("ack:job-{i}");
        assert_eq!(&*got, expected.as_bytes(), "round {i} mismatch");
    }
}

#[tokio::test]
async fn router_dealer_rep_two_concurrent_clients() {
    let frontend = inproc("broker-multi-fe-tok");
    let backend = inproc("broker-multi-be-tok");

    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(frontend.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.bind(backend.clone()).await.unwrap();

    let req1 = Socket::new(SocketType::Req, Options::default());
    req1.connect(frontend.clone()).await.unwrap();
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(frontend).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.connect(backend).await.unwrap();

    // Both clients send simultaneously.
    req1.send(Message::single("alpha")).await.unwrap();
    req2.send(Message::single("beta")).await.unwrap();

    // Broker + worker process two request/reply cycles sequentially.
    for _ in 0..2 {
        let request = tokio::time::timeout(Duration::from_secs(3), router.recv())
            .await
            .expect("router recv timed out")
            .unwrap();
        dealer.send(request).await.unwrap();

        let m = tokio::time::timeout(Duration::from_secs(3), rep.recv())
            .await
            .expect("rep recv timed out")
            .unwrap();
        let mut ok = b"ok-".to_vec();
        ok.extend_from_slice(&m.part_bytes(0).unwrap());
        rep.send(Message::single(ok)).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(3), dealer.recv())
            .await
            .expect("dealer recv timed out")
            .unwrap();
        router.send(reply).await.unwrap();
    }

    // Each client must now have its reply queued.
    let r1 = tokio::time::timeout(Duration::from_secs(3), req1.recv())
        .await
        .expect("req1 recv timed out")
        .unwrap()
        .part_bytes(0)
        .unwrap()
        .to_vec();
    let r2 = tokio::time::timeout(Duration::from_secs(3), req2.recv())
        .await
        .expect("req2 recv timed out")
        .unwrap()
        .part_bytes(0)
        .unwrap()
        .to_vec();

    assert!(r1.starts_with(b"ok-"), "req1 got bad reply: {r1:?}");
    assert!(r2.starts_with(b"ok-"), "req2 got bad reply: {r2:?}");
    // Each client must get exactly their own reply body.
    let bodies: std::collections::HashSet<Vec<u8>> = [r1, r2].into_iter().collect();
    assert_eq!(bodies.len(), 2, "both clients must get distinct replies");
}
