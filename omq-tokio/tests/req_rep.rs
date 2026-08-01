//! REQ / REP integration tests.
//!
//! Verifies:
//! - Basic request/reply roundtrip with envelope.
//! - REQ strict alternation: second send without intervening recv errors.
//! - REP strict alternation: send without prior recv errors.
//! - REP envelope restore lets ROUTER-style clients (DEALER) talk to a
//!   REP server.
//! - REP survives a client disconnect mid-cycle and serves the next client.

mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

async fn recv_from_any_rep(reps: &[Socket]) -> (usize, Message) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        for (idx, rep) in reps.iter().enumerate() {
            match rep.try_recv() {
                Ok(msg) => return (idx, msg),
                Err(Error::WouldBlock) => {}
                Err(e) => panic!("rep {idx} recv failed: {e:?}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no REP received request"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn req_duplicate_tcp_connect_is_ignored() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep.clone()).await.unwrap();
    req.connect(ep).await.unwrap();

    rep.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("rep did not see req");
    req.wait_connected(1, Duration::from_secs(1))
        .await
        .expect("req did not connect");
    test_support::assert_no_second_connection(&rep, "rep").await;
    test_support::assert_no_second_connection(&req, "req").await;

    req.send(Message::single("hello")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), rep.recv())
        .await
        .expect("rep did not receive request")
        .unwrap();
    assert_eq!(got, Message::single("hello"));
}

#[tokio::test]
async fn req_rep_basic_roundtrip() {
    let ep = inproc_ep("rr-basic");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("hello")).await.unwrap();

    let request = rep.recv().await.unwrap();
    assert_eq!(request, Message::single("hello"));

    rep.send(Message::single("world")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("world"));
}

#[tokio::test]
async fn req_rejects_double_send() {
    let ep = inproc_ep("rr-req-double");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    req.send(Message::single("one")).await.unwrap();
    let second = req.send(Message::single("two")).await;
    assert!(matches!(second, Err(Error::Protocol(_))), "got {second:?}");
}

#[tokio::test]
async fn rep_rejects_send_before_recv() {
    let ep = inproc_ep("rr-rep-noreq");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    let r = rep.send(Message::single("oops")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn req_rep_multiple_rounds() {
    let ep = inproc_ep("rr-many");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    for i in 0..5 {
        req.send(Message::single(format!("q-{i}"))).await.unwrap();
        let got = rep.recv().await.unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), format!("q-{i}").as_bytes());
        rep.send(Message::single(format!("a-{i}"))).await.unwrap();
        let reply = req.recv().await.unwrap();
        assert_eq!(reply.part_bytes(0).unwrap(), format!("a-{i}").as_bytes());
    }
}

#[tokio::test]
async fn req_round_robins_across_connected_reps() {
    const REPS: usize = 5;
    const ROUNDS: usize = 2;

    let ep = inproc_ep("rr-req-round-robin");
    let req = Socket::new(SocketType::Req, Options::default());
    req.bind(ep.clone()).await.unwrap();

    let mut reps = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let rep = Socket::new(SocketType::Rep, Options::default());
        rep.connect(ep.clone()).await.unwrap();
        reps.push(rep);
    }
    req.wait_connected(REPS, Duration::from_secs(1))
        .await
        .expect("REQ did not see all REPs");

    let mut counts = [0usize; REPS];
    for i in 0..(REPS * ROUNDS) {
        let question = format!("q-{i}");
        req.send(Message::single(question.clone())).await.unwrap();

        let (idx, request) = recv_from_any_rep(&reps).await;
        assert_eq!(request.part_bytes(0).unwrap(), question.as_bytes());
        counts[idx] += 1;

        let answer = format!("a-{idx}-{i}");
        reps[idx]
            .send(Message::single(answer.clone()))
            .await
            .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(1), req.recv())
            .await
            .expect("REQ did not receive reply")
            .unwrap();
        assert_eq!(reply.part_bytes(0).unwrap(), answer.as_bytes());
    }

    assert_eq!(counts, [ROUNDS; REPS]);
}

#[tokio::test]
async fn dealer_to_rep_envelope() {
    // DEALER sends [empty, body]; REP saves envelope + empty delim and
    // returns just the body to the user. Reply goes back through the
    // envelope correctly.
    let ep = inproc_ep("rr-dealer-rep");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"cli")),
    );
    dealer.connect(ep).await.unwrap();

    // Emulate a REQ-style send: empty delim + body.
    dealer
        .send(Message::multipart(["", "hello"]))
        .await
        .unwrap();

    let got = rep.recv().await.unwrap();
    assert_eq!(got, Message::single("hello"));

    rep.send(Message::single("world")).await.unwrap();

    // DEALER receives [empty, body] from REP via the envelope restore.
    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 2);
    assert!(reply.part_bytes(0).unwrap().is_empty());
    assert_eq!(reply.part_bytes(1).unwrap(), &b"world"[..]);
}

#[tokio::test]
async fn dealer_to_rep_preserves_large_envelope_on_reply() {
    let ep = inproc_ep("rr-dealer-rep-big-envelope");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let dealer = Socket::new(SocketType::Dealer, Options::default());
    dealer.connect(ep).await.unwrap();
    dealer
        .wait_connected(1, Duration::from_secs(1))
        .await
        .expect("dealer did not connect");

    dealer
        .send(Message::multipart(["X", "Y", "", "request"]))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), rep.recv())
        .await
        .expect("REP did not receive dealer request")
        .unwrap();
    assert_eq!(got, Message::single("request"));

    rep.send(Message::single("reply")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(1), dealer.recv())
        .await
        .expect("DEALER did not receive REP reply")
        .unwrap();
    assert_eq!(reply, Message::multipart(["X", "Y", "", "reply"]));
}

#[tokio::test]
async fn rep_survives_client_disconnect_mid_cycle() {
    // REP receives a request; the client drops before REP sends the
    // reply.  REP must clear its stale envelope and serve the next
    // client correctly.
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    // First client: sends a request then drops immediately.
    {
        let req1 = Socket::new(SocketType::Req, Options::default());
        req1.connect(ep.clone()).await.unwrap();
        test_support::wait_for_handshake(&req1).await;
        req1.send(Message::single("drop-me")).await.unwrap();

        // Let REP receive the request (stale envelope now held).
        let m = tokio::time::timeout(Duration::from_millis(300), rep.recv())
            .await
            .expect("REP recv timed out for first client")
            .unwrap();
        assert_eq!(m, Message::single("drop-me"));
        req1.close().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Second client: full roundtrip must succeed.
    let req2 = Socket::new(SocketType::Req, Options::default());
    req2.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req2).await;

    req2.send(Message::single("real")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), rep.recv())
        .await
        .expect("REP did not receive second client's request")
        .unwrap();
    assert_eq!(got, Message::single("real"));

    rep.send(Message::single("reply")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req2.recv())
        .await
        .expect("REQ2 did not receive reply")
        .unwrap();
    assert_eq!(reply, Message::single("reply"));
}

#[tokio::test]
async fn rep_reply_to_disconnected_client_is_accepted() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    req.send(Message::single("drop-me")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(1), rep.recv())
        .await
        .expect("REP recv timed out")
        .unwrap();
    assert_eq!(got, Message::single("drop-me"));

    req.close_with_linger(Some(Duration::ZERO)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !rep.connections().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("REP still had live peer");

    rep.send(Message::single("dropped-reply"))
        .await
        .expect("REP reply to a disconnected peer must be accepted");
}

#[tokio::test]
async fn req_rep_1000_cycles_tcp() {
    // 1 000 sequential request-reply cycles over TCP.
    // Scales beyond inproc to reveal framing races, backpressure issues,
    // and timer/wake latency at real socket throughput.
    const CYCLES: usize = 1_000;

    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    let rep_worker = rep.clone();
    let rep_task = tokio::spawn(async move {
        for _ in 0..CYCLES {
            let m = rep_worker.recv().await.unwrap();
            rep_worker.send(m).await.unwrap(); // echo
        }
    });

    for i in 0..CYCLES {
        req.send(Message::single(format!("{i}"))).await.unwrap();
        let r = tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .unwrap_or_else(|_| panic!("cycle {i} timed out"))
            .unwrap();
        let expected = format!("{i}");
        assert_eq!(r.part_bytes(0).unwrap(), expected.as_bytes(), "cycle {i}");
    }

    rep_task.await.unwrap();
}

/// Three REQ sockets connect to one REP. Each sends a request and
/// expects its own reply back. Verifies that `DirectIo` (installed for
/// the first peer) does not misroute replies once more peers arrive.
#[tokio::test]
async fn three_req_to_one_rep_direct_io_routing() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let mut reqs: Vec<Socket> = Vec::new();
    for _ in 0..3 {
        let req = Socket::new(SocketType::Req, Options::default());
        req.connect(ep.clone()).await.unwrap();
        reqs.push(req);
    }
    test_support::wait_for_handshake(&rep).await;

    for (i, req) in reqs.iter().enumerate() {
        req.send(Message::single(format!("from-{i}")))
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(5), rep.recv())
            .await
            .unwrap_or_else(|_| panic!("rep.recv timed out on req {i}"))
            .unwrap();
        assert_eq!(msg.part_bytes(0).unwrap(), format!("from-{i}").as_bytes());

        rep.send(Message::single(format!("reply-{i}")))
            .await
            .unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .unwrap_or_else(|_| panic!("req[{i}].recv timed out"))
            .unwrap();
        assert_eq!(
            reply.part_bytes(0).unwrap(),
            format!("reply-{i}").as_bytes()
        );
    }
}

/// Multiple REQ peers may have requests queued before REP calls `recv()`. The
/// body queue must retain each request's routing envelope independently.
#[tokio::test]
async fn queued_requests_from_multiple_reqs_preserve_envelopes() {
    const PEERS: usize = 3;

    let rep = Socket::new(SocketType::Rep, Options::default());
    let ep = rep.bind(tcp_ep(0)).await.unwrap();

    let mut reqs = Vec::with_capacity(PEERS);
    for i in 0..PEERS {
        let req = Socket::new(SocketType::Req, Options::default());
        req.connect(ep.clone()).await.unwrap();
        test_support::wait_for_handshake(&req).await;
        req.send(Message::single(format!("queued-{i}")))
            .await
            .unwrap();
        reqs.push(req);
    }

    for _ in 0..PEERS {
        let request = tokio::time::timeout(Duration::from_secs(5), rep.recv())
            .await
            .expect("REP recv timed out")
            .unwrap();
        let body = request.part_bytes(0).unwrap();
        let index = std::str::from_utf8(&body)
            .unwrap()
            .strip_prefix("queued-")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        rep.send(Message::single(format!("reply-{index}")))
            .await
            .unwrap();
    }

    for (i, req) in reqs.iter().enumerate() {
        let reply = tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .expect("REQ recv timed out")
            .unwrap();
        assert_eq!(reply, Message::single(format!("reply-{i}")));
    }
}

/// REP serves multiple REQ clients that connect and disconnect in
/// sequence. Each new REQ must complete a full request/reply cycle.
#[tokio::test]
async fn rep_tcp_serves_sequential_clients() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = test_support::bind_loopback(&rep).await;

    for round in 0..4u32 {
        let req = Socket::new(SocketType::Req, Options::default());
        req.connect(tcp_ep(port)).await.unwrap();

        let question = format!("q-{round}");
        req.send(Message::single(question.clone())).await.unwrap();

        let got = tokio::time::timeout(Duration::from_secs(2), rep.recv())
            .await
            .expect("rep.recv timed out")
            .unwrap();
        assert_eq!(got.part_bytes(0).unwrap(), question.as_bytes());

        let answer = format!("a-{round}");
        rep.send(Message::single(answer.clone())).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(2), req.recv())
            .await
            .expect("req.recv timed out")
            .unwrap();
        assert_eq!(reply.part_bytes(0).unwrap(), answer.as_bytes());

        drop(req);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
