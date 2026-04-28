//! REQ / REP integration tests.
//!
//! Verifies:
//! - Basic request/reply roundtrip with envelope.
//! - REQ strict alternation: second send without intervening recv errors.
//! - REP strict alternation: send without prior recv errors.
//! - REP envelope restore lets ROUTER-style clients (DEALER) talk to a
//!   REP server.

use std::time::Duration;

use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

async fn wait_ready() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn req_rep_basic_roundtrip() {
    let ep = inproc_ep("rr-basic");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    wait_ready().await;

    req.send(Message::single("hello")).await.unwrap();

    let request = rep.recv().await.unwrap();
    assert_eq!(request.len(), 1);
    assert_eq!(request.parts()[0].coalesce(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply.parts()[0].coalesce(), &b"world"[..]);
}

#[tokio::test]
async fn req_rejects_double_send() {
    let ep = inproc_ep("rr-req-double");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    wait_ready().await;

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
    wait_ready().await;

    for i in 0..5 {
        req.send(Message::single(format!("q-{i}"))).await.unwrap();
        let got = rep.recv().await.unwrap();
        assert_eq!(got.parts()[0].coalesce(), format!("q-{i}").as_bytes());
        rep.send(Message::single(format!("a-{i}"))).await.unwrap();
        let reply = req.recv().await.unwrap();
        assert_eq!(reply.parts()[0].coalesce(), format!("a-{i}").as_bytes());
    }
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
    wait_ready().await;

    // Emulate a REQ-style send: empty delim + body.
    dealer
        .send(Message::multipart(["", "hello"]))
        .await
        .unwrap();

    let got = rep.recv().await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got.parts()[0].coalesce(), &b"hello"[..]);

    rep.send(Message::single("world")).await.unwrap();

    // DEALER receives [empty, body] from REP via the envelope restore.
    let reply = tokio::time::timeout(Duration::from_millis(500), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 2);
    assert!(reply.parts()[0].is_empty());
    assert_eq!(reply.parts()[1].coalesce(), &b"world"[..]);
}
