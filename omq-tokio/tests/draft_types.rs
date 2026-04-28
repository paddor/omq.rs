//! Draft socket types: CLIENT/SERVER, SCATTER/GATHER, CHANNEL, PEER.
//! RADIO/DISH have their own group/JOIN semantics tested in
//! `tests/radio_dish.rs`.

use std::time::Duration;

use omq_tokio::{Endpoint, Error, Message, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn client_server_basic_roundtrip() {
    let ep = inproc_ep("draft-client-server");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(bytes::Bytes::from_static(b"cli1")),
    );
    client.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.send(Message::single("ping")).await.unwrap();

    // Server receives [routing_id, body].
    let got = tokio::time::timeout(Duration::from_millis(500), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got.parts()[0].coalesce(), &b"cli1"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"ping"[..]);

    // Server replies via [routing_id, body].
    server
        .send(Message::multipart(["cli1", "pong"]))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_millis(500), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply.parts()[0].coalesce(), &b"pong"[..]);
}

#[tokio::test]
async fn client_rejects_multipart_send() {
    let ep = inproc_ep("draft-client-multi");
    let client = Socket::new(SocketType::Client, Options::default());
    client.bind(ep).await.unwrap();
    let r = client
        .send(Message::multipart(["a", "b"]))
        .await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn server_requires_routing_id_envelope() {
    let ep = inproc_ep("draft-server-noid");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep).await.unwrap();
    // Single-part send is invalid for SERVER; must be [id, body].
    let r = server.send(Message::single("nobody")).await;
    assert!(matches!(r, Err(Error::Protocol(_))), "got {r:?}");
}

#[tokio::test]
async fn scatter_gather_single_frame_roundtrip() {
    let ep = inproc_ep("draft-scatter-gather");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();

    let scatter = Socket::new(SocketType::Scatter, Options::default());
    scatter.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..3 {
        scatter.send(Message::single(format!("m{i}"))).await.unwrap();
    }
    for i in 0..3 {
        let m = tokio::time::timeout(Duration::from_millis(500), gather.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.parts()[0].coalesce(), format!("m{i}").as_bytes());
    }
}

#[tokio::test]
async fn scatter_rejects_multipart() {
    let ep = inproc_ep("draft-scatter-multi");
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.send(Message::multipart(["a", "b"])).await;
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn channel_pair_one_to_one() {
    let ep = inproc_ep("draft-channel");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    a.send(Message::single("hi")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"hi"[..]);

    b.send(Message::single("there")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"there"[..]);
}

#[tokio::test]
async fn channel_rejects_multipart() {
    let ep = inproc_ep("draft-channel-multi");
    let s = Socket::new(SocketType::Channel, Options::default());
    s.bind(ep).await.unwrap();
    let r = s.send(Message::multipart(["a", "b"])).await;
    assert!(matches!(r, Err(Error::Protocol(_))));
}

#[tokio::test]
async fn peer_bidirectional_identity_routing() {
    let ep = inproc_ep("draft-peer");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(bytes::Bytes::from_static(b"peer-a")),
    );
    a.bind(ep.clone()).await.unwrap();
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(bytes::Bytes::from_static(b"peer-b")),
    );
    b.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // PEER is multi-part-capable; first frame is routing identity.
    b.send(Message::multipart(["peer-a", "hello a"])).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"peer-b"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"hello a"[..]);

    a.send(Message::multipart(["peer-b", "hello b"])).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(500), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"peer-a"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"hello b"[..]);
}
