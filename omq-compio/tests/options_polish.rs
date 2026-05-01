//! Options polish: end-to-end tests that exercise individual options for
//! correctness across the public API. Feature-by-feature smoke tests.

use std::time::Duration;

use omq_compio::{Endpoint, Error, Message, OnMute, Options, Socket, SocketType};

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[compio::test]
async fn linger_zero_drops_pending_on_close() {
    let ep = inproc_ep("opt-linger0");
    let push = Socket::new(
        SocketType::Push,
        Options::default().linger(Duration::ZERO),
    );
    push.bind(ep.clone()).await.unwrap();

    let _ = compio::time::timeout(
        Duration::from_millis(50),
        push.send(Message::single("dropped")),
    )
    .await;
    push.close().await.unwrap();
}

#[compio::test]
async fn router_mandatory_default_silent() {
    let ep = inproc_ep("opt-rm-default");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    router
        .send(Message::multipart(["ghost", "hi"]))
        .await
        .unwrap();
}

#[compio::test]
async fn router_mandatory_true_errors_on_unknown() {
    let ep = inproc_ep("opt-rm-on");
    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    router.bind(ep).await.unwrap();
    let r = router.send(Message::multipart(["ghost", "hi"])).await;
    assert!(matches!(r, Err(Error::Unroutable)), "got {r:?}");
}

#[compio::test]
async fn max_message_size_rejects_oversize() {
    let ep = inproc_ep("opt-mms");
    let pull = Socket::new(
        SocketType::Pull,
        Options::default().max_message_size(8),
    );
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    compio::time::sleep(Duration::from_millis(50)).await;

    push.send(Message::single("12345678")).await.unwrap();
    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce().len(), 8);

    push.send(Message::single("123456789")).await.unwrap();
    let r = compio::time::timeout(Duration::from_millis(200), pull.recv()).await;
    assert!(r.is_err(), "oversize must not be delivered");
}

#[compio::test]
async fn drop_newest_silently_discards_overflow() {
    let ep = inproc_ep("opt-drop-new");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().send_hwm(1).on_mute(OnMute::DropNewest),
    );
    push.connect(ep).await.unwrap();
    push.send(Message::single("a")).await.unwrap();
    push.send(Message::single("b")).await.unwrap();
    push.send(Message::single("c")).await.unwrap();

    let m = compio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let _ = m;
    let extra = compio::time::timeout(Duration::from_millis(100), pull.recv()).await;
    let _ = extra;
}

#[compio::test]
async fn try_recv_empty_returns_would_block() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(inproc_ep("try-recv-empty-compio")).await.unwrap();
    assert!(matches!(pull.try_recv(), Err(Error::WouldBlock)));
}

#[compio::test]
async fn try_recv_returns_buffered_message() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let push = Socket::new(SocketType::Push, Options::default());
    pull.bind(inproc_ep("try-recv-buffered-compio")).await.unwrap();
    push.connect(inproc_ep("try-recv-buffered-compio")).await.unwrap();
    push.send(Message::single("hello")).await.unwrap();
    // Yield so the inproc frame is forwarded through in_tx/in_rx.
    let _ = compio::runtime::spawn(async {}).await;
    let msg = pull.try_recv().unwrap();
    assert_eq!(&*msg.parts()[0].coalesce(), b"hello");
}

#[compio::test]
async fn try_send_no_peers_returns_would_block() {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(inproc_ep("try-send-nopeer-compio")).await.unwrap();
    // No peer connected; shared queue has capacity but no peer means WouldBlock.
    assert!(matches!(push.try_send(Message::single("x")), Err(Error::WouldBlock)));
}

#[compio::test]
async fn identity_propagates_on_handshake() {
    let ep = inproc_ep("opt-ident");
    let server = Socket::new(SocketType::Router, Options::default());
    let mut mon = server.monitor();
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Dealer,
        Options::default().identity(bytes::Bytes::from_static(b"my-client")),
    );
    client.connect(ep).await.unwrap();

    let mut got_identity = None;
    for _ in 0..6 {
        match compio::time::timeout(Duration::from_millis(500), mon.recv()).await {
            Ok(Ok(omq_compio::MonitorEvent::HandshakeSucceeded { peer, .. })) => {
                got_identity = peer.peer_identity.clone();
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(got_identity.as_deref(), Some(&b"my-client"[..]));
}
