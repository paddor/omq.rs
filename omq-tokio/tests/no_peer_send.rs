use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType, TrySendError};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn free_tcp_ep() -> Endpoint {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    tcp_ep(listener.local_addr().unwrap().port())
}

#[tokio::test]
async fn bound_push_without_peer_mutes() {
    let push = Socket::new(SocketType::Push, Options::default().send_hwm(1));
    push.bind(tcp_ep(0)).await.unwrap();

    let err = push.try_send(Message::single("x")).unwrap_err();
    assert!(matches!(err, TrySendError::Full(_)));

    let mut send = tokio::spawn({
        let push = push.clone();
        async move { push.send(Message::single("wait")).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut send)
            .await
            .is_err(),
        "bound no-peer send must wait for a pipe"
    );
    push.close().await.unwrap();
    assert!(matches!(send.await.unwrap(), Err(omq_tokio::Error::Closed)));
}

#[tokio::test]
async fn connect_side_push_queues_in_pre_ready_pipe() {
    let ep = free_tcp_ep();
    let push = Socket::new(SocketType::Push, Options::default().send_hwm(2));
    push.connect(ep.clone()).await.unwrap();

    push.try_send(Message::single("first")).unwrap();
    push.try_send(Message::single("second")).unwrap();
    let err = push.try_send(Message::single("third")).unwrap_err();
    assert!(matches!(err, TrySendError::Full(_)));

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.part_bytes(0).unwrap().as_ref(), b"first");
    assert_eq!(second.part_bytes(0).unwrap().as_ref(), b"second");
}

#[tokio::test]
async fn connect_side_req_queues_pre_ready_request() {
    let ep = free_tcp_ep();
    let req = Socket::new(SocketType::Req, Options::default().send_hwm(1));
    req.connect(ep.clone()).await.unwrap();

    req.send(Message::single("question")).await.unwrap();

    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();
    let question = tokio::time::timeout(Duration::from_secs(2), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(question.part_bytes(0).unwrap().as_ref(), b"question");

    rep.send(Message::single("answer")).await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(answer.part_bytes(0).unwrap().as_ref(), b"answer");
}

#[tokio::test]
async fn finite_linger_waits_for_connect_side_pre_ready_pipe_without_peer() {
    let ep = free_tcp_ep();
    let push = Socket::new(
        SocketType::Push,
        Options::default()
            .send_hwm(1)
            .linger(Duration::from_millis(200)),
    );
    push.connect(ep).await.unwrap();
    push.send(Message::single("linger")).await.unwrap();

    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(1), push.close())
        .await
        .unwrap()
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "finite linger returned before pre-ready pipe expired"
    );
}

#[tokio::test]
async fn bound_push_mutes_after_last_peer_disconnects() {
    let push = Socket::new(SocketType::Push, Options::default().send_hwm(1));
    let ep = push.bind(tcp_ep(0)).await.unwrap();
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.connect(ep).await.unwrap();
    push.wait_connected(1, Duration::from_secs(2))
        .await
        .unwrap();

    push.send(Message::single("live")).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .unwrap()
        .unwrap();
    pull.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let err = push.try_send(Message::single("after")).unwrap_err();
    assert!(matches!(err, TrySendError::Full(_)));
}
