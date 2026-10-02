#![cfg(feature = "ws")]

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::Duration;

use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

#[tokio::test]
async fn disconnect_drops_stalled_http_and_tls_attempts() {
    use tokio::io::AsyncReadExt;

    for scheme in ["ws", "wss"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint: Endpoint = format!("{scheme}://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let socket = Socket::new(SocketType::Push, Options::default());
        socket.connect(endpoint.clone()).await.unwrap();
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        // Observe setup on the wire before canceling. The server deliberately
        // sends neither an HTTP upgrade response nor a TLS ServerHello.
        let mut first = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut first))
            .await
            .unwrap()
            .unwrap();
        socket.disconnect(endpoint).await.unwrap();
        let mut remaining = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            stream.take(65536).read_to_end(&mut remaining),
        )
        .await
        .expect("disconnect left a pending setup connection alive")
        .unwrap();
        assert!(remaining.len() < 65536, "expected EOF, not the read cap");
        socket.close().await.unwrap();
    }
}

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_reconnect_after_server_restart() {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let pull1 = Socket::new(SocketType::Pull, Options::default());
    let bound = pull1.bind(ws_endpoint(port)).await.unwrap();
    let port = get_port(&bound);

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_endpoint(port)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("before")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull1.recv())
        .await
        .expect("recv timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"before"[..]);

    pull1.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull2 = Socket::new(SocketType::Pull, Options::default());
    let mut bound = false;
    for _ in 0..20 {
        if pull2.bind(ws_endpoint(port)).await.is_ok() {
            bound = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound, "pull2 failed to bind after pull1 closed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::single("after")).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), pull2.recv())
        .await
        .expect("recv after restart timed out")
        .unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"after"[..]);
}
