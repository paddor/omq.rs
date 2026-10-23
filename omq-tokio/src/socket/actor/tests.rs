use super::*;
use bytes::Bytes;
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc { name: name.into() }
}

#[tokio::test]
async fn bind_connect_send_recv_inproc() {
    use super::super::Socket;
    let ep = inproc_ep("sock-basic");
    let server = Socket::new(SocketType::Pair, Options::default());
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(SocketType::Pair, Options::default());
    client.connect(ep).await.unwrap();

    client.send(Message::single("hello")).await.unwrap();
    let msg = server.recv().await.unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);

    server.send(Message::single("world")).await.unwrap();
    let msg = client.recv().await.unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"world"[..]);

    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn send_queues_until_peer_ready() {
    use super::super::Socket;
    let ep = inproc_ep("sock-queue");
    let server = Socket::new(SocketType::Pair, Options::default());
    let client = Socket::new(SocketType::Pair, Options::default());

    // Send before connect: message queues inside the socket.
    let send_task = {
        let c = client.clone();
        tokio::spawn(async move { c.send(Message::single("early")).await })
    };
    // Now set up the pair.
    server.bind(ep.clone()).await.unwrap();
    client.connect(ep).await.unwrap();

    send_task.await.unwrap().unwrap();
    let msg = server.recv().await.unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"early"[..]);
}

#[tokio::test]
async fn close_returns_when_idle() {
    use super::super::Socket;
    let s = Socket::new(SocketType::Pair, Options::default());
    s.close().await.unwrap();
}

#[tokio::test]
async fn identity_propagates() {
    use super::super::Socket;
    let ep = inproc_ep("sock-id");
    let server = Socket::new(SocketType::Pair, Options::default());
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Pair,
        Options::default().identity(Bytes::from_static(b"abc")),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("ping")).await.unwrap();
    let _ = server.recv().await.unwrap();
    // Identity validation surfaces via monitor events; here we
    // only confirm the socket accepts a non-empty identity option.
    server.close().await.unwrap();
    client.close().await.unwrap();
}
