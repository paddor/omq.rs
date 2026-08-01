//! STREAM socket integration tests.
//!
//! STREAM bypasses ZMTP framing: raw TCP bytes in, identity-prefixed
//! messages out. Send `[identity, data]` to route to a peer. Empty
//! data frame closes the peer connection.

mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::endpoint::Host;
#[cfg(unix)]
use omq_tokio::endpoint::IpcPath;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

fn resolved_addr(ep: &Endpoint) -> std::net::SocketAddr {
    match ep {
        Endpoint::Tcp { host, port } => {
            let ip = match host {
                Host::Ip(ip) => *ip,
                _ => panic!("expected IP host"),
            };
            std::net::SocketAddr::new(ip, *port)
        }
        _ => panic!("expected TCP endpoint"),
    }
}

#[tokio::test]
async fn stream_basic_roundtrip() {
    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream.bind(tcp_ep(0)).await.unwrap();
    let addr = resolved_addr(&ep);

    let mut client = TcpStream::connect(addr).await.unwrap();

    // Connect notification: empty data with the peer identity.
    let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .expect("timed out waiting for connect notification")
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();
    assert!(!identity.is_empty(), "identity should be non-empty");
    let data = connect_msg.part_bytes(1).unwrap();
    assert!(data.is_empty(), "connect notification data should be empty");

    // Client sends raw bytes.
    client.write_all(b"hello stream").await.unwrap();

    let recv_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .expect("timed out waiting for data")
        .unwrap();
    let recv_ident = recv_msg.part_bytes(0).unwrap();
    assert_eq!(recv_ident, identity, "identity should match");
    let recv_data = recv_msg.part_bytes(1).unwrap();
    assert_eq!(&recv_data[..], b"hello stream");

    // STREAM sends reply using identity routing.
    let reply = Message::multipart([identity.clone(), Bytes::from_static(b"reply back")]);
    stream.send(reply).await.unwrap();

    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
        .await
        .expect("timed out waiting for reply")
        .unwrap();
    assert_eq!(&buf[..n], b"reply back");
}

#[tokio::test]
async fn stream_large_raw_payload_roundtrip() {
    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream.bind(tcp_ep(0)).await.unwrap();
    let addr = resolved_addr(&ep);

    let mut client = TcpStream::connect(addr).await.unwrap();

    let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .expect("timed out waiting for connect notification")
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();
    assert!(!identity.is_empty(), "identity should be non-empty");
    assert!(connect_msg.part_bytes(1).unwrap().is_empty());

    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();
    client.write_all(&payload).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(3), async {
        let mut received = Vec::with_capacity(payload.len());
        while received.len() < payload.len() {
            let msg = stream.recv().await.unwrap();
            assert_eq!(msg.len(), 2);
            assert_eq!(msg.part_bytes(0).unwrap(), identity);

            let chunk = msg.part_bytes(1).unwrap();
            assert!(!chunk.is_empty(), "unexpected empty STREAM notification");
            received.extend_from_slice(&chunk);
        }
        received
    })
    .await
    .expect("timed out receiving large raw payload");
    assert_eq!(received, payload);

    let reply: Vec<u8> = payload.iter().map(|b| b ^ 0xA5).collect();
    stream
        .send(Message::multipart([
            identity,
            Bytes::copy_from_slice(&reply),
        ]))
        .await
        .unwrap();

    let mut got_reply = vec![0u8; reply.len()];
    tokio::time::timeout(Duration::from_secs(3), client.read_exact(&mut got_reply))
        .await
        .expect("timed out waiting for large STREAM reply")
        .unwrap();
    assert_eq!(got_reply, reply);
}

#[tokio::test]
async fn stream_peer_disconnect() {
    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream.bind(tcp_ep(0)).await.unwrap();
    let addr = resolved_addr(&ep);

    let client = TcpStream::connect(addr).await.unwrap();

    // Consume connect notification.
    let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .unwrap()
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();

    // Drop the client.
    drop(client);

    // Should get a disconnect notification (empty data).
    let disconnect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .expect("timed out waiting for disconnect notification")
        .unwrap();
    let disc_ident = disconnect_msg.part_bytes(0).unwrap();
    assert_eq!(disc_ident, identity);
    let disc_data = disconnect_msg.part_bytes(1).unwrap();
    assert!(
        disc_data.is_empty(),
        "disconnect notification data should be empty"
    );
}

#[tokio::test]
async fn stream_non_tcp_rejected() {
    let stream = Socket::new(SocketType::Stream, Options::default());

    #[cfg(unix)]
    {
        let ipc_result = stream
            .bind(Endpoint::Ipc(IpcPath::Filesystem(
                "/tmp/omq-stream-test.sock".into(),
            )))
            .await;
        assert!(ipc_result.is_err(), "IPC should be rejected for STREAM");
    }

    let inproc_result = stream
        .bind(Endpoint::Inproc {
            name: "stream-test".into(),
        })
        .await;
    assert!(
        inproc_result.is_err(),
        "inproc should be rejected for STREAM"
    );

    #[cfg(unix)]
    {
        let connect_ipc = stream
            .connect(Endpoint::Ipc(IpcPath::Filesystem(
                "/tmp/omq-stream-test2.sock".into(),
            )))
            .await;
        assert!(
            connect_ipc.is_err(),
            "IPC connect should be rejected for STREAM"
        );
    }
}

#[tokio::test]
async fn stream_multiple_peers() {
    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream.bind(tcp_ep(0)).await.unwrap();
    let addr = resolved_addr(&ep);

    let mut clients = Vec::new();
    let mut identities = Vec::new();

    // Connect all clients first and collect identities.
    for _ in 0..3u8 {
        let client = TcpStream::connect(addr).await.unwrap();
        clients.push(client);

        let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
            .await
            .unwrap()
            .unwrap();
        let ident = connect_msg.part_bytes(0).unwrap();
        assert!(!ident.is_empty());
        identities.push(ident);
    }

    // Now send data from each client.
    for (i, client) in clients.iter_mut().enumerate() {
        client
            .write_all(format!("msg-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Receive data from each client.
    let mut received = std::collections::HashMap::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
            .await
            .unwrap()
            .unwrap();
        let ident = msg.part_bytes(0).unwrap();
        let data = msg.part_bytes(1).unwrap();
        received.insert(ident, data);
    }

    // Verify all identities received data.
    for (i, ident) in identities.iter().enumerate() {
        let data = received.get(ident).unwrap_or_else(|| {
            panic!("missing data for peer {i}");
        });
        assert_eq!(&data[..], format!("msg-{i}").as_bytes());
    }

    // Send replies to each peer using their identity.
    for (i, ident) in identities.iter().enumerate() {
        let reply = Message::multipart([ident.clone(), Bytes::from(format!("reply-{i}"))]);
        stream.send(reply).await.unwrap();
    }

    // Verify each client receives its reply.
    for (i, client) in clients.iter_mut().enumerate() {
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("timed out waiting for reply")
            .unwrap();
        assert_eq!(&buf[..n], format!("reply-{i}").as_bytes());
    }
}

#[tokio::test]
async fn stream_empty_send_closes_peer() {
    let stream = Socket::new(SocketType::Stream, Options::default());
    let ep = stream.bind(tcp_ep(0)).await.unwrap();
    let addr = resolved_addr(&ep);

    let mut client = TcpStream::connect(addr).await.unwrap();

    let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .unwrap()
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();

    // Send empty data frame to close the peer.
    let close_msg = Message::multipart([identity, Bytes::new()]);
    stream.send(close_msg).await.unwrap();

    // Client should see EOF.
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
        .await
        .expect("timed out waiting for EOF")
        .unwrap();
    assert_eq!(n, 0, "expected EOF after empty send");
}

#[tokio::test]
async fn stream_connect_to_raw_listener() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ep = Endpoint::Tcp {
        host: Host::Ip(addr.ip()),
        port: addr.port(),
    };

    let stream = Socket::new(SocketType::Stream, Options::default());
    stream.connect(ep).await.unwrap();

    let (mut peer, _peer_addr) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("timed out waiting for accept")
        .unwrap();

    // Consume the connect notification from the STREAM socket.
    let connect_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .unwrap()
        .unwrap();
    let identity = connect_msg.part_bytes(0).unwrap();
    assert!(!identity.is_empty());

    // Raw server sends data.
    peer.write_all(b"from server").await.unwrap();

    let data_msg = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data_msg.part_bytes(0).unwrap(), identity);
    assert_eq!(&data_msg.part_bytes(1).unwrap()[..], b"from server");

    // STREAM sends to raw server.
    let reply = Message::multipart([identity, Bytes::from_static(b"from stream")]);
    stream.send(reply).await.unwrap();

    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), peer.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"from stream");
}
