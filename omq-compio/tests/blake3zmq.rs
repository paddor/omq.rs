//! BLAKE3ZMQ end-to-end integration tests for omq-compio.

#![cfg(feature = "blake3zmq")]

use std::time::Duration;

use omq_compio::{
    Blake3ZmqKeypair, Endpoint, IpcPath, Message, Options, Socket, SocketType,
};

fn temp_ipc(name: &str) -> Endpoint {
    let mut p = std::env::temp_dir();
    p.push(format!("omq-compio-blake3-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    Endpoint::Ipc(IpcPath::Filesystem(p))
}

#[compio::test]
async fn blake3zmq_push_pull_roundtrip() {
    let server_kp = Blake3ZmqKeypair::generate();
    let client_kp = Blake3ZmqKeypair::generate();
    let server_pub = server_kp.public;
    let ep = temp_ipc("blake3-pp");

    let server = Socket::new(
        SocketType::Pull,
        Options::default().blake3zmq_server(server_kp),
    );
    server.bind(ep.clone()).await.unwrap();

    let client = Socket::new(
        SocketType::Push,
        Options::default().blake3zmq_client(client_kp, server_pub),
    );
    client.connect(ep).await.unwrap();

    client.send(Message::single("hello over blake3zmq")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over blake3zmq"[..]);
}
