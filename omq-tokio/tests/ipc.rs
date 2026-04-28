//! IPC (Unix domain socket) end-to-end tests.

use std::time::Duration;

use omq_tokio::{Endpoint, IpcPath, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!("omq-ipc-test-{name}-{}.sock", std::process::id()));
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

#[tokio::test]
async fn ipc_push_pull_roundtrip() {
    let ep = temp_ipc("push-pull");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over ipc")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over ipc"[..]);
}

#[tokio::test]
async fn ipc_req_rep_roundtrip() {
    let ep = temp_ipc("req-rep");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req.send(Message::single("ping")).await.unwrap();
    let got = rep.recv().await.unwrap();
    assert_eq!(got.parts()[0].coalesce(), &b"ping"[..]);
    rep.send(Message::single("pong")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.parts()[0].coalesce(), &b"pong"[..]);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn ipc_abstract_push_pull_roundtrip() {
    let name = format!(
        "omq-ipc-abs-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    );
    let ep = Endpoint::Ipc(IpcPath::Abstract(name));

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("hello over abstract ipc"))
        .await
        .unwrap();
    let m = tokio::time::timeout(Duration::from_millis(500), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"hello over abstract ipc"[..]);
}

#[tokio::test]
async fn ipc_socket_file_cleaned_on_close() {
    let ep = temp_ipc("cleanup");
    let path = match &ep {
        Endpoint::Ipc(IpcPath::Filesystem(p)) => p.clone(),
        _ => unreachable!(),
    };
    let s = Socket::new(SocketType::Pull, Options::default());
    s.bind(ep).await.unwrap();
    assert!(path.exists(), "bind must create the socket file");
    s.close().await.unwrap();
    // The driver task may take a moment to drop the listener; give it a tick.
    for _ in 0..20 {
        if !path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("close should remove the socket file");
}
