//! IPC PUSH→PULL roundtrip on compio.

use std::time::Duration;

use omq_compio::endpoint::IpcPath;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "omq-compio-ipc-{name}-{}-{}.sock",
        std::process::id(),
        rand::random::<u32>()
    ));
    Endpoint::Ipc(IpcPath::Filesystem(dir))
}

#[compio::test]
async fn ipc_push_pull_single_message() {
    let ep = temp_ipc("single");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    push.send(Message::single("over-ipc")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(m.parts()[0].coalesce(), &b"over-ipc"[..]);
}

#[compio::test]
async fn ipc_push_pull_burst() {
    const N: u32 = 200;
    let ep = temp_ipc("burst");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();

    for i in 0..N {
        push.send(Message::single(format!("m-{i:04}"))).await.unwrap();
    }
    for i in 0..N {
        let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let want = format!("m-{i:04}");
        assert_eq!(m.parts()[0].coalesce(), want.as_bytes());
    }
}
