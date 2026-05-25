//! IPC PUSH→PULL roundtrip on compio.

use std::time::Duration;

use omq_compio::endpoint::IpcPath;
use omq_compio::options::ReconnectPolicy;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "omq-ib-{}-{:x}.sock",
        &name[..name.len().min(8)],
        u64::from(std::process::id()) ^ u64::from(rand::random::<u32>()),
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
    assert_eq!(m.part_bytes(0).unwrap(), &b"over-ipc"[..]);
}

#[compio::test]
async fn ipc_connect_before_bind() {
    let ep = temp_ipc("connect-before-bind");

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    compio::time::sleep(Duration::from_millis(60)).await;

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();

    push.send(Message::single("late-bind")).await.unwrap();
    let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after late bind")
        .unwrap();
    assert_eq!(m.part_bytes(0).unwrap(), &b"late-bind"[..]);
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
        push.send(Message::single(format!("m-{i:04}")))
            .await
            .unwrap();
    }
    for i in 0..N {
        let m = compio::time::timeout(Duration::from_secs(2), pull.recv())
            .await
            .expect("recv timeout")
            .unwrap();
        let want = format!("m-{i:04}");
        assert_eq!(m.part_bytes(0).unwrap(), want.as_bytes());
    }
}
