//! IPC end-to-end tests.
//!
//! **Platform-specific implementations:**
//! - Linux: Abstract namespace sockets (`IpcPath::Abstract`)
//! - Windows: Named pipes (`IpcPath::NamedPipe`)
//! - macOS/BSD: Filesystem sockets (`IpcPath::Filesystem`)

mod test_support;

use std::time::Duration;

#[cfg(unix)]
use omq_tokio::IpcPath;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

fn temp_ipc(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("ipc-{name}"))
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
    assert_eq!(m, Message::single("hello over ipc"));
}

#[tokio::test]
async fn ipc_req_rep_roundtrip() {
    let ep = temp_ipc("req-rep");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();
    test_support::wait_for_handshake(&req).await;

    req.send(Message::single("ping")).await.unwrap();
    let got = rep.recv().await.unwrap();
    assert_eq!(got, Message::single("ping"));
    rep.send(Message::single("pong")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_millis(500), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, Message::single("pong"));
}

#[test]
fn ipc_owned_req_rep_after_idle_with_and_without_recv_spin() {
    const EXCHANGES: usize = 64;
    const DEADLINE: Duration = Duration::from_secs(5);
    for spin in [Duration::ZERO, Duration::from_micros(50)] {
        let context = omq_tokio::Context::new();
        let options = Options::default().recv_spin(spin).linger(DEADLINE);
        let rep = context.blocking_socket(SocketType::Rep, options.clone());
        let req = context.blocking_socket(SocketType::Req, options);
        let endpoint = rep.bind(temp_ipc("owned-req-rep")).unwrap();
        req.connect(endpoint).unwrap();
        req.wait_connected(1, DEADLINE).unwrap();
        let responder = std::thread::spawn(move || {
            for _ in 0..2 * 3 * EXCHANGES {
                let message = rep.recv_timeout(DEADLINE).unwrap();
                rep.send_timeout(message, DEADLINE).unwrap();
            }
            rep.close().unwrap();
        });
        for _ in 0..2 {
            // Let the IO polling window expire before restarting traffic.
            std::thread::sleep(Duration::from_millis(10));
            for size in [16, 256, 1024] {
                for sequence in 0..EXCHANGES {
                    let message = Message::multipart([
                        bytes::Bytes::from_static(b"topic"),
                        bytes::Bytes::from(vec![sequence as u8; size]),
                    ]);
                    req.send_timeout(message.clone(), DEADLINE).unwrap();
                    assert_eq!(req.recv_timeout(DEADLINE).unwrap(), message);
                }
            }
        }
        responder.join().unwrap();
        req.close().unwrap();
    }
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
    assert_eq!(m, Message::single("hello over abstract ipc"));
}

#[tokio::test]
async fn ipc_connect_before_bind() {
    // Connect before the socket file exists; the dialer must retry on ENOENT
    // until the listener creates the file, then deliver the message.
    let ep = temp_ipc("connect-before-bind");

    let push = Socket::new(
        SocketType::Push,
        Options {
            reconnect: ReconnectPolicy::Fixed(Duration::from_millis(20)),
            ..Default::default()
        },
    );
    push.connect(ep.clone()).await.unwrap();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    test_support::wait_for_handshake(&pull).await;

    push.send(Message::single("late-bind")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), pull.recv())
        .await
        .expect("recv timed out after late bind")
        .unwrap();
    assert_eq!(m, Message::single("late-bind"));
}

#[cfg(unix)]
#[tokio::test]
async fn ipc_socket_file_cleaned_on_close() {
    let dir = std::path::PathBuf::from(format!("/tmp/omq-ipc-clean-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    let ep = Endpoint::Ipc(IpcPath::Filesystem(dir.clone()));
    let path = dir;
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

#[cfg(unix)]
#[tokio::test]
async fn ipc_close_keeps_a_newer_bind_of_the_same_path() {
    let path =
        std::path::PathBuf::from(format!("/tmp/omq-ipc-rebound-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let ep = Endpoint::Ipc(IpcPath::Filesystem(path.clone()));
    let old = Socket::new(SocketType::Pull, Options::default());
    old.bind(ep.clone()).await.unwrap();
    // A later bind replaces the socket file, as after a crashed owner.
    let new = Socket::new(SocketType::Pull, Options::default());
    new.bind(ep.clone()).await.unwrap();
    old.close().await.unwrap();
    assert!(path.exists(), "the old owner removed the newer socket file");

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    push.send(Message::single("to new")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), new.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("to new"));
    new.close().await.unwrap();
    assert!(!path.exists(), "close should remove its own socket file");
}
