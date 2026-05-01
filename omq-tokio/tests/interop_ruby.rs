//! Wire-compatibility tests against the Ruby OMQ implementation.
//!
//! Spawns `omq` (the Ruby CLI; gem install omq-cli) and checks that Rust
//! OMQ talks to it in either direction over both TCP and IPC. Each test
//! self-skips with a printed notice if the binary is not on PATH.
//!
//! Handshake/subscription propagation uses the Rust socket monitor stream
//! to wait deterministically for `HandshakeSucceeded`, then a small
//! propagation delay for SUBSCRIBE / JOIN / READY-driven side effects.
//! Avoids fixed Ruby-boot sleeps so the suite stays fast.

use std::io::Write;
use std::net::TcpListener as StdTcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::{Host, IpcPath};
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn omq_available() -> bool {
    Command::new("omq")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn skip_if_no_omq() -> bool {
    if !omq_available() {
        eprintln!("skip: `omq` (gem install omq-cli) not on PATH");
        return true;
    }
    false
}

/// One transport per case. The CLI accepts the same `tcp://...` /
/// `ipc://...` URIs Rust does, so the test body can be transport-agnostic.
#[derive(Clone, Debug)]
struct Transport {
    rust: Endpoint,
    cli: String,
}

fn tcp_transport() -> Transport {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    Transport {
        rust: Endpoint::Tcp {
            host: Host::Ip("127.0.0.1".parse().unwrap()),
            port,
        },
        cli: format!("tcp://127.0.0.1:{port}"),
    }
}

fn ipc_transport(name: &str) -> Transport {
    // Filesystem path under the test target dir keeps the socket inside
    // the cargo workspace and avoids /tmp permission quirks.
    let path = std::env::temp_dir().join(format!(
        "omq-rs-interop-{name}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    // Stale leftover from a previous run would prevent bind.
    let _ = std::fs::remove_file(&path);
    let cli = format!("ipc://{}", path.display());
    Transport {
        rust: Endpoint::Ipc(IpcPath::Filesystem(path)),
        cli,
    }
}

/// Wait until the socket reports a successful ZMTP handshake with at least
/// one peer. Falls back to an absolute deadline so a hung test fails fast
/// rather than blocking the whole suite.
async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor stream closed before handshake: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("handshake did not arrive within 5s");
}

// ---------------------------------------------------------------------
// PUSH / PULL -- exercises base ZMTP framing in both directions.
// ---------------------------------------------------------------------

async fn rust_push_to_ruby_pull(t: Transport) {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(t.rust.clone()).await.unwrap();

    let child = Command::new("omq")
        .args(["pull", "-c", &t.cli, "-A", "-n", "5"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq pull");

    wait_for_handshake(&push).await;

    for i in 0..5 {
        push.send(Message::single(format!("hello-{i}")))
            .await
            .unwrap();
    }

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(out.status.success(), "omq pull failed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["hello-0", "hello-1", "hello-2", "hello-3", "hello-4"]
    );
}

#[tokio::test]
async fn rust_push_to_ruby_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_push_to_ruby_pull(tcp_transport()).await;
}

#[tokio::test]
async fn rust_push_to_ruby_pull_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_push_to_ruby_pull(ipc_transport("push-pull")).await;
}

async fn ruby_push_to_rust_pull(t: Transport) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(t.rust.clone()).await.unwrap();

    let mut child = Command::new("omq")
        .args(["push", "-c", &t.cli, "-A"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq push");

    {
        let mut stdin = child.stdin.take().unwrap();
        for i in 0..4 {
            writeln!(stdin, "from-ruby-{i}").unwrap();
        }
    }

    for i in 0..4 {
        let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(
            msg.parts()[0].coalesce(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let _ = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn ruby_push_to_rust_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    ruby_push_to_rust_pull(tcp_transport()).await;
}

#[tokio::test]
async fn ruby_push_to_rust_pull_ipc() {
    if skip_if_no_omq() {
        return;
    }
    ruby_push_to_rust_pull(ipc_transport("push-pull-rev")).await;
}

// ---------------------------------------------------------------------
// REQ / REP -- exercises envelope (empty delimiter) handling on Rust REQ
// against Ruby's Routing::Rep envelope restore.
// ---------------------------------------------------------------------

async fn rust_req_to_ruby_rep(t: Transport) {
    let mut child = Command::new("omq")
        .args(["rep", "-b", &t.cli, "--echo", "-n", "3"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq rep");

    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(t.rust.clone()).await.unwrap();
    wait_for_handshake(&req).await;

    for i in 0..3 {
        let payload = format!("ping-{i}");
        req.send(Message::single(payload.clone())).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .expect("reply timed out")
            .unwrap();
        assert_eq!(reply.parts()[0].coalesce(), payload.as_bytes());
    }

    let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    assert!(status.success(), "omq rep exited with {status:?}");
}

#[tokio::test]
async fn rust_req_to_ruby_rep_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_req_to_ruby_rep(tcp_transport()).await;
}

#[tokio::test]
async fn rust_req_to_ruby_rep_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_req_to_ruby_rep(ipc_transport("req-rep")).await;
}

// ---------------------------------------------------------------------
// PUB / SUB -- subscription propagation + prefix filtering.
// ---------------------------------------------------------------------

async fn rust_pub_to_ruby_sub(t: Transport) {
    let pubs = Socket::new(SocketType::Pub, Options::default());
    pubs.bind(t.rust.clone()).await.unwrap();

    let child = Command::new("omq")
        .args(["sub", "-c", &t.cli, "-s", "weather.", "-A", "-n", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq sub");

    wait_for_handshake(&pubs).await;
    // SUBSCRIBE arrives just after the handshake. Small grace before we
    // publish so the topic table has the prefix registered.
    tokio::time::sleep(Duration::from_millis(100)).await;

    pubs.send(Message::multipart(["weather.eu", "sunny"]))
        .await
        .unwrap();
    pubs.send(Message::multipart(["news.global", "ignored"]))
        .await
        .unwrap();
    pubs.send(Message::multipart(["weather.us", "rainy"]))
        .await
        .unwrap();

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(out.status.success(), "omq sub failed: {out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["weather.eu\tsunny", "weather.us\trainy"]
    );
}

#[tokio::test]
async fn rust_pub_to_ruby_sub_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_pub_to_ruby_sub(tcp_transport()).await;
}

#[tokio::test]
async fn rust_pub_to_ruby_sub_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_pub_to_ruby_sub(ipc_transport("pub-sub")).await;
}

// ---------------------------------------------------------------------
// ROUTER / DEALER -- one-way: Rust ROUTER must see the DEALER's announced
// ZMTP READY identity prefixed onto the received message. Round-trip is
// exercised by the pure-Rust router_dealer suite; here we only validate
// the wire-level identity contract Ruby emits.
// ---------------------------------------------------------------------

async fn rust_router_sees_ruby_dealer_identity(t: Transport) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(t.rust.clone()).await.unwrap();

    let child = Command::new("omq")
        .args([
            "dealer",
            "-c",
            &t.cli,
            "--identity",
            "worker-7",
            "-A",
            "-D",
            "from-dealer",
            "-n",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq dealer");

    let got = tokio::time::timeout(Duration::from_secs(5), router.recv())
        .await
        .expect("router recv timed out")
        .unwrap();
    assert_eq!(got.len(), 2, "router message is [identity, body]");
    assert_eq!(got.parts()[0].coalesce(), &b"worker-7"[..]);
    assert_eq!(got.parts()[1].coalesce(), &b"from-dealer"[..]);

    // Ruby DEALER with `-n 1` exits after a single send; reap it.
    let _ = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn rust_router_sees_ruby_dealer_identity_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_router_sees_ruby_dealer_identity(tcp_transport()).await;
}

#[tokio::test]
async fn rust_router_sees_ruby_dealer_identity_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_router_sees_ruby_dealer_identity(ipc_transport("router-dealer")).await;
}

// ---------------------------------------------------------------------
// RADIO / DISH -- group routing draft. Validates Rust's RFC 48 group
// header encoding against Ruby's DISH decoding.
// ---------------------------------------------------------------------

async fn rust_radio_to_ruby_dish(t: Transport) {
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(t.rust.clone()).await.unwrap();

    let child = Command::new("omq")
        .args(["dish", "-c", &t.cli, "-j", "weather", "-A", "-n", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn omq dish");

    wait_for_handshake(&radio).await;
    // JOIN command lands shortly after the handshake.
    tokio::time::sleep(Duration::from_millis(100)).await;

    radio
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["news", "skipped"]))
        .await
        .unwrap();
    radio
        .send(Message::multipart(["weather", "rainy"]))
        .await
        .unwrap();

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(out.status.success(), "omq dish failed: {out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["weather\tsunny", "weather\trainy"]
    );
}

#[tokio::test]
async fn rust_radio_to_ruby_dish_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_radio_to_ruby_dish(tcp_transport()).await;
}

#[tokio::test]
async fn rust_radio_to_ruby_dish_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_radio_to_ruby_dish(ipc_transport("radio-dish")).await;
}
