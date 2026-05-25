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
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::{Host, IpcPath};
#[cfg(feature = "curve")]
use omq_proto::proto::mechanism::CurveKeypair;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

/// Kills and waits for a child process on drop so orphaned Ruby processes
/// cannot outlive a panicking test.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn take(&mut self) -> Child {
        self.0.take().expect("ChildGuard already consumed")
    }
}

impl std::ops::Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        self.0.as_ref().expect("ChildGuard already consumed")
    }
}

impl std::ops::DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("ChildGuard already consumed")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

const MIN_OMQ_CLI: (u32, u32, u32) = (0, 17, 1);

fn parse_cli_version(out: &str) -> Option<(u32, u32, u32)> {
    let token = out.split_whitespace().find(|w| w.contains('.'))?;
    let mut it = token.split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next()?.parse().ok()?;
    let patch = it.next()?.parse().ok()?;
    Some((major, minor, patch))
}

fn omq_available() -> bool {
    let out = match Command::new("omq").arg("--version").output() {
        Ok(o) if o.status.success() => o,
        _ => return false,
    };
    let Some(version) = parse_cli_version(&String::from_utf8_lossy(&out.stdout)) else {
        return false;
    };
    version >= MIN_OMQ_CLI
}

fn skip_if_no_omq() -> bool {
    if !omq_available() {
        let (mj, mn, pt) = MIN_OMQ_CLI;
        eprintln!("skip: needs `omq` CLI >= {mj}.{mn}.{pt} (gem install omq-cli)");
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

/// Ephemeral TCP transport with a pre-reserved port. Used only when
/// Ruby binds (REQ/REP test). Rust-bind tests use `bind_tcp` instead
/// to avoid the bind-drop-rebind TOCTOU race.
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

/// Bind the Rust socket and return the CLI endpoint string. For TCP,
/// binds port 0 and reads the kernel-assigned port from the monitor
/// to avoid the TOCTOU race in `tcp_transport()`. For IPC, binds
/// directly and returns `t.cli`.
async fn bind_tcp_or_ipc(sock: &Socket, t: &Transport) -> String {
    if matches!(&t.rust, Endpoint::Tcp { .. }) {
        let mut mon = sock.monitor();
        sock.bind(Endpoint::Tcp {
            host: Host::Ip("127.0.0.1".parse().unwrap()),
            port: 0,
        })
        .await
        .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
                Ok(Ok(MonitorEvent::Listening {
                    endpoint: Endpoint::Tcp { port, .. },
                })) => return format!("tcp://127.0.0.1:{port}"),
                Ok(Ok(_)) => {}
                other => panic!("expected Listening event, got {other:?}"),
            }
        }
    } else {
        sock.bind(t.rust.clone()).await.unwrap();
        t.cli.clone()
    }
}

fn ipc_transport(name: &str) -> Transport {
    // Keep the path short: macOS SUN_LEN is 104 bytes and
    // std::env::temp_dir() on macOS is ~50 chars already.
    let path = std::env::temp_dir().join(format!("omq-{name}-{}.sock", std::process::id()));
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
    let cli = bind_tcp_or_ipc(&push, &t).await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["pull", "-c", &cli, "-A", "-n", "5"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq pull"),
    );

    wait_for_handshake(&push).await;

    for i in 0..5 {
        push.send(Message::single(format!("hello-{i}")))
            .await
            .unwrap();
    }

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
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
    let cli = bind_tcp_or_ipc(&pull, &t).await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["push", "-c", &cli, "-A"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq push"),
    );

    {
        let mut stdin = guard.stdin.take().unwrap();
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
            msg.part_bytes(0).unwrap(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let _ = tokio::task::spawn_blocking(move || guard.take().wait().unwrap())
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
    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["rep", "-b", &t.cli, "--echo", "-n", "3"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq rep"),
    );

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
        assert_eq!(reply.part_bytes(0).unwrap(), payload.as_bytes());
    }

    let status = tokio::task::spawn_blocking(move || guard.take().wait().unwrap())
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
    let cli = bind_tcp_or_ipc(&pubs, &t).await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["sub", "-c", &cli, "-s", "weather.", "-A", "-n", "2"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq sub"),
    );

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

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
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
    let cli = bind_tcp_or_ipc(&router, &t).await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args([
                "dealer",
                "-c",
                &cli,
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
            .expect("spawn omq dealer"),
    );

    let got = tokio::time::timeout(Duration::from_secs(5), router.recv())
        .await
        .expect("router recv timed out")
        .unwrap();
    assert_eq!(got.len(), 2, "router message is [identity, body]");
    assert_eq!(got.part_bytes(0).unwrap(), &b"worker-7"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"from-dealer"[..]);

    // Ruby DEALER with `-n 1` exits after a single send; reap it.
    let _ = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
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
    let cli = bind_tcp_or_ipc(&radio, &t).await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["dish", "-c", &cli, "-j", "weather", "-A", "-n", "2"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq dish"),
    );

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

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
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

// ---------------------------------------------------------------------
// CURVE -- Rust PUSH (server) to Ruby PULL (client) over TCP with CURVE.
// Validates the ZMTP CURVE handshake + encrypted MESSAGE framing
// (including monotonic nonce counters) against Ruby's implementation.
// ---------------------------------------------------------------------

#[cfg(feature = "curve")]
fn curve_available() -> bool {
    Command::new("omq")
        .args(["push", "--help"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .is_ok_and(|o| {
            let help = String::from_utf8_lossy(&o.stderr);
            help.contains("curve") || o.status.success()
        })
}

#[cfg(feature = "curve")]
#[tokio::test]
async fn rust_curve_push_to_ruby_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    if !curve_available() {
        eprintln!("skip: omq CLI does not support --curve-server-key");
        return;
    }

    let server_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();

    let opts = Options::default().curve_server(server_kp);
    let push = Socket::new(SocketType::Push, opts);
    let cli = bind_tcp_or_ipc(
        &push,
        &Transport {
            rust: Endpoint::Tcp {
                host: Host::Ip("127.0.0.1".parse().unwrap()),
                port: 0,
            },
            cli: String::new(),
        },
    )
    .await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args([
                "pull",
                "-c",
                &cli,
                "--curve-server-key",
                &server_pub_z85,
                "-A",
                "-n",
                "3",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq pull --curve-server-key"),
    );

    wait_for_handshake(&push).await;

    for i in 0..3 {
        push.send(Message::single(format!("encrypted-{i}")))
            .await
            .unwrap();
    }

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "omq pull --curve failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["encrypted-0", "encrypted-1", "encrypted-2"]
    );
}

#[cfg(feature = "curve")]
#[tokio::test]
async fn ruby_curve_push_to_rust_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    if !curve_available() {
        eprintln!("skip: omq CLI does not support --curve-server");
        return;
    }

    let server_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();

    let opts = Options::default().curve_server(server_kp);
    let pull = Socket::new(SocketType::Pull, opts);
    let cli = bind_tcp_or_ipc(
        &pull,
        &Transport {
            rust: Endpoint::Tcp {
                host: Host::Ip("127.0.0.1".parse().unwrap()),
                port: 0,
            },
            cli: String::new(),
        },
    )
    .await;

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args([
                "push",
                "-c",
                &cli,
                "--curve-server-key",
                &server_pub_z85,
                "-A",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq push --curve-server-key"),
    );

    {
        let mut stdin = guard.stdin.take().unwrap();
        for i in 0..3 {
            writeln!(stdin, "from-ruby-{i}").unwrap();
        }
    }

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let _ = tokio::task::spawn_blocking(move || guard.take().wait().unwrap())
        .await
        .unwrap();
}
