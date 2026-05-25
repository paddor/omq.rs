//! Wire-compatibility tests against the Ruby OMQ implementation,
//! compio backend. Spawns the `omq` Ruby CLI (`gem install omq-cli`).
//! Self-skips with a printed notice if the binary is not on PATH.
//!
//! Mirrors `omq-tokio/tests/interop_ruby.rs`. Sync child-process
//! waits go through an OS thread + flume oneshot so the single-thread
//! compio runtime can keep driving the Rust socket while Ruby runs.

use std::io::Write;
use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::Duration;

use omq_compio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use omq_proto::endpoint::{Host, IpcPath};

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

/// Run `f` on a fresh OS thread and await its result on the compio
/// runtime. Equivalent to tokio's `spawn_blocking` for our purposes:
/// keeps blocking work (sync `child.wait_with_output()`) off the
/// single-thread runtime so async sockets can keep making progress.
async fn await_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = flume::bounded::<T>(1);
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_async()
        .await
        .expect("blocking thread dropped sender")
}

async fn wait_with_output(child: Child) -> Output {
    await_blocking(move || child.wait_with_output().expect("wait_with_output")).await
}

async fn wait_status(mut child: Child) -> ExitStatus {
    await_blocking(move || child.wait().expect("wait")).await
}

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

fn tcp_endpoint_port_zero() -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port: 0,
    }
}

fn cli_addr(sock: &Socket) -> String {
    match sock.last_bound_endpoint().expect("no bound endpoint") {
        Endpoint::Tcp { host, port } => format!("tcp://{host}:{port}"),
        Endpoint::Ipc(IpcPath::Filesystem(p)) => format!("ipc://{}", p.display()),
        other => panic!("unexpected endpoint: {other:?}"),
    }
}

fn ipc_transport(name: &str) -> Transport {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "omq-ri-{}-{:x}.sock",
        &name[..name.len().min(8)],
        (nanos as u64) ^ u64::from(std::process::id()),
    ));
    let _ = std::fs::remove_file(&path);
    let cli = format!("ipc://{}", path.display());
    Transport {
        rust: Endpoint::Ipc(IpcPath::Filesystem(path)),
        cli,
    }
}

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
    compio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("handshake did not arrive within 5s");
}

// =====================================================================
// PUSH / PULL
// =====================================================================

async fn rust_push_to_ruby_pull(ep: Endpoint) {
    let push = Socket::new(SocketType::Push, Options::default());
    push.bind(ep).await.unwrap();
    let cli = cli_addr(&push);

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

    let out = wait_with_output(guard.take()).await;
    assert!(out.status.success(), "omq pull failed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["hello-0", "hello-1", "hello-2", "hello-3", "hello-4"]
    );
}

#[compio::test]
async fn rust_push_to_ruby_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_push_to_ruby_pull(tcp_endpoint_port_zero()).await;
}

#[compio::test]
async fn rust_push_to_ruby_pull_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_push_to_ruby_pull(ipc_transport("push-pull").rust).await;
}

async fn ruby_push_to_rust_pull(ep: Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep).await.unwrap();
    let cli = cli_addr(&pull);

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
        let msg = compio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let _ = wait_status(guard.take()).await;
}

#[compio::test]
async fn ruby_push_to_rust_pull_tcp() {
    if skip_if_no_omq() {
        return;
    }
    ruby_push_to_rust_pull(tcp_endpoint_port_zero()).await;
}

#[compio::test]
async fn ruby_push_to_rust_pull_ipc() {
    if skip_if_no_omq() {
        return;
    }
    ruby_push_to_rust_pull(ipc_transport("push-pull-rev").rust).await;
}

// =====================================================================
// REQ / REP
// =====================================================================

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
    req.connect(t.rust).await.unwrap();
    wait_for_handshake(&req).await;

    for i in 0..3 {
        let payload = format!("ping-{i}");
        req.send(Message::single(payload.clone())).await.unwrap();
        let reply = compio::time::timeout(Duration::from_secs(5), req.recv())
            .await
            .expect("reply timed out")
            .unwrap();
        assert_eq!(reply.part_bytes(0).unwrap(), payload.as_bytes());
    }

    let status = wait_status(guard.take()).await;
    assert!(status.success(), "omq rep exited with {status:?}");
}

#[compio::test]
async fn rust_req_to_ruby_rep_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_req_to_ruby_rep(tcp_transport()).await;
}

#[compio::test]
async fn rust_req_to_ruby_rep_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_req_to_ruby_rep(ipc_transport("req-rep")).await;
}

// =====================================================================
// PUB / SUB
// =====================================================================

async fn rust_pub_to_ruby_sub(ep: Endpoint) {
    let pubs = Socket::new(SocketType::Pub, Options::default());
    pubs.bind(ep).await.unwrap();
    let cli = cli_addr(&pubs);

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["sub", "-c", &cli, "-s", "weather.", "-A", "-n", "2"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq sub"),
    );

    wait_for_handshake(&pubs).await;
    compio::time::sleep(Duration::from_millis(100)).await;

    pubs.send(Message::multipart(["weather.eu", "sunny"]))
        .await
        .unwrap();
    pubs.send(Message::multipart(["news.global", "ignored"]))
        .await
        .unwrap();
    pubs.send(Message::multipart(["weather.us", "rainy"]))
        .await
        .unwrap();

    let out = wait_with_output(guard.take()).await;
    assert!(out.status.success(), "omq sub failed: {out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["weather.eu\tsunny", "weather.us\trainy"]
    );
}

#[compio::test]
async fn rust_pub_to_ruby_sub_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_pub_to_ruby_sub(tcp_endpoint_port_zero()).await;
}

#[compio::test]
async fn rust_pub_to_ruby_sub_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_pub_to_ruby_sub(ipc_transport("pub-sub").rust).await;
}

// =====================================================================
// ROUTER / DEALER
// =====================================================================

async fn rust_router_sees_ruby_dealer_identity(ep: Endpoint) {
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep).await.unwrap();
    let cli = cli_addr(&router);

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

    let got = compio::time::timeout(Duration::from_secs(5), router.recv())
        .await
        .expect("router recv timed out")
        .unwrap();
    assert_eq!(got.len(), 2, "router message is [identity, body]");
    assert_eq!(got.part_bytes(0).unwrap(), &b"worker-7"[..]);
    assert_eq!(got.part_bytes(1).unwrap(), &b"from-dealer"[..]);

    let _ = wait_with_output(guard.take()).await;
}

#[compio::test]
async fn rust_router_sees_ruby_dealer_identity_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_router_sees_ruby_dealer_identity(tcp_endpoint_port_zero()).await;
}

#[compio::test]
async fn rust_router_sees_ruby_dealer_identity_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_router_sees_ruby_dealer_identity(ipc_transport("router-dealer").rust).await;
}

// =====================================================================
// RADIO / DISH
// =====================================================================

async fn rust_radio_to_ruby_dish(ep: Endpoint) {
    let radio = Socket::new(SocketType::Radio, Options::default());
    radio.bind(ep).await.unwrap();
    let cli = cli_addr(&radio);

    let mut guard = ChildGuard::new(
        Command::new("omq")
            .args(["dish", "-c", &cli, "-j", "weather", "-A", "-n", "2"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omq dish"),
    );

    wait_for_handshake(&radio).await;
    compio::time::sleep(Duration::from_millis(100)).await;

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

    let out = wait_with_output(guard.take()).await;
    assert!(out.status.success(), "omq dish failed: {out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .collect::<Vec<_>>(),
        vec!["weather\tsunny", "weather\trainy"]
    );
}

#[compio::test]
async fn rust_radio_to_ruby_dish_tcp() {
    if skip_if_no_omq() {
        return;
    }
    rust_radio_to_ruby_dish(tcp_endpoint_port_zero()).await;
}

#[compio::test]
async fn rust_radio_to_ruby_dish_ipc() {
    if skip_if_no_omq() {
        return;
    }
    rust_radio_to_ruby_dish(ipc_transport("radio-dish").rust).await;
}
