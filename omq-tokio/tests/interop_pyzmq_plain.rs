//! Wire-compatibility tests against libzmq via pyzmq, exercising the
//! PLAIN mechanism (RFC 24). Spawns `python3` with an inline script
//! that drives a pyzmq socket as the peer; asserts framing + handshake
//! interop in both directions over TCP.

#![cfg(feature = "plain")]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn python3_command() -> Command {
    Command::new(std::env::var_os("OMQ_PYTHON3").unwrap_or_else(|| "python3".into()))
}

fn pyzmq_available() -> bool {
    python3_command()
        .args(["-c", "import zmq"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The pyzmq peer is optional locally and mandatory in CI. With
/// `OMQ_INTEROP_REQUIRED=1` a missing peer fails the test instead of
/// letting the suite pass without exercising any interop.
fn skip_if_no_pyzmq() -> bool {
    if !pyzmq_available() {
        assert!(
            std::env::var_os("OMQ_INTEROP_REQUIRED").is_none(),
            "OMQ_INTEROP_REQUIRED=1 but python3 + pyzmq is not available",
        );
        eprintln!("skip: python3 + pyzmq not available");
        return true;
    }
    false
}

fn loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port,
    }
}

async fn bind_loopback(sock: &Socket) -> u16 {
    let mut mon = sock.monitor();
    sock.bind(loopback(0)).await.unwrap();
    loop {
        match mon.recv().await {
            Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            }) => return port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    }
}

async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return Ok::<(), String>(()),
                Ok(MonitorEvent::HandshakeFailed { reason, .. }) => {
                    return Err(format!("HandshakeFailed: {reason:?}"));
                }
                Ok(_) => {}
                Err(e) => return Err(format!("monitor closed: {e:?}")),
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("PLAIN handshake error: {e}"),
        Err(e) => panic!("PLAIN handshake did not complete within 5s: {e}"),
    }
}

// Rust PLAIN PULL (server) <- pyzmq PLAIN PUSH (client)
#[tokio::test]
async fn rust_plain_pull_from_pyzmq_push() {
    if skip_if_no_pyzmq() {
        return;
    }

    let pull = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(|peer| {
            peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("s3cret")
        }),
    );
    let port = bind_loopback(&pull).await;

    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUSH)
s.plain_username = b"alice"
s.plain_password = b"s3cret"
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
for i in range(5):
    s.send(f"hello-{i}".encode())
s.close(linger=2000)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("PORT", port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 push");

    wait_for_handshake(&pull).await;

    for i in 0..5 {
        let m = if let Ok(r) = tokio::time::timeout(Duration::from_secs(5), pull.recv()).await {
            r.unwrap()
        } else {
            let out = child.wait_with_output().unwrap();
            panic!(
                "recv #{i} timed out\nstdout={}\nstderr={}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        };
        assert_eq!(m.part_bytes(0).unwrap(), format!("hello-{i}").as_bytes());
    }

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "pyzmq push exited non-zero: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// Rust PLAIN PUSH (client) -> pyzmq PLAIN PULL (server)
#[tokio::test]
async fn rust_plain_push_to_pyzmq_pull() {
    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
import sys, zmq, zmq.auth.thread
ctx = zmq.Context.instance()
auth = zmq.auth.thread.ThreadAuthenticator(ctx)
auth.start()
auth.allow("127.0.0.1")
auth.configure_plain(domain="global", passwords={"alice": "s3cret"})
s = ctx.socket(zmq.PULL)
s.zap_domain = b"global"
s.plain_server = True
port = s.bind_to_random_port("tcp://127.0.0.1")
sys.stdout.write(f"{port}\n"); sys.stdout.flush()
for _ in range(5):
    sys.stdout.write(s.recv().decode() + "\n"); sys.stdout.flush()
s.close(linger=0)
auth.stop()
ctx.term()
"#;

    let mut child = python3_command()
        .args(["-c", script])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 pull");

    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<u16>();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).ok();
        let _ = ready_tx.send(first.trim().parse::<u16>().expect("port from python"));
        let mut lines = Vec::new();
        for _ in 0..5 {
            let mut buf = String::new();
            if r.read_line(&mut buf).unwrap_or(0) == 0 {
                break;
            }
            lines.push(buf.trim().to_string());
        }
        lines
    });
    let port = tokio::time::timeout(Duration::from_secs(5), ready_rx)
        .await
        .expect("python bind timed out")
        .unwrap();

    let push = Socket::new(
        SocketType::Push,
        Options::default().plain_client("alice", "s3cret"),
    );
    push.connect(loopback(port)).await.unwrap();
    wait_for_handshake(&push).await;

    for i in 0..5 {
        push.send(Message::single(format!("from-rust-{i}")))
            .await
            .unwrap();
    }

    let lines = if let Ok(r) = tokio::time::timeout(Duration::from_secs(10), reader).await {
        r.unwrap()
    } else {
        let _ = child.kill();
        let mut err = String::new();
        let _ = stderr.read_to_string(&mut err);
        panic!("python recv loop timed out\nstderr={err}");
    };
    assert_eq!(
        lines,
        (0..5).map(|i| format!("from-rust-{i}")).collect::<Vec<_>>()
    );

    let _ = tokio::task::spawn_blocking(move || {
        let _ = child.wait();
        let _ = stderr;
    })
    .await;
}

// Wrong credentials: pyzmq PUSH with bad password must not deliver.
#[tokio::test]
async fn rust_plain_pull_rejects_wrong_pyzmq_credentials() {
    if skip_if_no_pyzmq() {
        return;
    }

    let pull = Socket::new(
        SocketType::Pull,
        Options::default().plain_server(|peer| {
            peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("s3cret")
        }),
    );
    let port = bind_loopback(&pull).await;

    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUSH)
s.plain_username = b"alice"
s.plain_password = b"WRONG"
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
try:
    s.send(b"should-not-arrive", flags=zmq.NOBLOCK)
except zmq.Again:
    pass
s.close(linger=200)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn python3 push with wrong creds");

    let recv = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await;
    assert!(
        recv.is_err(),
        "pyzmq client with wrong password must not deliver to PLAIN server"
    );

    let _ = tokio::task::spawn_blocking(move || child.wait_with_output()).await;
}

// NULL pyzmq client must not be admitted by PLAIN server.
#[tokio::test]
async fn rust_plain_pull_rejects_null_pyzmq_push() {
    if skip_if_no_pyzmq() {
        return;
    }

    let pull = Socket::new(SocketType::Pull, Options::default().plain_server(|_| true));
    let port = bind_loopback(&pull).await;

    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUSH)
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
try:
    s.send(b"should-not-arrive", flags=zmq.NOBLOCK)
except zmq.Again:
    pass
s.close(linger=200)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn python3 null push");

    let recv = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await;
    assert!(
        recv.is_err(),
        "NULL pyzmq client must not deliver to PLAIN server"
    );

    let _ = tokio::task::spawn_blocking(move || child.wait_with_output()).await;
}
