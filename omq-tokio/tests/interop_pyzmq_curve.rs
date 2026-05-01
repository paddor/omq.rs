//! Wire-compatibility tests against libzmq via pyzmq, exercising the
//! CURVE mechanism (RFC 26). Spawns `python3` with an inline script
//! that drives a pyzmq socket as the peer; asserts framing + handshake
//! interop in both directions over TCP. Self-skips with a printed
//! notice if `python3` is missing, or if the available pyzmq build was
//! linked against a libzmq without CURVE support.

#![cfg(feature = "curve")]
#![allow(clippy::match_wild_err_arm)]

use std::io::Read;
use std::net::TcpListener as StdTcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{CurveKeypair, Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn pyzmq_curve_available() -> bool {
    Command::new("python3")
        .args([
            "-c",
            "import sys, zmq; sys.exit(0 if zmq.has('curve') else 1)",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn skip_if_no_pyzmq_curve() -> bool {
    if !pyzmq_curve_available() {
        eprintln!("skip: python3 + pyzmq with CURVE not available");
        return true;
    }
    false
}

fn free_tcp_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port,
    }
}

/// Wait for the Rust socket to log at least one successful CURVE
/// handshake, with an absolute deadline to fail fast on hangs.
async fn wait_for_curve_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { peer, .. }) => {
                    let _ = peer;
                    return Ok::<(), String>(());
                }
                Ok(MonitorEvent::HandshakeFailed { reason, .. }) => {
                    return Err(format!("HandshakeFailed: {reason:?}"));
                }
                Ok(_) => {}
                Err(e) => return Err(format!("monitor stream closed: {e:?}")),
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("CURVE handshake error: {e}"),
        Err(_) => panic!("CURVE handshake did not complete within 5s"),
    }
}

// ---------------------------------------------------------------------
// Rust CURVE PULL <- pyzmq CURVE PUSH
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_curve_pull_from_pyzmq_push() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();
    let client_pub_z85 = client_kp.public.to_z85();
    let client_sec_z85 = client_kp.secret.to_z85();

    let port = free_tcp_port();
    let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    pull.bind(loopback(port)).await.unwrap();

    // pyzmq PUSH client: 5 messages, then close.
    let script = r#"
import os, sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUSH)
s.curve_secretkey = os.environ['CLI_SEC'].encode()
s.curve_publickey = os.environ['CLI_PUB'].encode()
s.curve_serverkey = os.environ['SRV_PUB'].encode()
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
for i in range(5):
    s.send(f"hello-{i}".encode())
s.close(linger=2000)
ctx.term()
"#;

    let child = Command::new("python3")
        .args(["-c", script])
        .env("PORT", port.to_string())
        .env("SRV_PUB", &server_pub_z85)
        .env("CLI_PUB", &client_pub_z85)
        .env("CLI_SEC", &client_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 push");

    wait_for_curve_handshake(&pull).await;

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
        assert_eq!(m.parts()[0].coalesce(), format!("hello-{i}").as_bytes());
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

// ---------------------------------------------------------------------
// Rust CURVE PUSH -> pyzmq CURVE PULL
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_curve_push_to_pyzmq_pull() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();
    let server_sec_z85 = server_kp.secret.to_z85();
    let server_pub_for_client = server_kp.public;

    let port = free_tcp_port();

    // pyzmq PULL server: bind, recv 5 msgs, print each on its own line,
    // then close. Rust will then read its stdout to assert framing.
    let script = r#"
import os, sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PULL)
s.curve_server = True
s.curve_secretkey = os.environ['SRV_SEC'].encode()
s.curve_publickey = os.environ['SRV_PUB'].encode()
s.bind(f"tcp://127.0.0.1:{os.environ['PORT']}")
sys.stdout.write("READY\n"); sys.stdout.flush()
for _ in range(5):
    sys.stdout.write(s.recv().decode() + "\n"); sys.stdout.flush()
s.close(linger=0)
ctx.term()
"#;

    let mut child = Command::new("python3")
        .args(["-c", script])
        .env("PORT", port.to_string())
        .env("SRV_PUB", &server_pub_z85)
        .env("SRV_SEC", &server_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 pull");

    // Read READY line so we know the bind landed before we connect.
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).ok();
        let _ = ready_tx.send(first.trim() == "READY");
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
    let ready = tokio::time::timeout(Duration::from_secs(5), ready_rx)
        .await
        .expect("python bind timed out")
        .unwrap();
    assert!(ready, "python pull did not signal READY");

    let push = Socket::new(
        SocketType::Push,
        Options::default().curve_client(client_kp, server_pub_for_client),
    );
    push.connect(loopback(port)).await.unwrap();
    wait_for_curve_handshake(&push).await;

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

// ---------------------------------------------------------------------
// Sanity: a non-CURVE pyzmq client must NOT be admitted by a CURVE
// server. Catches regressions where the mechanism string is omitted or
// the server falls back to NULL.
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_curve_pull_rejects_null_pyzmq_push() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let port = free_tcp_port();
    let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    pull.bind(loopback(port)).await.unwrap();

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

    let child = Command::new("python3")
        .args(["-c", script])
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn python3 null push");

    // Give the doomed handshake a moment, then assert nothing arrives.
    let recv = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await;
    assert!(
        recv.is_err(),
        "NULL pyzmq client must not deliver to CURVE server"
    );

    let _ = tokio::task::spawn_blocking(move || child.wait_with_output()).await;
}
