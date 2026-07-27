//! Wire-compatibility tests against libzmq via pyzmq, exercising the
//! CURVE mechanism (RFC 26). Spawns `python3` with an inline script
//! that drives a pyzmq socket as the peer; asserts framing + handshake
//! interop in both directions over TCP. Self-skips with a printed
//! notice if `python3` is missing, or if the available pyzmq build was
//! linked against a libzmq without CURVE support.

#![cfg(feature = "curve")]
#![allow(clippy::match_wild_err_arm)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{CurveKeypair, Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn python3_command() -> Command {
    Command::new(std::env::var_os("OMQ_PYTHON3").unwrap_or_else(|| "python3".into()))
}

fn pyzmq_curve_available() -> bool {
    python3_command()
        .args([
            "-c",
            "import sys, zmq; sys.exit(0 if zmq.has('curve') else 1)",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The pyzmq peer is optional locally and mandatory in CI. With
/// `OMQ_INTEROP_REQUIRED=1` a missing peer fails the test instead of
/// letting the suite pass without exercising any interop.
fn skip_if_no_pyzmq_curve() -> bool {
    if !pyzmq_curve_available() {
        assert!(
            std::env::var_os("OMQ_INTEROP_REQUIRED").is_none(),
            "OMQ_INTEROP_REQUIRED=1 but python3 + pyzmq with CURVE is not available",
        );
        eprintln!("skip: python3 + pyzmq with CURVE not available");
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

/// Bind a socket to an ephemeral loopback port and return the
/// kernel-assigned port number, read from the monitor stream.
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

    let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
    let port = bind_loopback(&pull).await;

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

    let child = python3_command()
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

// ---------------------------------------------------------------------
// Rust CURVE REP <- pyzmq CURVE REQ
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_curve_rep_from_pyzmq_req() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();
    let client_pub_z85 = client_kp.public.to_z85();
    let client_sec_z85 = client_kp.secret.to_z85();

    let rep = Socket::new(SocketType::Rep, Options::default().curve_server(server_kp));
    let port = bind_loopback(&rep).await;
    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.REQ)
s.curve_secretkey = os.environ['CLI_SEC'].encode()
s.curve_publickey = os.environ['CLI_PUB'].encode()
s.curve_serverkey = os.environ['SRV_PUB'].encode()
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
s.send(b"curve-req")
assert s.recv() == b"curve-rep"
s.close(linger=2000)
ctx.term()
"#;
    let child = python3_command()
        .args(["-c", script])
        .env("PORT", port.to_string())
        .env("SRV_PUB", &server_pub_z85)
        .env("CLI_PUB", &client_pub_z85)
        .env("CLI_SEC", &client_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 req");

    wait_for_curve_handshake(&rep).await;
    let request = tokio::time::timeout(Duration::from_secs(5), rep.recv())
        .await
        .expect("CURVE REQ timed out")
        .unwrap();
    assert_eq!(request.part_bytes(0).unwrap(), &b"curve-req"[..]);
    rep.send(Message::single("curve-rep")).await.unwrap();

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "pyzmq req exited non-zero: stderr={}",
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

    // pyzmq PULL server: bind to an ephemeral port, print the port as
    // the first stdout line, recv 5 msgs, print each, then close.
    let script = r#"
import os, sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PULL)
s.curve_server = True
s.curve_secretkey = os.environ['SRV_SEC'].encode()
s.curve_publickey = os.environ['SRV_PUB'].encode()
port = s.bind_to_random_port("tcp://127.0.0.1")
sys.stdout.write(f"{port}\n"); sys.stdout.flush()
for _ in range(5):
    sys.stdout.write(s.recv().decode() + "\n"); sys.stdout.flush()
s.close(linger=0)
ctx.term()
"#;

    let mut child = python3_command()
        .args(["-c", script])
        .env("SRV_PUB", &server_pub_z85)
        .env("SRV_SEC", &server_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 pull");

    // First stdout line is the ephemeral port.
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).ok();
        let _ = port_tx.send(first.trim().to_string());
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
    let port: u16 = tokio::time::timeout(Duration::from_secs(5), port_rx)
        .await
        .expect("python bind timed out")
        .unwrap()
        .parse()
        .expect("python did not print a valid port");

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
    let pull = Socket::new(SocketType::Pull, Options::default().curve_server(server_kp));
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

    // Give the doomed handshake a moment, then assert nothing arrives.
    let recv = tokio::time::timeout(Duration::from_millis(500), pull.recv()).await;
    assert!(
        recv.is_err(),
        "NULL pyzmq client must not deliver to CURVE server"
    );

    let _ = tokio::task::spawn_blocking(move || child.wait_with_output()).await;
}

// ---------------------------------------------------------------------
// Rust CURVE PUB -> pyzmq CURVE SUB
// Exercises the inbound SUBSCRIBE-over-CURVE path: libzmq's SUB sends
// SUBSCRIBE as a CURVE MESSAGE with the COMMAND bit (0x02) in the
// encrypted inner flags byte. omq must read that bit to demux the
// subscription; without it the PUB never registers the filter and the
// SUB receives nothing.
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_curve_pub_to_pyzmq_sub() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();
    let client_pub_z85 = client_kp.public.to_z85();
    let client_sec_z85 = client_kp.secret.to_z85();

    let pub_sock = Socket::new(SocketType::Pub, Options::default().curve_server(server_kp));
    let port = bind_loopback(&pub_sock).await;

    // pyzmq SUB client: subscribe(""), receive 3 messages, print each.
    let script = r#"
import os, sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.SUB)
s.curve_secretkey = os.environ['CLI_SEC'].encode()
s.curve_publickey = os.environ['CLI_PUB'].encode()
s.curve_serverkey = os.environ['SRV_PUB'].encode()
s.subscribe(b"")
s.connect(f"tcp://127.0.0.1:{os.environ['PORT']}")
for _ in range(3):
    sys.stdout.write(s.recv().decode() + "\n"); sys.stdout.flush()
s.close(linger=0)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("PORT", port.to_string())
        .env("SRV_PUB", &server_pub_z85)
        .env("CLI_PUB", &client_pub_z85)
        .env("CLI_SEC", &client_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 sub");

    wait_for_curve_handshake(&pub_sock).await;

    // Retry loop: the SUB's subscription may take a moment to propagate.
    for _ in 0..60 {
        let _ = pub_sock.send(Message::single("curve-pubsub")).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "pyzmq sub exited non-zero: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines: Vec<&str> = std::str::from_utf8(&out.stdout).unwrap().lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines.iter().all(|l| *l == "curve-pubsub"));
}

// ---------------------------------------------------------------------
// pyzmq CURVE PUB -> Rust CURVE SUB
// Exercises the outbound SUBSCRIBE-over-CURVE path: omq's SUB must
// set the COMMAND bit (0x02) in the encrypted inner flags byte when
// sending SUBSCRIBE, so that libzmq's PUB recognizes it as a command
// and registers the subscription filter.
// ---------------------------------------------------------------------

#[tokio::test]
async fn pyzmq_curve_pub_to_rust_sub() {
    if skip_if_no_pyzmq_curve() {
        return;
    }

    let server_kp = CurveKeypair::generate();
    let client_kp = CurveKeypair::generate();
    let server_pub_z85 = server_kp.public.to_z85();
    let server_sec_z85 = server_kp.secret.to_z85();
    let server_pub_for_client = server_kp.public;

    // pyzmq PUB server: bind to an ephemeral port, print the port as
    // the first stdout line, then publish 60 messages with 50ms spacing
    // (enough for the SUB's subscription to propagate).
    let script = r#"
import os, sys, zmq, time
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUB)
s.curve_server = True
s.curve_secretkey = os.environ['SRV_SEC'].encode()
s.curve_publickey = os.environ['SRV_PUB'].encode()
port = s.bind_to_random_port("tcp://127.0.0.1")
sys.stdout.write(f"{port}\n"); sys.stdout.flush()
for i in range(60):
    s.send(f"msg-{i}".encode())
    time.sleep(0.05)
s.close(linger=0)
ctx.term()
"#;

    let mut child = python3_command()
        .args(["-c", script])
        .env("SRV_PUB", &server_pub_z85)
        .env("SRV_SEC", &server_sec_z85)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3 pub");

    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).ok();
        let _ = port_tx.send(first.trim().to_string());
    });
    let port: u16 = tokio::time::timeout(Duration::from_secs(5), port_rx)
        .await
        .expect("python bind timed out")
        .unwrap()
        .parse()
        .expect("python did not print a valid port");

    let sub = Socket::new(
        SocketType::Sub,
        Options::default().curve_client(client_kp, server_pub_for_client),
    );
    sub.subscribe("").await.unwrap();
    sub.connect(loopback(port)).await.unwrap();

    let m = match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => panic!("recv error: {e}"),
        Err(_) => {
            let _ = child.kill();
            let mut err = String::new();
            let _ = stderr.read_to_string(&mut err);
            panic!("SUB never received from pyzmq PUB over CURVE\nstderr={err}");
        }
    };
    assert!(
        std::str::from_utf8(&m.part_bytes(0).unwrap())
            .unwrap()
            .starts_with("msg-")
    );

    let _ = tokio::task::spawn_blocking(move || {
        let _ = child.wait();
        let _ = stderr;
    })
    .await;
}
