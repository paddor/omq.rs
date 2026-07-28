//! Wire-compatibility tests against libzmq via pyzmq, exercising the
//! NULL mechanism over TCP/IPC and the STREAM socket.

mod test_support;

use std::io::Read;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

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

async fn recv(sock: &Socket, context: &str) -> Message {
    tokio::time::timeout(Duration::from_secs(5), sock.recv())
        .await
        .unwrap_or_else(|_| panic!("{context}: recv timed out"))
        .unwrap()
}

async fn wait_success(child: Child, context: &str) -> Output {
    let output = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{context} exited non-zero\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

async fn rust_pull_from_pyzmq_push(endpoint: Endpoint) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let endpoint = pull.bind(endpoint).await.unwrap();

    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUSH)
s.connect(os.environ["ENDPOINT"])
for i in range(3):
    s.send(f"py-null-{i}".encode())
s.close(linger=2000)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("ENDPOINT", endpoint.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyzmq PUSH");

    for i in 0..3 {
        let msg = recv(&pull, "pyzmq PUSH -> Rust PULL").await;
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            format!("py-null-{i}").as_bytes()
        );
    }

    wait_success(child, "pyzmq PUSH").await;
}

async fn rust_push_to_pyzmq_pull(endpoint: Endpoint) {
    let script = r#"
import os, sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PULL)
endpoint = os.environ["ENDPOINT"]
if endpoint == "tcp://127.0.0.1:0":
    port = s.bind_to_random_port("tcp://127.0.0.1")
    endpoint = f"tcp://127.0.0.1:{port}"
else:
    s.bind(endpoint)
sys.stdout.write(endpoint + "\n"); sys.stdout.flush()
for _ in range(3):
    sys.stdout.write(s.recv().decode() + "\n"); sys.stdout.flush()
s.close(linger=0)
ctx.term()
"#;

    let mut child = python3_command()
        .args(["-c", script])
        .env("ENDPOINT", endpoint.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyzmq PULL");

    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel::<Endpoint>();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).unwrap();
        let _ = endpoint_tx.send(first.trim().parse().unwrap());
        let mut lines = Vec::new();
        for _ in 0..3 {
            let mut buf = String::new();
            if r.read_line(&mut buf).unwrap_or(0) == 0 {
                break;
            }
            lines.push(buf.trim().to_string());
        }
        lines
    });
    let connect_endpoint = tokio::time::timeout(Duration::from_secs(5), endpoint_rx)
        .await
        .expect("pyzmq PULL did not become ready")
        .unwrap();

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(connect_endpoint).await.unwrap();
    for i in 0..3 {
        push.send(Message::single(format!("rust-null-{i}")))
            .await
            .unwrap();
    }

    let lines = if let Ok(r) = tokio::time::timeout(Duration::from_secs(10), reader).await {
        r.unwrap()
    } else {
        let _ = child.kill();
        let mut err = String::new();
        let _ = stderr.read_to_string(&mut err);
        panic!("pyzmq PULL timed out\nstderr={err}");
    };
    assert_eq!(
        lines,
        (0..3).map(|i| format!("rust-null-{i}")).collect::<Vec<_>>()
    );

    let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    assert!(status.success(), "pyzmq PULL exited with {status}");
}

#[tokio::test]
async fn null_tcp_pyzmq_push_to_rust_pull() {
    if skip_if_no_pyzmq() {
        return;
    }
    rust_pull_from_pyzmq_push(test_support::tcp_loopback(0)).await;
}

#[tokio::test]
async fn null_tcp_rust_push_to_pyzmq_pull() {
    if skip_if_no_pyzmq() {
        return;
    }
    rust_push_to_pyzmq_pull(test_support::tcp_loopback(0)).await;
}

#[cfg(unix)]
#[tokio::test]
async fn null_ipc_pyzmq_push_to_rust_pull() {
    if skip_if_no_pyzmq() {
        return;
    }
    rust_pull_from_pyzmq_push(test_support::ipc_endpoint("interop-null-py-push")).await;
}

#[cfg(unix)]
#[tokio::test]
async fn null_ipc_rust_push_to_pyzmq_pull() {
    if skip_if_no_pyzmq() {
        return;
    }
    rust_push_to_pyzmq_pull(test_support::ipc_endpoint("interop-null-py-pull")).await;
}

#[tokio::test]
async fn stream_pyzmq_connects_to_rust() {
    if skip_if_no_pyzmq() {
        return;
    }

    let stream = Socket::new(SocketType::Stream, Options::default());
    let endpoint = stream.bind(test_support::tcp_loopback(0)).await.unwrap();

    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.STREAM)
s.connect(os.environ["ENDPOINT"])
ident, empty = s.recv_multipart()
assert empty == b""
s.send_multipart([ident, b"from-pyzmq"])
ident2, body = s.recv_multipart()
assert ident2 == ident
assert body == b"from-rust"
s.close(linger=0)
ctx.term()
"#;

    let child = python3_command()
        .args(["-c", script])
        .env("ENDPOINT", endpoint.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyzmq STREAM");

    let connect = recv(&stream, "STREAM connect notification").await;
    let identity = connect.part_bytes(0).unwrap();
    assert!(!identity.is_empty());
    assert!(connect.part_bytes(1).unwrap().is_empty());

    let data = recv(&stream, "STREAM data from pyzmq").await;
    assert_eq!(data.part_bytes(0).unwrap(), identity);
    assert_eq!(data.part_bytes(1).unwrap(), &b"from-pyzmq"[..]);

    stream
        .send(Message::multipart([
            identity.clone(),
            Bytes::from_static(b"from-rust"),
        ]))
        .await
        .unwrap();

    wait_success(child, "pyzmq STREAM").await;
}

#[tokio::test]
async fn stream_rust_connects_to_pyzmq() {
    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
import sys, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.STREAM)
port = s.bind_to_random_port("tcp://127.0.0.1")
sys.stdout.write(f"tcp://127.0.0.1:{port}\n"); sys.stdout.flush()
ident, empty = s.recv_multipart()
assert empty == b""
ident2, body = s.recv_multipart()
assert ident2 == ident
assert body == b"from-rust"
s.send_multipart([ident, b"from-pyzmq"])
s.close(linger=0)
ctx.term()
"#;

    let mut child = python3_command()
        .args(["-c", script])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyzmq STREAM");

    let stdout = child.stdout.take().unwrap();
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel::<String>();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).unwrap();
        let _ = endpoint_tx.send(first.trim().to_string());
    });
    let endpoint: Endpoint = tokio::time::timeout(Duration::from_secs(5), endpoint_rx)
        .await
        .expect("pyzmq STREAM bind timed out")
        .unwrap()
        .parse()
        .unwrap();

    let stream = Socket::new(SocketType::Stream, Options::default());
    stream.connect(endpoint).await.unwrap();

    let connect = recv(&stream, "STREAM connect notification").await;
    let identity = connect.part_bytes(0).unwrap();
    assert!(!identity.is_empty());
    assert!(connect.part_bytes(1).unwrap().is_empty());

    stream
        .send(Message::multipart([
            identity.clone(),
            Bytes::from_static(b"from-rust"),
        ]))
        .await
        .unwrap();

    let reply = recv(&stream, "STREAM reply from pyzmq").await;
    assert_eq!(reply.part_bytes(0).unwrap(), identity);
    assert_eq!(reply.part_bytes(1).unwrap(), &b"from-pyzmq"[..]);

    reader.await.unwrap();
    wait_success(child, "pyzmq STREAM").await;
}
