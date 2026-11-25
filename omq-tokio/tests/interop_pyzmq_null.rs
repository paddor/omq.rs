//! Wire-compatibility tests against libzmq via pyzmq, exercising the
//! NULL mechanism over TCP/IPC, PUB/SUB, and the STREAM socket.

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

/// Reads the endpoint a pyzmq PULL prints once bound (the resolved port for
/// `tcp://...:0`). A blocking reader then collects up to `lines` received
/// messages, one per line.
async fn read_endpoint_then_lines(
    stdout: std::process::ChildStdout,
    lines: usize,
) -> (Endpoint, tokio::task::JoinHandle<Vec<String>>) {
    let (endpoint_tx, endpoint_rx) = tokio::sync::oneshot::channel::<Endpoint>();
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};

        let mut r = BufReader::new(stdout);
        let mut first = String::new();
        r.read_line(&mut first).unwrap();
        let _ = endpoint_tx.send(first.trim().parse().unwrap());

        let mut collected = Vec::new();
        for _ in 0..lines {
            let mut buf = String::new();
            if r.read_line(&mut buf).unwrap_or(0) == 0 {
                break;
            }

            collected.push(buf.trim().to_string());
        }

        collected
    });

    let endpoint = tokio::time::timeout(Duration::from_secs(5), endpoint_rx)
        .await
        .expect("pyzmq PULL did not become ready")
        .unwrap();

    (endpoint, reader)
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
    let (connect_endpoint, reader) = read_endpoint_then_lines(stdout, 3).await;

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

/// A pyzmq DEALER whose identity starts with a zero byte is admitted and
/// routed by a Rust ROUTER, as libzmq does.
#[tokio::test]
async fn null_tcp_pyzmq_zero_prefixed_dealer_to_rust_router() {
    if skip_if_no_pyzmq() {
        return;
    }

    let router = Socket::new(
        SocketType::Router,
        Options::default().router_mandatory(true),
    );
    let endpoint = router.bind(test_support::tcp_loopback(0)).await.unwrap();
    let script = r#"
import os, zmq
ctx = zmq.Context.instance()
s = ctx.socket(zmq.DEALER)
s.setsockopt(zmq.IDENTITY, b"\x00\x00")
s.setsockopt(zmq.RCVTIMEO, 5000)
s.connect(os.environ["ENDPOINT"])
s.send(b"ping")
assert s.recv() == b"pong"
s.close(linger=2000)
ctx.term()
"#;
    let child = python3_command()
        .args(["-c", script])
        .env("ENDPOINT", endpoint.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyzmq DEALER");

    let request = recv(&router, "pyzmq DEALER -> Rust ROUTER").await;
    assert_eq!(request, Message::multipart([&b"\0\0"[..], b"ping"]));
    router
        .send(Message::multipart([&b"\0\0"[..], b"pong"]))
        .await
        .unwrap();

    wait_success(child, "pyzmq DEALER").await;
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

// ---------------------------------------------------------------------
// PUB/SUB with KV-event style messages: topic, 8-byte big-endian sequence
// number, payload.
// ---------------------------------------------------------------------

/// Python helpers shared by the PUB/SUB scripts.
const PY_KV: &str = r#"
import os, sys, zmq

def bind_random(s):
    port = s.bind_to_random_port("tcp://127.0.0.1")
    return f"tcp://127.0.0.1:{port}"

def kv(s, topic, seq, payload):
    s.send_multipart([topic, seq.to_bytes(8, "big"), payload])

def wait_subscribe(s, prefix):
    while s.recv() != b"\x01" + prefix:
        pass
"#;

fn spawn_python_kv(script: &str, envs: &[(&str, String)]) -> Child {
    let mut command = python3_command();
    command
        .args(["-c", &format!("{PY_KV}{script}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for (key, value) in envs {
        command.env(key, value);
    }

    command.spawn().expect("spawn pyzmq script")
}

/// Reads the endpoint a pyzmq script prints first.
async fn read_endpoint(child: &mut Child) -> Endpoint {
    let (endpoint, _reader) = read_endpoint_then_lines(child.stdout.take().unwrap(), 0).await;

    endpoint
}

fn kv_event(topic: &str, seq: u64, payload: &[u8]) -> Message {
    Message::multipart([
        Bytes::copy_from_slice(topic.as_bytes()),
        Bytes::copy_from_slice(&seq.to_be_bytes()),
        Bytes::copy_from_slice(payload),
    ])
}

fn kv_parts(message: &Message) -> (Bytes, u64, Bytes) {
    assert_eq!(message.len(), 3, "KV event parts");
    let seq = message.part_bytes(1).unwrap();

    (
        message.part_bytes(0).unwrap(),
        u64::from_be_bytes(seq[..].try_into().expect("8-byte sequence")),
        message.part_bytes(2).unwrap(),
    )
}

/// libzmq filters by the prefix a Rust SUB subscribed, and multipart
/// events arrive whole, including a 1 MiB payload. XPUB lets the script
/// publish once the subscription arrived.
#[tokio::test]
async fn null_tcp_pyzmq_pub_to_rust_sub_filters_multipart_events() {
    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
ctx = zmq.Context.instance()
s = ctx.socket(zmq.XPUB)
s.setsockopt(zmq.RCVTIMEO, 10000)
sys.stdout.write(bind_random(s) + "\n"); sys.stdout.flush()
wait_subscribe(s, b"kv.")
kv(s, b"other", 0, b"dropped")
kv(s, b"kv.a", 1, b"small")
kv(s, b"kv", 2, b"no dot")
kv(s, b"kv.b", 3, b"x" * (1 << 20))
kv(s, b"kv.end", 4, b"")
s.close(linger=5000)
ctx.term()
"#;
    let mut child = spawn_python_kv(script, &[]);
    let endpoint = read_endpoint(&mut child).await;

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("kv.").await.unwrap();
    sub.connect(endpoint).await.unwrap();

    let (topic, seq, payload) = kv_parts(&recv(&sub, "kv.a").await);
    assert_eq!(
        (&topic[..], seq, &payload[..]),
        (&b"kv.a"[..], 1, &b"small"[..])
    );

    let (topic, seq, payload) = kv_parts(&recv(&sub, "kv.b").await);
    assert_eq!((&topic[..], seq), (&b"kv.b"[..], 3));
    assert_eq!(payload.len(), 1 << 20);
    assert!(payload.iter().all(|byte| *byte == b'x'));

    let (topic, seq, payload) = kv_parts(&recv(&sub, "kv.end").await);
    assert_eq!(
        (&topic[..], seq, &payload[..]),
        (&b"kv.end"[..], 4, &b""[..])
    );

    wait_success(child, "pyzmq XPUB").await;
}

/// A plain libzmq PUB, as a KV event publisher uses, streams into a Rust
/// SUB in order and without gaps once the subscription took effect.
#[tokio::test]
async fn null_tcp_pyzmq_pub_streams_in_order_to_rust_sub() {
    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
import select
ctx = zmq.Context.instance()
s = ctx.socket(zmq.PUB)
sys.stdout.write(bind_random(s) + "\n"); sys.stdout.flush()
for seq in range(20000):
    kv(s, b"kv.events", seq, b"payload-%d" % seq)
    if select.select([sys.stdin], [], [], 0.001)[0]:
        break
else:
    sys.exit("the subscriber never asked to stop")
s.close(linger=0)
ctx.term()
"#;
    let mut child = spawn_python_kv(script, &[]);
    let endpoint = read_endpoint(&mut child).await;

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("kv.").await.unwrap();
    sub.connect(endpoint).await.unwrap();

    let (_, first, _) = kv_parts(&recv(&sub, "first streamed event").await);
    for expected in first + 1..first + 200 {
        let (topic, seq, payload) = kv_parts(&recv(&sub, "streamed event").await);
        assert_eq!(&topic[..], b"kv.events");
        assert_eq!(seq, expected, "events arrive in order without gaps");
        assert_eq!(payload, format!("payload-{seq}").as_bytes());
    }

    {
        use std::io::Write;

        child.stdin.take().unwrap().write_all(b"stop\n").unwrap();
    }

    wait_success(child, "pyzmq PUB").await;
}

/// A libzmq SUB's subscription reaches a Rust PUB, which filters by it and
/// sends multipart events whole.
#[tokio::test]
async fn null_tcp_rust_pub_to_pyzmq_sub_filters_multipart_events() {
    if skip_if_no_pyzmq() {
        return;
    }

    let publisher = Socket::new(SocketType::Pub, Options::default());
    let endpoint = publisher.bind(test_support::tcp_loopback(0)).await.unwrap();

    let script = r#"
ctx = zmq.Context.instance()
s = ctx.socket(zmq.SUB)
s.setsockopt(zmq.RCVTIMEO, 10000)
s.subscribe(b"kv.")
s.connect(os.environ["ENDPOINT"])
for _ in range(3):
    topic, seq, payload = s.recv_multipart()
    assert topic != b"kv.b" or payload == b"x" * len(payload)
    sys.stdout.write(f"{topic.decode()} {int.from_bytes(seq, 'big')} {len(payload)}\n")
    sys.stdout.flush()
s.close(linger=0)
ctx.term()
"#;
    let child = spawn_python_kv(script, &[("ENDPOINT", endpoint.to_string())]);

    publisher
        .wait_subscribed(1, Duration::from_secs(10))
        .await
        .expect("pyzmq SUB subscribed");

    for event in [
        kv_event("other", 0, b"dropped"),
        kv_event("kv.a", 1, b"small"),
        kv_event("kv", 2, b"no dot"),
        kv_event("kv.b", 3, &vec![b'x'; 1 << 20]),
        kv_event("kv.end", 4, b""),
    ] {
        publisher.send(event).await.unwrap();
    }

    let output = wait_success(child, "pyzmq SUB").await;
    assert_eq!(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["kv.a 1 5", "kv.b 3 1048576", "kv.end 4 0"]
    );
}

/// A frame over `max_message_size` from libzmq drops the connection
/// instead of being allocated. The SUB reconnects and subscribes again,
/// and later events arrive.
#[tokio::test]
async fn null_tcp_rust_sub_drops_oversized_pyzmq_event_and_resubscribes() {
    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
ctx = zmq.Context.instance()
s = ctx.socket(zmq.XPUB)
s.setsockopt(zmq.XPUB_VERBOSE, 1)
s.setsockopt(zmq.RCVTIMEO, 10000)
sys.stdout.write(bind_random(s) + "\n"); sys.stdout.flush()
wait_subscribe(s, b"kv.")
kv(s, b"kv.small", 0, b"a")
kv(s, b"kv.big", 1, b"x" * 4096)
# The subscriber dropped the connection and came back.
wait_subscribe(s, b"kv.")
kv(s, b"kv.after", 2, b"b")
s.close(linger=5000)
ctx.term()
"#;
    let mut child = spawn_python_kv(script, &[]);
    let endpoint = read_endpoint(&mut child).await;

    let sub = Socket::new(SocketType::Sub, Options::default().max_message_size(1024));
    sub.subscribe("kv.").await.unwrap();
    sub.connect(endpoint).await.unwrap();

    let (topic, seq, _) = kv_parts(&recv(&sub, "event before the oversized one").await);
    assert_eq!((&topic[..], seq), (&b"kv.small"[..], 0));

    let after = tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("event after reconnecting")
        .unwrap();
    let (topic, seq, payload) = kv_parts(&after);
    assert_eq!(
        (&topic[..], seq, &payload[..]),
        (&b"kv.after"[..], 2, &b"b"[..])
    );

    wait_success(child, "pyzmq XPUB").await;
}

/// `connect` returns while the publisher is down. The SUB connects once a
/// libzmq publisher binds, and again after it restarts on the same port.
#[tokio::test]
async fn null_tcp_rust_sub_connects_before_pyzmq_pub_binds_and_follows_restart() {
    if skip_if_no_pyzmq() {
        return;
    }

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("kv.").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        sub.connect(test_support::tcp_loopback(port)),
    )
    .await
    .expect("connect returns while nothing listens")
    .unwrap();

    let script = r#"
ctx = zmq.Context.instance()
s = ctx.socket(zmq.XPUB)
s.setsockopt(zmq.RCVTIMEO, 10000)
s.bind(f"tcp://127.0.0.1:{os.environ['PORT']}")
wait_subscribe(s, b"kv.")
kv(s, os.environ["TOPIC"].encode(), int(os.environ["SEQ"]), b"")
s.close(linger=5000)
ctx.term()
"#;

    for (seq, topic) in [(0_u64, "kv.first"), (1, "kv.restarted")] {
        let child = spawn_python_kv(
            script,
            &[
                ("PORT", port.to_string()),
                ("TOPIC", topic.to_string()),
                ("SEQ", seq.to_string()),
            ],
        );

        let event = tokio::time::timeout(Duration::from_secs(15), sub.recv())
            .await
            .unwrap_or_else(|_| panic!("{topic} not received"))
            .unwrap();
        let (received, received_seq, _) = kv_parts(&event);
        assert_eq!((&received[..], received_seq), (topic.as_bytes(), seq));

        wait_success(child, topic).await;
    }
}

/// `recv` futures dropped by `select!` while events arrive consume
/// nothing: every event from libzmq arrives once, in order.
#[tokio::test]
async fn null_tcp_rust_sub_canceled_recv_loses_no_pyzmq_event() {
    const EVENTS: u64 = 3000;

    if skip_if_no_pyzmq() {
        return;
    }

    let script = r#"
import time
ctx = zmq.Context.instance()
s = ctx.socket(zmq.XPUB)
s.setsockopt(zmq.SNDHWM, 0)
s.setsockopt(zmq.RCVTIMEO, 10000)
sys.stdout.write(bind_random(s) + "\n"); sys.stdout.flush()
wait_subscribe(s, b"kv.")
for seq in range(int(os.environ["EVENTS"])):
    kv(s, b"kv.c", seq, b"")
    if seq % 16 == 0:
        time.sleep(0.001)
s.close(linger=5000)
ctx.term()
"#;
    let mut child = spawn_python_kv(script, &[("EVENTS", EVENTS.to_string())]);
    let endpoint = read_endpoint(&mut child).await;

    let sub = Socket::new(SocketType::Sub, Options::default());
    sub.subscribe("kv.").await.unwrap();
    sub.connect(endpoint).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut expected = 0;
    let mut canceled = 0_u32;

    while expected < EVENTS {
        assert!(
            tokio::time::Instant::now() < deadline,
            "received {expected} of {EVENTS} events"
        );

        tokio::select! {
            message = sub.recv() => {
                let (_, seq, _) = kv_parts(&message.unwrap());
                assert_eq!(seq, expected, "no event lost or repeated");
                expected += 1;
            }
            () = tokio::time::sleep(Duration::from_micros(200)) => canceled += 1,
        }
    }

    assert!(canceled > 0, "no recv was canceled");
    wait_success(child, "pyzmq XPUB").await;
}
