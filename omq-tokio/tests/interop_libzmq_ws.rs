//! Interop: omq-tokio <-> libzmq 4.3.5 (draft) over ws://.
//! Requires the `zmq_ws_peer` helper binary built against libzmq
//! with `ENABLE_DRAFTS=ON`.

#![cfg(feature = "ws")]

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;
use std::process::Command;
use std::time::Duration;

const HELPER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/helpers/zmq_ws_peer");

/// The helper is optional locally and mandatory in CI. With
/// `OMQ_INTEROP_REQUIRED=1` a missing helper fails the test instead of
/// letting the suite pass without exercising any interop.
fn skip_if_no_helper() -> bool {
    if !std::path::Path::new(HELPER).exists() {
        assert!(
            std::env::var_os("OMQ_INTEROP_REQUIRED").is_none(),
            "OMQ_INTEROP_REQUIRED=1 but zmq_ws_peer helper not found at {HELPER}",
        );
        eprintln!("SKIP: zmq_ws_peer helper not found at {HELPER}");
        return true;
    }
    false
}

fn ws_ep(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        _ => panic!("expected ws"),
    }
}

/// omq PULL (bind) <- libzmq PUSH (connect)
#[tokio::test]
async fn libzmq_push_to_omq_pull() {
    if skip_if_no_helper() {
        return;
    }
    let pull = Socket::new(SocketType::Pull, Options::default());
    let bound = pull.bind(ws_ep(0)).await.unwrap();
    let port = get_port(&bound);

    let count = 10;
    let size = 64;
    let mut child = Command::new(HELPER)
        .args([
            "push",
            &format!("ws://127.0.0.1:{port}"),
            &count.to_string(),
            &size.to_string(),
        ])
        .spawn()
        .expect("spawn zmq_ws_peer");

    for i in 0..count {
        let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out at {i}/{count}"))
            .unwrap();
        let data = msg.part_bytes(0).unwrap();
        let expected = format!("msg-{i}");
        assert!(
            data.starts_with(expected.as_bytes()),
            "msg {i}: expected prefix {expected:?}, got {:?}",
            String::from_utf8_lossy(&data[..expected.len().min(data.len())])
        );
    }
    let status = child.wait().unwrap();
    assert!(status.success(), "zmq_ws_peer exited with {status}");
}

/// omq PUSH (connect) -> libzmq PULL (bind)
#[tokio::test]
async fn omq_push_to_libzmq_pull() {
    if skip_if_no_helper() {
        return;
    }
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let count = 10;
    let size = 64;
    let child = Command::new(HELPER)
        .args([
            "pull",
            &format!("ws://127.0.0.1:{port}"),
            &count.to_string(),
            &size.to_string(),
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn zmq_ws_peer");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(ws_ep(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..count {
        let mut buf = vec![0u8; size];
        let s = format!("msg-{i}");
        buf[..s.len()].copy_from_slice(s.as_bytes());
        push.send(Message::from(Bytes::from(buf))).await.unwrap();
    }

    let output =
        tokio::task::spawn_blocking(move || child.wait_with_output().expect("wait zmq_ws_peer"))
            .await
            .unwrap();
    assert!(
        output.status.success(),
        "zmq_ws_peer: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("OK {count}")),
        "expected OK {count}, got: {stdout}"
    );
}
