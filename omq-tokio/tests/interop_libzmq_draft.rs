//! Interop: omq-tokio <-> libzmq draft socket types over TCP.
//! Requires `zmq_draft_peer` built against libzmq with
//! `ENABLE_DRAFTS=ON`.

mod test_support;

use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Message, MonitorEvent, Options, Socket, SocketType};

const HELPER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/helpers/zmq_draft_peer");

fn skip_if_no_helper() -> bool {
    if !std::path::Path::new(HELPER).exists() {
        assert!(
            std::env::var_os("OMQ_INTEROP_REQUIRED").is_none(),
            "OMQ_INTEROP_REQUIRED=1 but zmq_draft_peer helper not found at {HELPER}",
        );
        eprintln!("skip: zmq_draft_peer helper not found at {HELPER}");
        return true;
    }
    false
}

fn spawn_helper(args: &[&str]) -> Child {
    Command::new(HELPER)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn zmq_draft_peer")
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

async fn recv(sock: &Socket, context: &str) -> Message {
    tokio::time::timeout(Duration::from_secs(5), sock.recv())
        .await
        .unwrap_or_else(|_| panic!("{context}: recv timed out"))
        .unwrap()
}

async fn wait_for_join(mon: &mut omq_tokio::MonitorStream) {
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::JoinReceived { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before JOIN: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("JOIN did not arrive");
}

#[tokio::test]
async fn libzmq_radio_to_omq_dish() {
    if skip_if_no_helper() {
        return;
    }

    let dish = Socket::new(SocketType::Dish, Options::default());
    dish.join("weather").await.unwrap();
    let endpoint = dish.bind(test_support::tcp_loopback(0)).await.unwrap();

    let child = spawn_helper(&[
        "radio-connect-send",
        &endpoint.to_string(),
        "weather",
        "sunny",
    ]);

    let msg = recv(&dish, "libzmq RADIO -> omq DISH").await;
    assert_eq!(msg, Message::multipart(["weather", "sunny"]));
    wait_success(child, "libzmq RADIO").await;
}

#[tokio::test]
async fn omq_radio_to_libzmq_dish() {
    if skip_if_no_helper() {
        return;
    }

    let radio = Socket::new(SocketType::Radio, Options::default());
    let mut mon = radio.monitor();
    let endpoint = radio.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "dish-connect-recv",
        &endpoint.to_string(),
        "weather",
        "rain",
    ]);

    wait_for_join(&mut mon).await;
    radio
        .send(Message::multipart(["weather", "rain"]))
        .await
        .unwrap();
    wait_success(child, "libzmq DISH").await;
}

#[tokio::test]
async fn libzmq_scatter_to_omq_gather() {
    if skip_if_no_helper() {
        return;
    }

    let gather = Socket::new(SocketType::Gather, Options::default());
    let endpoint = gather.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "scatter-connect-send",
        &endpoint.to_string(),
        "from-scatter",
    ]);

    let msg = recv(&gather, "libzmq SCATTER -> omq GATHER").await;
    assert_eq!(msg, Message::single("from-scatter"));
    wait_success(child, "libzmq SCATTER").await;
}

#[tokio::test]
async fn omq_scatter_to_libzmq_gather() {
    if skip_if_no_helper() {
        return;
    }

    let scatter = Socket::new(SocketType::Scatter, Options::default());
    let endpoint = scatter.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&["gather-connect-recv", &endpoint.to_string(), "from-scatter"]);

    test_support::wait_for_handshake(&scatter).await;
    scatter.send(Message::single("from-scatter")).await.unwrap();
    wait_success(child, "libzmq GATHER").await;
}

#[tokio::test]
async fn libzmq_client_to_omq_server() {
    if skip_if_no_helper() {
        return;
    }

    let server = Socket::new(SocketType::Server, Options::default());
    let endpoint = server.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "client-connect-request",
        &endpoint.to_string(),
        "from-client",
        "from-server",
    ]);

    let request = recv(&server, "libzmq CLIENT -> omq SERVER").await;
    assert_eq!(request, Message::single("from-client"));
    let routing_id = request.routing_id().expect("SERVER routing id");
    server
        .send(Message::single("from-server").with_routing_id(routing_id))
        .await
        .unwrap();
    wait_success(child, "libzmq CLIENT").await;
}

#[tokio::test]
async fn omq_client_to_libzmq_server() {
    if skip_if_no_helper() {
        return;
    }

    let client = Socket::new(SocketType::Client, Options::default());
    let endpoint = client.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "server-connect-reply",
        &endpoint.to_string(),
        "from-client",
        "from-server",
    ]);

    test_support::wait_for_handshake(&client).await;
    client.send(Message::single("from-client")).await.unwrap();
    let reply = recv(&client, "omq CLIENT -> libzmq SERVER").await;
    assert_eq!(reply, Message::single("from-server"));
    wait_success(child, "libzmq SERVER").await;
}

#[tokio::test]
async fn libzmq_channel_to_omq_channel() {
    if skip_if_no_helper() {
        return;
    }

    let channel = Socket::new(SocketType::Channel, Options::default());
    let endpoint = channel.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "channel-connect-request",
        &endpoint.to_string(),
        "from-channel",
        "from-omq",
    ]);

    let request = recv(&channel, "libzmq CHANNEL -> omq CHANNEL").await;
    assert_eq!(request, Message::single("from-channel"));
    channel.send(Message::single("from-omq")).await.unwrap();
    wait_success(child, "libzmq CHANNEL").await;
}

#[tokio::test]
async fn libzmq_peer_to_omq_peer() {
    if skip_if_no_helper() {
        return;
    }

    let peer = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"omq-peer")),
    );
    let endpoint = peer.bind(test_support::tcp_loopback(0)).await.unwrap();
    let child = spawn_helper(&[
        "peer-connect-request",
        &endpoint.to_string(),
        "from-peer",
        "from-omq",
    ]);

    let request = recv(&peer, "libzmq PEER -> omq PEER").await;
    assert_eq!(request.part_bytes(1).unwrap(), &b"from-peer"[..]);
    let routing_id = request.part_bytes(0).unwrap();
    peer.send(Message::multipart([
        routing_id,
        Bytes::from_static(b"from-omq"),
    ]))
    .await
    .unwrap();
    wait_success(child, "libzmq PEER").await;
}
