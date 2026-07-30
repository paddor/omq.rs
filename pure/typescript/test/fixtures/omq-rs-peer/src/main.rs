use std::io::{self, Write};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{Endpoint, MechanismPeerInfo, Message, MonitorEvent, Options, Socket, SocketType};

fn accept_alice(peer: &MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

fn options(auth: &str) -> Options {
    match auth {
        "plain" => Options::default().plain_server(accept_alice),
        "null" => Options::default(),
        other => panic!("unknown auth mode: {other}"),
    }
}

async fn wait_handshake(sock: &Socket) {
    let mut monitor = sock.monitor();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match monitor.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    })
    .await
    .expect("handshake timed out");
}

fn print_endpoint(endpoint: &Endpoint) {
    println!("ENDPOINT {endpoint}");
    io::stdout().flush().unwrap();
}

async fn pull_bind(endpoint: Endpoint, expected: String, auth: &str) {
    let pull = Socket::new(SocketType::Pull, options(auth));
    let bound = pull.bind(endpoint).await.expect("pull bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("pull recv timed out")
        .expect("pull recv");
    assert_eq!(msg.part_bytes(0).unwrap(), expected.as_bytes());
}

async fn push_bind(endpoint: Endpoint, payload: String, auth: &str) {
    let push = Socket::new(SocketType::Push, options(auth));
    let bound = push.bind(endpoint).await.expect("push bind");
    print_endpoint(&bound);

    wait_handshake(&push).await;
    push.send(Message::from(Bytes::from(payload)))
        .await
        .expect("push send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("mode");
    let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
    let payload = args.next().expect("payload");
    let auth = args.next().unwrap_or_else(|| "null".to_string());

    match mode.as_str() {
        "pull-bind" => pull_bind(endpoint, payload, &auth).await,
        "push-bind" => push_bind(endpoint, payload, &auth).await,
        other => panic!("unknown mode: {other}"),
    }
}
