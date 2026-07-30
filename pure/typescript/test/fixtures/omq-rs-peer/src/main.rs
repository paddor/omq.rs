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

async fn pull_restart_bind(endpoint: Endpoint, before: String, after: String, auth: &str) {
    let pull1 = Socket::new(SocketType::Pull, options(auth));
    let bound = pull1.bind(endpoint).await.expect("pull1 bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), pull1.recv())
        .await
        .expect("pull1 recv timed out")
        .expect("pull1 recv");
    assert_eq!(msg.part_bytes(0).unwrap(), before.as_bytes());

    pull1.close().await.expect("pull1 close");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull2 = Socket::new(SocketType::Pull, options(auth));
    let mut bound_again = false;
    for _ in 0..40 {
        if pull2.bind(bound.clone()).await.is_ok() {
            bound_again = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound_again, "pull2 failed to bind after pull1 closed");

    let msg = tokio::time::timeout(Duration::from_secs(10), pull2.recv())
        .await
        .expect("pull2 recv timed out")
        .expect("pull2 recv");
    assert_eq!(msg.part_bytes(0).unwrap(), after.as_bytes());
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

async fn rep_bind(endpoint: Endpoint, expected: String, reply: String, auth: &str) {
    let rep = Socket::new(SocketType::Rep, options(auth));
    let bound = rep.bind(endpoint).await.expect("rep bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), rep.recv())
        .await
        .expect("rep recv timed out")
        .expect("rep recv");
    assert_eq!(msg.part_bytes(0).unwrap(), expected.as_bytes());
    rep.send(Message::from(Bytes::from(reply)))
        .await
        .expect("rep send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn pub_bind(endpoint: Endpoint, topic: String, payload: String, auth: &str) {
    let pub_ = Socket::new(SocketType::Pub, options(auth));
    let bound = pub_.bind(endpoint).await.expect("pub bind");
    print_endpoint(&bound);

    wait_handshake(&pub_).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    pub_.send(Message::multipart([topic, payload]))
        .await
        .expect("pub send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("mode");
    let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();

    match mode.as_str() {
        "pull-bind" => {
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pull_bind(endpoint, payload, &auth).await;
        }
        "push-bind" => {
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            push_bind(endpoint, payload, &auth).await;
        }
        "pull-restart-bind" => {
            let before = args.next().expect("before");
            let after = args.next().expect("after");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pull_restart_bind(endpoint, before, after, &auth).await;
        }
        "rep-bind" => {
            let payload = args.next().expect("payload");
            let reply = args.next().expect("reply");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            rep_bind(endpoint, payload, reply, &auth).await;
        }
        "pub-bind" => {
            let topic = args.next().expect("topic");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pub_bind(endpoint, topic, payload, &auth).await;
        }
        other => panic!("unknown mode: {other}"),
    }
}
