//! `ZGuide` 10 — Binary Star: primary server.
//!
//! REP binds a service endpoint, PUB binds a heartbeat endpoint.
//! Two tasks: one sends "HB" heartbeats every 50ms, the other
//! serves REQ clients with "primary:{body}" replies.
//!
//!     cargo run -p zguide-10-binary-star --bin primary [service_ep] [heartbeat_ep]

use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let service_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-10-primary");
    let heartbeat_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-10-heartbeat");

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(service_ep).await.unwrap();

    let pub_socket = ctx.socket(SocketType::Pub, Options::default());
    pub_socket.bind(heartbeat_ep).await.unwrap();

    // Heartbeat task: send "HB" every 50ms.
    tokio::spawn(async move {
        loop {
            pub_socket.send(Message::single("HB")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Server task: recv on REP, reply with "primary:{body}".
    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        println!("primary: served {body}");
        rep.send(Message::single(format!("primary:{body}")))
            .await
            .unwrap();
    }
}
