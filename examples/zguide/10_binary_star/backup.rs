//! `ZGuide` 10 — Binary Star: backup server.
//!
//! SUB connects to the primary's heartbeat endpoint. Phase 1: monitor
//! heartbeats with a 300ms timeout. On timeout (primary is dead),
//! transition to phase 2: serve REQ clients with "backup:{body}" replies.
//!
//!     cargo run -p zguide-10-binary-star --bin backup [heartbeat_ep] [service_ep]

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
    let heartbeat_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-10-heartbeat");
    let service_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-10-backup");

    let sub = ctx.socket(SocketType::Sub, Options::default());
    sub.connect(heartbeat_ep).await.unwrap();
    sub.subscribe("HB").await.unwrap();

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(service_ep).await.unwrap();

    // Phase 1: monitor heartbeats.
    let timeout = Duration::from_millis(300);
    loop {
        match tokio::time::timeout(timeout, sub.recv()).await {
            Ok(Ok(_)) => {} // heartbeat received, primary is alive
            Ok(Err(e)) => {
                eprintln!("backup: recv error: {e}");
                return;
            }
            Err(_) => {
                println!("backup: primary heartbeat lost -- taking over!");
                break;
            }
        }
    }

    // Phase 2: serve requests.
    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        println!("backup: served {body}");
        rep.send(Message::single(format!("backup:{body}")))
            .await
            .unwrap();
    }
}
