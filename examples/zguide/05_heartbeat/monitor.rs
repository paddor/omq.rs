//! `ZGuide` 05 — Heartbeat monitor (SUB).
//!
//! Subscribes to heartbeats and detects alive/dead/recovered state
//! transitions based on a 150ms timeout (3x the 50ms heartbeat interval).
//!
//!     cargo run -p zguide-05-heartbeat --bin monitor [endpoint]

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
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-05-heartbeat");

    let sub = ctx.socket(SocketType::Sub, Options::default());
    sub.connect(ep).await.unwrap();
    sub.subscribe("HEARTBEAT").await.unwrap();

    let timeout = Duration::from_millis(150);
    let mut alive = false;
    let mut events: Vec<String> = Vec::new();

    for _ in 0..20 {
        match tokio::time::timeout(timeout, sub.recv()).await {
            Ok(Ok(msg)) => {
                let body = msg_str(&msg, 0);
                if !alive {
                    events.push("alive".to_string());
                    alive = true;
                    println!("monitor: ALIVE ({body})");
                }
            }
            Ok(Err(e)) => {
                eprintln!("monitor: recv error: {e}");
                break;
            }
            Err(_) => {
                if alive {
                    events.push("dead".to_string());
                    alive = false;
                    println!("monitor: DEAD (timeout)");
                }
            }
        }
    }

    println!("monitor: events: {events:?}");
}
