//! `ZGuide` 05 — Heartbeat publisher (PUB).
//!
//! Sends periodic heartbeats with a simulated failure gap in the middle.
//!
//! - Phase 1: 8 heartbeats, 50ms apart
//! - Phase 2: 300ms pause (simulate failure)
//! - Phase 3: 8 heartbeats, 50ms apart
//!
//!     cargo run -p zguide-05-heartbeat --bin publisher [endpoint]

use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-05-heartbeat");

    let pub_socket = ctx.socket(SocketType::Pub, Options::default());
    pub_socket.bind(ep).await.unwrap();

    // Brief pause for subscribers to connect.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 1: alive
    for i in 0..8 {
        pub_socket.send(Message::single("HEARTBEAT")).await.unwrap();
        println!("publisher: heartbeat {i}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: simulate failure
    println!("publisher: simulating failure (300ms pause)");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Phase 3: recover
    for i in 8..16 {
        pub_socket.send(Message::single("HEARTBEAT")).await.unwrap();
        println!("publisher: heartbeat {i}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("publisher: done");
}
