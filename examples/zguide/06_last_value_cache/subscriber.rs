//! `ZGuide` 06 — Last Value Cache (late-joining subscriber).
//!
//! Connects to the cache's REP socket to fetch a snapshot, then
//! optionally subscribes for live updates.
//!
//!     cargo run -p zguide-06-last-value-cache --bin subscriber \
//!         [snapshot_ep] [sub_ep]

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
    let snapshot_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-06-snapshot");
    let sub_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-06-subscriber");

    // Request snapshot from cache.
    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(snapshot_ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    req.send(Message::single("SNAPSHOT")).await.unwrap();
    let reply = req.recv().await.unwrap();
    let body = msg_str(&reply, 0);

    println!("subscriber: snapshot from cache:");
    if body.is_empty() {
        println!("  (empty)");
    } else {
        for line in body.lines() {
            println!("  {line}");
        }
    }

    // Subscribe for live updates.
    let sub = ctx.socket(SocketType::Sub, Options::default());
    sub.connect(sub_ep.clone()).await.unwrap();
    sub.subscribe("").await.unwrap();

    println!("subscriber: listening for live updates (2s) ...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        tokio::select! {
            msg = sub.recv() => {
                let body = msg_str(&msg.unwrap(), 0);
                println!("  live: {body}");
            }
            () = tokio::time::sleep_until(deadline) => {
                break;
            }
        }
    }

    println!("subscriber: done");
}
