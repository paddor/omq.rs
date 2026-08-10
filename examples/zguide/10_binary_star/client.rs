//! `ZGuide` 10 — Binary Star: client.
//!
//! Sends N requests. For each: create REQ, connect to primary, send,
//! recv with 200ms timeout. On timeout: drop socket, create new REQ,
//! connect to backup, send, recv with 1s timeout.
//!
//!     cargo run -p zguide-10-binary-star --bin client [primary_ep] [backup_ep] [n]

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
    let primary_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-10-primary");
    let backup_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-10-backup");
    let n: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);

    for i in 0..n {
        let body = format!("req-{i}");

        // Try primary first.
        let req = ctx.socket(SocketType::Req, Options::default());
        req.connect(primary_ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        req.send(Message::single(body.clone())).await.unwrap();

        match tokio::time::timeout(Duration::from_millis(200), req.recv()).await {
            Ok(Ok(reply)) => {
                println!("client: {body} -> {}", msg_str(&reply, 0));
                continue;
            }
            Ok(Err(e)) => {
                eprintln!("client: recv error from primary: {e}");
            }
            Err(_) => {
                println!("client: primary timeout for {body}, trying backup");
            }
        }
        drop(req);

        // Fallback to backup.
        let req = ctx.socket(SocketType::Req, Options::default());
        req.connect(backup_ep.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        req.send(Message::single(body.clone())).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(1), req.recv()).await {
            Ok(Ok(reply)) => {
                println!("client: {body} -> {}", msg_str(&reply, 0));
            }
            Ok(Err(e)) => {
                eprintln!("client: recv error from backup: {e}");
            }
            Err(_) => {
                eprintln!("client: backup also timed out for {body}");
            }
        }
    }

    println!("client: done ({n} requests)");
}
