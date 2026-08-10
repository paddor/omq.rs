//! `ZGuide` 11 — Freelance: server.
//!
//! REP server that replies with "{name}:{body}". Optional delay before
//! each reply to simulate a slow server.
//!
//!     cargo run -p zguide-11-freelance --bin server [endpoint] [name] [delay_secs]

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
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-11-server1");
    let name = args.get(2).cloned().unwrap_or_else(|| "server".to_string());
    let delay_secs: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        if delay_secs > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;
        }
        println!("{name}: served {body}");
        rep.send(Message::single(format!("{name}:{body}")))
            .await
            .unwrap();
    }
}
