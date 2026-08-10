//! `ZGuide` 06 — Last Value Cache (publisher).
//!
//! PUSH-connects to the cache and sends weather updates.
//!
//!     cargo run -p zguide-06-last-value-cache --bin publisher \
//!         [pub_ep] [count]

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
    let pub_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-06-publisher");
    let count: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    let push = ctx.socket(SocketType::Push, Options::default());
    push.connect(pub_ep.clone()).await.unwrap();

    println!("publisher: connected to {pub_ep}, sending {count} rounds");

    // Let the connection establish.
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..count {
        let nyc = format!("weather.nyc {}F", 70 + i);
        let sfo = format!("weather.sfo {}F", 60 + i);

        push.send(Message::single(nyc.clone())).await.unwrap();
        push.send(Message::single(sfo.clone())).await.unwrap();
        println!("publisher: {nyc}, {sfo}");

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("publisher: done ({count} rounds)");
}
