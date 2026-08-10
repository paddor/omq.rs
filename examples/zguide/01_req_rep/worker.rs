//! `ZGuide` 01 — REP worker.
//!
//! Connects to the broker's DEALER backend. Receives requests and
//! replies with an echo prefixed by the worker ID.
//!
//!     cargo run -p zguide-01-req-rep --bin worker [backend] [id]

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
    let backend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-backend");
    let id = args.get(2).map_or("0", |s| s.as_str());

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.connect(backend_ep).await.unwrap();

    println!("worker-{id}: ready");

    loop {
        let msg = rep.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        let reply = format!("worker-{id}:{body}");
        println!("worker-{id}: {body} -> {reply}");
        rep.send(Message::single(reply)).await.unwrap();
    }
}
