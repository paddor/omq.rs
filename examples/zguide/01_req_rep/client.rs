//! `ZGuide` 01 — REQ client.
//!
//! Connects to the broker's ROUTER frontend. Sends requests and prints
//! replies.
//!
//!     cargo run -p zguide-01-req-rep --bin client [frontend] [n_requests]

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
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-frontend");
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(9);

    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(frontend_ep).await.unwrap();

    for i in 0..n {
        let request = format!("request-{i}");
        req.send(Message::single(request.clone())).await.unwrap();
        let reply = req.recv().await.unwrap();
        let body = msg_str(&reply, 0);
        println!("client: {request} -> {body}");
    }

    println!("done: {n} replies");
}
