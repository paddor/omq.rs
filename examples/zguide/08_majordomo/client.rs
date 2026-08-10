//! `ZGuide` 08 — Majordomo client.
//!
//! REQ socket connects to the broker frontend. Sends requests for
//! different services and prints replies.
//!
//!     cargo run -p zguide-08-majordomo --bin client [frontend_ep]

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
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-08-frontend");

    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(frontend_ep).await.unwrap();

    let requests = [
        ("echo", "hello"),
        ("echo", "world"),
        ("upper", "foo"),
        ("echo", "test"),
        ("upper", "bar"),
        ("upper", "baz"),
    ];

    for (service, body) in requests {
        req.send(Message::multipart([service, body])).await.unwrap();
        let reply = req.recv().await.unwrap();
        let reply_body = msg_str(&reply, 0);
        println!("client: {service}({body}) -> {reply_body}");
    }

    println!("done: {} requests", requests.len());
}
