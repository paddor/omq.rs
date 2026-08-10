//! `ZGuide` 09 — Titanic client.
//!
//! Submits 3 requests to the frontend, collects ticket IDs, then polls
//! for results.
//!
//!     cargo run -p zguide-09-titanic --bin client [frontend_ep]

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
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-09-frontend");

    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(frontend_ep).await.unwrap();

    let requests = [("echo", "hello"), ("upper", "world"), ("echo", "foo")];

    // Submit requests, collect tickets.
    let mut tickets = Vec::new();
    for (service, body) in &requests {
        req.send(Message::single(format!("SUBMIT|{service}|{body}")))
            .await
            .unwrap();
        let reply = req.recv().await.unwrap();
        let reply = msg_str(&reply, 0);
        let (status, ticket) = reply.split_once('|').expect("bad reply");
        assert_eq!(status, "TICKET");
        println!("client: submitted {service}({body}) -> ticket {ticket}");
        tickets.push(ticket.to_owned());
    }

    // Give the dispatcher time to process.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Poll for results.
    for ticket in &tickets {
        req.send(Message::single(format!("RESULT|{ticket}")))
            .await
            .unwrap();
        let reply = req.recv().await.unwrap();
        let reply = msg_str(&reply, 0);
        let (status, result) = reply.split_once('|').expect("bad reply");
        assert_eq!(status, "OK", "expected result for {ticket}");
        println!("client: result for {ticket} -> {result}");
    }

    println!(
        "done: {} requests persisted, dispatched, and retrieved",
        tickets.len()
    );
}
