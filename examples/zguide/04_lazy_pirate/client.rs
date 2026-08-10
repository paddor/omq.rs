//! `ZGuide` 04 — Lazy Pirate client (REQ).
//!
//! Sends 5 requests over a single REQ socket. If a reply does not
//! arrive within 400ms, the socket is dropped and recreated before
//! retrying (max 3 retries per request). This is the core Lazy Pirate
//! technique: destroy and rebuild the socket on timeout.
//!
//!     cargo run -p zguide-04-lazy-pirate --bin client [endpoint]

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

async fn new_req(ctx: &Context, ep: &Endpoint) -> omq_tokio::Socket {
    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(ep.clone()).await.unwrap();
    // Brief pause so the connection can establish.
    tokio::time::sleep(Duration::from_millis(20)).await;
    req
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-04-server");

    let max_retries = 3;
    let mut total_retries: u32 = 0;
    let mut replies: Vec<String> = Vec::new();

    let mut req = new_req(&ctx, &ep).await;

    for seq in 0..5 {
        let request = format!("request-{seq}");
        let mut attempts = 0;

        loop {
            req.send(Message::single(request.clone())).await.unwrap();

            match tokio::time::timeout(Duration::from_millis(400), req.recv()).await {
                Ok(Ok(reply)) => {
                    let body = msg_str(&reply, 0);
                    println!("client: {request} -> {body}");
                    replies.push(body);
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("client: recv error on {request}: {e}");
                    break;
                }
                Err(_) => {
                    attempts += 1;
                    total_retries += 1;
                    println!("client: timeout on {request}, retry {attempts}");
                    // Destroy and recreate the socket (Lazy Pirate pattern).
                    let _ = req.close().await;
                    req = new_req(&ctx, &ep).await;
                    if attempts >= max_retries {
                        println!("client: giving up on {request}");
                        break;
                    }
                }
            }
        }
    }

    println!("done: {} replies, {total_retries} retries", replies.len());
}
