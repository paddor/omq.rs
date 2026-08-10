//! `ZGuide` 11 — Freelance Model 1: sequential failover.
//!
//! For each request, try endpoints in order. Create a fresh REQ per
//! attempt, send, recv with 150ms timeout. On timeout: drop socket,
//! try the next endpoint.
//!
//!     cargo run -p zguide-11-freelance --bin client_sequential [ep1] [ep2] ...

use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

fn msg_str(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap()).to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let endpoints: Vec<Endpoint> = if args.len() > 1 {
        args[1..]
            .iter()
            .map(|s| s.parse().expect("invalid endpoint"))
            .collect()
    } else {
        vec![
            "ipc://@omq-zguide-11-server1".parse().unwrap(),
            "ipc://@omq-zguide-11-server2".parse().unwrap(),
            "ipc://@omq-zguide-11-server3".parse().unwrap(),
        ]
    };

    for i in 0..3 {
        let body = format!("request-{i}");
        let mut served = false;

        for ep in &endpoints {
            let req = ctx.socket(SocketType::Req, Options::default());
            req.connect(ep.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;

            req.send(Message::single(body.clone())).await.unwrap();

            match tokio::time::timeout(Duration::from_millis(150), req.recv()).await {
                Ok(Ok(reply)) => {
                    println!("client: {body} -> {}", msg_str(&reply, 0));
                    served = true;
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("client: recv error on {ep}: {e}");
                }
                Err(_) => {
                    println!("client: timeout on {ep}, trying next");
                }
            }
        }

        if !served {
            eprintln!("client: all endpoints failed for {body}");
        }
    }

    println!("client: done (3 requests)");
}
