//! `ZGuide` 11 — Freelance Model 3: tracked failover.
//!
//! Maintains a `known_good` endpoint index. For each request, try
//! `known_good` first (if set), then remaining endpoints. On success,
//! remember the endpoint. On timeout, clear it.
//!
//! The `--kill-after N` flag tells run.sh which request to kill a
//! server after; the client itself just sends 6 requests.
//!
//!     cargo run -p zguide-11-freelance --bin client_tracked [ep1] [ep2] ...

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
        ]
    };

    let mut known_good: Option<usize> = None;

    for i in 0..6 {
        let body = format!("request-{i}");

        // Build try order: known_good first, then the rest.
        let try_order: Vec<usize> = if let Some(kg) = known_good {
            let mut order = vec![kg];
            for idx in 0..endpoints.len() {
                if idx != kg {
                    order.push(idx);
                }
            }
            order
        } else {
            (0..endpoints.len()).collect()
        };

        let mut served = false;
        for &idx in &try_order {
            let ep = &endpoints[idx];
            let req = ctx.socket(SocketType::Req, Options::default());
            req.connect(ep.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;

            req.send(Message::single(body.clone())).await.unwrap();

            match tokio::time::timeout(Duration::from_millis(200), req.recv()).await {
                Ok(Ok(reply)) => {
                    let reply_str = msg_str(&reply, 0);
                    println!("client: {body} -> {reply_str} (via {ep})");
                    known_good = Some(idx);
                    served = true;
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("client: recv error on {ep}: {e}");
                }
                Err(_) => {
                    println!("client: {ep} timed out, rotating");
                    if known_good == Some(idx) {
                        known_good = None;
                    }
                }
            }
        }

        if !served {
            eprintln!("client: all endpoints failed for {body}");
        }

        // Pace requests so run.sh can kill a server mid-run.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!("client: done (6 requests)");
}
