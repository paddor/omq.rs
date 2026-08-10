//! `ZGuide` 04 — Lazy Pirate server (REP).
//!
//! Binds a REP socket, receives requests and replies. On request #3,
//! sleeps 500ms before replying to simulate a crash/slowdown. Exits
//! when no request arrives within 3 seconds.
//!
//!     cargo run -p zguide-04-lazy-pirate --bin server [endpoint]

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
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-04-server");

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(ep).await.unwrap();

    let mut handled: u32 = 0;
    loop {
        let msg = match tokio::time::timeout(Duration::from_secs(3), rep.recv()).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => {
                eprintln!("server: recv error: {e}");
                continue;
            }
            Err(_) => {
                println!("server: no request for 3s, exiting");
                break;
            }
        };

        handled += 1;
        let body = msg_str(&msg, 0);

        if handled == 3 {
            println!("server: simulating crash on '{body}'");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let reply = format!("reply:{body}");
        match rep.send(Message::single(reply)).await {
            Ok(()) => println!("server: replied to {body}"),
            Err(e) => {
                // Stale retry from client that already reconnected; skip it.
                eprintln!("server: send error for {body}: {e}");
            }
        }
    }

    println!("server: handled {handled} requests");
}
