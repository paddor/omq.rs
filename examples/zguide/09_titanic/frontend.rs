//! `ZGuide` 09 — Titanic frontend.
//!
//! REP socket accepts client requests (`SUBMIT` / `RESULT`), persists
//! them to disk, and pushes ticket IDs to the dispatcher via PUSH.
//!
//!     cargo run -p zguide-09-titanic --bin frontend \
//!         [frontend_ep] [dispatch_ep] [store_dir]

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

static NEXT: AtomicU64 = AtomicU64::new(1);

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
    let dispatch_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-09-dispatch");
    let store_dir = args.get(3).map_or("/tmp/omq-titanic", String::as_str);

    std::fs::create_dir_all(store_dir).expect("cannot create store dir");

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(frontend_ep.clone()).await.unwrap();

    let push = ctx.socket(SocketType::Push, Options::default());
    push.bind(dispatch_ep.clone()).await.unwrap();

    println!("frontend: {frontend_ep} dispatch={dispatch_ep} store={store_dir}");

    loop {
        let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(3), rep.recv()).await else {
            break;
        };
        let body = msg_str(&msg, 0);
        let parts: Vec<&str> = body.splitn(3, '|').collect();

        match parts[0] {
            "SUBMIT" if parts.len() == 3 => {
                let service = parts[1];
                let payload = parts[2];
                let ticket = NEXT.fetch_add(1, Ordering::Relaxed);
                let ticket = format!("{ticket:016x}");

                let req_path = Path::new(store_dir).join(format!("{ticket}.req"));
                std::fs::write(&req_path, format!("{service}|{payload}"))
                    .expect("write .req failed");

                rep.send(Message::single(format!("TICKET|{ticket}")))
                    .await
                    .unwrap();
                push.send(Message::single(ticket.clone())).await.unwrap();

                println!("frontend: accepted {ticket} for '{service}'");
            }
            "RESULT" if parts.len() >= 2 => {
                let ticket = parts[1];
                let res_path = Path::new(store_dir).join(format!("{ticket}.res"));

                if res_path.exists() {
                    let contents = std::fs::read_to_string(&res_path).expect("read .res failed");
                    rep.send(Message::single(format!("OK|{contents}")))
                        .await
                        .unwrap();
                    println!("frontend: served result for {ticket}");
                } else {
                    rep.send(Message::single("PENDING")).await.unwrap();
                }
            }
            _ => {
                rep.send(Message::single("ERROR|unknown command"))
                    .await
                    .unwrap();
            }
        }
    }

    println!("frontend: done (recv timeout)");
}
