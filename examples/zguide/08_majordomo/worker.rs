//! `ZGuide` 08 — Majordomo worker.
//!
//! DEALER with explicit identity connects to the broker backend.
//! Sends `["READY", service_name]`, then loops processing requests.
//!
//!     cargo run -p zguide-08-majordomo --bin worker \
//!         [backend_ep] [service_name] [worker_id]

use bytes::Bytes;
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
    let backend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-08-backend");
    let service: String = args.get(2).map_or_else(|| "echo".into(), Clone::clone);
    let id: String = args.get(3).map_or_else(|| "0".into(), Clone::clone);

    let identity = Bytes::from(format!("{service}-{id}"));
    let dealer = ctx.socket(SocketType::Dealer, Options::default().identity(identity));
    dealer.connect(backend_ep).await.unwrap();

    // Register with broker
    dealer
        .send(Message::multipart(["READY".to_string(), service.clone()]))
        .await
        .unwrap();
    println!("worker({service}-{id}): ready");

    // Process requests
    loop {
        let msg = dealer.recv().await.unwrap();
        // DEALER sees: [client_id, "", body]
        let client_id = msg.part_bytes(0).unwrap();
        let body = msg_str(&msg, 2);

        let reply = match service.as_str() {
            "echo" => format!("echo:{body}"),
            "upper" => body.to_uppercase(),
            _ => body.clone(),
        };

        println!("worker({service}-{id}): {body} -> {reply}");

        dealer
            .send(Message::multipart([
                client_id,
                Bytes::from_static(b""),
                Bytes::from(reply),
            ]))
            .await
            .unwrap();
    }
}
