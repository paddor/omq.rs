//! `ZGuide` 08 — Majordomo broker.
//!
//! Two ROUTER sockets: frontend (clients) and backend (workers).
//! Workers register with `["READY", service_name]`. Client requests
//! are routed to the matching service pool using LRU.
//!
//!     cargo run -p zguide-08-majordomo --bin broker \
//!         [frontend_ep] [backend_ep] [n_workers]

use std::collections::HashMap;

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
    let frontend_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-08-frontend");
    let backend_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-08-backend");
    let n_workers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(3);

    let frontend = ctx.socket(SocketType::Router, Options::default());
    frontend.bind(frontend_ep.clone()).await.unwrap();

    let backend = ctx.socket(SocketType::Router, Options::default());
    backend.bind(backend_ep.clone()).await.unwrap();

    println!("broker: frontend={frontend_ep} backend={backend_ep} n_workers={n_workers}");

    // Worker registration: service_name -> [worker_identity, ...]
    let mut services: HashMap<String, Vec<Bytes>> = HashMap::new();

    for _ in 0..n_workers {
        let msg = backend.recv().await.unwrap();
        let worker_id = msg.part_bytes(0).unwrap();
        let command = msg_str(&msg, 1);
        let service = msg_str(&msg, 2);
        if command == "READY" {
            println!(
                "broker: worker '{}' registered for '{service}'",
                String::from_utf8_lossy(&worker_id)
            );
            services.entry(service).or_default().push(worker_id);
        }
    }

    // Route requests: recv from frontend, dispatch to worker, relay reply.
    loop {
        let msg = frontend.recv().await.unwrap();
        // REQ client -> ROUTER: [client_id, "", service, body]
        let client_id = msg.part_bytes(0).unwrap();
        // frame 1 is empty delimiter from REQ
        let service = msg_str(&msg, 2);
        let body = msg.part_bytes(3).unwrap();

        let Some(worker_id) = services.get_mut(&service).and_then(|pool| {
            if pool.is_empty() {
                None
            } else {
                Some(pool.remove(0))
            }
        }) else {
            println!("broker: no worker for service '{service}'");
            continue;
        };

        println!(
            "broker: routing '{service}' request to {}",
            String::from_utf8_lossy(&worker_id)
        );

        // Send to backend ROUTER: [worker_id, client_id, "", body]
        backend
            .send(Message::multipart([
                worker_id.clone(),
                client_id.clone(),
                Bytes::from_static(b""),
                body,
            ]))
            .await
            .unwrap();

        // Recv reply from backend: [worker_id, client_id, "", reply]
        let reply_msg = backend.recv().await.unwrap();
        let reply_worker_id = reply_msg.part_bytes(0).unwrap();
        let reply_client_id = reply_msg.part_bytes(1).unwrap();
        // frame 2 is empty delimiter
        let reply_body = reply_msg.part_bytes(3).unwrap();

        // Return worker to pool
        services.entry(service).or_default().push(reply_worker_id);

        // Forward to frontend ROUTER: [client_id, "", reply]
        frontend
            .send(Message::multipart([
                reply_client_id,
                Bytes::from_static(b""),
                reply_body,
            ]))
            .await
            .unwrap();
    }
}
