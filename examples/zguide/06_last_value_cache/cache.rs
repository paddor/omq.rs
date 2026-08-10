//! `ZGuide` 06 — Last Value Cache (caching proxy).
//!
//! Sits between publishers and subscribers. Caches the latest value per
//! topic and serves snapshots to late joiners via REQ/REP.
//!
//!     cargo run -p zguide-06-last-value-cache --bin cache \
//!         [pub_ep] [sub_ep] [snapshot_ep]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    let pub_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-06-publisher");
    let sub_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-06-subscriber");
    let snapshot_ep = endpoint_or(&args, 3, "ipc://@omq-zguide-06-snapshot");

    let pull = ctx.socket(SocketType::Pull, Options::default());
    pull.bind(pub_ep.clone()).await.unwrap();

    let pub_ = ctx.socket(SocketType::Pub, Options::default());
    pub_.bind(sub_ep.clone()).await.unwrap();

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(snapshot_ep.clone()).await.unwrap();

    println!("cache: PULL bound to {pub_ep}");
    println!("cache: PUB  bound to {sub_ep}");
    println!("cache: REP  bound to {snapshot_ep}");

    let cache: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // Forward task: recv from PULL, cache, forward via PUB.
    let cache_fwd = Arc::clone(&cache);
    let forward = tokio::spawn(async move {
        loop {
            let msg = pull.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if let Some((topic, value)) = body.split_once(' ') {
                cache_fwd
                    .lock()
                    .unwrap()
                    .insert(topic.to_owned(), value.to_owned());
                println!("cache: cached {topic}={value}");
            }
            pub_.send(Message::single(body)).await.unwrap();
        }
    });

    // Snapshot task: serve cached state on REQ/REP.
    let cache_snap = Arc::clone(&cache);
    let snapshot = tokio::spawn(async move {
        loop {
            let msg = rep.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if body == "SNAPSHOT" {
                let payload = {
                    let locked = cache_snap.lock().unwrap();
                    let p: String = locked
                        .iter()
                        .map(|(k, v)| format!("{k} {v}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!("cache: snapshot served ({} entries)", locked.len());
                    p
                };
                rep.send(Message::single(payload)).await.unwrap();
            }
        }
    });

    tokio::select! {
        r = forward => r.unwrap(),
        r = snapshot => r.unwrap(),
    }
}
