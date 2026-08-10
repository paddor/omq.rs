//! `ZGuide` 07 — Clone (server).
//!
//! Maintains a key-value store, publishes updates via PUB, and serves
//! snapshots via REQ/REP. Each update carries a sequence number so
//! clients can merge snapshots with buffered live updates.
//!
//!     cargo run -p zguide-07-clone --bin server \
//!         [updates_ep] [snapshot_ep]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

struct Entry {
    value: String,
    seq: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let updates_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-07-updates");
    let snapshot_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-07-snapshot");

    let pub_ = ctx.socket(SocketType::Pub, Options::default());
    pub_.bind(updates_ep.clone()).await.unwrap();

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(snapshot_ep.clone()).await.unwrap();

    println!("server: PUB bound to {updates_ep}");
    println!("server: REP bound to {snapshot_ep}");

    let store: Arc<Mutex<HashMap<String, Entry>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut seq: u64 = 0;

    // Snapshot task: serve snapshot requests.
    let store_snap = Arc::clone(&store);
    let snapshot_task = tokio::spawn(async move {
        loop {
            let msg = rep.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            if body == "SNAPSHOT" {
                let payload = {
                    let locked = store_snap.lock().unwrap();
                    let p: String = locked
                        .iter()
                        .map(|(k, e)| format!("{}|{k}|{}", e.seq, e.value))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!("server: snapshot served ({} entries)", locked.len());
                    p
                };
                rep.send(Message::single(payload)).await.unwrap();
            }
        }
    });

    // Give subscribers time to connect.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish initial updates.
    for i in 0..5 {
        seq += 1;
        let cur = seq;

        let key = format!("key-{i}");
        let val = format!("val-{i}");
        store.lock().unwrap().insert(
            key.clone(),
            Entry {
                value: val.clone(),
                seq: cur,
            },
        );

        let msg = format!("{cur}|{key}|{val}");
        pub_.send(Message::single(msg)).await.unwrap();
        println!("server: published {key}={val} (seq={cur})");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Pause so client can request snapshot.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish post-snapshot updates.
    for i in 0..3 {
        seq += 1;
        let cur = seq;

        let key = format!("key-{i}");
        let val = format!("updated-{i}");
        store.lock().unwrap().insert(
            key.clone(),
            Entry {
                value: val.clone(),
                seq: cur,
            },
        );

        let msg = format!("{cur}|{key}|{val}");
        pub_.send(Message::single(msg)).await.unwrap();
        println!("server: published {key}={val} (seq={cur})");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Keep serving snapshots briefly, then exit.
    tokio::select! {
        r = snapshot_task => r.unwrap(),
        () = tokio::time::sleep(Duration::from_secs(3)) => {
            println!("server: done");
        }
    }
}
