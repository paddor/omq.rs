//! `ZGuide` 07 — Clone (client).
//!
//! Subscribes for live updates first, then requests a snapshot. Merges
//! buffered updates (seq > snapshot seq) into the local store.
//!
//!     cargo run -p zguide-07-clone --bin client \
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let updates_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-07-updates");
    let snapshot_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-07-snapshot");

    // Subscribe for live updates first (before snapshot) so we don't
    // miss anything published between snapshot reply and SUB connect.
    let sub = ctx.socket(SocketType::Sub, Options::default());
    sub.connect(updates_ep.clone()).await.unwrap();
    sub.subscribe("").await.unwrap();
    println!("client: SUB connected to {updates_ep}");

    // Buffer live updates in a background task.
    let buffer: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let buffer_tx = Arc::clone(&buffer);
    let buffer_task = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            tokio::select! {
                msg = sub.recv() => {
                    let body = msg_str(&msg.unwrap(), 0);
                    buffer_tx.lock().unwrap().push(body);
                }
                () = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }
        }
    });

    // Give SUB time to connect and subscribe.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request snapshot.
    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(snapshot_ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    req.send(Message::single("SNAPSHOT")).await.unwrap();
    let reply = req.recv().await.unwrap();
    let snapshot_body = msg_str(&reply, 0);

    let mut store: HashMap<String, String> = HashMap::new();
    let mut snapshot_seq: u64 = 0;

    for line in snapshot_body.lines() {
        if let Some((seq_str, rest)) = line.split_once('|')
            && let Some((key, val)) = rest.split_once('|')
        {
            let s: u64 = seq_str.parse().unwrap_or(0);
            store.insert(key.to_owned(), val.to_owned());
            if s > snapshot_seq {
                snapshot_seq = s;
            }
            println!("client (snapshot): {key}={val} seq={s}");
        }
    }
    println!(
        "client: snapshot has {} entries (up to seq={snapshot_seq})",
        store.len()
    );

    // Wait for buffered updates to finish arriving.
    buffer_task.await.unwrap();

    // Apply buffered updates where seq > snapshot_seq.
    let buffered = buffer.lock().unwrap();
    for line in buffered.iter() {
        if let Some((seq_str, rest)) = line.split_once('|')
            && let Some((key, val)) = rest.split_once('|')
        {
            let s: u64 = seq_str.parse().unwrap_or(0);
            if s > snapshot_seq {
                store.insert(key.to_owned(), val.to_owned());
                println!("client (live): {key}={val} seq={s}");
            } else {
                println!("client (skip): {key}={val} seq={s} (already in snapshot)");
            }
        }
    }

    println!("client: final store ({} entries):", store.len());
    let mut keys: Vec<_> = store.keys().collect();
    keys.sort();
    for k in keys {
        println!("  {k} = {}", store[k]);
    }

    println!("client: done");
}
