//! `ZGuide` 02 — SUB/PUB forwarding proxy.
//!
//! Connects a SUB socket to an upstream PUB (subscribes to all topics)
//! and binds a PUB socket downstream. Receives messages from upstream
//! and re-publishes them downstream.
//!
//!     cargo run -p zguide-02-pub-sub --bin proxy [upstream] [downstream]
//!
//! Runs indefinitely (Ctrl-C to stop).
//!
//! NOTE: This uses SUB/PUB relay instead of the canonical XSUB/XPUB
//! proxy because `XSUB.send()` is not yet supported.

use omq_tokio::{Context, Endpoint, Options, SocketType};

fn endpoint_or(args: &[String], index: usize, default: &str) -> Endpoint {
    args.get(index).map_or_else(
        || default.parse().unwrap(),
        |s| s.parse().expect("invalid endpoint"),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let args: Vec<String> = std::env::args().collect();
    let upstream_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-02-upstream");
    let downstream_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-02-downstream");

    // Upstream: SUB connects to the publisher and subscribes to everything.
    let upstream = ctx.socket(SocketType::Sub, Options::default());
    upstream.connect(upstream_ep.clone()).await.unwrap();
    upstream.subscribe("").await.unwrap();

    // Downstream: PUB binds so downstream subscribers can connect.
    let downstream = ctx.socket(SocketType::Pub, Options::default());
    downstream.bind(downstream_ep.clone()).await.unwrap();

    println!("proxy: upstream={upstream_ep} downstream={downstream_ep}");

    loop {
        let msg = upstream.recv().await.unwrap();
        downstream.send(msg).await.unwrap();
    }
}
