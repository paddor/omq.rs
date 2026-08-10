//! `ZGuide` 03 — Ventilator (task producer).
//!
//! PUSH socket binds and sends N tasks. Workers are killed by the
//! run script after the sink has collected all results.
//!
//!     cargo run -p zguide-03-pipeline --bin ventilator [vent_ep] [n_tasks]

use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

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
    let vent_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-03-ventilator");
    let n_tasks: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1000);

    let push = ctx.socket(
        SocketType::Push,
        Options::default().linger(Duration::from_secs(2)),
    );
    push.bind(vent_ep.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..n_tasks {
        push.send(Message::single(format!("task-{i}")))
            .await
            .unwrap();
    }

    println!("ventilator: sent {n_tasks} tasks on {vent_ep}");
    push.close().await.unwrap();
}
