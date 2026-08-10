//! `ZGuide` 03 — Pipeline worker.
//!
//! PULL connects to ventilator, PUSH connects to sink. Forwards each
//! task with a worker ID prefix. Runs until killed.
//!
//!     cargo run -p zguide-03-pipeline --bin worker [vent_ep] [sink_ep] [worker_id]

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
    let vent_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-03-ventilator");
    let sink_ep = endpoint_or(&args, 2, "ipc://@omq-zguide-03-sink");
    let id = args.get(3).map_or("0", |s| s.as_str());

    let pull = ctx.socket(SocketType::Pull, Options::default());
    pull.connect(vent_ep).await.unwrap();

    let push = ctx.socket(SocketType::Push, Options::default());
    push.connect(sink_ep).await.unwrap();

    println!("worker-{id}: ready");

    loop {
        let msg = pull.recv().await.unwrap();
        let body = msg_str(&msg, 0);
        let result = format!("worker-{id}:{body}");
        push.send(Message::single(result)).await.unwrap();
    }
}
