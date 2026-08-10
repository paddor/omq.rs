//! `ZGuide` 01 — Basic REQ/REP echo.
//!
//! Single-process demo: REP server echoes messages back to a REQ client.
//!
//!     cargo run -p zguide-01-req-rep --bin echo [endpoint]

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
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-01-echo");

    let rep = ctx.socket(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();

    let req = ctx.socket(SocketType::Req, Options::default());
    req.connect(ep).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let msg = rep.recv().await.unwrap();
            let body = msg_str(&msg, 0);
            rep.send(Message::single(format!("echo:{body}")))
                .await
                .unwrap();
        }
    });

    for i in 0..3 {
        let request = format!("hello-{i}");
        req.send(Message::single(request.clone())).await.unwrap();
        let reply = req.recv().await.unwrap();
        let body = msg_str(&reply, 0);
        println!("client: {request} -> {body}");
    }

    server.await.unwrap();
    println!("done: 3 request-reply cycles");
}
