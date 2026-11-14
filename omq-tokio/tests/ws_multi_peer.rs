#![cfg(feature = "ws")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

fn ws_endpoint(port: u16) -> Endpoint {
    format!("ws://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Ws { port, .. } => *port,
        other => panic!("expected Ws, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_push_to_multiple_pulls() {
    const N: usize = 30;

    let push = Socket::new(SocketType::Push, Options::default());
    let bound = push.bind(ws_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let pulls: Vec<Socket> = (0..3)
        .map(|_| Socket::new(SocketType::Pull, Options::default()))
        .collect();

    for p in &pulls {
        p.connect(ws_endpoint(port)).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..N {
        push.send(Message::single(format!("m{i}"))).await.unwrap();
    }

    let counts: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut handles = Vec::new();

    for (p, c) in pulls.into_iter().zip(counts.iter().cloned()) {
        handles.push(tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(500), p.recv()).await {
                    Ok(Ok(_)) => {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => return,
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let total: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    assert_eq!(total, N, "every message must reach exactly one pull");
}
