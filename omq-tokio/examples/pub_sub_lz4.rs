//! Pub/sub with lz4+tcp:// compression.
//!
//! Run with:
//!   cargo run -p omq-tokio --example `pub_sub_lz4` --features lz4

use std::time::Duration;

use omq_tokio::{Context, Message, Options, SocketType};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ctx = Context::new();
    let publisher = ctx.socket(SocketType::Pub, Options::default());
    publisher.bind("lz4+tcp://127.0.0.1:5556").await.unwrap();

    let subscriber = ctx.socket(SocketType::Sub, Options::default());
    subscriber
        .connect("lz4+tcp://127.0.0.1:5556")
        .await
        .unwrap();
    subscriber.subscribe("news.").await.unwrap(); // prefix match

    // SUBSCRIBE travels from sub to pub over the wire; give it a moment.
    tokio::time::sleep(Duration::from_millis(50)).await;

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await
        .unwrap();
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await
        .unwrap(); // filtered out

    let m = subscriber.recv().await.unwrap(); // only "news.sports" arrives
    let topic = m.part_bytes(0).unwrap();
    let body = m.part_bytes(1).unwrap();
    assert_eq!(&*topic, b"news.sports");
    assert_eq!(&*body, b"ball scores");

    println!(
        "{}: {}",
        String::from_utf8_lossy(&topic),
        String::from_utf8_lossy(&body),
    );
}
