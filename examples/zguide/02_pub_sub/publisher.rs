//! `ZGuide` 02 — PUB publisher.
//!
//! Binds a PUB socket and publishes weather and sports data in a loop.
//!
//!     cargo run -p zguide-02-pub-sub --bin publisher [endpoint] [count]
//!
//! If `count` is given, publishes that many rounds then exits. Otherwise
//! runs indefinitely (Ctrl-C to stop).

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
    let ep = endpoint_or(&args, 1, "ipc://@omq-zguide-02-pubsub");
    let count: Option<usize> = args.get(2).and_then(|s| s.parse().ok());

    let pub_ = ctx.socket(SocketType::Pub, Options::default());
    pub_.bind(ep.clone()).await.unwrap();

    println!("publisher: bound to {ep}");

    // Give subscribers time to connect and send SUBSCRIBE commands.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let rounds = count.unwrap_or(usize::MAX);
    for i in 0..rounds {
        let nyc_temp = 55 + (i % 30);
        let sfo_temp = 60 + (i % 20);
        let chi_temp = 40 + (i % 35);

        pub_.send(Message::single(format!("weather.nyc {nyc_temp}F")))
            .await
            .unwrap();
        pub_.send(Message::single(format!("weather.sfo {sfo_temp}F")))
            .await
            .unwrap();
        pub_.send(Message::single(format!("weather.chi {chi_temp}F")))
            .await
            .unwrap();
        pub_.send(Message::single(format!("sports.nba score-{i}")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("publisher: done ({rounds} rounds)");
}
