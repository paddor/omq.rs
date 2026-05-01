//! REP echo server. Binds to every endpoint on ARGV and replies to each
//! request with the same payload, ASCII letters upper-cased, other bytes
//! left as-is.
//!
//! Usage: omq-rep <endpoint> [<endpoint> ...]
//!
//! Supported transports: tcp://, ipc://, lz4+tcp:// (--features lz4),
//! zstd+tcp:// (--features zstd).

use std::str::FromStr;

use bytes::Bytes;
use omq_compio::{Endpoint, Message, Options, Socket, SocketType};
use omq_proto::message::Payload;

fn ascii_upper(src: Bytes) -> Bytes {
    if !src.iter().any(u8::is_ascii_lowercase) {
        return src;
    }
    src.iter()
        .map(|&b| b.to_ascii_uppercase())
        .collect::<Vec<u8>>()
        .into()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: omq-rep <endpoint> [<endpoint> ...]");
        std::process::exit(1);
    }

    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(run(args));
}

async fn run(args: Vec<String>) {
    let socket = Socket::new(SocketType::Rep, Options::default());

    for ep_str in &args {
        let ep = Endpoint::from_str(ep_str).unwrap_or_else(|e| {
            eprintln!("bad endpoint {ep_str:?}: {e}");
            std::process::exit(1);
        });
        socket.bind(ep).await.unwrap_or_else(|e| {
            eprintln!("bind {ep_str}: {e}");
            std::process::exit(1);
        });
        eprintln!("listening on {ep_str}");
    }

    loop {
        let req = match socket.recv().await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("recv: {e}");
                return;
            }
        };
        let mut reply = Message::new();
        for part in req.parts() {
            reply.push_part(Payload::from_bytes(ascii_upper(part.coalesce())));
        }
        if let Err(e) = socket.send(reply).await {
            eprintln!("send: {e}");
            return;
        }
    }
}
