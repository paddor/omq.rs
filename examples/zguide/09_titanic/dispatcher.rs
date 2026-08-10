//! `ZGuide` 09 — Titanic dispatcher.
//!
//! PULL socket receives ticket IDs, reads the corresponding `.req` file,
//! processes the request (echo / upper), and writes a `.res` file.
//!
//!     cargo run -p zguide-09-titanic --bin dispatcher \
//!         [dispatch_ep] [store_dir]

use std::path::Path;
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
    let dispatch_ep = endpoint_or(&args, 1, "ipc://@omq-zguide-09-dispatch");
    let store_dir = args.get(2).map_or("/tmp/omq-titanic", String::as_str);

    let pull = ctx.socket(SocketType::Pull, Options::default());
    pull.connect(dispatch_ep.clone()).await.unwrap();

    println!("dispatcher: {dispatch_ep} store={store_dir}");

    loop {
        let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(3), pull.recv()).await else {
            break;
        };
        let ticket = msg_str(&msg, 0);
        let req_path = Path::new(store_dir).join(format!("{ticket}.req"));

        let Ok(contents) = std::fs::read_to_string(&req_path) else {
            continue;
        };

        let (service, body) = contents.split_once('|').unwrap_or(("", &contents));

        let result = match service {
            "echo" => format!("echo:{body}"),
            "upper" => body.to_uppercase(),
            _ => format!("unknown service: {service}"),
        };

        let res_path = Path::new(store_dir).join(format!("{ticket}.res"));
        std::fs::write(&res_path, &result).expect("write .res failed");

        println!("dispatcher: processed {ticket} -> {result}");
    }

    println!("dispatcher: done (recv timeout)");
}
