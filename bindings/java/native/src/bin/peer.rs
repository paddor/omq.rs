use std::env;
use std::str::FromStr;
use std::time::Duration;

use omq_tokio::{Context, Endpoint, Message, Options, SocketType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().ok_or("missing mode")?;
    let endpoint = args.next().ok_or("missing endpoint")?;
    let endpoint = Endpoint::from_str(&endpoint)?;
    let payload = args.next().unwrap_or_else(|| "rust-hello".to_string());

    let ctx = Context::new();
    match mode.as_str() {
        "push" => {
            let socket = ctx.blocking_socket(SocketType::Push, Options::default());
            socket.connect(endpoint)?;
            socket.wait_connected(1, Duration::from_secs(5))?;
            socket.send(Message::from_slice(payload.as_bytes()))?;
            socket.close()?;
        }
        "pull" => {
            let socket = ctx.blocking_socket(SocketType::Pull, Options::default());
            let bound = socket.bind(endpoint)?;
            println!("{bound}");
            let message = socket.recv_timeout(Duration::from_secs(5))?;
            let part = message.part_bytes(0).unwrap_or_default();
            println!("{}", String::from_utf8_lossy(&part));
            socket.close()?;
        }
        other => return Err(format!("unknown mode: {other}").into()),
    }

    ctx.term();
    Ok(())
}
