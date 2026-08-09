use std::env;
use std::str::FromStr;
use std::time::Duration;

use omq_tokio::{
    Context, CurveKeypair, CurvePublicKey, CurveSecretKey, Endpoint, Message, Options, SocketType,
};

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
            std::thread::sleep(Duration::from_millis(100));
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
        "curve-push" => {
            let options = Options::default().curve_client(
                curve_keypair_from_env("OMQ_CURVE_CLIENT_PUBLIC", "OMQ_CURVE_CLIENT_SECRET")?,
                curve_public_from_env("OMQ_CURVE_SERVER_PUBLIC")?,
            );
            let socket = ctx.blocking_socket(SocketType::Push, options);
            socket.connect(endpoint)?;
            socket.wait_connected(1, Duration::from_secs(5))?;
            socket.send(Message::from_slice(payload.as_bytes()))?;
            std::thread::sleep(Duration::from_millis(100));
            socket.close()?;
        }
        "curve-pull" => {
            let options = Options::default().curve_server(curve_keypair_from_env(
                "OMQ_CURVE_SERVER_PUBLIC",
                "OMQ_CURVE_SERVER_SECRET",
            )?);
            let socket = ctx.blocking_socket(SocketType::Pull, options);
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

fn curve_keypair_from_env(
    public_name: &str,
    secret_name: &str,
) -> Result<CurveKeypair, Box<dyn std::error::Error>> {
    let public = curve_public_from_env(public_name)?;
    let secret = CurveSecretKey::from_z85(&env::var(secret_name)?)?;
    if public != secret.derive_public() {
        return Err("CURVE public key does not match secret key".into());
    }
    Ok(CurveKeypair { public, secret })
}

fn curve_public_from_env(name: &str) -> Result<CurvePublicKey, Box<dyn std::error::Error>> {
    Ok(CurvePublicKey::from_z85(&env::var(name)?)?)
}
