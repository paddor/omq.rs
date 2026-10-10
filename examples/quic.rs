//! PUSH/PULL over QUIC.
//!
//! Run: `cargo run -p omq-tokio --example quic --features quic`

use omq_tokio::{Context, Message, Options, SocketType};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate a certificate so the example needs no external files.
    let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])?;
    let cert_pem = certificate.cert.pem().into_bytes();

    let mut server = Options::default();
    server.quic.server_cert_pem = Some(cert_pem.clone());
    server.quic.server_key_pem = Some(certificate.signing_key.serialize_pem().into_bytes());

    let mut client = Options::default();
    client.quic.trust_pem = Some(cert_pem);

    let context = Context::new();
    let pull = context.socket(SocketType::Pull, server);
    let endpoint = pull.bind("quic://127.0.0.1:0").await?;

    let push = context.socket(SocketType::Push, client);
    push.connect(endpoint).await?;
    push.send(Message::single("hello over QUIC")).await?;

    let message = pull.recv().await?;
    println!("{}", std::str::from_utf8(&message[0])?);

    push.close().await?;
    pull.close().await?;
    Ok(())
}
