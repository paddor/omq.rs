//! Public blocking-API peer for cross-process pyomq QUIC tests.

use std::{error::Error, io::Write, path::Path, time::Duration};

use omq_tokio::{Context, Message, Options, SocketType};

fn cases() -> Vec<Message> {
    let patterned = |size: usize| {
        (0..size)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect::<Vec<_>>()
    };
    vec![
        Message::single(Vec::new()),
        Message::single(vec![0, 255, 128]),
        Message::multipart([b"head".to_vec(), Vec::new(), b"tail".to_vec()]),
        Message::single(patterned(256 * 1024)),
        Message::multipart([patterned(256 * 1024), Vec::new(), patterned(4097)]),
    ]
}

fn print_line(line: impl std::fmt::Display) -> std::io::Result<()> {
    println!("{line}");
    std::io::stdout().flush()
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if let [command, directory] = args.as_slice()
        && command == "certs"
    {
        let directory = Path::new(directory);
        for stem in ["server", "unrelated"] {
            let tls =
                rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
            std::fs::write(directory.join(format!("{stem}.pem")), tls.cert.pem())?;
            std::fs::write(
                directory.join(format!("{stem}.key")),
                tls.signing_key.serialize_pem(),
            )?;
        }
        return Ok(());
    }
    let [command, endpoint, directory] = args.as_slice() else {
        return Err(
            "usage: quic_interop_peer certs DIR | bind-rep/connect-req ENDPOINT DIR".into(),
        );
    };
    let mut options = Options::default();
    options.quic.trust_system = false;
    options.quic.stream_window = omq_tokio::options::QuicOptions::MIN_STREAM_WINDOW;
    let directory = Path::new(directory);
    let cert = std::fs::read(directory.join("server.pem"))?;
    let context = Context::new();
    match command.as_str() {
        "bind-rep" | "listen" => {
            options.quic.server_cert_pem = Some(cert);
            options.quic.server_key_pem = Some(std::fs::read(directory.join("server.key"))?);
            let rep = context.blocking_socket(SocketType::Rep, options);
            print_line(rep.bind(endpoint)?)?;
            if command == "bind-rep" {
                for _ in cases() {
                    let message = rep.recv_timeout(Duration::from_secs(10))?;
                    rep.send(message)?;
                    print_line("REPLIED")?;
                }
            }
            // Keep the listener alive until the parent has received every
            // reply or observed a rejected TLS handshake.
            let mut stop = String::new();
            std::io::stdin().read_line(&mut stop)?;
            rep.close()?;
        }
        "connect-req" => {
            options.quic.trust_pem = Some(cert);
            options.quic.server_name = Some("localhost".into());
            let req = context.blocking_socket(SocketType::Req, options);
            req.connect(endpoint)?;
            print_line("CONNECTED")?;
            for expected in cases() {
                req.send(expected.clone())?;
                let actual = req.recv_timeout(Duration::from_secs(10))?;
                if actual != expected {
                    return Err("reply changed message bytes or multipart boundaries".into());
                }
            }
            print_line("OK")?;
            req.close()?;
        }
        _ => return Err("unknown command".into()),
    }
    Ok(())
}
