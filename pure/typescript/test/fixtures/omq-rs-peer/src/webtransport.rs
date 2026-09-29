//! WebTransport browser fixture: REP echo, PUB, PULL with a state REP.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use omq_tokio::options::QuicOptions;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};

/// Browsers accept `serverCertificateHashes` only for ECDSA certificates
/// valid for at most 14 days.
fn short_lived_certificate() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::days(1);
    params.not_after = now + time::Duration::days(10);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    (
        cert.pem().into_bytes(),
        key.serialize_pem().into_bytes(),
        cert.der().to_vec(),
    )
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn server_options(cert: &[u8], key: &[u8]) -> Options {
    let mut quic = QuicOptions {
        server_cert_pem: Some(cert.to_vec()),
        server_key_pem: Some(key.to_vec()),
        ..QuicOptions::default()
    };
    if let Ok(origin) = std::env::var("OMQ_TS_BROWSER_ORIGIN") {
        quic.allowed_origins = vec![origin];
    }
    Options {
        quic,
        ..Options::default()
    }
}

async fn bind(kind: SocketType, host: &str, path: &str, options: &Options) -> Socket {
    let mut options = options.clone();
    options.quic.webtransport_path = path.into();
    let socket = Socket::new(kind, options);
    let endpoint: Endpoint = format!("quic://{host}:0").parse().unwrap();
    let bound = socket.bind(endpoint).await.expect("bind WebTransport");
    let address = bound.to_string().replace("quic://", "https://");
    println!("ENDPOINT {path} {address}{path}");
    let mut monitor = socket.monitor();
    let path = path.to_owned();
    tokio::spawn(async move {
        while let Ok(event) = monitor.recv().await {
            eprintln!("{path}: {event:?}");
        }
    });
    socket
}

pub async fn webtransport_browser(host: String) {
    let (cert, key, der) = short_lived_certificate();
    println!("CERT_DER {}", hex(&der));
    let options = server_options(&cert, &key);
    let rep = bind(SocketType::Rep, &host, "/rep", &options).await;
    let publisher = bind(SocketType::Pub, &host, "/pub", &options).await;
    let pull = bind(SocketType::Pull, &host, "/pull", &options).await;
    let state = bind(SocketType::Rep, &host, "/state", &options).await;
    println!("READY");

    tokio::spawn(async move {
        while let Ok(request) = rep.recv().await {
            if rep.send(request).await.is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        let mut seq = 0u64;
        loop {
            let msg = Message::multipart([
                bytes::Bytes::from_static(b"wt.topic"),
                bytes::Bytes::from(seq.to_string()),
            ]);
            let _ = publisher.send(msg).await;
            seq += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let pulled = Arc::new(AtomicUsize::new(0));
    let counter = pulled.clone();
    tokio::spawn(async move {
        while pull.recv().await.is_ok() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    while let Ok(_request) = state.recv().await {
        let reply = pulled.load(Ordering::Relaxed).to_string();
        if state.send(Message::single(reply)).await.is_err() {
            return;
        }
    }
}
