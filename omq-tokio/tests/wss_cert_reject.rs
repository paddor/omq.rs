#![cfg(feature = "ws")]

use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::{Options, WssTls};
use omq_proto::proto::SocketType;
use omq_tokio::Socket;

fn self_signed_tls() -> (Vec<u8>, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert_pem = certified.cert.pem().into_bytes();
    let key_pem = certified.signing_key.serialize_pem().into_bytes();
    (cert_pem, key_pem)
}

fn wss_endpoint(port: u16) -> Endpoint {
    format!("wss://127.0.0.1:{port}/").parse().unwrap()
}

fn get_port(ep: &Endpoint) -> u16 {
    match ep {
        Endpoint::Wss { port, .. } => *port,
        other => panic!("expected Wss, got {other:?}"),
    }
}

#[tokio::test]
async fn wss_rejects_invalid_cert() {
    let (cert_pem, key_pem) = self_signed_tls();

    let server_opts = Options {
        wss_tls: WssTls {
            server_cert_pem: Some(cert_pem),
            server_key_pem: Some(key_pem),
            accept_invalid_certs: false,
            ..WssTls::default()
        },
        ..Options::default()
    };

    let pull = Socket::new(SocketType::Pull, server_opts);
    let bound = pull.bind(wss_endpoint(0)).await.unwrap();
    let port = get_port(&bound);

    let client_opts = Options {
        wss_tls: WssTls {
            accept_invalid_certs: false,
            ..WssTls::default()
        },
        ..Options::default()
    };

    let push = Socket::new(SocketType::Push, client_opts);
    push.connect(wss_endpoint(port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    push.send(Message::from(Bytes::from_static(b"should not arrive")))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), pull.recv()).await;
    assert!(
        result.is_err(),
        "expected timeout: TLS handshake should fail with invalid cert"
    );
}
