#![cfg(feature = "webtransport")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_tokio::Socket;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

const DATA_PREFACE: &[u8; 8] = b"OMQD\x01\0\0\0";
const LIVENESS_PREFACE: &[u8; 8] = b"OMQL\x01\0\0\0";

struct Tls {
    cert: Vec<u8>,
    key: Vec<u8>,
}

fn tls() -> Tls {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    Tls {
        cert: certified.cert.pem().into_bytes(),
        key: certified.signing_key.serialize_pem().into_bytes(),
    }
}

fn server_options(tls: &Tls) -> Options {
    let mut options = Options::default();
    options.quic.server_cert_pem = Some(tls.cert.clone());
    options.quic.server_key_pem = Some(tls.key.clone());
    options.quic.allowed_origins = vec!["https://app.example.com".into()];
    options
}

fn port_of(endpoint: &Endpoint) -> u16 {
    match endpoint {
        Endpoint::Quic { port, .. } => *port,
        other => panic!("expected quic endpoint, got {other}"),
    }
}

struct WebTransportPeer {
    _endpoint: quinn::Endpoint,
    _session: omq_webtrans_quinn::Session,
    data_send: omq_webtrans_quinn::SendStream,
    data_recv: omq_webtrans_quinn::RecvStream,
    _liveness_send: omq_webtrans_quinn::SendStream,
    _liveness_recv: omq_webtrans_quinn::RecvStream,
    codec: Connection,
}

impl WebTransportPeer {
    async fn connect(cert: &[u8], port: u16, socket_type: SocketType) -> Self {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(cert).unwrap())
            .unwrap();
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(quinn::VarInt::from_u32(0))
            .max_concurrent_uni_streams(quinn::VarInt::from_u32(3))
            .datagram_receive_buffer_size(Some(64 * 1024))
            .datagram_send_buffer_size(64 * 1024);
        config.transport_config(Arc::new(transport));
        let endpoint =
            quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
        let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let connection = endpoint
            .connect_with(config, remote, "127.0.0.1")
            .unwrap()
            .await
            .unwrap();
        let url: url::Url = format!("https://127.0.0.1:{port}/omq").parse().unwrap();
        let session = omq_webtrans_quinn::Session::connect_with_origin(
            connection,
            url,
            Some("https://app.example.com".into()),
        )
        .await
        .unwrap();
        let (mut data_send, data_recv) = session.open_bi().await.unwrap();
        let (mut liveness_send, liveness_recv) = session.open_bi().await.unwrap();
        data_send.write_all(DATA_PREFACE).await.unwrap();
        liveness_send.write_all(LIVENESS_PREFACE).await.unwrap();
        let mut peer = Self {
            _endpoint: endpoint,
            _session: session,
            data_send,
            data_recv,
            _liveness_send: liveness_send,
            _liveness_recv: liveness_recv,
            codec: Connection::new(ConnectionConfig::new(Role::Client, socket_type)),
        };
        tokio::time::timeout(Duration::from_secs(5), peer.handshake())
            .await
            .expect("WebTransport ZMTP handshake timed out");
        peer
    }

    async fn flush(&mut self) {
        loop {
            let bytes = self.codec.poll_transmit();
            if bytes.is_empty() {
                break;
            }
            self.data_send.write_all(&bytes).await.unwrap();
            self.codec.advance_transmit(bytes.len());
        }
    }

    async fn read(&mut self) {
        let mut buf = vec![0; 64 * 1024];
        let size = self
            .data_recv
            .read(&mut buf)
            .await
            .unwrap()
            .expect("data EOF");
        self.codec
            .handle_input(Bytes::copy_from_slice(&buf[..size]))
            .unwrap();
    }

    async fn handshake(&mut self) {
        while !self.codec.is_ready() {
            self.flush().await;
            self.read().await;
        }
        self.flush().await;
    }

    async fn send(&mut self, message: Message) {
        self.codec.send_message(&message).unwrap();
        self.flush().await;
    }

    async fn recv(&mut self) -> Message {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(message) = self.codec.poll_message() {
                    return message;
                }
                self.read().await;
                self.flush().await;
            }
        })
        .await
        .expect("WebTransport message timed out")
    }
}

#[tokio::test]
async fn one_quic_listener_accepts_raw_and_webtransport() {
    let tls = tls();
    let pull = Socket::new(SocketType::Pull, server_options(&tls));
    let bound = pull.bind("quic://127.0.0.1:0").await.unwrap();
    let port = port_of(&bound);

    let mut raw_options = Options::default();
    raw_options.quic.trust_pem = Some(tls.cert.clone());
    raw_options.quic.trust_system = false;
    let raw = Socket::new(SocketType::Push, raw_options);
    raw.connect(format!("quic://127.0.0.1:{port}").as_str())
        .await
        .unwrap();
    let mut web_peer = WebTransportPeer::connect(&tls.cert, port, SocketType::Push).await;
    raw.send(Message::single("raw")).await.unwrap();
    web_peer
        .send(Message::multipart([
            Bytes::from_static(b"web"),
            Bytes::new(),
        ]))
        .await;

    let first = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), pull.recv())
        .await
        .unwrap()
        .unwrap();
    let messages = [first, second];
    assert!(
        messages
            .iter()
            .any(|m| m.part_bytes(0).as_deref() == Some(&b"raw"[..]))
    );
    assert!(
        messages
            .iter()
            .any(|m| m.part_bytes(0).as_deref() == Some(&b"web"[..]) && m.len() == 2)
    );

    let large = Bytes::from(vec![9u8; 2 * 1024 * 1024]);
    web_peer.send(Message::single(large.clone())).await;
    let received = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.part_bytes(0).as_deref(), Some(&large[..]));
}

#[tokio::test]
async fn webtransport_receives_zmtp_messages() {
    let tls = tls();
    let push = Socket::new(SocketType::Push, server_options(&tls));
    let bound = push.bind("quic://127.0.0.1:0").await.unwrap();
    let mut web_peer =
        WebTransportPeer::connect(&tls.cert, port_of(&bound), SocketType::Pull).await;
    push.send(Message::single("from server")).await.unwrap();
    let message = web_peer.recv().await;
    assert_eq!(message.part_bytes(0).as_deref(), Some(&b"from server"[..]));

    let large = Bytes::from(vec![7u8; 2 * 1024 * 1024]);
    push.send(Message::single(large.clone())).await.unwrap();
    assert_eq!(
        web_peer.recv().await.part_bytes(0).as_deref(),
        Some(&large[..])
    );
}

#[tokio::test]
async fn webtransport_req_rep() {
    let tls = tls();
    let rep = Socket::new(SocketType::Rep, server_options(&tls));
    let bound = rep.bind("quic://127.0.0.1:0").await.unwrap();
    let mut web_peer = WebTransportPeer::connect(&tls.cert, port_of(&bound), SocketType::Req).await;

    web_peer
        .send(Message::multipart([
            Bytes::new(),
            Bytes::from_static(b"question"),
        ]))
        .await;
    let request = tokio::time::timeout(Duration::from_secs(5), rep.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.part_bytes(0).as_deref(), Some(&b"question"[..]));
    rep.send(Message::single("answer")).await.unwrap();
    let reply = web_peer.recv().await;
    assert_eq!(reply.len(), 2);
    assert_eq!(reply.part_bytes(0).as_deref(), Some(&b""[..]));
    assert_eq!(reply.part_bytes(1).as_deref(), Some(&b"answer"[..]));
}
