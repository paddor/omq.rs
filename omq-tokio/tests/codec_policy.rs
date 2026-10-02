//! Reject invalid local codec profiles before DNS or wire IO.

#![cfg(any(feature = "lz4", feature = "zstd"))]

#[cfg(feature = "curve")]
use omq_tokio::CurveKeypair;
use omq_tokio::{
    CompressionKind, CompressionOptions, Endpoint, Error, Options, Socket, SocketType,
};
#[cfg(feature = "curve")]
use std::time::Duration;

fn endpoints() -> Vec<Endpoint> {
    let mut endpoints = vec![
        #[cfg(feature = "lz4")]
        "lz4+tcp://omq-codec-policy.invalid:1".parse().unwrap(),
        #[cfg(feature = "zstd")]
        "zstd+tcp://omq-codec-policy.invalid:1".parse().unwrap(),
        #[cfg(all(feature = "ws", feature = "lz4"))]
        "lz4+ws://omq-codec-policy.invalid:1/omq".parse().unwrap(),
        #[cfg(feature = "lz4")]
        Endpoint::Lz4Tcp {
            host: omq_proto::endpoint::Host::Ip("127.0.0.1".parse().unwrap()),
            port: 1,
        },
        #[cfg(feature = "zstd")]
        Endpoint::ZstdTcp {
            host: omq_proto::endpoint::Host::Ip("127.0.0.1".parse().unwrap()),
            port: 1,
        },
        #[cfg(all(feature = "ws", feature = "lz4"))]
        Endpoint::Lz4Ws {
            host: omq_proto::endpoint::Host::Ip("127.0.0.1".parse().unwrap()),
            port: 1,
            path: "/omq".into(),
        },
    ];
    for kind in [
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ] {
        endpoints.push(
            "tcp://omq-codec-policy.invalid:1"
                .parse::<Endpoint>()
                .unwrap()
                .with_compression(kind)
                .unwrap(),
        );
    }
    #[cfg(all(feature = "lz4", feature = "ws"))]
    endpoints.push(
        "ws://omq-codec-policy.invalid:1/omq"
            .parse::<Endpoint>()
            .unwrap()
            .with_compression(CompressionKind::Lz4)
            .unwrap(),
    );
    endpoints
}

#[tokio::test]
async fn raw_stream_rejects_compression_without_attempting_dns() {
    for endpoint in endpoints().into_iter().filter(Endpoint::is_tcp_family) {
        let socket = Socket::new(SocketType::Stream, Options::default());
        assert!(matches!(
            socket.connect(endpoint.clone()).await,
            Err(Error::Config(_))
        ));
        assert!(matches!(socket.bind(endpoint).await, Err(Error::Config(_))));
        socket.close().await.unwrap();
    }
}

#[cfg(feature = "curve")]
#[tokio::test]
async fn encrypted_codecs_fail_before_dns_connect_or_bind() {
    let server = CurveKeypair::generate();
    let client = CurveKeypair::generate();
    let server_options = Options::default().curve_server(server.clone());
    let client_options = Options::default().curve_client(client, server.public);
    for options in [server_options, client_options] {
        let socket = Socket::new(SocketType::Dealer, options);
        for endpoint in endpoints() {
            assert!(matches!(
                socket
                    .connect_with_compression_options(
                        endpoint.clone(),
                        CompressionOptions::default()
                    )
                    .await,
                Err(Error::Config(_))
            ));
            assert!(matches!(
                socket
                    .bind_with_compression_options(endpoint.clone(), CompressionOptions::default())
                    .await,
                Err(Error::Config(_))
            ));
            let connect =
                tokio::time::timeout(Duration::from_secs(1), socket.connect(endpoint.clone()))
                    .await
                    .unwrap();
            assert!(matches!(connect, Err(Error::Config(_))));
            let bind = tokio::time::timeout(Duration::from_secs(1), socket.bind(endpoint))
                .await
                .unwrap();
            assert!(matches!(bind, Err(Error::Config(_))));
        }
        assert!(socket.connections().await.unwrap().is_empty());
        socket.close().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_codec_snapshots_fail_before_dns_or_carrier_io() {
    let socket = Socket::new(SocketType::Dealer, Options::default());
    for endpoint in endpoints() {
        let compression = CompressionOptions {
            level: Some(5),
            ..CompressionOptions::default()
        };
        assert!(matches!(
            socket
                .connect_with_compression_options(endpoint.clone(), compression.clone())
                .await,
            Err(Error::Config(_))
        ));
        assert!(matches!(
            socket
                .bind_with_compression_options(endpoint, compression)
                .await,
            Err(Error::Config(_))
        ));
    }
    #[cfg(feature = "zstd")]
    {
        let endpoint: Endpoint = "zstd+tcp://codec-snapshot.invalid:1".parse().unwrap();
        let compression = CompressionOptions {
            dict: Some(bytes::Bytes::from_static(b"invalid-zstd-dictionary")),
            ..CompressionOptions::default()
        };
        assert!(matches!(
            socket
                .connect_with_compression_options(endpoint.clone(), compression.clone())
                .await,
            Err(Error::Config(_))
        ));
        assert!(matches!(
            socket
                .bind_with_compression_options(endpoint, compression)
                .await,
            Err(Error::Config(_))
        ));
    }
    assert!(socket.connections().await.unwrap().is_empty());
    socket.close().await.unwrap();
}
