use super::*;
use bytes::Bytes;
use omq_proto::proto::SocketType;
use tokio::io::AsyncWrite;
#[cfg(unix)]
use tokio::net::UnixStream;

fn snapshot() -> InprocPeerSnapshot {
    InprocPeerSnapshot {
        socket_type: SocketType::Pair,
        identity: Bytes::new(),
    }
}

async fn bind_result_for_test(endpoint: &Endpoint) -> Result<BoundListener> {
    #[cfg(any(feature = "ws", feature = "quic"))]
    let carrier_options = omq_proto::Options::default();
    let inproc_registry = Arc::new(inproc_transport::InprocRegistry::new());
    bind_any(
        &inproc_registry,
        endpoint,
        &snapshot(),
        &inproc_transport::RecvConfig {
            direct: false,
            send_hwm: 1000,
        },
        #[cfg(any(feature = "ws", feature = "quic"))]
        CarrierAccept {
            options: &carrier_options,
            setup: crate::transport::setup::AcceptSetup {
                admission: crate::transport::setup::Admission::new(128),
                timeout: std::time::Duration::from_secs(30),
                cancel: tokio_util::sync::CancellationToken::new(),
                monitor: crate::socket::monitor::MonitorPublisher::new(
                    omq_proto::options::DEFAULT_MONITOR_CAPACITY,
                ),
                io_pool: crate::context::IoPoolHandle::none(),
            },
        },
    )
    .await
}

async fn bind_for_test(endpoint: &Endpoint) -> BoundListener {
    bind_result_for_test(endpoint).await.unwrap()
}

#[test]
fn any_stream_tcp_reports_write_vectored() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tcp = TcpStream::connect(addr).await.unwrap();
        assert!(tcp.is_write_vectored());
        let any = AnyStream::Tcp(tcp);
        assert!(any.is_write_vectored());
    });
}

#[cfg(unix)]
#[test]
fn any_stream_ipc_reports_write_vectored() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("omq-test-writev-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let _ = listener.accept().await.unwrap();
        assert!(client.is_write_vectored());
        let any = AnyStream::Ipc(client);
        assert!(any.is_write_vectored());
        let _ = std::fs::remove_file(&path);
    });
}

#[tokio::test]
async fn bind_any_returns_resolved_tcp_endpoint() {
    let endpoint = Endpoint::Tcp {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
    };
    let bound = bind_for_test(&endpoint).await;

    match bound.endpoint {
        Endpoint::Tcp {
            host: Host::Ip(ip),
            port,
        } => {
            assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            assert_ne!(port, 0);
        }
        other => panic!("expected resolved TCP endpoint, got {other:?}"),
    }
}

#[tokio::test]
async fn bind_any_rejects_unresolved_tcp_name() {
    let endpoint = Endpoint::Tcp {
        host: Host::Name("omq-bind-preflight.invalid".into()),
        port: 5555,
    };

    assert!(bind_result_for_test(&endpoint).await.is_err());
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn bind_any_returns_resolved_ws_endpoint() {
    let endpoint = Endpoint::Ws {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
        path: "/z".into(),
    };
    let bound = bind_for_test(&endpoint).await;

    match bound.listener.local_endpoint() {
        Endpoint::Ws {
            host: Host::Ip(ip),
            port,
            path,
        } => {
            assert_eq!(*ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            assert_ne!(*port, 0);
            assert_eq!(path, "/z");
        }
        other => panic!("expected resolved WS listener endpoint, got {other:?}"),
    }
    match bound.endpoint {
        Endpoint::Ws {
            host: Host::Ip(ip),
            port,
            path,
        } => {
            assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            assert_ne!(port, 0);
            assert_eq!(path, "/z");
        }
        other => panic!("expected resolved WS endpoint, got {other:?}"),
    }
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn bind_any_rejects_unresolved_ws_name() {
    let endpoint = Endpoint::Ws {
        host: Host::Name("omq-bind-ws-preflight.invalid".into()),
        port: 5555,
        path: "/z".into(),
    };

    assert!(bind_result_for_test(&endpoint).await.is_err());
}

#[cfg(all(feature = "lz4", feature = "ws"))]
#[tokio::test]
async fn bind_any_returns_resolved_lz4_ws_endpoint() {
    let endpoint = Endpoint::Lz4Ws {
        host: Host::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        port: 0,
        path: "/lz4".into(),
    };
    let bound = bind_for_test(&endpoint).await;

    match bound.endpoint {
        Endpoint::Lz4Ws {
            host: Host::Ip(ip),
            port,
            path,
        } => {
            assert_eq!(ip, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            assert_ne!(port, 0);
            assert_eq!(path, "/lz4");
        }
        other => panic!("expected resolved LZ4 WS endpoint, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_preflight_rejects_tcp_wildcard() {
    let endpoint = Endpoint::Tcp {
        host: Host::Wildcard,
        port: 5555,
    };

    assert!(matches!(
        preflight_connect_endpoint_resolution(&endpoint).await,
        Err(Error::InvalidEndpoint(_))
    ));
}

#[tokio::test]
async fn connect_preflight_rejects_unresolved_tcp_name() {
    let endpoint = Endpoint::Tcp {
        host: Host::Name("omq-connect-preflight.invalid".into()),
        port: 5555,
    };

    assert!(
        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn connect_preflight_accepts_named_tcp_without_socket_connect() {
    let endpoint = Endpoint::Tcp {
        host: Host::Name("127.0.0.1".into()),
        port: 9,
    };

    preflight_connect_endpoint_resolution(&endpoint)
        .await
        .unwrap();
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn connect_preflight_accepts_named_lz4_tcp_without_socket_connect() {
    let endpoint = Endpoint::Lz4Tcp {
        host: Host::Name("127.0.0.1".into()),
        port: 9,
    };

    preflight_connect_endpoint_resolution(&endpoint)
        .await
        .unwrap();
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn connect_preflight_rejects_unresolved_ws_name() {
    let endpoint = Endpoint::Ws {
        host: Host::Name("omq-connect-ws-preflight.invalid".into()),
        port: 5555,
        path: "/z".into(),
    };

    assert!(
        preflight_connect_endpoint_resolution(&endpoint)
            .await
            .is_err()
    );
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn connect_preflight_accepts_named_ws_without_socket_connect() {
    let endpoint = Endpoint::Ws {
        host: Host::Name("127.0.0.1".into()),
        port: 9,
        path: "/z".into(),
    };

    preflight_connect_endpoint_resolution(&endpoint)
        .await
        .unwrap();
}

#[cfg(all(feature = "lz4", feature = "ws"))]
#[tokio::test]
async fn connect_preflight_accepts_named_lz4_ws_without_socket_connect() {
    let endpoint = Endpoint::Lz4Ws {
        host: Host::Name("127.0.0.1".into()),
        port: 9,
        path: "/z".into(),
    };

    preflight_connect_endpoint_resolution(&endpoint)
        .await
        .unwrap();
}
