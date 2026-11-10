use super::*;
use std::net::{IpAddr, Ipv4Addr};

fn loopback_port_zero() -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
    }
}

#[tokio::test]
async fn bind_connect_accept() {
    let mut listener = TcpTransport::bind(&loopback_port_zero()).await.unwrap();
    let local = listener.local_endpoint().clone();

    let Endpoint::Tcp { port, .. } = local else {
        panic!()
    };

    assert!(port != 0, "OS should assign a port");

    let connect_target = Endpoint::Tcp {
        host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port,
    };
    let connect = tokio::spawn(async move { TcpTransport::connect(&connect_target).await });
    let (_srv_stream, peer) = listener.accept().await.unwrap();
    let _cli_stream = connect.await.unwrap().unwrap();

    match peer {
        PeerIdent::Socket(_) => {}
        _ => panic!("expected Socket peer ident"),
    }
}

#[tokio::test]
async fn bind_rejects_non_tcp_endpoint() {
    let ep = Endpoint::Inproc { name: "x".into() };
    assert!(matches!(
        TcpTransport::bind(&ep).await,
        Err(Error::InvalidEndpoint(_))
    ));
}

#[tokio::test]
async fn connect_rejects_wildcard() {
    let ep = Endpoint::Tcp {
        host: Host::Wildcard,
        port: 5555,
    };
    assert!(matches!(
        TcpTransport::connect(&ep).await,
        Err(Error::InvalidEndpoint(_))
    ));
}

#[tokio::test]
async fn connect_tries_later_resolved_address() {
    let bad = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let bad_addr = bad.local_addr().unwrap();
    let listener = TokioTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let good_addr = listener.local_addr().unwrap();
    drop(bad);

    let connect = tokio::spawn(connect_any(vec![bad_addr, good_addr]));
    let (_accepted, _) = listener.accept().await.unwrap();
    let stream = connect.await.unwrap().unwrap();

    assert_eq!(stream.peer_addr().unwrap(), good_addr);
}
