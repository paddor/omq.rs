//! Socket-type x transport coverage matrix for omq-tokio.

mod test_support;

use std::time::Duration;

use bytes::Bytes;
use omq_tokio::{
    CompressionOptions, Context, Endpoint, Error, Message, Options, PayloadPool, Socket, SocketType,
};

#[tokio::test]
async fn endpoint_arguments_work_with_owned_async_sockets() {
    let context = Context::new();
    let uri = "tcp://127.0.0.1:0".to_owned();
    let typed: Endpoint = uri.parse().unwrap();
    for argument in 0..5 {
        let pull = context.socket(SocketType::Pull, Options::default());
        let push = context.socket(SocketType::Push, Options::default());
        let bound = match argument {
            0 => omq_proto::socket_api::SocketApi::bind(&pull, uri.as_str()).await,
            1 => pull.bind(uri.clone()).await,
            2 => pull.bind(&uri).await,
            3 => pull.bind(typed.clone()).await,
            _ => pull.bind(&typed).await,
        }
        .unwrap();
        let address = bound.to_string();
        match argument {
            0 => push.connect(address.as_str()).await,
            1 => push.connect(address.clone()).await,
            2 => push.connect(&address).await,
            3 => push.connect(bound.clone()).await,
            _ => omq_proto::socket_api::SocketApi::connect(&push, &bound).await,
        }
        .unwrap();
        push.send(Message::single("endpoint arguments"))
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(5), pull.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message, Message::single("endpoint arguments"));
        omq_proto::socket_api::SocketApi::disconnect(&push, &bound)
            .await
            .unwrap();
        omq_proto::socket_api::SocketApi::unbind(&pull, address)
            .await
            .unwrap();
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[test]
fn endpoint_arguments_work_with_blocking_sockets() {
    let context = Context::new();
    let uri = "tcp://127.0.0.1:0".to_owned();
    let typed: Endpoint = uri.parse().unwrap();
    for argument in 0..5 {
        let pull = context.blocking_socket(SocketType::Pull, Options::default());
        let push = context.blocking_socket(SocketType::Push, Options::default());
        let bound = match argument {
            0 => pull.bind(uri.as_str()),
            1 => pull.bind(uri.clone()),
            2 => pull.bind(&uri),
            3 => pull.bind(typed.clone()),
            _ => pull.bind(&typed),
        }
        .unwrap();
        let address = bound.to_string();
        match argument {
            0 => push.connect(address.as_str()),
            1 => push.connect(address.clone()),
            2 => push.connect(&address),
            3 => push.connect(bound.clone()),
            _ => push.connect(&bound),
        }
        .unwrap();
        push.send(Message::single("endpoint arguments")).unwrap();
        assert_eq!(
            pull.recv_timeout(Duration::from_secs(5)).unwrap(),
            Message::single("endpoint arguments")
        );
        push.disconnect(&bound).unwrap();
        pull.unbind(address).unwrap();
        push.close().unwrap();
        pull.close().unwrap();
    }
}

#[test]
fn endpoint_arguments_work_with_blocking_deadlines() {
    let context = Context::new();
    let pull = context.blocking_socket(SocketType::Pull, Options::default());
    let push = context.blocking_socket(SocketType::Push, Options::default());
    let uri = "tcp://127.0.0.1:0".to_owned();
    let bound = pull.bind_timeout(&uri, Duration::from_secs(5)).unwrap();
    push.connect_timeout(&bound, Duration::from_secs(5))
        .unwrap();
    push.send(Message::single("deadline arguments")).unwrap();
    assert_eq!(
        pull.recv_timeout(Duration::from_secs(5)).unwrap(),
        Message::single("deadline arguments")
    );
    push.close().unwrap();
    pull.close().unwrap();
}

#[tokio::test]
async fn invalid_endpoint_arguments_do_not_freeze_async_configuration() {
    let context = Context::new();
    let socket = context.socket(SocketType::Pull, Options::default());
    for uri in ["invalid", "unknown://host"] {
        for error in [
            socket.bind(uri).await.unwrap_err(),
            socket.connect(uri).await.unwrap_err(),
            socket.unbind(uri).await.unwrap_err(),
            socket.disconnect(uri).await.unwrap_err(),
            socket
                .bind_with_compression_options(uri, CompressionOptions::default())
                .await
                .unwrap_err(),
            socket
                .connect_with_compression_options(uri, CompressionOptions::default())
                .await
                .unwrap_err(),
        ] {
            assert!(matches!(
                error,
                Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_)
            ));
        }
    }
    assert!(socket.last_bound_endpoint().is_none());
    socket
        .set_recv_payload_pool(PayloadPool::new([(128, 1)]).unwrap())
        .unwrap();
    socket.bind("tcp://127.0.0.1:0").await.unwrap();
    socket.close().await.unwrap();
}

#[test]
fn invalid_endpoint_arguments_do_not_freeze_blocking_configuration() {
    let context = Context::new();
    let socket = context.blocking_socket(SocketType::Pull, Options::default());
    for uri in ["invalid", "unknown://host"] {
        for error in [
            socket.bind(uri).unwrap_err(),
            socket.connect(uri).unwrap_err(),
            socket.unbind(uri).unwrap_err(),
            socket.disconnect(uri).unwrap_err(),
            socket
                .bind_timeout(uri, Duration::from_secs(5))
                .unwrap_err(),
            socket
                .connect_timeout(uri, Duration::from_secs(5))
                .unwrap_err(),
        ] {
            assert!(matches!(
                error,
                Error::InvalidEndpoint(_) | Error::UnsupportedScheme(_)
            ));
        }
    }
    assert!(socket.last_bound_endpoint().is_none());
    socket
        .set_recv_payload_pool(PayloadPool::new([(128, 1)]).unwrap())
        .unwrap();
    socket.bind("tcp://127.0.0.1:0").unwrap();
    socket.close().unwrap();
}

fn ipc_ep(name: &str) -> Endpoint {
    test_support::ipc_endpoint(&format!("cov-{name}"))
}

fn inproc_ep(name: &str) -> Endpoint {
    Endpoint::Inproc {
        name: format!(
            "cov-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    }
}

async fn wait() {
    tokio::time::sleep(Duration::from_millis(60)).await;
}

async fn push_pull_roundtrip(server: &Socket, client_ep: Endpoint) {
    let push = Socket::new(SocketType::Push, Options::default());
    push.connect(client_ep).await.unwrap();
    push.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

#[tokio::test]
async fn explicit_buffers_work_over_inproc_and_tcp() {
    for endpoint in [inproc_ep("buffers"), test_support::tcp_loopback(0)] {
        let receive_pool = PayloadPool::new([(128, 1), (4096, 1)]).unwrap();
        let pull = Socket::new(SocketType::Pull, Options::default());
        pull.set_recv_payload_pool(receive_pool.clone()).unwrap();
        let push = Socket::new(SocketType::Push, Options::default());
        let bound = pull.bind(endpoint).await.unwrap();
        assert!(matches!(
            pull.set_recv_payload_pool(receive_pool),
            Err(omq_tokio::Error::Config(_))
        ));
        push.connect(bound).await.unwrap();
        let pool = PayloadPool::new([(128, 1)]).unwrap();
        let clone = pool.clone();
        for size in [0, 16, 55, 56, 128, 129, 4096] {
            let message = pool.message(size, |body| body.fill(7)).unwrap();
            let held = message.clone();
            let pooled = (56..=128).contains(&size);
            push.send(message).await.unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), pull.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(received.part_slice(0), Some(vec![7; size].as_slice()));
            if pooled {
                assert!(
                    clone
                        .try_message(size, |_| panic!("still held"))
                        .unwrap()
                        .is_none()
                );
            } else {
                assert_eq!(clone.available(), 1);
            }
            drop(held);
            drop(received);
        }
        push.close().await.unwrap();
        pull.close().await.unwrap();
    }
}

#[test]
fn explicit_buffers_work_with_blocking_sockets_and_outlive_them() {
    let context = omq_tokio::Context::new();
    let pull = context.blocking_socket(SocketType::Pull, Options::default().recv_hwm(4));
    let push = context.blocking_socket(SocketType::Push, Options::default().send_hwm(8192));
    let pools = pull.init_payload_pools().unwrap();
    assert!(pools.send.is_none());
    assert_eq!(
        pools.recv.unwrap().classes().collect::<Vec<_>>(),
        vec![(4096, 4)]
    );
    let pools = push.init_payload_pools().unwrap();
    assert!(pools.recv.is_none());
    assert_eq!(
        pools.send.unwrap().classes().collect::<Vec<_>>(),
        vec![(2048, 8192)]
    );
    let bound = pull.bind(test_support::tcp_loopback(0)).unwrap();
    assert!(matches!(
        pull.init_payload_pools(),
        Err(omq_tokio::Error::Config(_))
    ));
    push.connect(bound).unwrap();
    let pool = PayloadPool::new([(128, 1)]).unwrap();
    for size in [16, 128, 129] {
        let message = pool.message(size, |body| body.fill(9)).unwrap();
        push.send(message).unwrap();
        let received = pull.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(received.part_slice(0), Some(vec![9; size].as_slice()));
    }
    // Explicit pools and checked-out buffers have no socket lifecycle dependency.
    let mut buffer = pool.try_buffer(1).unwrap();
    buffer.writable()[..64].fill(3);
    buffer.set_len(64).unwrap();
    push.close().unwrap();
    pull.close().unwrap();
    context.term();
    let message = buffer.into_message();
    assert_eq!(message.part_slice(0), Some([3; 64].as_slice()));
    assert!(pool.try_buffer(1).is_none());
    drop(message);
    assert_eq!(pool.available(), 1);
}

async fn req_rep_roundtrip(server: &Socket, client_ep: Endpoint) {
    let req = Socket::new(SocketType::Req, Options::default());
    req.connect(client_ep).await.unwrap();
    req.send(Message::single("q")).await.unwrap();
    let q = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q, Message::single("q"));
    server.send(Message::single("a")).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(2), req.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a, Message::single("a"));
}

async fn dealer_router_roundtrip(server: &Socket, client_ep: Endpoint) {
    let dealer = Socket::new(
        SocketType::Dealer,
        Options::default().identity(Bytes::from_static(b"d1")),
    );
    dealer.connect(client_ep).await.unwrap();
    wait().await;
    dealer.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["d1", "hi"]));
    server
        .send(Message::multipart(["d1", "reply"]))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), dealer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("reply"));
}

async fn pair_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(SocketType::Pair, Options::default());
    b.connect(client_ep).await.unwrap();
    server.send(Message::single("x")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("x"));
}

async fn pub_sub_roundtrip(server: &Socket, client_ep: Endpoint) {
    let s = Socket::new(SocketType::Sub, Options::default());
    s.subscribe("").await.unwrap();
    s.connect(client_ep).await.unwrap();
    for _ in 0..30 {
        let _ = server.send(Message::single("hello")).await;
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
            assert_eq!(m, Message::single("hello"));
            return;
        }
    }
    panic!("SUB never received");
}

async fn client_server_roundtrip(server: &Socket, client_ep: Endpoint) {
    let client = Socket::new(
        SocketType::Client,
        Options::default().identity(Bytes::from_static(b"c1")),
    );
    client.connect(client_ep).await.unwrap();
    wait().await;
    client.send(Message::single("ping")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("ping"));
    let routing_id = m.routing_id().expect("SERVER routing id");
    server
        .send(Message::single("pong").with_routing_id(routing_id))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r, Message::single("pong"));
}

async fn scatter_gather_roundtrip(server: &Socket, client_ep: Endpoint) {
    let s = Socket::new(SocketType::Scatter, Options::default());
    s.connect(client_ep).await.unwrap();
    wait().await;
    s.send(Message::single("m")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("m"));
}

async fn channel_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(SocketType::Channel, Options::default());
    b.connect(client_ep).await.unwrap();
    wait().await;
    server.send(Message::single("hi")).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::single("hi"));
}

async fn peer_roundtrip(server: &Socket, client_ep: Endpoint) {
    let b = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pb")),
    );
    b.connect(client_ep).await.unwrap();
    wait().await;
    b.send(Message::multipart(["pa", "hi a"])).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m, Message::multipart(["pb", "hi a"]));
}

#[tokio::test]
async fn push_pull_inproc() {
    let ep = inproc_ep("pp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    push_pull_roundtrip(&pull, ep).await;
}
#[tokio::test]
async fn req_rep_inproc() {
    let ep = inproc_ep("rr");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    req_rep_roundtrip(&rep, ep).await;
}
#[tokio::test]
async fn dealer_router_inproc() {
    let ep = inproc_ep("dr");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();
    dealer_router_roundtrip(&router, ep).await;
}
#[tokio::test]
async fn pair_inproc() {
    let ep = inproc_ep("pair");
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(ep.clone()).await.unwrap();
    pair_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn pub_sub_inproc() {
    let ep = inproc_ep("ps");
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(ep.clone()).await.unwrap();
    pub_sub_roundtrip(&p, ep).await;
}
#[tokio::test]
async fn client_server_inproc() {
    let ep = inproc_ep("cs");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();
    client_server_roundtrip(&server, ep).await;
}
#[tokio::test]
async fn scatter_gather_inproc() {
    let ep = inproc_ep("sg");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();
    scatter_gather_roundtrip(&gather, ep).await;
}
#[tokio::test]
async fn channel_inproc() {
    let ep = inproc_ep("ch");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    channel_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn peer_inproc() {
    let ep = inproc_ep("pp");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep.clone()).await.unwrap();
    peer_roundtrip(&a, ep).await;
}

#[tokio::test]
async fn recv_spin_does_not_block_current_thread_runtime() {
    let ctx = omq_tokio::Context::current();
    let ep = inproc_ep("recv-spin-async");
    let pull = ctx.socket(
        SocketType::Pull,
        Options::default().recv_spin(Duration::from_secs(5)),
    );
    pull.bind(ep.clone()).await.unwrap();
    let push = ctx.socket(SocketType::Push, Options::default());
    push.connect(ep).await.unwrap();
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        push.send(Message::single("ready")).await.unwrap();
        push
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pull.recv())
            .await
            .unwrap()
            .unwrap(),
        Message::single("ready")
    );
    let _push = sender.await.unwrap();
}

#[tokio::test]
async fn push_pull_ipc() {
    let ep = ipc_ep("pp");
    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(ep.clone()).await.unwrap();
    push_pull_roundtrip(&pull, ep).await;
}
#[tokio::test]
async fn req_rep_ipc() {
    let ep = ipc_ep("rr");
    let rep = Socket::new(SocketType::Rep, Options::default());
    rep.bind(ep.clone()).await.unwrap();
    req_rep_roundtrip(&rep, ep).await;
}
#[tokio::test]
async fn dealer_router_ipc() {
    let ep = ipc_ep("dr");
    let router = Socket::new(SocketType::Router, Options::default());
    router.bind(ep.clone()).await.unwrap();
    dealer_router_roundtrip(&router, ep).await;
}
#[tokio::test]
async fn pair_ipc() {
    let ep = ipc_ep("pair");
    let a = Socket::new(SocketType::Pair, Options::default());
    a.bind(ep.clone()).await.unwrap();
    pair_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn pub_sub_ipc() {
    let ep = ipc_ep("ps");
    let p = Socket::new(SocketType::Pub, Options::default());
    p.bind(ep.clone()).await.unwrap();
    pub_sub_roundtrip(&p, ep).await;
}
#[tokio::test]
async fn client_server_ipc() {
    let ep = ipc_ep("cs");
    let server = Socket::new(SocketType::Server, Options::default());
    server.bind(ep.clone()).await.unwrap();
    client_server_roundtrip(&server, ep).await;
}
#[tokio::test]
async fn scatter_gather_ipc() {
    let ep = ipc_ep("sg");
    let gather = Socket::new(SocketType::Gather, Options::default());
    gather.bind(ep.clone()).await.unwrap();
    scatter_gather_roundtrip(&gather, ep).await;
}
#[tokio::test]
async fn channel_ipc() {
    let ep = ipc_ep("ch");
    let a = Socket::new(SocketType::Channel, Options::default());
    a.bind(ep.clone()).await.unwrap();
    channel_roundtrip(&a, ep).await;
}
#[tokio::test]
async fn peer_ipc() {
    let ep = ipc_ep("pp");
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    a.bind(ep.clone()).await.unwrap();
    peer_roundtrip(&a, ep).await;
}

#[tokio::test]
async fn push_pull_tcp() {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let port = test_support::bind_loopback(&pull).await;
    push_pull_roundtrip(&pull, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn req_rep_tcp() {
    let rep = Socket::new(SocketType::Rep, Options::default());
    let port = test_support::bind_loopback(&rep).await;
    req_rep_roundtrip(&rep, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn dealer_router_tcp() {
    let router = Socket::new(SocketType::Router, Options::default());
    let port = test_support::bind_loopback(&router).await;
    dealer_router_roundtrip(&router, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn pair_tcp() {
    let a = Socket::new(SocketType::Pair, Options::default());
    let port = test_support::bind_loopback(&a).await;
    pair_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn pub_sub_tcp() {
    let p = Socket::new(SocketType::Pub, Options::default());
    let port = test_support::bind_loopback(&p).await;
    pub_sub_roundtrip(&p, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn client_server_tcp() {
    let server = Socket::new(SocketType::Server, Options::default());
    let port = test_support::bind_loopback(&server).await;
    client_server_roundtrip(&server, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn scatter_gather_tcp() {
    let gather = Socket::new(SocketType::Gather, Options::default());
    let port = test_support::bind_loopback(&gather).await;
    scatter_gather_roundtrip(&gather, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn channel_tcp() {
    let a = Socket::new(SocketType::Channel, Options::default());
    let port = test_support::bind_loopback(&a).await;
    channel_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
#[tokio::test]
async fn peer_tcp() {
    let a = Socket::new(
        SocketType::Peer,
        Options::default().identity(Bytes::from_static(b"pa")),
    );
    let port = test_support::bind_loopback(&a).await;
    peer_roundtrip(&a, test_support::tcp_loopback(port)).await;
}
