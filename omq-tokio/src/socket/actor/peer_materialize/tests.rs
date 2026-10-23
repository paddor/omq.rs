use super::recv_bypass::{can_use_yring_recv_bypass, fanin_recv_sink};
use super::*;

#[cfg(target_os = "linux")]
#[test]
fn shm_endpoint_assignments_stay_on_one_thread_despite_peer_load() {
    let context = crate::Context::with_config(crate::ContextConfig { io_threads: 4 });
    let pool = context.core().io_pool_handle();
    let endpoint: Endpoint = "shm://@single-lane-test".parse().unwrap();
    let first = reserve_peer_io(&pool, &endpoint);
    let peers: Vec<_> = (0..32).map(|_| reserve_peer_io(&pool, &endpoint)).collect();
    assert!(peers.iter().all(|peer| peer.index() == first.index()));
    let thread = first.index();
    drop(peers);
    drop(first);
    assert_eq!(reserve_peer_io(&pool, &endpoint).index(), thread);
}

#[tokio::test]
async fn fanin_attachment_tolerates_receive_shutdown_before_actor_close() {
    let blocking = crate::socket::recv::BlockingRecvWaker::new();
    let signal = Arc::new(crate::engine::signal::DataSignal::new());
    let fanin = crate::socket::fanin::Fanin::new(16, signal, blocking.clone());
    let (recv_tx, _consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking);
    let crate::engine::RecvSink::Fanin(mut live) = fanin_recv_sink(&fanin, &recv_tx) else {
        panic!("live receive side must retain the fan-in bypass");
    };
    assert!(live.push(omq_proto::Message::single("before-close")));
    live.flush();
    assert_eq!(
        fanin.try_recv().unwrap().part_slice(0).unwrap(),
        b"before-close"
    );

    // Last-handle drop closes fan-in before the actor sees its command
    // channel close. A queued Accepted/Connected event can run meanwhile.
    fanin.close();
    assert!(!live.push(omq_proto::Message::single("after-close")));
    let crate::engine::RecvSink::Channel(closed) = fanin_recv_sink(&fanin, &recv_tx) else {
        panic!("late attachment must use a closed receive sink");
    };
    assert!(Arc::ptr_eq(&closed, &recv_tx));
    assert!(matches!(
        closed.send(omq_proto::Message::single("late-peer")).await,
        Err(omq_proto::Error::Closed)
    ));
}

#[test]
fn req_uses_yring_recv_bypass_only_for_latency_profile() {
    assert!(can_use_yring_recv_bypass(SocketType::Req, true));
    assert!(!can_use_yring_recv_bypass(SocketType::Req, false));
    assert!(can_use_yring_recv_bypass(SocketType::Pull, false));
    assert!(!can_use_yring_recv_bypass(SocketType::Client, false));
}

#[test]
fn direct_tcp_writer_supports_latency_round_robin_types() {
    for socket_type in [
        SocketType::Req,
        SocketType::Rep,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Server,
        SocketType::Client,
        SocketType::Pair,
    ] {
        assert!(supports_direct_tcp_writer(socket_type));
    }
    assert!(!supports_direct_tcp_writer(SocketType::Push));
    assert!(supports_direct_tcp_writer(SocketType::Channel));
    assert!(supports_direct_tcp_writer(SocketType::Peer));
}
