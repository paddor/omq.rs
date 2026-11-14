use super::*;
use crate::socket::actor::{DriverEnv, HandleLink, HandleShared};
use omq_proto::Message;
use omq_proto::error::TrySendError;

#[tokio::test]
async fn late_peer_after_fanin_close_gets_closed_receive() {
    let options = Options::default();
    let pool = crate::context::IoPoolHandle::none();

    let blocking = crate::socket::recv::BlockingRecvWaker::new();
    let (recv_tx, _consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
    let mut spsc = crate::socket::recv::SpscHandles::new(blocking.clone(), false);
    let fanin = crate::socket::fanin::Fanin::new(16, spsc.recv_signal.clone(), blocking);
    spsc.fanin = Some(fanin.clone());
    let (_, cmd_rx) = mpsc::channel(1);

    let link = HandleLink {
        cmd_rx,
        recv_tx: recv_tx.clone(),
        cancel: CancellationToken::new(),
        monitor: crate::socket::monitor::MonitorPublisher::new(16),
        spsc,
        peer_recv_routes: None,
        shared: HandleShared::new(),
    };

    let env = DriverEnv {
        io_pool: pool.clone(),
        inproc_registry: Arc::new(crate::transport::inproc::InprocRegistry::new()),
        recv_sink_config: None,
    };
    let send_strategy =
        crate::routing::SendStrategy::for_socket_type(SocketType::Pull, &options, &pool);
    let mut driver = SocketDriver::new(SocketType::Pull, options, send_strategy, link, env);

    // Last-handle drop closes fan-in before a late DART peer attaches.
    fanin.close();
    let RecvSink::Channel(pipe) = driver.dart_recv_sink(0) else {
        panic!("late attachment must use the shared receive pipe");
    };

    assert!(Arc::ptr_eq(&pipe, &recv_tx));
    assert!(matches!(
        pipe.try_send(Message::single("late-peer")),
        Err(TrySendError::Closed)
    ));
}
