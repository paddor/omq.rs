use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{LatencySend, Peer};
use crate::engine::driver::DirectRoutes;
use crate::engine::transmit_slot::{PeerTransmitSlot, SlotConfig};
use crate::engine::{ActorPeerDriverHandle, PeerDriverCommand};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::frame_buffer::ARENA_THRESHOLD;
use omq_proto::message::Message;
use omq_proto::options::Options;

fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();

    (client, server)
}

fn direct_peer_handle(
    slot: Arc<PeerTransmitSlot>,
) -> (
    ActorPeerDriverHandle,
    tokio::sync::mpsc::Receiver<crate::engine::PeerDriverData>,
) {
    let (tcp, _peer) = tcp_pair();
    let direct = crate::socket::direct_writer::DirectTcpWriter::new(tcp);
    let (inbox, _rx) = tokio::sync::mpsc::channel::<PeerDriverCommand>(1);
    let (data_inbox, data_rx) = tokio::sync::mpsc::channel(1);

    (
        ActorPeerDriverHandle {
            inbox: inbox.into(),
            data_inbox: data_inbox.into(),
            cancel: CancellationToken::new(),
            routes: DirectRoutes {
                transmit_slot: Some(slot),
                direct_tcp_writer: Some(Arc::new(direct)),
                ..Default::default()
            },
        },
        data_rx,
    )
}

#[tokio::test]
async fn direct_writer_full_inbox_waits_for_space_signal() {
    let mut send = LatencySend::new(&Options::default().send_hwm(1));
    let submitter = send.submitter();

    let slot = PeerTransmitSlot::with_config(
        7,
        SlotConfig {
            arena_cap: 1024,
            cap: 1024 * 1024,
            msg_cap: 1,
            ..SlotConfig::default()
        },
    );

    slot.mark_handshake_done();
    let (handle, mut receiver) = direct_peer_handle(slot.clone());
    send.connection_added(7, &handle);

    submitter
        .try_send(Message::single(Bytes::from(vec![
            0x5a;
            ARENA_THRESHOLD + 1
        ])))
        .unwrap();

    let blocked = tokio::spawn({
        let submitter = submitter.clone();
        async move {
            submitter
                .send(Message::single(Bytes::from_static(b"after-space")))
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !blocked.is_finished(),
        "send should wait while inbox is full"
    );

    receiver.try_recv().unwrap();
    slot.space_available.notify_changed();

    tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("send did not wake after inbox space")
        .expect("send task panicked")
        .unwrap();
}

#[tokio::test]
async fn inbox_wait_observes_any_peer_capacity_and_route_retirement() {
    use futures::FutureExt;
    for peers in [1, 2] {
        let mut send = LatencySend::new(&Options::default());
        let submitter = send.submitter();
        let mut receivers = Vec::new();

        for id in 0..peers {
            let (data_inbox, receiver) = tokio::sync::mpsc::channel(1);
            let handle = ActorPeerDriverHandle {
                inbox: tokio::sync::mpsc::channel(1).0.into(),
                data_inbox: data_inbox.into(),
                cancel: CancellationToken::new(),
                routes: DirectRoutes::default(),
            };
            send.connection_added(id, &handle);
            receivers.push(receiver);
            submitter.try_send(Message::single("full")).unwrap();
        }

        assert!(matches!(
            submitter.try_send(Message::single("retry")),
            Err(omq_proto::TrySendError::Full(_))
        ));

        let wait = submitter.wait_send_progress();
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());

        receivers.last_mut().unwrap().try_recv().unwrap();
        assert!(
            wait.as_mut().now_or_never().is_some(),
            "any destination must wake the sender"
        );

        submitter.try_send(Message::single("refill")).unwrap();
        let retired = submitter.wait_send_progress();
        tokio::pin!(retired);
        assert!(retired.as_mut().now_or_never().is_none());

        send.connection_removed(peers - 1);
        assert!(retired.as_mut().now_or_never().is_some());
    }
}

#[test]
fn state_prefers_peer_space_before_pending_space() {
    let mut send = LatencySend::new(&Options::default().send_hwm(1));
    let _pending = send.make_connect_pipe(
        1,
        #[cfg(feature = "dart")]
        None,
    );

    let slot = PeerTransmitSlot::with_config(
        2,
        SlotConfig {
            arena_cap: 1024,
            cap: 1024 * 1024,
            msg_cap: 1,
            ..SlotConfig::default()
        },
    );

    let (handle, _receiver) = direct_peer_handle(slot.clone());
    send.connection_added(2, &handle);

    let state = send.state.lock().expect("latency send state");
    let peer_signal = match &state.peers[0] {
        Peer {
            target: PeerOutbound::Wire { slot, .. },
            ..
        } => slot.space_available.clone(),
        _ => panic!("expected wire peer"),
    };

    assert!(Arc::ptr_eq(
        &state.space_available().expect("space signal"),
        &peer_signal
    ));
}

#[test]
fn taken_connect_pipe_keeps_its_queue_and_routes_again_under_a_new_route() {
    let mut send = LatencySend::new(&Options::default().send_hwm(4));
    let mut rx = send.make_connect_pipe(
        1,
        #[cfg(feature = "dart")]
        None,
    );
    let submitter = send.submitter();
    submitter.try_send(Message::from_slice(b"queued")).unwrap();

    let tx = send.take_connect_pipe(1).expect("route 1 has a pipe");
    assert!(
        submitter
            .try_send(Message::from_slice(b"no route"))
            .is_err()
    );
    assert!(send.take_connect_pipe(1).is_none());

    send.insert_connect_pipe(2, tx);
    submitter.try_send(Message::from_slice(b"after")).unwrap();
    let mut received = Vec::new();
    rx.drain_into(&mut received, 4, usize::MAX);
    assert_eq!(
        received,
        [
            Message::from_slice(b"queued"),
            Message::from_slice(b"after")
        ]
    );
}
