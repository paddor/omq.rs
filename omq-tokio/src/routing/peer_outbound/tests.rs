use super::PeerOutbound;
use crate::engine::PeerDriverData;
use omq_proto::message::Message;

#[test]
fn handshake_does_not_let_direct_send_overtake_inbox() {
    use crate::engine::transmit_slot::{PeerTransmitSlot, SlotConfig, TryFrameResult};
    use crate::socket::direct_writer::DirectTcpWriter;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut remote, _) = listener.accept().unwrap();
    tcp.set_nonblocking(true).unwrap();
    remote.set_nonblocking(true).unwrap();

    let slot = PeerTransmitSlot::with_config(
        1,
        SlotConfig {
            arena_threshold: 4096,
            arena_cap: 4096,
            cap: 8192,
            msg_cap: 4,
            ..SlotConfig::default()
        },
    );

    let (inbox, mut rx) = tokio::sync::mpsc::channel(4);
    let target = PeerOutbound::Wire {
        slot: slot.clone(),
        inbox: inbox.into(),
        direct: Some(Arc::new(DirectTcpWriter::new(tcp))),
    };
    assert_eq!(
        target.try_encode(&Message::from_slice(b"older")),
        TryFrameResult::Ok
    );
    slot.mark_handshake_done();
    assert_eq!(
        target.try_encode(&Message::from_slice(b"newer")),
        TryFrameResult::Ok
    );
    let mut bytes = [0; 64];
    assert_eq!(
        remote.read(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let PeerDriverData::SendMessage(first) = rx.try_recv().unwrap() else {
        panic!("message")
    };

    assert_eq!(first.part_slice(0), Some(b"older".as_slice()));
}

#[test]
fn send_in_uses_one_lane_per_socket_clone() {
    use crate::engine::data_inbox::{SenderLanes, channel};
    use crate::engine::send_pipe::SendPreparation;

    let (root, mut receiver) = channel(1);
    let target = PeerOutbound::Inbox(root);
    let first = SenderLanes::default();
    let second = SenderLanes::default();

    let send = |lanes, body: &[u8]| {
        target.try_send_in(lanes, Message::from_slice(body), SendPreparation::Plain)
    };

    assert!(send(&first, b"a").is_ok());
    assert!(matches!(
        send(&first, b"b"),
        Err(crate::engine::SendPipeError::Full(_))
    ));
    assert!(send(&second, b"c").is_ok());

    let mut batch = receiver.batch();
    let mut bodies = Vec::new();

    while let Ok(PeerDriverData::SendMessage(message)) = batch.try_recv() {
        bodies.push(message.part_slice(0).unwrap().to_vec());
    }

    bodies.sort();
    assert_eq!(bodies, [b"a".to_vec(), b"c".to_vec()]);
}

#[test]
fn inbox_peer_outbound_reports_queued_messages() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let target = PeerOutbound::Inbox(tx.clone().into());

    assert!(target.is_empty());
    tx.try_send(PeerDriverData::SendMessage(Message::from_slice(b"queued")))
        .unwrap();
    assert!(!target.is_empty());

    assert!(rx.try_recv().is_ok());
    assert!(target.is_empty());
}
