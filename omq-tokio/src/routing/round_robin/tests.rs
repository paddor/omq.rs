use super::{ActivePipe, ActivePipes, RoundRobinSend};
use crate::engine::ActorPeerDriverHandle;
use crate::engine::send_pipe::send_pipe;
use omq_proto::message::Message;
use omq_proto::options::Options;
use std::collections::VecDeque;
use tokio_util::sync::CancellationToken;

fn connect_pipe(send: &mut RoundRobinSend, route_id: u64) -> crate::engine::SendPipeConsumer {
    send.make_connect_pipe(
        route_id,
        #[cfg(feature = "dart")]
        None,
    )
}

#[test]
fn batches_preserve_round_robin_order_and_scalar_cursor() {
    let mut send = RoundRobinSend::new(&Options::default().send_hwm(16));
    let mut receivers: Vec<_> = (0..3).map(|id| connect_pipe(&mut send, id)).collect();
    let order: Vec<_> = send
        .active
        .lock()
        .active
        .iter()
        .map(|pipe| pipe.peer_id as usize)
        .collect();
    let submitter = send.submitter();
    let mut messages = (0..7_u8).map(|seq| Message::from_slice(&[seq])).collect();
    assert_eq!(submitter.try_send_many(&mut messages, 7).unwrap(), 7);
    submitter.try_send(Message::from_slice(&[7])).unwrap();

    for (position, &peer) in order.iter().enumerate() {
        let mut received = Vec::new();
        receivers[peer].drain_into(&mut received, 16, usize::MAX);
        let expected: Vec<_> = (position..8)
            .step_by(3)
            .map(|seq| Message::from_slice(&[u8::try_from(seq).unwrap()]))
            .collect();
        assert_eq!(received, expected);
    }
}

#[test]
fn partial_full_batches_publish_and_reactivate_without_losing_messages() {
    let mut send = RoundRobinSend::new(&Options::default().send_hwm(2));
    let mut receivers: Vec<_> = (0..2).map(|id| connect_pipe(&mut send, id)).collect();
    let submitter = send.submitter();
    let mut messages: VecDeque<_> = (0..7_u8).map(|seq| Message::from_slice(&[seq])).collect();
    assert_eq!(submitter.try_send_many(&mut messages, 7).unwrap(), 4);
    assert_eq!(messages.front().unwrap(), &Message::from_slice(&[4]));
    let mut received = Vec::new();

    for receiver in &mut receivers {
        assert_eq!(receiver.drain_into(&mut received, 2, usize::MAX), 2);
    }

    assert_eq!(submitter.try_send_many(&mut messages, 7).unwrap(), 3);
    assert!(messages.is_empty());

    for receiver in &mut receivers {
        receiver.drain_into(&mut received, 2, usize::MAX);
    }

    let mut sequences: Vec<_> = received
        .iter()
        .map(|message| message.part_slice(0).unwrap()[0])
        .collect();
    sequences.sort_unstable();
    assert_eq!(sequences, (0..7).collect::<Vec<_>>());
}

#[test]
fn batch_skips_closed_peer_and_delivers_to_the_remaining_peer() {
    let mut send = RoundRobinSend::new(&Options::default().send_hwm(16));
    let closed = connect_pipe(&mut send, 0);
    let mut receiver = connect_pipe(&mut send, 1);
    drop(closed);
    let submitter = send.submitter();

    let expected: Vec<_> = (0..7_u8).map(|seq| Message::from_slice(&[seq])).collect();
    let mut messages = expected.clone().into();
    assert_eq!(submitter.try_send_many(&mut messages, 7).unwrap(), 7);

    let mut batch = Vec::new();
    receiver.drain_into(&mut batch, 16, usize::MAX);
    assert_eq!(batch, expected);
}

#[test]
fn deactivation_preserves_active_peer_order() {
    let mut pipes = ActivePipes::default();
    for peer_id in 0..4 {
        pipes.active.push(ActivePipe {
            peer_id,
            tx: send_pipe(4).0,
        });
    }

    pipes.cursor = 2;

    pipes.deactivate(1);

    assert_eq!(
        pipes
            .active
            .iter()
            .map(|pipe| pipe.peer_id)
            .collect::<Vec<_>>(),
        vec![0, 2, 3]
    );

    assert_eq!(pipes.cursor, 1);
}

#[test]
fn inactive_pipe_reactivates_without_timer() {
    let (tx0, mut rx0) = send_pipe(2);
    let (tx1, _rx1) = send_pipe(2);
    let mut pipes = ActivePipes::default();
    pipes.active.push(ActivePipe {
        peer_id: 0,
        tx: tx0,
    });
    pipes.active.push(ActivePipe {
        peer_id: 1,
        tx: tx1,
    });

    pipes.active[0].tx.try_send(Message::single("a")).unwrap();
    pipes.active[0].tx.try_send(Message::single("b")).unwrap();
    assert!(matches!(
        pipes.active[0].tx.try_send(Message::single("c")),
        Err(crate::engine::SendPipeError::Full(_))
    ));
    pipes.deactivate(0);
    assert_eq!(pipes.inactive.len(), 1);

    let mut batch = Vec::new();
    assert_eq!(rx0.drain_into(&mut batch, 1, usize::MAX), 1);
    pipes.try_reactivate_one();

    assert_eq!(pipes.inactive.len(), 0);
    assert!(pipes.active.iter().any(|pipe| pipe.peer_id == 0));
}

#[test]
fn inactive_pipe_rechecks_occupancy_when_lwm_flag_is_stale() {
    let (mut tx, mut rx) = send_pipe(2);
    tx.try_send(Message::single("a")).unwrap();
    tx.try_send(Message::single("b")).unwrap();

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 2, usize::MAX), 2);

    // Models producer observing Full and setting above_lwm after the
    // consumer already drained below LWM, so no space wake fires.
    tx.above_lwm
        .store(true, std::sync::atomic::Ordering::Release);

    let mut pipes = ActivePipes::default();
    pipes.inactive.push(ActivePipe { peer_id: 0, tx });
    pipes.try_reactivate_when_empty();

    assert_eq!(pipes.inactive.len(), 0);
    assert_eq!(pipes.active.len(), 1);
    assert_eq!(pipes.active[0].peer_id, 0);
}

#[test]
fn no_pipe_try_send_is_full() {
    let send = RoundRobinSend::new(&Options::default().send_hwm(1));
    let submitter = send.submitter();

    let err = submitter.try_send(Message::single("mute")).unwrap_err();
    match err {
        omq_proto::error::TrySendError::Full(msg) => {
            assert_eq!(msg.part_bytes(0).unwrap(), &b"mute"[..]);
        }
        other => panic!("expected Full, got {other:?}"),
    }
}

#[test]
fn connect_pipe_buffers_before_peer_ready() {
    let mut send = RoundRobinSend::new(&Options::default().send_hwm(1));
    let submitter = send.submitter();
    let mut rx = connect_pipe(&mut send, 7);

    submitter.try_send(Message::single("pre")).unwrap();
    let err = submitter.try_send(Message::single("full")).unwrap_err();
    assert!(matches!(err, omq_proto::error::TrySendError::Full(_)));

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
    assert_eq!(batch[0].part_bytes(0).unwrap(), &b"pre"[..]);
}

#[test]
fn connection_added_reuses_connect_pipe() {
    let mut send = RoundRobinSend::new(&Options::default().send_hwm(2));
    let submitter = send.submitter();
    let mut rx = connect_pipe(&mut send, 7);

    submitter.try_send(Message::single("pre")).unwrap();

    let (send_pipe, mut send_pipe_rx) = send_pipe(1);
    let (inbox, _inbox_rx) = tokio::sync::mpsc::channel(1);
    let (data_inbox, _data_inbox_rx) = tokio::sync::mpsc::channel(1);

    let handle = ActorPeerDriverHandle {
        inbox: inbox.into(),
        data_inbox: data_inbox.into(),
        cancel: CancellationToken::new(),
        routes: crate::engine::driver::DirectRoutes {
            send_pipe: Some(std::sync::Arc::new(std::sync::Mutex::new(Some(send_pipe)))),
            ..Default::default()
        },
    };

    send.connection_added(7, &handle, false);

    submitter.try_send(Message::single("post")).unwrap();

    let mut batch = Vec::new();
    assert_eq!(rx.drain_into(&mut batch, 2, usize::MAX), 2);
    assert_eq!(batch[0].part_bytes(0).unwrap(), &b"pre"[..]);
    assert_eq!(batch[1].part_bytes(0).unwrap(), &b"post"[..]);
    assert_eq!(send_pipe_rx.drain_into(&mut batch, 1, usize::MAX), 0);
}
