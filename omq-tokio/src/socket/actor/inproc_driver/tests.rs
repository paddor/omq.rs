use super::*;
use crate::engine::{PeerDriverCommand, PeerEvent};
use omq_proto::inproc::InboundFrame;
use omq_proto::proto::SocketType;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Context for PAIR peer 7 with no sinks, routes, or size limit. Also
/// returns the socket receive pipe and its consumer, which must stay
/// alive for the pipe to accept messages.
fn pair_ctx(
    peer_control: mpsc::Sender<(u64, PeerEvent)>,
    peer_out: crate::engine::actor_output::PeerOutput,
    completion: crate::engine::peer_completion::CompletionProgress,
    cancel: CancellationToken,
) -> (
    InprocDriverCtx,
    Arc<crate::socket::recv::SharedRecvPipe>,
    yring::Consumer<Message>,
) {
    let blocking = crate::socket::recv::BlockingRecvWaker::new();
    let (recv, consumer, _, _) = crate::socket::recv::recv_pipe(16, blocking.clone());
    let ctx = InprocDriverCtx {
        peer_out,
        peer_control,
        subscriptions: SubscriptionGate::default(),
        completion,
        peer_id: 7,
        cancel,
        peer_props: omq_proto::proto::command::PeerProperties::default()
            .with_socket_type(SocketType::Pair),
        max_message_size: None,
        recv_direct: None,
        socket_close_state: recv.clone(),
        recv_sink: None,
        send_pipe_rx: None,
        blocking_recv_waker: blocking,
        inbound: None,
        outbound: None,
    };
    (ctx, recv, consumer)
}

fn unknown_command(name: &'static str) -> InboundFrame {
    InboundFrame::Command(Box::new(omq_proto::proto::Command::Unknown {
        name: name.into(),
        body: bytes::Bytes::new(),
    }))
}

#[tokio::test]
async fn inproc_completion_joins_with_its_handshake_mailbox_full() {
    let (commands, inbox) = crate::engine::control_inbox::channel(1);
    let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
    let (_incoming, in_rx) = crate::transport::inproc::relay_channel(1);
    let (out, mut outgoing) = crate::transport::inproc::relay_channel(1);
    let (peer_out, mut events) = mpsc::channel(1);
    let (completion, finished) = crate::engine::peer_completion::CompletionProgress::reserve(7);
    let (ctx, _recv, _consumer) = pair_ctx(
        peer_out.clone(),
        peer_out.into(),
        completion,
        CancellationToken::new(),
    );
    commands.try_send(PeerDriverCommand::Close).unwrap();

    let mut task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));
    let result = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
    if result.is_err() {
        task.abort();
    }
    result
        .expect("inproc closure waited for data-mailbox credit")
        .unwrap();

    let result = finished.await.unwrap();
    assert_eq!(result.admitted_events, 1);
    assert_eq!(result.reason, omq_proto::DisconnectReason::PeerClosed);
    assert!(outgoing.recv().await.is_none());
    assert!(matches!(
        events.recv().await.unwrap().1,
        PeerEvent::Event(ZmtpEvent::HandshakeSucceeded { .. })
    ));
    assert!(events.recv().await.is_none());
}

#[tokio::test]
async fn inproc_completion_counts_only_admitted_commands_and_messages() {
    for mode in ["close", "cancel", "abort"] {
        let (commands, inbox) = crate::engine::control_inbox::channel(1);
        let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
        let (incoming, in_rx) = crate::transport::inproc::relay_channel(2);
        let (out, _outgoing) = crate::transport::inproc::relay_channel(1);
        let (peer_out, mut events) = mpsc::channel(2);
        let (completion, finished) = crate::engine::peer_completion::CompletionProgress::reserve(7);
        let cancel = CancellationToken::new();
        let (ctx, _recv, _consumer) = pair_ctx(
            peer_out.clone(),
            peer_out.into(),
            completion,
            cancel.clone(),
        );
        let mut task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));
        assert!(matches!(
            events.recv().await.unwrap().1,
            PeerEvent::Event(ZmtpEvent::HandshakeSucceeded { .. })
        ));

        // Admit one command and one message, then stop the task by `mode`.
        commands
            .try_send(PeerDriverCommand::ActivateDataPlane)
            .unwrap();
        incoming.try_send(unknown_command("OLDER")).unwrap();
        incoming
            .try_send(InboundFrame::Message(Message::single("last")))
            .unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            while events.len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match mode {
            "cancel" => cancel.cancel(),
            "abort" => task.abort(),
            _ => commands.try_send(PeerDriverCommand::Close).unwrap(),
        }

        let result = tokio::time::timeout(Duration::from_millis(500), &mut task)
            .await
            .expect("inproc completion waited for its admitted prefix to drain");
        if mode == "abort" {
            assert!(result.unwrap_err().is_cancelled());
        } else {
            result.unwrap();
        }
        let completion = finished.await.unwrap();
        assert_eq!(completion.admitted_events, 3);
        assert_eq!(
            matches!(completion.reason, omq_proto::DisconnectReason::Error(_)),
            mode == "abort"
        );
        assert!(matches!(events.recv().await.unwrap().1,
            PeerEvent::Event(ZmtpEvent::Command(omq_proto::proto::Command::Unknown { name, .. }))
                if name == "OLDER"));
        assert!(matches!(events.recv().await.unwrap().1,
            PeerEvent::Event(ZmtpEvent::Message(message)) if message.part_slice(0) == Some(b"last".as_slice())));
        assert!(events.recv().await.is_none());
    }
}

#[tokio::test]
async fn inproc_control_progresses_while_actor_data_is_full() {
    for mode in ["actor", "sink", "port"] {
        let (commands, inbox) = crate::engine::control_inbox::channel(1);
        let (_data, data_inbox) = crate::engine::data_inbox::channel(1);
        let (incoming, in_rx) = crate::transport::inproc::relay_channel(2);
        let (out, _outgoing) = crate::transport::inproc::relay_channel(1);
        let (control, mut events) = mpsc::channel(4);
        let (sender, _actor_data) = fanring::mpsc::channel_with_policy(1);
        let mut peer_out = crate::engine::actor_output::PeerOutput::actor(sender);
        peer_out
            .try_send(7, Message::single("occupied"), false)
            .unwrap();
        let (completion, finished) = crate::engine::peer_completion::CompletionProgress::reserve(7);
        let cancel = CancellationToken::new();
        let (mut ctx, recv, _consumer) = pair_ctx(control, peer_out, completion, cancel.clone());

        // Fill every receive route `mode` can use, so data stays blocked.
        for _ in 0..16 {
            recv.try_send(Message::single("occupied")).unwrap();
        }
        ctx.inbound = (mode == "port").then(crate::transport::inproc::InprocPort::new);
        if let Some(port) = &ctx.inbound {
            port.open(crate::transport::inproc::OpenPort {
                sink: crate::engine::RecvSink::Channel(recv.clone()),
                identity: None,
                max_message_size: None,
                cancel,
            });
        }
        ctx.recv_sink = (mode == "sink").then(|| crate::engine::RecvSink::Channel(recv.clone()));
        let task = tokio::spawn(inproc_peer_driver(inbox, data_inbox, in_rx, out, ctx));

        events.recv().await.unwrap();
        commands
            .try_send(PeerDriverCommand::ActivateDataPlane)
            .unwrap();
        incoming
            .try_send(InboundFrame::Message(Message::single("blocked")))
            .unwrap();
        tokio::task::yield_now().await;
        incoming.try_send(unknown_command("CONTROL")).unwrap();

        let event = tokio::time::timeout(Duration::from_millis(500), events.recv())
            .await
            .expect("data backpressure buried inproc control")
            .unwrap();
        assert!(matches!(event.1, PeerEvent::Event(ZmtpEvent::Command(_))));
        commands.try_send(PeerDriverCommand::Close).unwrap();
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .expect("data backpressure blocked local close")
            .unwrap();
        assert_eq!(finished.await.unwrap().admitted_events, 2);
    }
}
