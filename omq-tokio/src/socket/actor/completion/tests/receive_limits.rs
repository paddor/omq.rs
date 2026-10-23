//! Exercise size admission through the actual actor materializer and drivers.
use super::*;
use crate::engine::{ConnectionDriver, PeerDriverCommand, PeerDriverData, PeerEvent};
use crate::socket::actor::{AnyConn, AnyStream};
use bytes::Bytes;
use futures::StreamExt;
use omq_proto::CompressionKind;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::MessageEncoder;
use std::time::Duration;

fn kinds() -> Vec<CompressionKind> {
    vec![
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ]
}

/// Remote DEALER driver wired to the actor through an in-memory stream.
struct Remote {
    id: u64,
    data: mpsc::Sender<PeerDriverData>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<omq_proto::Result<()>>,
    // Kept open for the test: dropping either could end the remote early.
    _control: mpsc::Sender<PeerDriverCommand>,
    _events: mpsc::Receiver<(u64, PeerEvent)>,
}

impl Remote {
    async fn send(&self, encoder: &mut MessageEncoder, message: &Message) {
        for wire in encoder.encode(message).unwrap() {
            self.data
                .send(PeerDriverData::SendMessage(wire))
                .await
                .unwrap();
        }
    }
}

/// Accept a remote peer, finish the handshake on both sides, and activate
/// the remote's data plane.
async fn accept_remote(
    actor: &mut SocketDriver,
    endpoint: Endpoint,
    options: Arc<Options>,
) -> Remote {
    let (native, remote) = tokio::io::duplex(16 * 1024);
    let (control, inbox) = mpsc::channel(16);
    let (data, data_inbox) = mpsc::channel(16);
    let (events, mut remote_events) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let remote_driver = ConnectionDriver::new(
        remote,
        Connection::new(ConnectionConfig::new(Role::Client, SocketType::Dealer)),
        inbox,
        events,
        0,
        cancel.clone(),
    )
    .with_data_inbox(data_inbox);
    let task = tokio::spawn(remote_driver.run());

    actor
        .handle_internal_event(InternalEvent::Accepted {
            conn: AnyConn::ByteStream {
                stream: AnyStream::Memory(native),
                peer_ident: PeerIdent::Socket("127.0.0.1:12345".parse().unwrap()),
                leftover: Bytes::new(),
                setup: None,
            },
            endpoint,
            options,
        })
        .await;
    let (id, event) = actor.mail.peer_control_rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    actor.handle_peer_output(id, event).await;
    assert!(matches!(
        remote_events.recv().await.unwrap().1,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    control
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();

    Remote {
        id,
        data,
        cancel,
        task,
        _control: control,
        _events: remote_events,
    }
}

async fn receive_boundary(
    kind: CompressionKind,
    message: Message,
    dict: Option<Bytes>,
    threshold: usize,
) {
    let (mut actor, _consumer) = fixture_for(SocketType::Router);
    let limit = message.max_message_size_len();
    let options = Arc::new(Options {
        max_message_size: Some(limit),
        ..Options::default()
    });
    let send_options = Options {
        compression_dict: dict,
        compression_threshold: Some(threshold),
        ..(*options).clone()
    };
    let endpoint = kind.tcp_endpoint(
        omq_proto::endpoint::Host::Ip("127.0.0.1".parse().unwrap()),
        1,
    );
    let remote = accept_remote(&mut actor, endpoint, options).await;

    // A message at the limit arrives intact, even when its wire form
    // (uncompressed, or with a dictionary shipment) exceeds the limit.
    let (mut encoder, _) = MessageEncoder::for_compression_kind(kind, &send_options)
        .unwrap()
        .unwrap();
    let wire = encoder.encode(&message).unwrap();
    if send_options.compression_dict.is_some() {
        assert_eq!(wire.len(), 2);
        assert!(wire[0].max_message_size_len() > limit);
    } else if threshold == usize::MAX {
        assert!(wire[0].max_message_size_len() > limit);
    }
    for wire in wire {
        remote
            .data
            .send(PeerDriverData::SendMessage(wire))
            .await
            .unwrap();
    }
    let received = actor
        .mail
        .peer_out_rx
        .as_mut()
        .unwrap()
        .recv_async()
        .await
        .unwrap();
    assert_eq!(received.peer_id, remote.id);
    assert_eq!(received.message, message);

    assert_oversize_closes_peer(&mut actor, &remote, &mut encoder, limit).await;
    actor
        .peers
        .get_mut(&remote.id)
        .unwrap()
        .task
        .take()
        .unwrap()
        .await
        .unwrap();
    actor.routing.send_strategy.shutdown();
    remote.cancel.cancel();
    let _ = remote.task.await.unwrap();
}

/// Compressible oversize bodies must fail the decoded budget even when
/// their entire wire message fits the enlarged framing allowance.
async fn assert_oversize_closes_peer(
    actor: &mut SocketDriver,
    remote: &Remote,
    encoder: &mut MessageEncoder,
    limit: usize,
) {
    let excess = Message::single(Bytes::from(vec![
        0x5a;
        limit
            - size_of::<omq_proto::message::Payload>(
            )
            + 1
    ]));
    remote.send(encoder, &excess).await;

    let completion = actor.mail.peer_completions.next().await.unwrap().unwrap();
    assert_eq!(completion.peer_id, remote.id);
    assert!(
        matches!(&completion.reason, omq_proto::DisconnectReason::Error(reason)
            if reason.contains("message too large")),
        "{completion:?}"
    );
    assert!(matches!(
        actor.mail.peer_out_rx.as_mut().unwrap().try_recv(),
        Err(fanring::mpsc::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn materialized_codec_receive_enforces_exact_logical_limits() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for kind in kinds() {
            for message in [
                Message::single(Bytes::new()),
                Message::single(Bytes::from_static(b"0123456789abcdef")),
                Message::multipart([Bytes::new(), Bytes::from_static(b"x")]),
                Message::multipart([Bytes::new(), Bytes::from(vec![0x5a; 4096])]),
            ] {
                receive_boundary(kind, message.clone(), None, usize::MAX).await;
                receive_boundary(kind, message, None, 0).await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn materialized_codec_receive_admits_dictionary_setup_with_a_tiny_limit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for kind in kinds() {
            let dict = match kind {
                #[cfg(feature = "lz4")]
                CompressionKind::Lz4 => Bytes::from(vec![0x5a; 8192]),
                #[cfg(feature = "zstd")]
                CompressionKind::Zstd => {
                    let samples = vec![&b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..]; 200];
                    omq_proto::proto::transform::train_zdict(&samples, 8192).unwrap()
                }
                _ => unreachable!("test enumerates only enabled codecs"),
            };
            receive_boundary(kind, Message::single(Bytes::new()), Some(dict), usize::MAX).await;
        }
    })
    .await
    .unwrap();
}
