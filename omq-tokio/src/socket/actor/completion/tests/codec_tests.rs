use super::*;
use crate::engine::{ConnectionDriver, PeerDriverCommand, PeerEvent};
use crate::socket::actor::{AnyConn, AnyStream, SocketCommand, endpoint_resolution};
use bytes::Bytes;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder};
use omq_proto::{CompressionKind, CompressionOptions};
use std::time::Duration;

struct RemoteWire {
    events: mpsc::Receiver<(u64, PeerEvent)>,
    commands: mpsc::Sender<PeerDriverCommand>,
    encoder: Option<MessageEncoder>,
    decoder: Option<MessageDecoder>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<omq_proto::Result<()>>,
}

impl RemoteWire {
    fn spawn(stream: tokio::io::DuplexStream, endpoint: &Endpoint, options: &Options) -> Self {
        let (commands, inbox) = mpsc::channel(16);
        let (events, receive) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let driver = ConnectionDriver::new(
            stream,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Sub)),
            inbox,
            events,
            0,
            cancel.clone(),
        );
        let (encoder, decoder) = MessageEncoder::for_endpoint(endpoint, options)
            .unwrap()
            .unzip();
        Self {
            events: receive,
            commands,
            encoder,
            decoder,
            cancel,
            task: tokio::spawn(async move { driver.run().await }),
        }
    }

    async fn assert_publication(&mut self, message: &Message) {
        let expected = if let Some(encoder) = &mut self.encoder {
            encoder.encode(message).unwrap()
        } else {
            smallvec::smallvec![message.clone()]
        };
        let mut decoded = Vec::new();
        for expected in expected {
            let (_, event) = self.events.recv().await.unwrap();
            let PeerEvent::Event(Event::Message(actual)) = event else {
                panic!("unexpected wire event: {event:?}");
            };
            assert!(
                actual == expected,
                "materialized bytes must match the captured codec"
            );
            let plain = if let Some(decoder) = &mut self.decoder {
                decoder.decode(actual).unwrap()
            } else {
                Some(actual)
            };
            decoded.extend(plain);
        }
        assert!(
            decoded == [message.clone()],
            "receiver must recover the original message"
        );
    }
}

fn endpoint(kind: Option<CompressionKind>) -> Endpoint {
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    if let Some(kind) = kind {
        return kind.tcp_endpoint(
            omq_proto::endpoint::Host::Ip("127.0.0.1".parse().unwrap()),
            1,
        );
    }
    let _ = kind;
    "tcp://127.0.0.1:1".parse().unwrap()
}

async fn attach(
    driver: &mut SocketDriver,
    endpoint: Endpoint,
    options: Arc<Options>,
) -> RemoteWire {
    attach_on_route(driver, endpoint, options, None).await
}

async fn attach_on_route(
    driver: &mut SocketDriver,
    endpoint: Endpoint,
    options: Arc<Options>,
    route_id: Option<u64>,
) -> RemoteWire {
    let (native, remote) = tokio::io::duplex(16 * 1024);
    let mut remote = RemoteWire::spawn(remote, &endpoint, &options);
    let conn = AnyConn::ByteStream {
        stream: AnyStream::Memory(native),
        peer_ident: PeerIdent::Socket("127.0.0.1:12345".parse().unwrap()),
        leftover: Bytes::new(),
        setup: None,
    };
    let event = match route_id {
        Some(route_id) => InternalEvent::Connected {
            conn,
            endpoint,
            route_id,
        },
        None => InternalEvent::Accepted {
            conn,
            endpoint,
            options,
        },
    };
    driver.handle_internal_event(event).await;
    let (id, event) = driver.peer_control_rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    driver.handle_peer_output(id, event).await;
    assert!(matches!(
        remote.events.recv().await.unwrap().1,
        PeerEvent::Event(Event::HandshakeSucceeded { .. })
    ));
    remote
        .commands
        .send(PeerDriverCommand::ActivateDataPlane)
        .await
        .unwrap();
    remote
        .commands
        .send(PeerDriverCommand::SendCommand(Command::Subscribe(
            Bytes::new(),
        )))
        .await
        .unwrap();
    let (id, event) = driver.peer_control_rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PeerEvent::Event(Event::Command(Command::Subscribe(_)))
    ));
    driver.handle_peer_output(id, event).await;
    remote
}

#[expect(
    clippy::used_underscore_binding,
    reason = "Await the retained dial task before releasing test setup admission"
)]
async fn stop_dial_transport(driver: &mut SocketDriver) -> u64 {
    assert_eq!(driver.dialers.len(), 1);
    let dialer = &mut driver.dialers[0];
    dialer.cancel.cancel();
    dialer._task.abort();
    let _ = (&mut dialer._task).await;
    dialer.route_id
}

async fn stop(driver: &mut SocketDriver, peers: Vec<RemoteWire>) {
    driver.send_strategy.shutdown();
    for peer in driver.peers.values() {
        peer.handle.cancel.cancel();
    }
    for remote in peers {
        remote.cancel.cancel();
        remote.task.await.unwrap().unwrap();
    }
    for peer in driver.peers.values_mut() {
        if let Some(task) = peer.task.take() {
            task.await.unwrap();
        }
    }
}

#[tokio::test]
async fn queued_generations_materialize_original_codec_settings_after_defaults_change() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for kind in [
            None,
            #[cfg(feature = "lz4")]
            Some(CompressionKind::Lz4),
            #[cfg(feature = "zstd")]
            Some(CompressionKind::Zstd),
        ] {
            let (mut driver, _consumer) = fixture_for(SocketType::Pub);
            driver.options.max_message_size = Some(8192);
            driver.options.max_recv_dict_size = Some(4096);
            let first = driver.capture_endpoint_options(Some(CompressionOptions {
                threshold: Some(64),
                ..CompressionOptions::default()
            }));
            let second = driver.capture_endpoint_options(Some(CompressionOptions {
                threshold: Some(8192),
                ..CompressionOptions::default()
            }));
            driver.options.compression_threshold = Some(512);
            let mut peers = vec![
                attach(&mut driver, endpoint(kind), first).await,
                attach(&mut driver, endpoint(kind), second).await,
            ];
            for _ in 0..3 {
                let message = Message::multipart([
                    Bytes::from_static(b"topic"),
                    Bytes::from(vec![0x5a; 4096]),
                ]);
                driver
                    .send_strategy
                    .submitter()
                    .send(message.clone())
                    .await
                    .unwrap();
                for peer in &mut peers {
                    peer.assert_publication(&message).await;
                }
            }
            for peer in driver.peers.values() {
                assert_eq!(peer.options.max_message_size, Some(8192));
                assert_eq!(peer.options.max_recv_dict_size, Some(4096));
            }
            stop(&mut driver, peers).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn resolution_handoff_and_peer_reconnect_retain_the_operation_snapshot() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for kind in [
            None,
            #[cfg(feature = "lz4")]
            Some(CompressionKind::Lz4),
            #[cfg(feature = "zstd")]
            Some(CompressionKind::Zstd),
        ] {
            let (mut driver, _consumer) = fixture_for(SocketType::Pub);
            driver.options.reconnect = ReconnectPolicy::Fixed(Duration::from_millis(10));
            driver.setup_admission = crate::transport::setup::Admission::new(1);
            let held = driver.setup_admission.try_acquire().unwrap();
            let resolved = endpoint(kind);
            let named = resolved.rewrap_tcp(Endpoint::Tcp {
                host: omq_proto::endpoint::Host::Name("snapshot.invalid".into()),
                port: 1,
            });
            let (ack, _pending_ack) = futures::channel::oneshot::channel();
            driver
                .handle_command(SocketCommand::Connect {
                    endpoint: named,
                    compression: Some(CompressionOptions {
                        threshold: Some(64),
                        ..CompressionOptions::default()
                    }),
                    ack,
                })
                .await;
            assert_eq!(driver.pending_endpoints.len(), 1);
            let id = *driver.pending_endpoints.keys().next().unwrap();
            driver.options.compression_threshold = Some(8192);
            driver.options.reconnect = ReconnectPolicy::Disabled;
            let (ack, completed) = futures::channel::oneshot::channel();
            // Hold admission and inject completion so no OS DNS/socket IO runs.
            driver
                .finish_endpoint_resolution(
                    id,
                    endpoint_resolution::Ack::Connect(ack),
                    Ok(endpoint_resolution::ResolvedEndpoint {
                        endpoint: resolved.clone(),
                        first_deadline: None,
                        admission: None,
                    }),
                )
                .await;
            completed.await.unwrap().unwrap();
            let route = stop_dial_transport(&mut driver).await;
            let original = driver.dialers[0].options.clone();
            assert_eq!(original.compression_threshold, Some(64));
            assert_eq!(
                original.reconnect,
                ReconnectPolicy::Fixed(Duration::from_millis(10))
            );
            drop(held);
            let mut remote =
                attach_on_route(&mut driver, resolved.clone(), original.clone(), Some(route)).await;
            let message = Message::single(Bytes::from(vec![0x5a; 4096]));
            driver
                .send_strategy
                .submitter()
                .send(message.clone())
                .await
                .unwrap();
            remote.assert_publication(&message).await;
            let peer_id = *driver.peers.keys().next().unwrap();
            assert!(Arc::ptr_eq(&driver.peers[&peer_id].options, &original));
            let held = driver.setup_admission.try_acquire().unwrap();
            remote.cancel.cancel();
            remote.task.await.unwrap().unwrap();
            driver
                .handle_internal_event(InternalEvent::PeerClosed {
                    peer_id,
                    reason: DisconnectReason::PeerClosed,
                })
                .await;
            let route = stop_dial_transport(&mut driver).await;
            assert!(Arc::ptr_eq(&driver.dialers[0].options, &original));
            drop(held);
            let mut remote = attach_on_route(&mut driver, resolved, original, Some(route)).await;
            driver
                .send_strategy
                .submitter()
                .send(message.clone())
                .await
                .unwrap();
            remote.assert_publication(&message).await;
            stop(&mut driver, vec![remote]).await;
        }
    })
    .await
    .unwrap();
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn old_listener_dictionary_survives_new_profile_and_late_subscriber() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut driver, _consumer) = fixture_for(SocketType::Pub);
        let first = driver.capture_endpoint_options(Some(CompressionOptions {
            dict: Some(Bytes::from_static(b"old-dictionary")),
            ..CompressionOptions::default()
        }));
        let second = driver.capture_endpoint_options(Some(CompressionOptions {
            dict: Some(Bytes::from_static(b"new-dictionary")),
            ..CompressionOptions::default()
        }));
        let endpoint = endpoint(Some(CompressionKind::Lz4));
        let mut peers = vec![attach(&mut driver, endpoint.clone(), first.clone()).await];
        let message =
            Message::multipart([Bytes::from_static(b"topic"), Bytes::from(vec![0x5a; 4096])]);
        driver
            .send_strategy
            .submitter()
            .send(message.clone())
            .await
            .unwrap();
        peers[0].assert_publication(&message).await;
        peers.push(attach(&mut driver, endpoint.clone(), second).await);
        peers.push(attach(&mut driver, endpoint, first).await);
        for _ in 0..2 {
            driver
                .send_strategy
                .submitter()
                .send(message.clone())
                .await
                .unwrap();
            for peer in &mut peers {
                peer.assert_publication(&message).await;
            }
        }
        stop(&mut driver, peers).await;
    })
    .await
    .unwrap();
}
