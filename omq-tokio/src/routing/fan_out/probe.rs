//! Mixed-codec validation and profiling over bounded in-memory byte streams.
//! These measurements do not replace TCP/WS/WSS network acceptance.

use super::{FanOutMode, FanOutSend};
use crate::engine::codec::CodecProfile;
use crate::engine::framing::WireFraming;
use crate::engine::transmit_slot::PeerTransmitSlot;
use crate::engine::{ConnectionDriver, PeerDriverCommand, PeerDriverHandle, PeerEvent};
use bytes::Bytes;
use omq_proto::Message;
use omq_proto::options::Options;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::{CompressionKind, MessageEncoder};
use omq_proto::proto::{Event, SocketType};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct ProbePeer {
    handle: PeerDriverHandle,
    remote_inbox: mpsc::Sender<PeerDriverCommand>,
    tasks: [tokio::task::JoinHandle<omq_proto::Result<()>>; 2],
}

impl ProbePeer {
    fn spawn(
        id: u64,
        kind: Option<CompressionKind>,
        options: &Options,
        events: &mpsc::Sender<(u64, PeerEvent)>,
    ) -> Self {
        let (outgoing, incoming) = tokio::io::duplex(64 * 1024);
        let (inbox, commands) = mpsc::channel(16);
        let (data_inbox, data) = mpsc::channel(64);
        let (remote_inbox, remote_commands) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let slot = PeerTransmitSlot::new(
            id,
            kind.is_some(),
            kind.map(|kind| CodecProfile::new(kind, options)),
            None,
            4096,
            16384,
            512 * 1024,
            1024,
            WireFraming::Zmtp,
        );
        let mut sender = ConnectionDriver::new(
            outgoing,
            Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pub)),
            commands,
            events.clone(),
            id * 2,
            cancel.clone(),
        )
        .with_data_inbox(data)
        .with_transmit_slot(slot.clone());
        let mut receiver = ConnectionDriver::new(
            incoming,
            Connection::new(ConnectionConfig::new(Role::Client, SocketType::Sub)),
            remote_commands,
            events.clone(),
            id * 2 + 1,
            cancel.clone(),
        );
        if let Some(kind) = kind {
            let (encoder, decoder) = MessageEncoder::for_compression_kind(kind, options)
                .unwrap()
                .unwrap();
            sender = sender.with_encoder(encoder);
            receiver = receiver.with_decoder(decoder);
        }
        Self {
            handle: PeerDriverHandle {
                inbox,
                data_inbox,
                cancel,
                transmit_slot: Some(slot),
                direct_tcp_writer: None,
                send_pipe: None,
                inproc: None,
            },
            remote_inbox,
            tasks: [
                tokio::spawn(async move { sender.run().await }),
                tokio::spawn(async move { receiver.run().await }),
            ],
        }
    }
}

async fn run_probe(messages: usize, size: usize, kinds: &[Option<CompressionKind>]) -> Duration {
    let message = Message::multipart([Bytes::from_static(b"topic"), Bytes::from(vec![0x5a; size])]);
    run_message_probe(messages, message, kinds).await
}

async fn run_message_probe(
    messages: usize,
    message: Message,
    kinds: &[Option<CompressionKind>],
) -> Duration {
    let mut options = Options::default().compression_auto_train(false);
    options.xpub_nodrop = true;
    let mut fanout = FanOutSend::new(
        SocketType::Pub,
        &options,
        FanOutMode::SubscriptionPrefix,
        &crate::context::IoPoolHandle::none(),
    );
    let (events, mut receive) = mpsc::channel(256);
    let peers: Vec<_> = kinds
        .iter()
        .enumerate()
        .map(|(id, kind)| ProbePeer::spawn(id as u64, *kind, &options, &events))
        .collect();
    for _ in 0..peers.len() * 2 {
        assert!(matches!(
            receive.recv().await.unwrap().1,
            PeerEvent::Event(Event::HandshakeSucceeded { .. })
        ));
    }
    for (id, peer) in peers.iter().enumerate() {
        peer.handle
            .inbox
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        peer.remote_inbox
            .send(PeerDriverCommand::ActivateDataPlane)
            .await
            .unwrap();
        fanout.connection_added(id as u64, peer.handle.clone().into(), 0);
        if let Some(ack) = fanout.peer_subscribe(id as u64, Bytes::from_static(b"topic")) {
            ack.await.unwrap();
        }
    }
    let expected = message.clone();
    let peer_count = peers.len();
    let received = tokio::spawn(async move {
        let mut counts = vec![0; peer_count];
        for _ in 0..messages * peer_count {
            let (id, event) = receive.recv().await.unwrap();
            let PeerEvent::Event(Event::Message(actual)) = event else {
                panic!("unexpected data-plane event: {event:?}");
            };
            assert_eq!(id % 2, 1);
            assert_eq!(actual, expected);
            counts[id as usize / 2] += 1;
        }
        assert!(counts.iter().all(|count| *count == messages));
    });
    let submitter = fanout.submitter();
    let start = Instant::now();
    for _ in 0..messages {
        submitter.send(message.clone()).await.unwrap();
    }
    received.await.unwrap();
    let elapsed = start.elapsed();
    fanout.shutdown();
    for peer in &peers {
        peer.handle.cancel.cancel();
    }
    for peer in peers {
        for task in peer.tasks {
            task.await.unwrap().unwrap();
        }
    }
    elapsed
}

#[tokio::test]
async fn mixed_codecs_deliver_original_messages_through_real_drivers() {
    let kinds = [
        None,
        Some(CompressionKind::Lz4),
        Some(CompressionKind::Zstd),
    ];
    for reverse in [false, true] {
        let mut kinds = kinds;
        if reverse {
            kinds.reverse();
        }
        for size in [16, 4096] {
            tokio::time::timeout(Duration::from_secs(10), run_probe(100, size, &kinds))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn multipart_above_1024_parts_survives_bounded_driver_writes_and_reads() {
    let kinds = [
        None,
        Some(CompressionKind::Lz4),
        Some(CompressionKind::Zstd),
        None,
        Some(CompressionKind::Lz4),
        Some(CompressionKind::Zstd),
    ];
    let message = Message::multipart(
        std::iter::once(Bytes::from_static(b"topic"))
            .chain((0..1025).map(|_| Bytes::from_static(&[0x5a; 1024]))),
    );
    // More than 1024 gathered chunks and more than 1 MiB force continued
    // write/read turns. Repeated publications expose retained-prefix errors.
    tokio::time::timeout(
        Duration::from_secs(10),
        run_message_probe(3, message, &kinds),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "serial in-memory profiling probe; not a network performance gate"]
async fn profile_mixed_codecs() {
    let messages =
        std::env::var("OMQ_CODEC_PROBE_MESSAGES").map_or(100_000, |value| value.parse().unwrap());
    let size = std::env::var("OMQ_CODEC_PROBE_BYTES").map_or(4096, |value| value.parse().unwrap());
    let copies = std::env::var("OMQ_CODEC_PROBE_COPIES").map_or(3, |value| value.parse().unwrap());
    assert!((1..=1_000_000).contains(&messages));
    assert!((1..=1024 * 1024).contains(&size));
    assert!((1..=16).contains(&copies));
    let kinds: Vec<_> = (0..copies)
        .flat_map(|_| {
            [
                None,
                Some(CompressionKind::Lz4),
                Some(CompressionKind::Zstd),
            ]
        })
        .collect();
    let elapsed = tokio::time::timeout(Duration::from_mins(1), run_probe(messages, size, &kinds))
        .await
        .unwrap();
    let peer_count = kinds.len();
    println!(
        "codec_probe messages={messages} bytes={size} peers={peer_count} seconds={:.6} publications_per_second={:.0}",
        elapsed.as_secs_f64(),
        messages as f64 / elapsed.as_secs_f64()
    );
}
