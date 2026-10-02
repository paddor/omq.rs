//! Socket-factory size limits include payload slots; raw codec budgets do not.
use super::*;
use crate::error::Error;
use crate::message::{Message, Payload};
use crate::proto::SocketType;
use crate::proto::command::Command;
use crate::proto::connection::{Connection, ConnectionConfig, Role};
use bytes::Bytes;

fn kinds() -> Vec<CompressionKind> {
    vec![
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4,
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd,
    ]
}

#[test]
fn factory_decoder_enforces_body_and_payload_slots_at_exact_boundaries() {
    for kind in kinds() {
        for bodies in [
            vec![0],
            vec![1],
            vec![16],
            vec![4096],
            vec![0, 0],
            vec![0, 1, 4096],
        ] {
            let message =
                Message::multipart(bodies.into_iter().map(|size| Bytes::from(vec![0x5a; size])));
            let size = message.max_message_size_len();
            for (limit, allowed) in [(size, true), (size - 1, false)] {
                let options = Options {
                    max_message_size: Some(limit),
                    ..Options::default()
                };
                let (mut encoder, mut decoder) =
                    MessageEncoder::for_compression_kind(kind, &options)
                        .unwrap()
                        .unwrap();
                let wire = encoder.encode(&message).unwrap();
                assert_eq!(wire.len(), 1);
                let result = decoder.decode(wire[0].clone());
                if allowed {
                    assert_eq!(result.unwrap(), Some(message.clone()), "{kind:?}, {limit}");
                } else {
                    assert!(
                        matches!(result, Err(Error::MessageTooLarge { .. })),
                        "{kind:?}, {limit}: {result:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn raw_decoder_preserves_body_only_budget_for_empty_parts() {
    assert_eq!(size_of::<Payload>(), 64);
    let empty = Message::single(Bytes::new());
    for kind in kinds() {
        let (mut encoder, _) = MessageEncoder::for_compression_kind(kind, &Options::default())
            .unwrap()
            .unwrap();
        let wire = encoder.encode(&empty).unwrap().remove(0);
        let decoded = match kind {
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4 => Lz4Decoder::new()
                .with_max_message_size(Some(0))
                .decode(wire),
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd => ZstdDecoder::new()
                .with_max_message_size(Some(0))
                .decode(wire),
        };
        assert_eq!(decoded.unwrap(), Some(empty.clone()));
    }
}

fn dict(kind: CompressionKind) -> Bytes {
    match kind {
        #[cfg(feature = "lz4")]
        CompressionKind::Lz4 => Bytes::from(vec![0x5a; 8192]),
        #[cfg(feature = "zstd")]
        CompressionKind::Zstd => {
            let samples = vec![&b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..]; 200];
            train_zdict(&samples, 8192).unwrap()
        }
    }
}

fn carriers() -> Vec<bool> {
    vec![
        false,
        #[cfg(feature = "ws")]
        true,
    ]
}

fn ready_pair(logical: usize, wire: usize, ws: bool) -> (Connection, Connection) {
    let receive = ConnectionConfig::new(Role::Server, SocketType::Pair)
        .max_message_size(logical)
        .max_wire_message_size(wire);
    let send = ConnectionConfig::new(Role::Client, SocketType::Pair);
    #[cfg(feature = "ws")]
    let (receive, send) = if ws {
        use crate::proto::connection::WsRole;
        (
            receive.ws_role(WsRole::Server),
            send.ws_role(WsRole::Client),
        )
    } else {
        (receive, send)
    };
    #[cfg(not(feature = "ws"))]
    let _ = ws;
    let (mut receive, mut send) = (Connection::new(receive), Connection::new(send));
    for _ in 0..16 {
        let inbound = send.poll_transmit();
        let outbound = receive.poll_transmit();
        if inbound.is_empty() && outbound.is_empty() {
            break;
        }
        send.advance_transmit(inbound.len());
        receive.advance_transmit(outbound.len());
        receive.handle_input(inbound).unwrap();
        send.handle_input(outbound).unwrap();
    }
    assert!(receive.is_ready() && send.is_ready());
    assert!(receive.poll_event().is_some());
    assert!(send.poll_event().is_some());
    (receive, send)
}

fn through_framer(receive: &mut Connection, send: &mut Connection, message: &Message) -> Message {
    send.send_message(message).unwrap();
    let bytes = send.poll_transmit();
    send.advance_transmit(bytes.len());
    receive.handle_input(bytes).unwrap();
    receive.poll_message().unwrap()
}

#[test]
fn dictionary_setup_is_independent_of_a_tiny_logical_limit_and_ships_once() {
    let empty = Message::single(Bytes::new());
    for kind in kinds() {
        for ws in carriers() {
            let dictionary = dict(kind);
            let options = Options {
                max_message_size: Some(empty.max_message_size_len()),
                compression_dict: Some(dictionary),
                ..Options::default()
            };
            let (mut encoder, mut decoder) = MessageEncoder::for_compression_kind(kind, &options)
                .unwrap()
                .unwrap();
            let wire = encoder.encode(&empty).unwrap();
            assert_eq!(wire.len(), 2);
            assert!(wire[0].max_message_size_len() > empty.max_message_size_len());
            let (mut receive, mut send) = ready_pair(
                empty.max_message_size_len(),
                decoder.max_wire_message_size().unwrap(),
                ws,
            );
            let shipment = through_framer(&mut receive, &mut send, &wire[0]);
            assert!(decoder.decode(shipment.clone()).unwrap().is_none());
            let payload = through_framer(&mut receive, &mut send, &wire[1]);
            assert_eq!(decoder.decode(payload).unwrap(), Some(empty.clone()));
            assert!(matches!(decoder.decode(shipment), Err(Error::Protocol(_))));
        }
    }
}

#[test]
fn passthrough_allowance_does_not_raise_command_limits() {
    let message = Message::multipart([Bytes::new(), Bytes::from_static(b"x")]);
    let logical = message.max_message_size_len();
    for kind in kinds() {
        for ws in carriers() {
            let options = Options {
                max_message_size: Some(logical),
                ..Options::default()
            };
            let (mut encoder, mut decoder) = MessageEncoder::for_compression_kind(kind, &options)
                .unwrap()
                .unwrap();
            let wire = encoder.encode(&message).unwrap().remove(0);
            assert!(wire.max_message_size_len() > logical);
            let (mut receive, mut send) =
                ready_pair(logical, decoder.max_wire_message_size().unwrap(), ws);
            let payload = through_framer(&mut receive, &mut send, &wire);
            assert_eq!(decoder.decode(payload).unwrap(), Some(message.clone()));
            send.send_command(&Command::Subscribe(Bytes::from(vec![0; logical])))
                .unwrap();
            let bytes = send.poll_transmit();
            send.advance_transmit(bytes.len());
            assert!(
                matches!(receive.handle_input(bytes), Err(Error::MessageTooLarge { max, .. }) if max == logical)
            );
        }
    }
}

#[test]
fn wire_limit_rejects_declared_oversize_from_the_header() {
    for kind in kinds() {
        let options = Options {
            max_message_size: Some(64),
            ..Options::default()
        };
        let (_, decoder) = MessageEncoder::for_compression_kind(kind, &options)
            .unwrap()
            .unwrap();
        let wire = decoder.max_wire_message_size().unwrap();
        let (mut receive, _) = ready_pair(64, wire, false);
        let mut header = vec![2]; // LONG data frame; no body is supplied.
        header.extend_from_slice(&u64::try_from(wire + 1).unwrap().to_be_bytes());
        assert!(
            matches!(receive.handle_input(Bytes::from(header)), Err(Error::MessageTooLarge { max, .. }) if max == wire)
        );
    }
}

#[test]
fn finite_wire_limits_saturate_without_overflow_and_unlimited_stays_explicit() {
    for kind in kinds() {
        for limit in [0, 63, 64, 8192, usize::MAX] {
            let options = Options {
                max_message_size: Some(limit),
                ..Options::default()
            };
            let (_, decoder) = MessageEncoder::for_compression_kind(kind, &options)
                .unwrap()
                .unwrap();
            let wire = decoder.max_wire_message_size().unwrap();
            assert!(wire <= isize::MAX.unsigned_abs());
            assert!(wire >= limit.min(isize::MAX.unsigned_abs()));
        }
        let (_, decoder) = MessageEncoder::for_compression_kind(kind, &Options::default())
            .unwrap()
            .unwrap();
        assert_eq!(decoder.max_wire_message_size(), None);
    }
}

#[cfg(feature = "lz4")]
#[test]
fn expanding_lz4_multiblock_output_fits_the_finite_wire_allowance() {
    let mut state = 1u64;
    let random: Vec<_> = (0..4096)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    let message = Message::multipart([
        Bytes::new(),
        Bytes::from(random),
        Bytes::from(vec![0x5a; 65]),
    ]);
    let limit = message.max_message_size_len();
    let mut encoder = Lz4Encoder::new().with_block_size(64).with_threshold(0);
    let mut decoder = MessageDecoder::Lz4(
        Lz4Decoder::new()
            .with_block_size(64)
            .with_max_message_size(Some(limit)),
    );
    let wire = encoder.encode(&message).unwrap().remove(0);
    assert!(wire.max_message_size_len() > limit);
    assert!(wire.max_message_size_len() <= decoder.max_wire_message_size().unwrap());
    let (mut receive, mut send) =
        ready_pair(limit, decoder.max_wire_message_size().unwrap(), false);
    let payload = through_framer(&mut receive, &mut send, &wire);
    assert_eq!(decoder.decode(payload).unwrap(), Some(message));
}
