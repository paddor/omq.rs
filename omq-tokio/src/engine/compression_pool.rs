use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use omq_proto::proto::transform::MessageEncoder;

/// Runtime-global pool of reusable compression encoders, shared across
/// all connections on a socket. Sized to `available_parallelism()`.
///
/// Encoders keep their warm `out_buf` and configured compression
/// context across borrows, avoiding per-message allocation.
pub(crate) struct CompressionPool {
    encoders: Mutex<Vec<MessageEncoder>>,
    in_flight: AtomicUsize,
    cap: usize,
}

impl std::fmt::Debug for CompressionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionPool")
            .field("cap", &self.cap)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl CompressionPool {
    pub(crate) fn new() -> Self {
        let cap = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            encoders: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            cap,
        }
    }

    /// Borrow a pool encoder matching the primary's variant, syncing
    /// its full configuration. Returns `None` when all `cap` encoders are
    /// in flight. New encoders are created on demand up to `cap`.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn try_take(&self, primary: &MessageEncoder) -> Option<MessageEncoder> {
        {
            let mut pool = self.encoders.lock().unwrap();
            if let Some(pos) = pool.iter().position(|e| e.variant_matches(primary)) {
                let mut enc = pool.swap_remove(pos);
                drop(pool);
                self.in_flight.fetch_add(1, Ordering::Relaxed);
                enc.sync_offload_config(primary);
                return Some(enc);
            }
        }
        let prev = self.in_flight.fetch_add(1, Ordering::Relaxed);
        if prev >= self.cap {
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(MessageEncoder::new_offload(primary))
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    pub(crate) fn put(&self, enc: MessageEncoder) {
        self.encoders.lock().unwrap().push(enc);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn clear(&self) {
        self.encoders.lock().unwrap().clear();
    }
}

#[cfg(all(test, any(feature = "lz4", feature = "zstd")))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use omq_proto::{CompressionKind, Message, Options};

    #[test]
    fn reused_encoder_keeps_the_new_primary_threshold() {
        for kind in [
            #[cfg(feature = "lz4")]
            CompressionKind::Lz4,
            #[cfg(feature = "zstd")]
            CompressionKind::Zstd,
        ] {
            let pool = CompressionPool::new();
            let message = Message::single(Bytes::from(vec![0x5a; 4096]));
            let (old, _) = MessageEncoder::for_compression_kind(
                kind,
                &Options::default().compression_threshold(64),
            )
            .unwrap()
            .unwrap();
            let mut warm = pool.try_take(&old).unwrap();
            let old_wire = warm.encode(&message).unwrap();
            pool.put(warm);
            let (mut primary, _) = MessageEncoder::for_compression_kind(
                kind,
                &Options::default().compression_threshold(8192),
            )
            .unwrap()
            .unwrap();
            let expected = primary.encode(&message).unwrap();
            assert_ne!(
                old_wire, expected,
                "fixture must distinguish configurations"
            );
            let mut reused = pool.try_take(&primary).unwrap();
            assert_eq!(reused.encode(&message).unwrap(), expected, "kind={kind:?}");
            pool.put(reused);
        }
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn reused_lz4_encoder_switches_and_removes_dictionaries_without_shipping() {
        let pool = CompressionPool::new();
        let mut state = 17_u64;
        let dictionaries: Vec<_> = (0..2)
            .map(|_| {
                Bytes::from(
                    (0..2048)
                        .map(|_| {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            state.to_le_bytes()[0]
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        for dictionary in [
            Some(dictionaries[0].clone()),
            Some(dictionaries[1].clone()),
            None,
            Some(dictionaries[0].clone()),
        ] {
            let message = Message::single(dictionary.as_ref().map_or_else(
                || Bytes::from(vec![0x5a; 1024]),
                |dict| dict.slice(256..1280),
            ));
            let options = Options {
                compression_dict: dictionary,
                compression_threshold: Some(0),
                ..Options::default()
            };
            let (mut primary, mut decoder) =
                MessageEncoder::for_compression_kind(CompressionKind::Lz4, &options)
                    .unwrap()
                    .unwrap();
            let mut expected = primary.encode(&message).unwrap();
            if let Some(shipment) = MessageEncoder::take_leading_dict_shipment(&mut expected) {
                assert!(decoder.decode(shipment).unwrap().is_none());
            }
            let mut reused = pool.try_take(&primary).unwrap();
            let actual = reused.encode(&message).unwrap();
            assert!(
                actual == expected,
                "offload dictionary must match the new primary"
            );
            assert_eq!(
                actual.len(),
                1,
                "offload encoders must never ship dictionaries"
            );
            assert_eq!(decoder.decode(actual[0].clone()).unwrap(), Some(message));
            pool.put(reused);
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn reused_zstd_encoder_rebuilds_a_warm_context_for_a_different_level() {
        use std::fmt::Write as _;
        let pool = CompressionPool::new();
        let mut body = String::new();
        for index in 0..2048 {
            writeln!(
                body,
                "{{\"id\":{},\"price\":{},\"quantity\":{},\"name\":\"structured-product-{}\"}}",
                index % 127,
                index % 179,
                index % 37,
                index % 63,
            )
            .unwrap();
        }
        let message = Message::single(body);
        let (old, _) = MessageEncoder::for_compression_kind(
            CompressionKind::Zstd,
            &Options::default().compression_level(-8),
        )
        .unwrap()
        .unwrap();
        let mut warm = pool.try_take(&old).unwrap();
        let old_wire = warm.encode(&message).unwrap();
        pool.put(warm);
        let (mut primary, mut decoder) = MessageEncoder::for_compression_kind(
            CompressionKind::Zstd,
            &Options::default().compression_level(4),
        )
        .unwrap()
        .unwrap();
        let expected = primary.encode(&message).unwrap();
        assert!(old_wire != expected, "fixture must distinguish levels");
        let mut reused = pool.try_take(&primary).unwrap();
        let actual = reused.encode(&message).unwrap();
        assert!(
            actual == expected,
            "context must use the new compression level"
        );
        assert_eq!(decoder.decode(actual[0].clone()).unwrap(), Some(message));
        pool.put(reused);
    }
}
