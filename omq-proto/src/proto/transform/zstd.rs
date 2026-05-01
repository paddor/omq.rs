//! `zstd+tcp://` per-part transform.
//!
//! Wire format per `omq-zstd/RFC.md`. Each post-handshake ZMTP message
//! part begins with one of three 4-byte sentinels:
//!
//! ```text
//! 00 00 00 00              uncompressed plaintext follows
//! 28 B5 2F FD              the wire part IS a Zstandard frame
//! 37 A4 30 EC              dict shipment (single-part ZMTP message)
//! ```
//!
//! Differences from LZ4:
//! - The compressed wire part *is* the Zstd frame; no extra length
//!   prefix. The decoder reads `Frame_Content_Size` from the Zstd
//!   header to bound the output buffer (RFC §5.4 / §5.6).
//! - Thresholds: 64 B with dict, 512 B without (RFC §5.5).
//! - Net-saving check: skip if compressed >= plaintext - 4 (RFC §5.5).
//! - Dict cap: 64 KiB total (sentinel + bytes) (RFC §6.2).
//!
//! Auto-trained dictionaries (RFC §6.5): opt-in via `with_auto_train`.
//! Samples flow through `encode` until either 1000 messages or
//! 100 KiB total plaintext have been collected, at which point we
//! call `zstd_safe::train_from_buffer` to produce an 8 KiB dict,
//! patch its dict-id field to a random user-range value
//! (`32768..2^31`), install it as the send dict, and ship it on the
//! next outbound message via the existing `SENTINEL_DICT` path. If
//! training fails (samples too uniform, etc.) auto-train is
//! disabled for the connection - no retry. Samples larger than
//! `TRAIN_MAX_SAMPLE_LEN` (1024 B) are skipped to keep the trainer
//! input balanced.

use bytes::Bytes;
use smallvec::SmallVec;
use zstd_safe::{CCtx, CParameter, DCtx};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, build_dict_shipment, plaintext_payload, take_budget,
    validate_dict,
};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const SENTINEL_DICT: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

/// RFC §5.5 thresholds.
const MIN_COMPRESS_NO_DICT: usize = 512;
const MIN_COMPRESS_WITH_DICT: usize = 64;

/// RFC §6.2: a dictionary message MUST NOT exceed 64 KiB total
/// (sentinel + dict bytes), so the dict body itself is capped at
/// `MAX_DICT_BYTES = 64 KiB - 4`.
pub const MAX_DICT_BYTES: usize = 64 * 1024 - 4;

/// RFC §5.2: default compression level. Negative = Zstd "fast" strategy.
const DEFAULT_LEVEL: i32 = -3;

/// Auto-train: trained dictionary capacity (RFC §6.5). 8 KiB hits the
/// sweet spot of "small enough to fit in a few packets" and "large
/// enough to capture redundancy across the early messages on the wire".
const DICT_CAPACITY: usize = 8 * 1024;

/// Auto-train: trigger thresholds. Whichever fires first wins.
const TRAIN_MAX_SAMPLES: usize = 1000;
const TRAIN_MAX_BYTES: usize = 100 * 1024;

/// Auto-train: skip samples larger than this. Keeps the trainer
/// input balanced; large parts skew toward themselves and produce a
/// dict that doesn't help the bulk of small messages.
const TRAIN_MAX_SAMPLE_LEN: usize = 1024;

/// User-range dict-id space per RFC § 6.5. The trained dict gets a
/// random id in this range so distinct sockets don't collide.
const USER_DICT_ID_MIN: u32 = 32_768;
const USER_DICT_ID_MAX: u32 = 0x7FFF_FFFF;

/// ZDICT magic (`37 A4 30 EC` LE) at offset 0 of a trained dict.
/// The dict-id sits at offset 4..8 as a little-endian u32 - that's
/// what we patch after training.
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

/// Per-connection Zstd state.
pub struct ZstdTransform {
    send_dict: Option<Bytes>,
    send_dict_shipped: bool,
    recv_dict: Option<Bytes>,
    max_message_size: Option<usize>,
    level: i32,
    cctx: CCtx<'static>,
    /// Whether `cctx` has had its compression level + (optional)
    /// dictionary applied since construction. We delay configuration
    /// to first encode so a `with_level` / `with_auto_train` builder
    /// chain doesn't have to re-apply parameters mid-construction.
    cctx_configured: bool,
    dctx: DCtx<'static>,
    /// Auto-train state. `Some` while collecting samples; cleared
    /// after a successful train (dict installed) or a failed train
    /// (auto-train permanently disabled for this connection).
    train: Option<TrainState>,
    /// Reusable output buffer for compress2. Each encode resizes it
    /// to `compress_bound(plain.len())`; sticky allocation after the
    /// first message keeps us out of the allocator on the hot path.
    out_buf: Vec<u8>,
}

struct TrainState {
    samples: Vec<Bytes>,
    total_bytes: usize,
}

impl std::fmt::Debug for ZstdTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdTransform")
            .field(
                "send_dict_len",
                &self.send_dict.as_ref().map(bytes::Bytes::len),
            )
            .field("send_dict_shipped", &self.send_dict_shipped)
            .field(
                "recv_dict_len",
                &self.recv_dict.as_ref().map(bytes::Bytes::len),
            )
            .field("max_message_size", &self.max_message_size)
            .field("level", &self.level)
            .field(
                "auto_train",
                &self
                    .train
                    .as_ref()
                    .map(|t| (t.samples.len(), t.total_bytes)),
            )
            .finish_non_exhaustive()
    }
}

impl Default for ZstdTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl ZstdTransform {
    pub fn new() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            recv_dict: None,
            max_message_size: None,
            level: DEFAULT_LEVEL,
            cctx: CCtx::create(),
            cctx_configured: false,
            dctx: DCtx::create(),
            train: None,
            out_buf: Vec::new(),
        }
    }

    /// Enable auto-trained dict mode. The first `TRAIN_MAX_SAMPLES`
    /// messages (or `TRAIN_MAX_BYTES` total bytes, whichever caps
    /// first) are sampled, then a dict is trained and shipped on
    /// the next outbound message. No-op if a static `send_dict` is
    /// already configured.
    #[must_use]
    pub fn with_auto_train(mut self) -> Self {
        if self.send_dict.is_none() {
            self.train = Some(TrainState {
                samples: Vec::with_capacity(64),
                total_bytes: 0,
            });
        }
        self
    }

    /// Construct with a send-side dictionary. Errors if the dict is
    /// empty or larger than [`MAX_DICT_BYTES`].
    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "Zstd", MAX_DICT_BYTES)?;
        let mut s = Self::new();
        s.send_dict = Some(dict);
        Ok(s)
    }

    /// Override the compression level (default -3, RFC §5.2).
    #[must_use]
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        // Force re-apply on next encode (in case a builder chain set
        // the level after the first encode - defensive; today the
        // builder pattern is finished before any encoding).
        self.cctx_configured = false;
        self
    }

    /// Set the decompression-size budget. `None` = unlimited.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        // Sample plaintext parts for auto-train BEFORE any compression
        // runs - we want raw user payloads, not transformed wire bytes.
        // If the threshold is hit here, a dict gets installed and the
        // very next encode iteration will ship it.
        for part in msg.parts() {
            self.maybe_train(&part.coalesce());
        }

        let mut out: TransformedOut = SmallVec::new();
        if let Some(dict) = self.send_dict.clone()
            && !self.send_dict_shipped
        {
            out.push(build_dict_shipment(SENTINEL_DICT, &dict));
            self.send_dict_shipped = true;
        }
        let mut wire = Message::new();
        for part in msg.parts() {
            wire.push_part(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    /// Append a plaintext sample to the trainer, and if the
    /// thresholds are hit, train + install + arm shipment. Failure
    /// (zstd returns ZDICT errors when samples are too uniform)
    /// permanently disables auto-train for the connection.
    fn maybe_train(&mut self, plain: &[u8]) {
        let Some(state) = self.train.as_mut() else {
            return;
        };
        if plain.len() >= TRAIN_MAX_SAMPLE_LEN {
            return;
        }
        state.samples.push(Bytes::copy_from_slice(plain));
        state.total_bytes += plain.len();
        if state.samples.len() < TRAIN_MAX_SAMPLES && state.total_bytes < TRAIN_MAX_BYTES {
            return;
        }
        // Threshold hit. Take ownership of the samples (the trainer
        // wants a single concatenated buffer + per-sample sizes) and
        // attempt a train. Either outcome ends `self.train`.
        let state = self.train.take().unwrap();
        let mut samples_buf: Vec<u8> = Vec::with_capacity(state.total_bytes);
        let mut sizes: Vec<usize> = Vec::with_capacity(state.samples.len());
        for s in &state.samples {
            samples_buf.extend_from_slice(s);
            sizes.push(s.len());
        }
        let mut dict_buf: Vec<u8> = Vec::with_capacity(DICT_CAPACITY);
        // train failed → auto-train disabled
        let Ok(trained_len) = zstd_safe::train_from_buffer(&mut dict_buf, &samples_buf, &sizes)
        else {
            return;
        };
        dict_buf.truncate(trained_len);
        if let Err(()) = patch_user_dict_id(&mut dict_buf) {
            // Trained output didn't have the ZDICT magic - shouldn't
            // happen, but bail out gracefully if it does.
            return;
        }
        let dict = Bytes::from(dict_buf);
        if validate_dict(&dict, "Zstd", MAX_DICT_BYTES).is_err() {
            return;
        }
        self.send_dict = Some(dict);
        self.send_dict_shipped = false;
        // Auto-train just installed a dict; force re-config so the
        // next encode loads it onto the cctx.
        self.cctx_configured = false;
    }

    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        let mut out = Message::new();
        let parts = msg.into_parts();
        let multipart = parts.len() > 1;
        let mut budget_left = self.max_message_size;
        for (idx, part) in parts.into_iter().enumerate() {
            let bytes = part.coalesce();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "zstd part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            if sentinel == SENTINEL_PLAIN {
                let body_len = bytes.len() - 4;
                take_budget(&mut budget_left, body_len)?;
                out.push_part(Payload::from_bytes(bytes.slice(4..)));
            } else if sentinel == ZSTD_MAGIC {
                let part = self.decode_zstd(&bytes, &mut budget_left)?;
                out.push_part(part);
            } else if sentinel == SENTINEL_DICT {
                if multipart || idx != 0 {
                    return Err(Error::Protocol(
                        "zstd dict shipment must be a single-part message".into(),
                    ));
                }
                if self.recv_dict.is_some() {
                    return Err(Error::Protocol(
                        "zstd dict shipped twice on the same connection".into(),
                    ));
                }
                let dict = bytes.slice(4..);
                validate_dict(&dict, "Zstd", MAX_DICT_BYTES)?;
                self.recv_dict = Some(dict);
                return Ok(None);
            } else {
                return Err(Error::Protocol("unknown zstd sentinel".into()));
            }
        }
        Ok(Some(out))
    }

    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.coalesce();
        let threshold = if self.send_dict.is_some() {
            MIN_COMPRESS_WITH_DICT
        } else {
            MIN_COMPRESS_NO_DICT
        };
        if plain.len() < threshold {
            return Ok(plaintext_payload(plain));
        }

        // First-use: pin compression level and (if any) dictionary on
        // the cctx. ResetDirective::SessionOnly between calls keeps
        // these - no need to re-set them per message.
        if !self.cctx_configured {
            self.cctx
                .set_parameter(CParameter::CompressionLevel(self.level))
                .map_err(zstd_err)?;
            if let Some(dict) = self.send_dict.as_ref() {
                self.cctx.load_dictionary(dict).map_err(zstd_err)?;
            }
            self.cctx_configured = true;
        }

        let bound = zstd_safe::compress_bound(plain.len());
        // Reuse the output buffer; resize_with avoids re-zeroing
        // already-initialised tail bytes when growing.
        if self.out_buf.len() < bound {
            self.out_buf.resize(bound, 0);
        }

        // SessionOnly reset clears per-frame state (running hash
        // tables, etc.) but preserves the level and dictionary set
        // above. Setting `pledged_src_size` ensures Frame_Content_Size
        // is written into the Zstd header (RFC §5.4).
        self.cctx
            .reset(zstd_safe::ResetDirective::SessionOnly)
            .map_err(zstd_err)?;
        self.cctx
            .set_pledged_src_size(Some(plain.len() as u64))
            .map_err(zstd_err)?;
        let n = self
            .cctx
            .compress2(&mut self.out_buf[..bound], &plain)
            .map_err(zstd_err)?;
        // RFC §5.5: passthrough if net saving is non-positive. The
        // compressed envelope is just `n` (Zstd magic is its first 4
        // bytes); the plaintext envelope is `4 + plain.len()`.
        if n >= plain.len() - ENVELOPE_PLAIN {
            return Ok(plaintext_payload(plain));
        }
        // Copy out the compressed bytes; out_buf stays for the next
        // message. Could be a `Bytes::copy_from_slice` directly.
        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..n],
        )))
    }

    fn decode_zstd(&mut self, bytes: &Bytes, budget: &mut Option<usize>) -> Result<Payload> {
        // RFC §5.6: read Frame_Content_Size; reject if absent.
        let declared = match zstd_safe::get_frame_content_size(bytes) {
            Ok(Some(n)) => n,
            Ok(None) => {
                return Err(Error::Protocol(
                    "Zstd frame missing required Frame_Content_Size".into(),
                ));
            }
            Err(_) => {
                return Err(Error::Protocol("malformed Zstd frame header".into()));
            }
        };
        let decompressed_size = usize::try_from(declared)
            .map_err(|_| Error::Protocol("Zstd declared size exceeds usize".into()))?;
        // Bound the allocation BEFORE we vec![0u8; ...] (RFC §5.6).
        take_budget(budget, decompressed_size)?;

        let mut out = vec![0u8; decompressed_size];
        self.dctx
            .reset(zstd_safe::ResetDirective::SessionOnly)
            .map_err(zstd_err)?;
        let n = if let Some(dict) = self.recv_dict.as_ref() {
            self.dctx
                .decompress_using_dict(&mut out, bytes, dict)
                .map_err(zstd_err)?
        } else {
            self.dctx.decompress(&mut out, bytes).map_err(zstd_err)?
        };
        if n != decompressed_size {
            return Err(Error::Protocol(
                "Zstd decompressed length disagrees with declared".into(),
            ));
        }
        Ok(Payload::from_bytes(Bytes::from(out)))
    }
}

/// Overwrite the dict-id field (bytes 4..8 in a ZDICT-formatted
/// dictionary) with a random user-range id. Returns `Err(())` if
/// the buffer doesn't carry the ZDICT magic at offset 0.
fn patch_user_dict_id(dict: &mut [u8]) -> std::result::Result<(), ()> {
    if dict.len() < 8 || dict[..4] != ZDICT_MAGIC {
        return Err(());
    }
    let span = USER_DICT_ID_MAX - USER_DICT_ID_MIN + 1;
    let id = USER_DICT_ID_MIN + (rand::random::<u32>() % span);
    dict[4..8].copy_from_slice(&id.to_le_bytes());
    Ok(())
}

fn zstd_err(code: usize) -> Error {
    Error::Protocol(format!("zstd: {}", zstd_safe::get_error_name(code)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_part_uses_plaintext_sentinel() {
        let mut enc = ZstdTransform::new();
        let wire = enc.encode(&Message::single("hello")).unwrap();
        let bytes = wire[0].parts()[0].coalesce();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
        assert_eq!(&bytes[4..], &b"hello"[..]);
    }

    #[test]
    fn large_compressible_uses_zstd_magic() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdTransform::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].parts()[0].coalesce();
        assert_eq!(&bytes[..4], &ZSTD_MAGIC);

        let mut dec = ZstdTransform::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.parts()[0].coalesce().to_vec(), plain);
    }

    #[test]
    fn frame_content_size_is_present() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdTransform::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].parts()[0].coalesce();
        let declared = zstd_safe::get_frame_content_size(&bytes).unwrap();
        assert_eq!(declared, Some(plain.len() as u64));
    }

    #[test]
    fn dict_first_send_emits_shipment() {
        let dict = Bytes::from_static(b"a-shared-prefix-of-some-bytes-yeah");
        let mut enc = ZstdTransform::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("hi")).unwrap();
        assert_eq!(wire.len(), 2);
        let ship = wire[0].parts()[0].coalesce();
        assert_eq!(&ship[..4], &SENTINEL_DICT);
        assert_eq!(&ship[4..], &dict[..]);
    }

    #[test]
    fn dict_aware_roundtrip() {
        let dict = Bytes::from(vec![b'q'; 1024]);
        let plain = vec![b'q'; 80];
        let msg = Message::single(plain.clone());

        let mut enc = ZstdTransform::with_send_dict(dict.clone()).unwrap();
        let mut dec = ZstdTransform::with_send_dict(dict).unwrap();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        // Receiver consumes the dict shipment silently.
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());

        // Then the user message decodes against the installed dict.
        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.parts()[0].coalesce().to_vec(), plain);
    }

    #[test]
    fn rejects_short_part() {
        let mut dec = ZstdTransform::new();
        let m = Message::single(Bytes::from_static(b"abc"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_unknown_sentinel() {
        let mut dec = ZstdTransform::new();
        let m = Message::single(Bytes::from_static(b"NOPE-payload"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_second_dict() {
        let dict = Bytes::from_static(b"first-dict");
        let mut dec = ZstdTransform::new();
        let m1 = build_dict_shipment(SENTINEL_DICT, &dict);
        assert!(dec.decode(m1).unwrap().is_none());
        let m2 = build_dict_shipment(SENTINEL_DICT, &Bytes::from_static(b"second-dict"));
        assert!(matches!(dec.decode(m2).unwrap_err(), Error::Protocol(_)));
    }

    #[test]
    fn rejects_oversized_dict() {
        let big = Bytes::from(vec![0u8; MAX_DICT_BYTES + 1]);
        let err = ZstdTransform::with_send_dict(big).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn budget_lz4b_declared_size_check_runs_before_alloc() {
        let plain = vec![b'k'; 4096];
        let mut enc = ZstdTransform::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = ZstdTransform::new().with_max_message_size(Some(1024));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(
            matches!(err, Error::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    #[test]
    fn dict_shipment_does_not_count_budget() {
        let dict = Bytes::from(vec![b'd'; 4096]);
        let mut enc = ZstdTransform::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ok")).unwrap();

        let mut dec = ZstdTransform::new().with_max_message_size(Some(8));
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());
        let m = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(m.parts()[0].coalesce(), &b"ok"[..]);
    }

    #[test]
    fn auto_train_collects_samples_and_ships_dict() {
        let mut enc = ZstdTransform::new().with_auto_train();
        let mut dec = ZstdTransform::new();
        // Use distinct-but-related JSON-like samples so the trainer
        // has something to extract. Each sample is well under
        // TRAIN_MAX_SAMPLE_LEN.
        let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
        let mut roundtripped = 0usize;
        // Force the byte threshold (100 KiB ≈ 1750 of these samples).
        for _ in 0..2000 {
            let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
            for part in wire {
                if let Some(out) = dec.decode(part).unwrap() {
                    assert_eq!(out.parts()[0].coalesce(), &sample[..]);
                    roundtripped += 1;
                }
            }
            if enc.send_dict.is_some() {
                break;
            }
        }
        assert!(
            enc.send_dict.is_some(),
            "auto-train never produced a dict (rt={roundtripped})"
        );
        // The trained dict has the ZDICT magic and a user-range id.
        let dict = enc.send_dict.clone().unwrap();
        assert_eq!(&dict[..4], &ZDICT_MAGIC);
        let id = u32::from_le_bytes(dict[4..8].try_into().unwrap());
        assert!(
            (USER_DICT_ID_MIN..=USER_DICT_ID_MAX).contains(&id),
            "dict id {id} out of user range"
        );
        // Receiver hasn't seen the dict shipment yet because we
        // broke out before the post-train encode. Run one more
        // encode/decode cycle and confirm the dict ships and the
        // payload still roundtrips.
        let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
        let mut got_payload = false;
        for part in wire {
            if let Some(out) = dec.decode(part).unwrap() {
                assert_eq!(out.parts()[0].coalesce(), &sample[..]);
                got_payload = true;
            }
        }
        assert!(got_payload);
        assert_eq!(dec.recv_dict.as_ref().unwrap().as_ref(), dict.as_ref());
    }

    #[test]
    fn auto_train_skips_oversized_samples() {
        let mut enc = ZstdTransform::new().with_auto_train();
        // Send a single 64 KiB part - well over TRAIN_MAX_SAMPLE_LEN.
        // Auto-train should ignore it (no growth in samples).
        let big = Bytes::from(vec![b'x'; 65536]);
        let _ = enc.encode(&Message::single(big)).unwrap();
        let state = enc.train.as_ref().expect("auto-train still on");
        assert_eq!(state.samples.len(), 0);
        assert_eq!(state.total_bytes, 0);
    }

    #[test]
    fn auto_train_disabled_when_static_dict_present() {
        let dict = Bytes::from(vec![b'a'; 1024]);
        let enc = ZstdTransform::with_send_dict(dict)
            .unwrap()
            .with_auto_train();
        assert!(enc.train.is_none(), "static dict should disable auto-train");
    }
}
