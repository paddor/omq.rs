//! `zstd+tcp://` per-part transform backed by zrip.
//!
//! Dict shipment is a single-part ZMTP message containing ZDICT-format bytes.
//! The ZDICT magic (`37 A4 30 EC`) is the shipment discriminator. Compressed
//! parts are raw Zstd frames and must carry `Frame_Content_Size`.

use bytes::Bytes;
use smallvec::SmallVec;
use zrip::dict::Dictionary;
use zrip::{CompressContext, DecompressContext};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, plaintext_payload, take_budget, validate_dict,
};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];
const MIN_COMPRESS_NO_DICT: usize = 512;
const MIN_COMPRESS_WITH_DICT: usize = 64;
const MAX_DECOMPRESSED_SIZE: usize = 0x4000_0000 - 1;
const TRAIN_MAX_SAMPLES: usize = 1000;
const TRAIN_MAX_BYTES: usize = 100 * 1024;
const TRAIN_MAX_SAMPLE_LEN: usize = 2048;
const USER_DICT_ID_MIN: u32 = 32_768;
const USER_DICT_ID_MAX: u32 = 0x7FFF_FFFF;

pub const MAX_DICT_BYTES: usize = 8 * 1024;
pub const DEFAULT_LEVEL: i32 = 1;
pub const DEFAULT_DICT_CAPACITY: usize = 2048;

pub fn train_zdict(samples: &[&[u8]], capacity: usize) -> Option<Bytes> {
    use zrip::dict::fastcover::{FastCoverParams, select_segments};
    use zrip::dict::finalize::finalize_dictionary;

    if samples.is_empty() || capacity == 0 {
        return None;
    }
    let capacity = capacity.min(MAX_DICT_BYTES);
    let content = select_segments(samples, capacity, &FastCoverParams::default());
    let mut dict = finalize_dictionary(&content, samples, capacity);
    patch_user_dict_id(&mut dict).ok()?;
    Dictionary::from_bytes(&dict).ok()?;
    Some(Bytes::from(dict))
}

pub fn is_dict_shipment(msg: &Message) -> bool {
    msg.len() == 1
        && msg
            .part_bytes(0)
            .is_some_and(|part| part.starts_with(&ZDICT_MAGIC))
}

struct TrainState {
    samples: Vec<Vec<u8>>,
    total_bytes: usize,
}

pub struct ZstdEncoder {
    send_dict: Option<Bytes>,
    send_dict_shipped: bool,
    max_message_size: Option<usize>,
    level: i32,
    cctx: Option<CompressContext>,
    train: Option<TrainState>,
    threshold_override: Option<usize>,
    dict_capacity: usize,
}

impl std::fmt::Debug for ZstdEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdEncoder")
            .field("send_dict_len", &self.send_dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send_dict_shipped)
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

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            max_message_size: None,
            level: DEFAULT_LEVEL,
            cctx: None,
            train: None,
            threshold_override: None,
            dict_capacity: DEFAULT_DICT_CAPACITY,
        }
    }
}

impl ZstdEncoder {
    pub fn new() -> Self {
        Self::default()
    }

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

    #[must_use]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold_override = Some(threshold);
        self
    }

    #[must_use]
    pub fn with_dict_capacity(mut self, capacity: usize) -> Self {
        self.dict_capacity = capacity.min(MAX_DICT_BYTES);
        self
    }

    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "Zstd", MAX_DICT_BYTES)?;
        if dict.len() < 4 || dict[..4] != ZDICT_MAGIC {
            return Err(Error::Protocol(
                "Zstd dictionary must start with ZDICT magic".into(),
            ));
        }
        Dictionary::from_bytes(&dict)
            .map_err(|e| Error::Protocol(format!("invalid zstd dictionary: {e}")))?;
        Ok(Self {
            send_dict: Some(dict),
            train: None,
            ..Self::default()
        })
    }

    #[must_use]
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self.cctx = None;
        self
    }

    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    pub fn passthrough_threshold(&self) -> Option<usize> {
        if self.threshold_override.is_some() {
            Some(self.effective_threshold())
        } else if self.train.is_some() {
            None
        } else if self.send_dict.is_none() {
            Some(MIN_COMPRESS_NO_DICT)
        } else {
            None
        }
    }

    pub fn can_offload(&self) -> bool {
        self.train.is_none() && (self.send_dict.is_none() || self.send_dict_shipped)
    }

    #[must_use]
    pub fn new_offload(&self) -> Self {
        Self {
            send_dict: self.send_dict.clone(),
            send_dict_shipped: true,
            max_message_size: self.max_message_size,
            level: self.level,
            cctx: None,
            train: None,
            threshold_override: self.threshold_override,
            dict_capacity: self.dict_capacity,
        }
    }

    pub fn sync_dict(&mut self, primary: &Self) {
        let same = match (&self.send_dict, &primary.send_dict) {
            (None, None) => true,
            (Some(a), Some(b)) => a.as_ptr() == b.as_ptr() && a.len() == b.len(),
            _ => false,
        };
        if !same {
            self.send_dict.clone_from(&primary.send_dict);
            self.cctx = None;
        }
    }

    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        for part in &msg.parts_payload() {
            self.maybe_train(&part.as_bytes());
        }

        let mut out: TransformedOut = SmallVec::new();
        if let Some(dict) = self.send_dict.clone()
            && !self.send_dict_shipped
        {
            out.push(Message::single(dict));
            self.send_dict_shipped = true;
        }
        let mut wire = Message::new();
        for part in &msg.parts_payload() {
            wire.push_part_payload(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    fn effective_threshold(&self) -> usize {
        self.threshold_override
            .unwrap_or(if self.send_dict.is_some() {
                MIN_COMPRESS_WITH_DICT
            } else {
                MIN_COMPRESS_NO_DICT
            })
    }

    fn maybe_train(&mut self, plain: &[u8]) {
        let Some(state) = self.train.as_mut() else {
            return;
        };
        if plain.len() >= TRAIN_MAX_SAMPLE_LEN {
            return;
        }
        state.samples.push(plain.to_vec());
        state.total_bytes += plain.len();
        if state.samples.len() < TRAIN_MAX_SAMPLES && state.total_bytes < TRAIN_MAX_BYTES {
            return;
        }
        let state = self.train.take().unwrap();
        let samples: Vec<&[u8]> = state.samples.iter().map(Vec::as_slice).collect();
        let Some(dict) = train_zdict(&samples, self.dict_capacity) else {
            return;
        };
        self.send_dict = Some(dict);
        self.send_dict_shipped = false;
        self.cctx = None;
    }

    fn ensure_cctx(&mut self) -> Result<&mut CompressContext> {
        if self.cctx.is_none() {
            let ctx = if let Some(dict_raw) = &self.send_dict {
                let dict = Dictionary::from_bytes(dict_raw).map_err(|e| decompress_err(&e))?;
                CompressContext::with_dict(self.level, dict).map_err(|e| compress_err(&e))?
            } else {
                CompressContext::new(self.level).map_err(|e| compress_err(&e))?
            };
            self.cctx = Some(ctx);
        }
        Ok(self.cctx.as_mut().unwrap())
    }

    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.as_bytes();
        if plain.len() < self.effective_threshold() {
            return Ok(plaintext_payload(&plain));
        }
        let compressed = self
            .ensure_cctx()?
            .compress(&plain)
            .map_err(|e| compress_err(&e))?;
        if compressed.len() >= plain.len().saturating_sub(ENVELOPE_PLAIN) {
            return Ok(plaintext_payload(&plain));
        }
        Ok(Payload::from_bytes(Bytes::copy_from_slice(&compressed)))
    }
}

pub struct ZstdDecoder {
    recv_dict: Option<Bytes>,
    max_message_size: Option<usize>,
    max_recv_dict_size: usize,
    dctx: DecompressContext,
}

impl std::fmt::Debug for ZstdDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdDecoder")
            .field("recv_dict_len", &self.recv_dict.as_ref().map(Bytes::len))
            .field("max_message_size", &self.max_message_size)
            .finish_non_exhaustive()
    }
}

impl Default for ZstdDecoder {
    fn default() -> Self {
        Self {
            recv_dict: None,
            max_message_size: None,
            max_recv_dict_size: MAX_DICT_BYTES,
            dctx: DecompressContext::new(),
        }
    }
}

impl ZstdDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    #[must_use]
    pub fn with_max_recv_dict_size(mut self, max: usize) -> Self {
        self.max_recv_dict_size = max.min(MAX_DICT_BYTES);
        self
    }

    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        let mut out = Message::new();
        let parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        let mut budget_left = self.max_message_size;
        for (idx, part) in parts.into_iter().enumerate() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "zstd part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => {
                    let body_len = bytes.len() - 4;
                    take_budget(&mut budget_left, body_len)?;
                    out.push_part_payload(Payload::from_bytes(bytes.slice(4..)));
                }
                ZSTD_MAGIC => out.push_part_payload(self.decode_zstd(&bytes, &mut budget_left)?),
                ZDICT_MAGIC => {
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
                    validate_dict(&bytes, "Zstd", self.max_recv_dict_size)?;
                    let dict = Dictionary::from_bytes(&bytes).map_err(|e| decompress_err(&e))?;
                    self.dctx = DecompressContext::with_dict(dict);
                    self.recv_dict = Some(bytes);
                    return Ok(None);
                }
                _ => return Err(Error::Protocol("unknown zstd sentinel".into())),
            }
        }
        Ok(Some(out))
    }

    fn decode_zstd(&mut self, bytes: &Bytes, budget: &mut Option<usize>) -> Result<Payload> {
        let header =
            zrip::frame::header::parse_frame_header(bytes).map_err(|e| decompress_err(&e))?;
        let declared = header.frame_content_size.ok_or_else(|| {
            Error::Protocol("zstd frame missing required Frame_Content_Size".into())
        })?;
        let decompressed_size = usize::try_from(declared)
            .map_err(|_| Error::Protocol("zstd declared size exceeds usize".into()))?;
        if decompressed_size > MAX_DECOMPRESSED_SIZE {
            return Err(Error::Protocol(
                "zstd declared size exceeds absolute limit".into(),
            ));
        }
        take_budget(budget, decompressed_size)?;
        let result = self
            .dctx
            .decompress_with_limit(bytes, decompressed_size)
            .map_err(|e| decompress_err(&e))?;
        if result.len() != decompressed_size {
            return Err(Error::Protocol(
                "zstd decompressed length disagrees with declared".into(),
            ));
        }
        Ok(Payload::from_bytes(Bytes::from(result.into_owned())))
    }
}

fn patch_user_dict_id(dict: &mut [u8]) -> core::result::Result<(), ()> {
    if dict.len() < 8 || dict[..4] != ZDICT_MAGIC {
        return Err(());
    }
    let span = USER_DICT_ID_MAX - USER_DICT_ID_MIN + 1;
    let id = USER_DICT_ID_MIN + (rand::random::<u32>() % span);
    dict[4..8].copy_from_slice(&id.to_le_bytes());
    Ok(())
}

fn compress_err(e: &zrip::CompressError) -> Error {
    Error::Protocol(format!("zstd: {e}"))
}

fn decompress_err(e: &zrip::DecompressError) -> Error {
    Error::Protocol(format!("zstd: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trained_dict() -> Bytes {
        let samples: Vec<&[u8]> = (0..200)
            .map(|_| &b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..])
            .collect();
        train_zdict(&samples, MAX_DICT_BYTES).expect("training must succeed")
    }

    #[test]
    fn roundtrip_plain_and_compressed() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &ZSTD_MAGIC);
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);

        let wire = enc.encode(&Message::single("hi")).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
    }

    #[test]
    fn frame_content_size_is_present() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        let header = zrip::frame::header::parse_frame_header(&bytes).unwrap();
        assert_eq!(header.frame_content_size, Some(plain.len() as u64));
    }

    #[test]
    fn options_custom_level_reaches_encoder() {
        use crate::options::Options;
        use crate::proto::transform::{CompressionKind, MessageEncoder};

        let options = Options::new().compression_level(1);
        let (enc, _) = MessageEncoder::for_compression_kind(CompressionKind::Zstd, &options)
            .unwrap()
            .unwrap();
        match enc {
            MessageEncoder::Zstd(enc) => assert_eq!(enc.level, 1),
            #[cfg(feature = "lz4")]
            MessageEncoder::Lz4(_) => panic!("expected zstd encoder"),
        }
    }

    #[test]
    fn custom_level_roundtrip() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new().with_level(1);
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        assert_eq!(enc.level, 1);
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn dict_roundtrip() {
        let dict = trained_dict();
        let plain = b"the-quick-brown-fox-jumps-over-the-lazy-dog\n".repeat(2);
        let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[0].part_bytes(0).unwrap().as_ref(), &dict[..]);
        assert!(dec.decode(wire[0].clone()).unwrap().is_none());
        assert_eq!(dec.recv_dict.as_ref().unwrap().as_ref(), &dict[..]);
        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn raw_zdict_blob_is_a_dict_shipment() {
        let dict = trained_dict();
        let mut dec = ZstdDecoder::new();
        assert!(dec.decode(Message::single(dict)).unwrap().is_none());
    }

    #[test]
    fn dict_cap_applies_to_zdict_blob() {
        let mut shipment = vec![0u8; MAX_DICT_BYTES + 1];
        shipment[..4].copy_from_slice(&ZDICT_MAGIC);
        let mut dec = ZstdDecoder::new().with_max_recv_dict_size(usize::MAX);
        let err = dec
            .decode(Message::single(Bytes::from(shipment)))
            .unwrap_err();
        match err {
            Error::Protocol(msg) => assert!(msg.contains("exceeds max")),
            other => panic!("expected protocol error, got {other:?}"),
        }
    }

    #[test]
    fn budget_checked_before_decode() {
        let plain = vec![b'k'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain)).unwrap();
        let mut dec = ZstdDecoder::new().with_max_message_size(Some(1024));
        assert!(matches!(
            dec.decode(wire.into_iter().next().unwrap()).unwrap_err(),
            Error::MessageTooLarge { .. }
        ));
    }

    #[test]
    fn auto_train_ships_dict_and_blocks_offload() {
        let mut enc = ZstdEncoder::new().with_auto_train();
        assert!(!enc.can_offload());
        let mut dec = ZstdDecoder::new();
        let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
        for _ in 0..2000 {
            let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
            for part in wire {
                let _ = dec.decode(part).unwrap();
            }
            if enc.send_dict.is_some() {
                break;
            }
        }
        assert!(enc.send_dict.is_some(), "auto-train never produced dict");
        let dict = enc.send_dict.clone().unwrap();
        assert_eq!(&dict[..4], &ZDICT_MAGIC);
        let id = u32::from_le_bytes(dict[4..8].try_into().unwrap());
        assert!((USER_DICT_ID_MIN..=USER_DICT_ID_MAX).contains(&id));
    }
}
