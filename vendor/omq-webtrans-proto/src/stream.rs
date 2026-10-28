//! WebTransport stream type identifiers and helpers for encoding/decoding.

use bytes::{Buf, BufMut};

use super::{VarInt, VarIntUnexpectedEnd};
use crate::grease::is_grease_value;

/// Sent as the leading bytes on a unidirectional stream to indicate its stream type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UniStream(pub VarInt);

impl UniStream {
    /// Decode a unidirectional stream type identifier.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        Ok(UniStream(VarInt::decode(buf)?))
    }

    /// Encode this unidirectional stream type identifier.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        self.0.encode(buf)
    }

    /// Return `true` when this type uses RFC 9114 GREASE spacing.
    pub fn is_grease(&self) -> bool {
        is_grease_value(self.0.into_inner())
    }

    /// Build a unidirectional stream type from a known `u32` value.
    pub const fn from_u32(value: u32) -> Self {
        Self(VarInt::from_u32(value))
    }

    /// HTTP/3 control stream type.
    pub const CONTROL: UniStream = UniStream::from_u32(0x00);
    /// HTTP/3 push stream type.
    pub const PUSH: UniStream = UniStream::from_u32(0x01);
    /// HTTP/3 QPACK encoder stream type.
    pub const QPACK_ENCODER: UniStream = UniStream::from_u32(0x02);
    /// HTTP/3 QPACK decoder stream type.
    pub const QPACK_DECODER: UniStream = UniStream::from_u32(0x03);
    /// WebTransport unidirectional stream type.
    pub const WEBTRANSPORT: UniStream = UniStream::from_u32(0x54);
}
