//! QUIC variable-length integer encoding and decoding.

// Based on Quinn: https://github.com/quinn-rs/quinn/tree/main/quinn-proto/src
// Licensed under Apache-2.0 OR MIT

use std::{convert::TryInto, fmt};

use bytes::{Buf, BufMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// An integer less than 2^62.
///
/// Values of this type are suitable for encoding as QUIC variable-length integer.
// Rust does not currently model that the top two bits are reserved for the length tag.
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(pub(crate) u64);

impl VarInt {
    /// The largest representable value.
    pub const MAX: Self = Self((1 << 62) - 1);
    /// The largest encoded value length.
    pub const MAX_SIZE: usize = 8;

    /// Construct a `VarInt` infallibly.
    pub const fn from_u32(x: u32) -> Self {
        Self(x as u64)
    }

    /// Succeeds if `x` < 2^62.
    pub fn from_u64(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        if x <= Self::MAX.0 {
            Ok(Self(x))
        } else {
            Err(VarIntBoundsExceeded)
        }
    }

    /// Extract the integer value.
    pub const fn into_inner(self) -> u64 {
        self.0
    }

    /// Compute the number of bytes needed to encode this value.
    pub fn size(self) -> usize {
        let x = self.0;
        if x < (1 << 6) {
            1
        } else if x < (1 << 14) {
            2
        } else if x < (1 << 30) {
            4
        } else if x <= Self::MAX.0 {
            8
        } else {
            unreachable!("malformed VarInt");
        }
    }
}

impl From<VarInt> for u64 {
    fn from(x: VarInt) -> Self {
        x.0
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        Self(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        Self(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        Self(x.into())
    }
}

impl std::convert::TryFrom<u64> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x)
    }
}

impl std::convert::TryFrom<u128> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: u128) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x.try_into().map_err(|_| VarIntBoundsExceeded)?)
    }
}

impl std::convert::TryFrom<usize> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: usize) -> Result<Self, VarIntBoundsExceeded> {
        Self::try_from(x as u64)
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl VarInt {
    /// Decode a QUIC varint from an in-memory buffer.
    pub fn decode<B: Buf>(r: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        if !r.has_remaining() {
            return Err(VarIntUnexpectedEnd);
        }
        let mut buf = [0; 8];
        buf[0] = r.get_u8();
        let tag = buf[0] >> 6;
        buf[0] &= 0b0011_1111;
        let x = match tag {
            0b00 => u64::from(buf[0]),
            0b01 => {
                if r.remaining() < 1 {
                    return Err(VarIntUnexpectedEnd);
                }
                r.copy_to_slice(&mut buf[1..2]);
                u64::from(u16::from_be_bytes(buf[..2].try_into().unwrap()))
            }
            0b10 => {
                if r.remaining() < 3 {
                    return Err(VarIntUnexpectedEnd);
                }
                r.copy_to_slice(&mut buf[1..4]);
                u64::from(u32::from_be_bytes(buf[..4].try_into().unwrap()))
            }
            0b11 => {
                if r.remaining() < 7 {
                    return Err(VarIntUnexpectedEnd);
                }
                r.copy_to_slice(&mut buf[1..8]);
                u64::from_be_bytes(buf)
            }
            _ => unreachable!(),
        };
        Ok(Self(x))
    }

    /// Read a QUIC varint from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, VarIntUnexpectedEnd> {
        // Eight bytes is the maximum encoded length.
        let mut buf = [0; 8];

        // Read the first byte because it encodes the length tag.
        stream
            .read_exact(&mut buf[0..1])
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        // 0b00 = 1 byte, 0b01 = 2 bytes, 0b10 = 4 bytes, 0b11 = 8 bytes.
        let size = 1 << (buf[0] >> 6);
        stream
            .read_exact(&mut buf[1..size])
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        // Decode directly from the stack buffer slice.
        let mut slice = &buf[..size];
        let v = VarInt::decode(&mut slice).expect("buffer size is derived from the varint tag");

        Ok(v)
    }

    /// Encode this value as a QUIC varint into the given buffer.
    pub fn encode<B: BufMut>(&self, w: &mut B) {
        let x = self.0;
        if x < (1 << 6) {
            w.put_u8(x as u8);
        } else if x < (1 << 14) {
            w.put_u16((0b01 << 14) | x as u16);
        } else if x < (1 << 30) {
            w.put_u32((0b10 << 30) | x as u32);
        } else if x <= Self::MAX.0 {
            w.put_u64((0b11 << 62) | x);
        } else {
            unreachable!("malformed VarInt")
        }
    }

    /// Encode and write this value as a QUIC varint to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<(), VarIntUnexpectedEnd> {
        // Keep the temporary buffer on the stack to avoid allocation.
        let mut buf = [0u8; 8];
        let mut cursor: &mut [u8] = &mut buf;
        self.encode(&mut cursor);
        let size = 8 - cursor.len();

        let mut cursor = &buf[..size];
        stream
            .write_all_buf(&mut cursor)
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        Ok(())
    }
}

/// Error returned when constructing a `VarInt` from a value >= 2^62.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("value too large for varint encoding")]
pub struct VarIntBoundsExceeded;

#[derive(Error, Debug, Copy, Clone, Eq, PartialEq)]
#[error("unexpected end of buffer")]
/// Error returned when a varint decode reaches EOF before all bytes are available.
pub struct VarIntUnexpectedEnd;
