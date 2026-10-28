//! Capsule parsing and serialization for WebTransport over HTTP/3.

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{VarInt, VarIntUnexpectedEnd};

// The draft (draft-ietf-webtrans-http3-06) specifies type 0x2843, which encodes as 0x68 0x43.
// Some wire traces show 0x43 0x28 (decoded as 808), so implementations may diverge.
// Use 0x2843 per the current specification.
const CLOSE_WEBTRANSPORT_SESSION_TYPE: u64 = 0x2843;
const MAX_MESSAGE_SIZE: usize = 1024;
const MAX_CLOSE_PAYLOAD_SIZE: usize = 4 + MAX_MESSAGE_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
/// WebTransport HTTP/3 capsule payloads.
pub enum Capsule {
    /// CLOSE_WEBTRANSPORT_SESSION capsule carrying application close details.
    CloseWebTransportSession {
        /// Application close code in WebTransport space.
        code: u32,
        /// UTF-8 close reason.
        reason: String,
    },
    /// Any unknown capsule type preserved as raw bytes.
    Unknown {
        /// Unrecognized capsule type identifier.
        typ: VarInt,
        /// Raw payload bytes for the unknown type.
        payload: Bytes,
    },
}

impl Capsule {
    /// Decode one capsule from a complete in-memory buffer.
    /// Unknown types, including reserved Capsule GREASE values, are returned
    /// without interpreting their payload. The session layer may ignore them.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, CapsuleError> {
        let typ = VarInt::decode(buf)?;
        let length = VarInt::decode(buf)?;

        let length =
            usize::try_from(length.into_inner()).map_err(|_| CapsuleError::MessageTooLong)?;
        if length > MAX_CLOSE_PAYLOAD_SIZE {
            return Err(CapsuleError::MessageTooLong);
        }
        let mut payload = buf.take(length);

        if payload.remaining() < payload.limit() {
            return Err(CapsuleError::UnexpectedEnd);
        }

        match typ.into_inner() {
            CLOSE_WEBTRANSPORT_SESSION_TYPE => {
                if payload.remaining() < 4 {
                    return Err(CapsuleError::UnexpectedEnd);
                }

                let error_code = payload.get_u32();

                let message_len = payload.remaining();
                if message_len > MAX_MESSAGE_SIZE {
                    return Err(CapsuleError::MessageTooLong);
                }

                let message_bytes = payload.copy_to_bytes(message_len);
                let error_message = String::from_utf8(message_bytes.to_vec())
                    .map_err(|_| CapsuleError::InvalidUtf8)?;

                Ok(Self::CloseWebTransportSession {
                    code: error_code,
                    reason: error_message,
                })
            }
            _ => {
                let payload_bytes = payload.copy_to_bytes(payload.remaining());
                Ok(Self::Unknown {
                    typ,
                    payload: payload_bytes,
                })
            }
        }
    }

    /// Read and decode one capsule from an async stream.
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, CapsuleError> {
        let typ = VarInt::read(stream)
            .await
            .map_err(|_| CapsuleError::UnexpectedEnd)?;
        let length = VarInt::read(stream)
            .await
            .map_err(|_| CapsuleError::UnexpectedEnd)?;
        let length =
            usize::try_from(length.into_inner()).map_err(|_| CapsuleError::MessageTooLong)?;
        if length > MAX_CLOSE_PAYLOAD_SIZE {
            return Err(CapsuleError::MessageTooLong);
        }

        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;

        let mut capsule = Vec::with_capacity(VarInt::MAX_SIZE * 2 + length);
        typ.encode(&mut capsule);
        VarInt::try_from(length)
            .map_err(|_| CapsuleError::MessageTooLong)?
            .encode(&mut capsule);
        capsule.extend_from_slice(&payload);
        Self::decode(&mut capsule.as_slice())
    }

    /// Encode this capsule into the provided buffer.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), CapsuleError> {
        match self {
            Self::CloseWebTransportSession {
                code: error_code,
                reason: error_message,
            } => {
                if error_message.len() > MAX_MESSAGE_SIZE {
                    return Err(CapsuleError::MessageTooLong);
                }

                // Encode the capsule type.
                VarInt::from_u64(CLOSE_WEBTRANSPORT_SESSION_TYPE)
                    .unwrap()
                    .encode(buf);

                // Calculate and encode the payload length.
                let length = 4 + error_message.len();
                VarInt::from_u32(length as u32).encode(buf);

                // Encode the 32-bit error code.
                buf.put_u32(*error_code);

                // Encode the UTF-8 error message.
                buf.put_slice(error_message.as_bytes());
            }
            Self::Unknown { typ, payload } => {
                if payload.len() > MAX_CLOSE_PAYLOAD_SIZE {
                    return Err(CapsuleError::MessageTooLong);
                }

                // Encode the capsule type.
                typ.encode(buf);

                // Encode the payload length.
                VarInt::try_from(payload.len())
                    .map_err(|_| CapsuleError::MessageTooLong)?
                    .encode(buf);

                // Encode the payload bytes.
                buf.put_slice(payload);
            }
        }
        Ok(())
    }

    /// Encode and write this capsule to an async stream.
    pub async fn write<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<(), CapsuleError> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf)?;
        stream.write_all_buf(&mut buf).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
/// Errors returned by capsule encoding and decoding.
pub enum CapsuleError {
    #[error("unexpected end of buffer")]
    /// Input ended before the full capsule payload could be read.
    UnexpectedEnd,

    #[error("invalid UTF-8")]
    /// CLOSE_WEBTRANSPORT_SESSION reason bytes were not valid UTF-8.
    InvalidUtf8,

    #[error("message too long")]
    /// Capsule payload exceeded the implementation message limit.
    MessageTooLong,

    #[error("unknown capsule type: {0:?}")]
    /// Capsule type is unsupported by this implementation.
    UnknownType(VarInt),

    #[error("varint decode error: {0:?}")]
    /// Failed to decode a QUIC variable-length integer.
    VarInt(#[from] VarIntUnexpectedEnd),

    #[error("io error: {0}")]
    /// I/O error while reading from or writing to a stream.
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for CapsuleError {
    fn from(err: std::io::Error) -> Self {
        CapsuleError::Io(Arc::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn unknown_capsule_consumes_exactly_one_payload_even_for_grease_values() {
        // HTTP/3 frame GREASE and Capsule GREASE use different namespaces.
        for typ in [0x21, 0x17, 0x29 + 0x17] {
            let unknown = Capsule::Unknown {
                typ: VarInt::from_u64(typ).unwrap(),
                payload: Bytes::from_static(&[0xff, 0xff, 0xff]),
            };
            let close = Capsule::CloseWebTransportSession {
                code: 42,
                reason: "done".into(),
            };
            let mut bytes = Vec::new();
            unknown.encode(&mut bytes).unwrap();
            close.encode(&mut bytes).unwrap();
            let mut remaining = bytes.as_slice();
            assert_eq!(Capsule::decode(&mut remaining).unwrap(), unknown);
            assert_eq!(Capsule::decode(&mut remaining).unwrap(), close);
            assert!(remaining.is_empty());
        }
    }

    #[test]
    fn oversized_capsule_lengths_are_rejected_before_platform_conversion() {
        let mut bytes = Vec::new();
        VarInt::from_u32(0x17).encode(&mut bytes);
        VarInt::MAX.encode(&mut bytes);
        assert!(matches!(
            Capsule::decode(&mut bytes.as_slice()),
            Err(CapsuleError::MessageTooLong)
        ));
    }

    #[test]
    fn test_close_webtransport_session_decode() {
        // Validate the spec-defined type 0x2843 (encoded as 0x68 0x43).
        let mut data = Vec::new();
        VarInt::from_u64(0x2843).unwrap().encode(&mut data);
        VarInt::from_u32(8).encode(&mut data);
        data.extend_from_slice(b"\x00\x00\x01\xa4test");

        let mut buf = data.as_slice();
        let capsule = Capsule::decode(&mut buf).unwrap();

        match capsule {
            Capsule::CloseWebTransportSession {
                code: error_code,
                reason: error_message,
            } => {
                assert_eq!(error_code, 420);
                assert_eq!(error_message, "test");
            }
            _ => panic!("Expected CloseWebTransportSession"),
        }

        assert_eq!(buf.len(), 0); // All bytes should be consumed.
    }

    #[test]
    fn test_close_webtransport_session_encode() {
        let capsule = Capsule::CloseWebTransportSession {
            code: 420,
            reason: "test".to_string(),
        };

        let mut buf = Vec::new();
        capsule.encode(&mut buf).unwrap();

        // Expected format: type(0x2843 as varint = 0x68 0x43) + length(8 as varint)
        // + error_code(420 as u32 BE) + "test".
        assert_eq!(buf, b"\x68\x43\x08\x00\x00\x01\xa4test");
    }

    #[test]
    fn test_close_webtransport_session_roundtrip() {
        let original = Capsule::CloseWebTransportSession {
            code: 12345,
            reason: "Connection closed by application".to_string(),
        };

        let mut buf = Vec::new();
        original.encode(&mut buf).unwrap();

        let mut read_buf = buf.as_slice();
        let decoded = Capsule::decode(&mut read_buf).unwrap();

        assert_eq!(original, decoded);
        assert_eq!(read_buf.len(), 0); // All bytes should be consumed.
    }

    #[test]
    fn test_empty_error_message() {
        let capsule = Capsule::CloseWebTransportSession {
            code: 0,
            reason: String::new(),
        };

        let mut buf = Vec::new();
        capsule.encode(&mut buf).unwrap();

        // Type(0x2843 as varint = 0x68 0x43) + Length(4) + error_code(0).
        assert_eq!(buf, b"\x68\x43\x04\x00\x00\x00\x00");

        let mut read_buf = buf.as_slice();
        let decoded = Capsule::decode(&mut read_buf).unwrap();
        assert_eq!(capsule, decoded);
    }

    #[test]
    fn test_invalid_utf8() {
        // Create a capsule with invalid UTF-8 in the message.
        let mut data = Vec::new();
        VarInt::from_u64(0x2843).unwrap().encode(&mut data); // type
        VarInt::from_u32(5).encode(&mut data); // length(5)
        data.extend_from_slice(b"\x00\x00\x00\x00"); // error_code(0)
        data.push(0xFF); // Invalid UTF-8 byte.

        let mut buf = data.as_slice();
        let result = Capsule::decode(&mut buf);
        assert!(matches!(result, Err(CapsuleError::InvalidUtf8)));
    }

    #[test]
    fn test_truncated_error_code() {
        // Capsule length indicates 3 bytes, but the error code needs 4.
        let mut data = Vec::new();
        VarInt::from_u64(0x2843).unwrap().encode(&mut data); // type
        VarInt::from_u32(3).encode(&mut data); // length(3)
        data.extend_from_slice(b"\x00\x00\x00"); // incomplete error code.

        let mut buf = data.as_slice();
        let result = Capsule::decode(&mut buf);
        assert!(matches!(result, Err(CapsuleError::UnexpectedEnd)));
    }

    #[test]
    fn test_unknown_capsule() {
        // Verify handling of unknown capsule types.
        let unknown_type = 0x1234u64;
        let payload_data = b"unknown payload";

        let mut data = Vec::new();
        VarInt::from_u64(unknown_type).unwrap().encode(&mut data);
        VarInt::from_u32(payload_data.len() as u32).encode(&mut data);
        data.extend_from_slice(payload_data);

        let mut buf = data.as_slice();
        let capsule = Capsule::decode(&mut buf).unwrap();

        match capsule {
            Capsule::Unknown { typ, payload } => {
                assert_eq!(typ.into_inner(), unknown_type);
                assert_eq!(payload.as_ref(), payload_data);
            }
            _ => panic!("Expected Unknown capsule"),
        }
    }

    #[test]
    fn test_unknown_capsule_roundtrip() {
        let capsule = Capsule::Unknown {
            typ: VarInt::from_u64(0x9999).unwrap(),
            payload: Bytes::from("test payload"),
        };

        let mut buf = Vec::new();
        capsule.encode(&mut buf).unwrap();

        let mut read_buf = buf.as_slice();
        let decoded = Capsule::decode(&mut read_buf).unwrap();

        assert_eq!(capsule, decoded);
        assert_eq!(read_buf.len(), 0);
    }

    #[test]
    fn test_oversized_close_reason_is_rejected_when_encoding() {
        let capsule = Capsule::CloseWebTransportSession {
            code: 0,
            reason: "x".repeat(MAX_MESSAGE_SIZE + 1),
        };

        assert!(matches!(
            capsule.encode(&mut Vec::new()),
            Err(CapsuleError::MessageTooLong)
        ));
    }
}
