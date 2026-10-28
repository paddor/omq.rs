//! WebTransport protocol primitives shared across webtrans transports.

#![forbid(unsafe_code)]

mod capsule;
mod connect;
mod error;
mod frame;
mod grease;
mod huffman;
mod qpack;
mod settings;
mod stream;
mod varint;

pub use capsule::*;
pub use connect::*;
pub use error::*;
pub use frame::*;
pub use settings::*;
pub use stream::*;
pub use varint::*;

/// Decoder entry points used by the out-of-workspace fuzz package.
///
/// This module is intentionally available only with the `fuzzing` feature so
/// QPACK implementation details do not become part of the normal public API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    use bytes::Buf;

    /// Exercise the static-table QPACK decoder, including Huffman strings.
    pub fn decode_qpack<B: Buf>(buf: &mut B) {
        let _ = super::qpack::Headers::decode(buf);
    }

    /// Exercise the QPACK string decoder directly so arbitrary Huffman payloads
    /// are reachable without first constructing a complete header block.
    pub fn decode_qpack_string<B: Buf>(buf: &mut B) {
        let _ = super::qpack::decode_string(buf, 8);
    }
}
