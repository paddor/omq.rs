//! Shared fan-out encoded batch construction.

use bytes::Bytes;

use crate::frame_buffer::FrameBuffer;
use crate::message::Message;
use crate::proto::frame::message_wire_len;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Encoded message storage selected for fan-out.
pub enum FanOutFrame<'a> {
    /// One contiguous arena slice.
    Arena(&'a [u8]),
    /// Shared encoded chunks.
    Chunks(&'a [Bytes]),
}

/// Copy cost limit for one publication framed once for several peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanOutCost {
    /// Peers that receive the encoded publication.
    pub targets: usize,
    /// Bytes that may be copied across all targets before payloads are
    /// shared by reference instead.
    pub copy_budget: usize,
}

impl FanOutCost {
    fn copies_within_budget(self, len: usize) -> bool {
        len.saturating_mul(self.targets) <= self.copy_budget
    }
}

/// Reusable storage that encodes a publication once for several peers.
#[derive(Debug)]
pub struct FanOutEncoder {
    frames: FrameBuffer,
    chunks: Vec<Bytes>,
}

impl Default for FanOutEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FanOutEncoder {
    /// An empty encoder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            frames: FrameBuffer::one_shot(),
            chunks: Vec::new(),
        }
    }

    /// Encode one message and select its storage.
    pub fn build(&mut self, msg: &Message, cost: FanOutCost) -> FanOutFrame<'_> {
        self.encode(msg, cost);
        self.finish(cost)
    }

    /// Append one encoded message. Payloads are copied into the arena only
    /// when every target's copy fits the budget.
    pub fn encode(&mut self, msg: &Message, cost: FanOutCost) {
        if cost.copies_within_budget(message_wire_len(msg)) {
            self.frames.frame(msg);
        } else {
            self.frames.frame_gather(msg);
        }
    }

    /// Finalize the encoded messages and select arena or chunk storage.
    pub fn finish(&mut self, cost: FanOutCost) -> FanOutFrame<'_> {
        if self.frames.has_arena_only()
            && cost.copies_within_budget(self.frames.uncommitted_arena().len())
        {
            FanOutFrame::Arena(self.frames.uncommitted_arena())
        } else {
            self.chunks.clear();
            // This is one atomic prepared publication, not a wire-write turn.
            // A writev chunk cap here would split a large multipart message.
            self.frames.drain(&mut self.chunks, usize::MAX);
            FanOutFrame::Chunks(&self.chunks)
        }
    }

    /// Release the encoded publication.
    pub fn clear(&mut self) {
        self.frames.clear_arena();
        self.chunks.clear();
    }

    /// The underlying frame buffer, for records framed outside `encode`.
    #[inline]
    pub fn frames_mut(&mut self) -> &mut FrameBuffer {
        &mut self.frames
    }

    /// The underlying frame buffer.
    #[must_use]
    #[inline]
    pub fn frames(&self) -> &FrameBuffer {
        &self.frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cost(targets: usize) -> FanOutCost {
        FanOutCost {
            targets,
            copy_budget: 8 * 1024,
        }
    }

    #[test]
    fn arena_batch_when_total_copy_fits_budget() {
        let mut encoder = FanOutEncoder::new();
        let msg = Message::from(Bytes::from_static(&[0x11; 64]));

        let batch = encoder.build(&msg, cost(8));

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert_eq!(encoder.chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn chunk_batch_when_total_copy_exceeds_budget() {
        let mut encoder = FanOutEncoder::new();
        let msg = Message::from(Bytes::from(vec![0x22; 4 * 1024]));

        let batch = encoder.build(&msg, cost(8));

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert_eq!(encoder.chunks.len(), 2);
        assert_eq!(encoder.chunks[0].len(), 9);
        assert_eq!(encoder.chunks[1].len(), 4 * 1024);
    }

    #[test]
    fn chunk_batch_for_large_single_peer_message() {
        let mut encoder = FanOutEncoder::new();
        let msg = Message::from(Bytes::from(vec![0x33; 128 * 1024]));

        let batch = encoder.build(&msg, cost(1));

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert_ne!(encoder.chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn arena_batch_when_total_copy_equals_budget() {
        let mut encoder = FanOutEncoder::new();
        let msg = Message::from(Bytes::from(vec![0x44; 254]));

        let batch = encoder.build(&msg, cost(32));

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert_eq!(encoder.chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn gathered_multipart_keeps_all_chunks_in_one_publication() {
        for parts in [600, 1025] {
            let msg = Message::multipart((0..parts).map(|_| Bytes::from_static(&[0x5a; 1024])));
            let mut expected = bytes::BytesMut::new();
            crate::proto::frame::encode_message_flat(&msg, &mut expected);
            let mut encoder = FanOutEncoder::new();
            let frame = encoder.build(&msg, cost(2));
            let FanOutFrame::Chunks(encoded_chunks) = frame else {
                panic!("large multipart must gather");
            };
            assert!(encoded_chunks.len() > 1024);
            let actual: Vec<u8> = encoded_chunks
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect();
            assert_eq!(actual, expected.as_ref());
            encoder.clear();
            assert!(encoder.frames().is_empty());
        }
    }

    #[test]
    fn message_wire_len_counts_short_and_long_headers() {
        let msg = Message::multipart([vec![0; 255], vec![0; 256], Vec::new()]);
        assert_eq!(message_wire_len(&msg), (2 + 255) + (9 + 256) + 2);
    }
}
