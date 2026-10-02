//! Shared fan-out encoded batch construction.

use bytes::Bytes;

use crate::frame_buffer::FrameBuffer;
use crate::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Encoded message storage selected for fan-out.
pub enum FanOutFrame<'a> {
    /// One contiguous arena slice.
    Arena(&'a [u8]),
    /// Shared encoded chunks.
    Chunks(&'a [Bytes]),
}

/// Encode one message for reuse across multiple destinations.
pub fn build_fan_out_frame<'a>(
    eq: &'a mut FrameBuffer,
    msg: &Message,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutFrame<'a> {
    encode_fan_out_message(eq, msg, target_count, copy_budget);
    finish_fan_out_frame(eq, chunks, target_count, copy_budget)
}

/// Append one encoded message to the frame buffer.
pub fn encode_fan_out_message(
    eq: &mut FrameBuffer,
    msg: &Message,
    target_count: usize,
    copy_budget: usize,
) {
    if encoded_message_len(msg).saturating_mul(target_count) > copy_budget {
        eq.frame_gather(msg);
    } else {
        eq.frame(msg);
    }
}

fn encoded_message_len(msg: &Message) -> usize {
    let mut total = 0usize;
    msg.iter_slices(|part| {
        total = total.saturating_add(frame_header_len(part.len()));
        total = total.saturating_add(part.len());
    });
    total
}

#[inline]
fn frame_header_len(payload_len: usize) -> usize {
    if payload_len > u8::MAX as usize { 9 } else { 2 }
}

/// Finalize the encoded frame and select arena or chunk storage.
pub fn finish_fan_out_frame<'a>(
    eq: &'a mut FrameBuffer,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutFrame<'a> {
    if eq.has_arena_only() && eq.uncommitted_arena().len() * target_count <= copy_budget {
        FanOutFrame::Arena(eq.uncommitted_arena())
    } else {
        chunks.clear();
        // This is one atomic prepared publication, not a wire-write turn.
        // A writev chunk cap here would split a large multipart message.
        eq.drain(chunks, usize::MAX);
        FanOutFrame::Chunks(chunks)
    }
}

/// Release temporary fan-out storage.
pub fn clear_fan_out_frame(eq: &mut FrameBuffer, chunks: &mut Vec<Bytes>) {
    eq.clear_arena();
    chunks.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_batch_when_total_copy_fits_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0x11; 64]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 8, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert_eq!(chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn chunk_batch_when_total_copy_exceeds_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x22; 4 * 1024]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 8, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 9);
        assert_eq!(chunks[1].len(), 4 * 1024);
    }

    #[test]
    fn chunk_batch_for_large_single_peer_message() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x33; 128 * 1024]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 1, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert_ne!(chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn arena_batch_when_total_copy_equals_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x44; 254]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 32, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert_eq!(chunks, [] as [Bytes; 0]);
    }

    #[test]
    fn gathered_multipart_keeps_all_chunks_in_one_publication() {
        for parts in [600, 1025] {
            let msg = Message::multipart((0..parts).map(|_| Bytes::from_static(&[0x5a; 1024])));
            let mut expected = bytes::BytesMut::new();
            crate::proto::frame::encode_message_flat(&msg, &mut expected);
            let mut eq = FrameBuffer::one_shot();
            let mut chunks = Vec::new();
            let frame = build_fan_out_frame(&mut eq, &msg, &mut chunks, 2, 8 * 1024);
            let FanOutFrame::Chunks(encoded_chunks) = frame else {
                panic!("large multipart must gather");
            };
            assert!(encoded_chunks.len() > 1024);
            let actual: Vec<u8> = encoded_chunks
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect();
            assert_eq!(actual, expected.as_ref());
            clear_fan_out_frame(&mut eq, &mut chunks);
            assert!(eq.is_empty());
        }
    }
}
