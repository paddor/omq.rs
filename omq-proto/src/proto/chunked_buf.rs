use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

use crate::message::{MAX_INLINE_PAYLOAD, Payload};

/// Growable byte queue that admits owned [`Bytes`] chunks without copying.
///
/// Used as the codec's inbound buffer. Each [`push`](Self::push) appends a
/// chunk zero-copy. [`advance`](Self::advance) and [`split_to`](Self::split_to)
/// consume from the front via zero-copy [`Bytes::slice`] / [`Bytes::split_to`]
/// so no memcpy occurs on read paths either.
#[derive(Debug, Default)]
pub(crate) struct ChunkedInputBuf {
    /// Current front chunk, accessed directly without `VecDeque` indirection.
    front: Bytes,
    /// Remaining chunks after `front`.
    rest: VecDeque<Bytes>,
    total_len: usize,
    front_offset: usize,
}

impl ChunkedInputBuf {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Append a chunk. Empty chunks are dropped silently.
    pub(crate) fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        self.total_len += chunk.len();
        if self.front_offset >= self.front.len() {
            self.front = chunk;
            self.front_offset = 0;
        } else {
            self.rest.push_back(chunk);
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Copy the first `N` bytes into a stack array without consuming them.
    /// Returns `None` if fewer than `N` bytes are buffered.
    #[inline]
    pub(crate) fn peek_array<const N: usize>(&self) -> Option<[u8; N]> {
        if self.total_len < N {
            return None;
        }
        let mut out = [0u8; N];
        let front_remaining = &self.front[self.front_offset..];
        if front_remaining.len() >= N {
            out.copy_from_slice(&front_remaining[..N]);
            return Some(out);
        }
        // Slow path: data spans front + rest chunks.
        let mut pos = 0;
        for &b in front_remaining {
            out[pos] = b;
            pos += 1;
            if pos == N {
                return Some(out);
            }
        }
        for chunk in &self.rest {
            for &b in chunk.as_ref() {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    return Some(out);
                }
            }
        }
        Some(out)
    }

    /// Copy `N` bytes starting at `offset` into a stack array without consuming.
    /// Returns `None` if fewer than `offset + N` bytes are buffered.
    #[cfg(feature = "ws")]
    pub(crate) fn peek_array_at<const N: usize>(&self, offset: usize) -> Option<[u8; N]> {
        if self.total_len < offset + N {
            return None;
        }
        let mut out = [0u8; N];
        let mut pos = 0;
        let mut skipped = 0;
        let front_remaining = &self.front[self.front_offset..];
        if skipped + front_remaining.len() > offset {
            let begin = offset - skipped;
            for &b in &front_remaining[begin..] {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    return Some(out);
                }
            }
        }
        skipped += front_remaining.len();
        for chunk in &self.rest {
            let slice = chunk.as_ref();
            if skipped + slice.len() <= offset {
                skipped += slice.len();
                continue;
            }
            let begin = offset.saturating_sub(skipped);
            for &b in &slice[begin..] {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    return Some(out);
                }
            }
            skipped += slice.len();
        }
        Some(out)
    }

    #[inline]
    fn advance_front(&mut self) {
        if let Some(next) = self.rest.pop_front() {
            self.front = next;
        } else {
            self.front = Bytes::new();
        }
        self.front_offset = 0;
    }

    /// Consume the first `n` bytes without returning them.
    /// Panics in debug mode if `n > self.len()`.
    #[inline]
    pub(crate) fn advance(&mut self, mut n: usize) {
        debug_assert!(n <= self.total_len, "advance past end");
        self.total_len -= n;
        while n > 0 {
            let avail = self.front.len() - self.front_offset;
            if n >= avail {
                n -= avail;
                self.advance_front();
            } else {
                self.front_offset += n;
                break;
            }
        }
    }

    /// Copy `n` bytes into `dest[..n]` and advance.
    #[inline]
    pub(crate) fn read_into(&mut self, n: usize, dest: &mut [u8]) {
        debug_assert!(n <= self.total_len);
        debug_assert!(n <= dest.len());
        if n == 0 {
            return;
        }
        self.total_len -= n;
        let avail = self.front.len() - self.front_offset;
        if n <= avail {
            dest[..n].copy_from_slice(&self.front[self.front_offset..self.front_offset + n]);
            self.front_offset += n;
            if self.front_offset >= self.front.len() {
                self.advance_front();
            }
            return;
        }
        let mut remaining = n;
        let mut pos = 0;
        while remaining > 0 {
            let start = self.front_offset;
            let avail = self.front.len() - start;
            let take = remaining.min(avail);
            dest[pos..pos + take].copy_from_slice(&self.front[start..start + take]);
            pos += take;
            remaining -= take;
            if take >= avail {
                self.advance_front();
            } else {
                self.front_offset += take;
            }
        }
    }

    /// Take the first `n` bytes as a [`Payload`], consuming them from the
    /// buffer. Each contiguous chunk contributes one chunk to the returned
    /// `Payload`; no copies are made.
    /// Panics in debug mode if `n > self.len()`.
    #[inline]
    pub(crate) fn split_to(&mut self, n: usize) -> Payload {
        debug_assert!(n <= self.total_len, "split_to past end");
        if n == 0 {
            return Payload::new();
        }
        self.total_len -= n;

        // Fast path: entirely within the front chunk past front_offset.
        let avail = self.front.len() - self.front_offset;
        if n <= avail {
            let start = self.front_offset;
            let payload = if n <= MAX_INLINE_PAYLOAD {
                Payload::inline(&self.front[start..start + n])
            } else {
                Payload::from_bytes(self.front.slice(start..start + n))
            };
            self.front_offset += n;
            if self.front_offset >= self.front.len() {
                self.advance_front();
            }
            return payload;
        }

        // Slow path: spans front + rest chunks. Coalesce into one contiguous buffer.
        let mut remaining = n;
        let mut buf = BytesMut::with_capacity(n);

        buf.extend_from_slice(&self.front[self.front_offset..]);
        remaining -= avail;
        self.advance_front();

        while remaining > 0 {
            let chunk_len = self.front.len();
            if remaining >= chunk_len {
                buf.extend_from_slice(&self.front);
                remaining -= chunk_len;
                self.advance_front();
            } else {
                buf.extend_from_slice(&self.front[..remaining]);
                self.front_offset = remaining;
                remaining = 0;
            }
        }

        if n <= MAX_INLINE_PAYLOAD {
            Payload::inline(&buf)
        } else {
            Payload::from_bytes(buf.freeze())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_bytes(buf: &mut ChunkedInputBuf, data: &[u8]) {
        buf.push(Bytes::copy_from_slice(data));
    }

    #[test]
    fn empty() {
        let buf = ChunkedInputBuf::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert!(buf.peek_array::<1>().is_none());
    }

    #[test]
    fn push_and_peek() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.peek_array::<3>(), Some(*b"hel"));
        assert_eq!(buf.peek_array::<5>(), Some(*b"hello"));
        assert!(buf.peek_array::<6>().is_none());
        assert_eq!(buf.len(), 5, "peek does not consume");
    }

    #[test]
    fn peek_spanning_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cd");
        assert_eq!(buf.peek_array::<4>(), Some(*b"abcd"));
        assert_eq!(buf.peek_array::<3>(), Some(*b"abc"));
    }

    #[test]
    fn advance_within_chunk() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        buf.advance(2);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.peek_array::<3>(), Some(*b"llo"));
    }

    #[test]
    fn advance_across_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cde");
        buf.advance(3);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.peek_array::<2>(), Some(*b"de"));
    }

    #[test]
    fn split_to_single_chunk() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcdef");
        let p = buf.split_to(3);
        assert_eq!(p.as_bytes(), &b"abc"[..]);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.peek_array::<3>(), Some(*b"def"));
    }

    #[test]
    fn split_to_spanning_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cd");
        push_bytes(&mut buf, b"ef");
        let p = buf.split_to(5);
        assert_eq!(p.as_bytes(), &b"abcde"[..]);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.peek_array::<1>(), Some(*b"f"));
    }

    #[test]
    fn split_to_empty_returns_empty_payload() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        let p = buf.split_to(0);
        assert!(p.is_empty());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn split_to_whole_buffer() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abc");
        push_bytes(&mut buf, b"def");
        let p = buf.split_to(6);
        assert_eq!(p.as_bytes(), &b"abcdef"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn front_offset_accumulates() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcdefghij"); // 10 bytes
        buf.advance(2); // front_offset = 2
        assert_eq!(buf.len(), 8);
        assert_eq!(buf.peek_array::<3>(), Some(*b"cde"));
        buf.advance(3); // front_offset = 5
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.peek_array::<5>(), Some(*b"fghij"));
    }

    #[test]
    fn front_offset_resets_on_pop() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cdef");
        buf.advance(1); // front_offset = 1 within "ab"
        assert_eq!(buf.peek_array::<1>(), Some(*b"b"));
        buf.advance(1); // consumes rest of "ab", pops, front_offset = 0
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.peek_array::<4>(), Some(*b"cdef"));
    }

    #[test]
    fn split_to_with_offset() {
        let mut buf = ChunkedInputBuf::new();
        // Simulate frame parsing: push 10 bytes (2 header + 8 payload)
        push_bytes(&mut buf, b"\x00\x08deadbeef");
        buf.advance(2); // skip header
        let p = buf.split_to(8); // extract payload
        assert_eq!(p.as_bytes(), &b"deadbeef"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn split_to_partial_with_offset_then_continue() {
        let mut buf = ChunkedInputBuf::new();
        // Two frames back-to-back in one chunk: [hdr1(2), pay1(3), hdr2(2), pay2(4)]
        push_bytes(&mut buf, b"XXabcYYdefg");
        buf.advance(2); // skip hdr1
        let p1 = buf.split_to(3);
        assert_eq!(p1.as_bytes(), &b"abc"[..]);
        assert_eq!(buf.len(), 6);
        buf.advance(2); // skip hdr2
        let p2 = buf.split_to(4);
        assert_eq!(p2.as_bytes(), &b"defg"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn split_to_spanning_with_offset() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcd");
        push_bytes(&mut buf, b"efgh");
        buf.advance(2); // front_offset=2, front="abcd" so visible="cd"
        let p = buf.split_to(5); // spans: "cd" from first + "efg" from second
        assert_eq!(p.as_bytes(), &b"cdefg"[..]);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.peek_array::<1>(), Some(*b"h"));
    }

    #[test]
    fn randomized_split_read_and_advance_match_contiguous_reference() {
        let cases = if cfg!(miri) { 2 } else { 64 };
        let steps = if cfg!(miri) { 64 } else { 512 };
        for case in 0..cases {
            let mut seed = 0xC0DE_FEED_BADC_0FFE ^ case;
            let mut buf = ChunkedInputBuf::new();
            let mut expected = Vec::new();
            let mut next_byte = case as u8;

            for _ in 0..steps {
                if expected.len() < 128 || next_random(&mut seed).is_multiple_of(3) {
                    let len = random_len(&mut seed);
                    let chunk = (0..len)
                        .map(|_| {
                            let byte = next_byte;
                            next_byte = next_byte.wrapping_add(31);
                            byte
                        })
                        .collect::<Vec<_>>();
                    expected.extend_from_slice(&chunk);
                    buf.push(Bytes::from(chunk));
                } else {
                    let n = 1 + (next_random(&mut seed) % expected.len());
                    match next_random(&mut seed) % 3 {
                        0 => {
                            buf.advance(n);
                        }
                        1 => {
                            let mut got = vec![0u8; n];
                            buf.read_into(n, &mut got);
                            assert_eq!(got, expected[..n], "case={case}, n={n}");
                        }
                        _ => {
                            let got = buf.split_to(n);
                            assert_eq!(got.as_slice(), &expected[..n], "case={case}, n={n}");
                        }
                    }
                    expected.drain(..n);
                }
                assert_chunked_matches_prefix(&buf, &expected, case);
            }
        }
    }

    #[test]
    fn read_into_zero_on_empty() {
        let mut buf = ChunkedInputBuf::new();
        let mut dest = [0u8; 4];
        buf.read_into(0, &mut dest);
        assert!(buf.is_empty());
    }

    #[test]
    fn read_into_zero_after_drain() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"\x00\x00");
        buf.advance(2);
        assert!(buf.is_empty());
        let mut dest = [0u8; 4];
        buf.read_into(0, &mut dest);
        assert!(buf.is_empty());
    }

    fn assert_chunked_matches_prefix(buf: &ChunkedInputBuf, expected: &[u8], case: u64) {
        assert_eq!(buf.len(), expected.len(), "case={case}");
        if expected.is_empty() {
            assert!(buf.is_empty(), "case={case}");
            assert!(buf.peek_array::<1>().is_none(), "case={case}");
        } else {
            assert_eq!(buf.peek_array::<1>(), Some([expected[0]]), "case={case}");
            if expected.len() >= 8 {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&expected[..8]);
                assert_eq!(buf.peek_array::<8>(), Some(prefix), "case={case}");
            }
        }
    }

    fn random_len(seed: &mut u64) -> usize {
        if cfg!(miri) {
            return match next_random(seed) % 6 {
                0 => 1,
                1 => 2,
                2 => 3,
                3 => 62,
                4 => 63,
                _ => (next_random(seed) % 128) + 1,
            };
        }
        match next_random(seed) % 10 {
            0 => 1,
            1 => 2,
            2 => 3,
            3 => 62,
            4 => 63,
            5 => 255,
            6 => 256,
            7 => 4095,
            8 => 4096,
            _ => (next_random(seed) % 1024) + 1,
        }
    }

    fn next_random(seed: &mut u64) -> usize {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed as usize
    }
}
