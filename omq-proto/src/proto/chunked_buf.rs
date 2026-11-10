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

    #[inline]
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

    #[cold]
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
    #[expect(
        clippy::inline_always,
        reason = "Inline frame bodies avoid an out-of-line copy and retain fixed-size loads"
    )]
    #[inline(always)]
    pub(crate) fn read_into(&mut self, n: usize, dest: &mut [u8]) {
        debug_assert!(n <= self.total_len);
        debug_assert!(n <= dest.len());

        if n == 0 {
            return;
        }

        let avail = self.front.len() - self.front_offset;
        if n <= avail {
            self.total_len -= n;
            let src = &self.front[self.front_offset..self.front_offset + n];

            // Constant lengths let LLVM use register copies for inline bodies.
            match n {
                1 => dest[0] = src[0],
                2 => dest[..2].copy_from_slice(src),
                4 => dest[..4].copy_from_slice(src),
                8 => dest[..8].copy_from_slice(src),
                16 => dest[..16].copy_from_slice(src),
                32 => dest[..32].copy_from_slice(src),
                _ => dest[..n].copy_from_slice(src),
            }

            self.front_offset += n;
            if self.front_offset >= self.front.len() {
                self.advance_front();
            }

            return;
        }

        self.read_spanning_into(n, dest);
    }

    #[cold]
    fn read_spanning_into(&mut self, n: usize, dest: &mut [u8]) {
        self.total_len -= n;
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

    /// Append `n` bytes directly to mutable storage and consume them. Unlike
    /// `split_to`, this never creates an intermediate coalesced allocation.
    #[cfg(feature = "ws")]
    pub(crate) fn copy_into(&mut self, mut n: usize, dest: &mut BytesMut) {
        debug_assert!(n <= self.total_len);
        crate::copy_stats::record(crate::copy_stats::Site::WebSocket, n);
        self.total_len -= n;

        while n > 0 {
            let start = self.front_offset;
            let avail = self.front.len() - start;
            let take = n.min(avail);
            dest.extend_from_slice(&self.front[start..start + take]);
            n -= take;

            if take == avail {
                self.advance_front();
            } else {
                self.front_offset += take;
            }
        }
    }

    /// Take the first `n` bytes as a [`Payload`], consuming them from the
    /// buffer. Contiguous bodies keep their byte views; bodies spanning chunks
    /// are coalesced into a single allocation.
    /// Panics in debug mode if `n > self.len()`.
    #[inline]
    pub(crate) fn split_to(&mut self, n: usize) -> Payload {
        self.split_to_with_pool(n, None)
    }

    /// Use configured storage only when coalescing needs a new destination.
    #[inline]
    pub(crate) fn split_to_with_pool(
        &mut self,
        n: usize,
        pool: Option<&crate::PayloadPool>,
    ) -> Payload {
        debug_assert!(n <= self.total_len, "split_to past end");
        if n == 0 {
            return Payload::new();
        }

        // Fast path: entirely within the front chunk past front_offset.
        let avail = self.front.len() - self.front_offset;
        if n <= avail {
            self.total_len -= n;
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
        crate::copy_stats::record(crate::copy_stats::Site::RecvAssemble, n);
        if n > MAX_INLINE_PAYLOAD
            && let Some(mut buffer) = pool.and_then(|pool| pool.try_buffer(n))
        {
            self.read_into(n, buffer.writable());
            buffer.set_len(n).expect("selected slot fits body");
            return buffer.into_payload();
        }

        self.total_len -= n;
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
            let capacity = buf.capacity();
            Payload::from_bytes_with_retained_size(buf.freeze(), capacity)
        }
    }
}

#[cfg(test)]
mod tests;
