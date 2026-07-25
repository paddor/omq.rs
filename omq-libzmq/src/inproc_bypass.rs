//! Lock-free inproc bypass: connects `zmq_send` and `zmq_recv` directly
//! via a SPSC byte ring, completely bypassing the io thread for eligible
//! socket types (PUSH/PULL).
//!
//! The bypass is installed when both sides of an inproc connection
//! are present (bind + connect, either order). The sender writes raw
//! payload bytes into the ring from its C thread; the receiver reads
//! them out on its C thread. Zero channel crossings, zero io thread
//! involvement, zero per-message heap allocation.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::ETERM;

/// Shared state between the sender and receiver halves of an inproc bypass.
pub(crate) struct InprocPipe {
    pub(crate) closed: AtomicBool,
    /// Receiver's notification handle. Signaled by the sender when the pipe
    /// transitions from empty to non-empty.
    pub(crate) recv_notify: crate::notify::RecvNotify,
    /// True when the sender is parked waiting for ring space.
    sender_waiting: AtomicBool,
    /// Handle for unparking the sender thread. Written by sender under
    /// the `sender_waiting` flag; read by consumer only when the flag is set.
    sender_thread: std::sync::Mutex<Option<std::thread::Thread>>,
}

impl std::fmt::Debug for InprocPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InprocPipe")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl InprocPipe {
    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.recv_notify.signal();
        }
        self.unpark_sender();
    }

    fn unpark_sender(&self) {
        if self.sender_waiting.load(Ordering::Acquire)
            && let Ok(guard) = self.sender_thread.lock()
            && let Some(t) = guard.as_ref()
        {
            t.unpark();
        }
    }
}

// ── SPSC byte ring ──────────────────────────────────────────────────
//
// Variable-length entries: [len: u32][payload: [u8; len]].
// When remaining contiguous space is too small for the next entry's
// header, a wrap sentinel (len = u32::MAX) is written and the
// producer wraps to offset 0. The consumer recognizes the sentinel
// and wraps its read position.

const HEADER_SIZE: usize = 4;
const WRAP_SENTINEL: u32 = u32::MAX;
const ALIGN_MASK: usize = HEADER_SIZE - 1;

/// Entry occupies `HEADER_SIZE + payload_len` bytes, rounded up to
/// `HEADER_SIZE` alignment. Keeps `tail` 4-aligned so `cap - tail_offset`
/// is always >= `HEADER_SIZE` (cap is a power of two).
#[inline]
const fn aligned_entry_size(payload_len: usize) -> usize {
    (HEADER_SIZE + payload_len + ALIGN_MASK) & !ALIGN_MASK
}

#[inline]
fn checked_aligned_entry_size(payload_len: usize) -> Option<usize> {
    if payload_len >= WRAP_SENTINEL as usize {
        return None;
    }
    HEADER_SIZE
        .checked_add(payload_len)?
        .checked_add(ALIGN_MASK)
        .map(|n| n & !ALIGN_MASK)
}

struct RingBuf {
    buf: Box<[UnsafeCell<u8>]>,
    capacity: usize,
    /// Producer write position (mod capacity).
    tail: AtomicU64,
    /// Consumer read position (mod capacity).
    head: AtomicU64,
}

impl std::fmt::Debug for RingBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingBuf")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl RingBuf {
    fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two();
        Self {
            buf: (0..cap)
                .map(|_| UnsafeCell::new(0u8))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            capacity: cap,
            tail: AtomicU64::new(0),
            head: AtomicU64::new(0),
        }
    }

    #[inline]
    fn free_space(&self, tail: u64, head: u64) -> usize {
        let used = tail.wrapping_sub(head);
        if used >= self.capacity as u64 {
            0
        } else {
            (self.capacity as u64 - used) as usize
        }
    }
}

pub(crate) struct RingProducer {
    ring: Arc<RingBuf>,
    tail: u64,
    cached_head: u64,
}

pub(crate) struct RingConsumer {
    ring: Arc<RingBuf>,
    head: u64,
    cached_tail: u64,
}

impl std::fmt::Debug for RingProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingProducer").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RingConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingConsumer").finish_non_exhaustive()
    }
}

// SAFETY: the ring buffer is shared via Arc. Producer and consumer
// access disjoint regions (producer writes [tail..], consumer reads
// [head..]). Atomic head/tail provide synchronization.
unsafe impl Send for RingProducer {}
unsafe impl Send for RingConsumer {}

#[expect(clippy::arc_with_non_send_sync)]
fn ring_pair(capacity: usize) -> (RingProducer, RingConsumer) {
    let ring = Arc::new(RingBuf::new(capacity));
    (
        RingProducer {
            ring: ring.clone(),
            tail: 0,
            cached_head: 0,
        },
        RingConsumer {
            ring,
            head: 0,
            cached_tail: 0,
        },
    )
}

impl RingProducer {
    /// Try to write `[len: u32][payload]` into the ring.
    /// Returns false if not enough space.
    #[inline]
    fn try_push(&mut self, data: &[u8]) -> bool {
        let Some(entry_size) = checked_aligned_entry_size(data.len()) else {
            return false;
        };
        let cap = self.ring.capacity;
        let mask = cap - 1;
        let tail = self.tail;
        let tail_offset = (tail as usize) & mask;

        // Check if we have enough total free space.
        let mut free = self.ring.free_space(tail, self.cached_head);
        if free < entry_size + HEADER_SIZE {
            self.cached_head = self.ring.head.load(Ordering::Acquire);
            free = self.ring.free_space(tail, self.cached_head);
            if free < entry_size + HEADER_SIZE {
                return false;
            }
        }

        let contiguous = cap - tail_offset;
        if contiguous < entry_size {
            // Not enough contiguous space at the end. Write wrap sentinel
            // and try from offset 0.
            if free < contiguous + entry_size + HEADER_SIZE {
                self.cached_head = self.ring.head.load(Ordering::Acquire);
                free = self.ring.free_space(tail, self.cached_head);
                if free < contiguous + entry_size + HEADER_SIZE {
                    return false;
                }
            }
            // SAFETY: tail_offset..tail_offset+4 is within buf. `contiguous >= HEADER_SIZE`
            // because capacity is a power of two and tail is always HEADER_SIZE-aligned
            // (aligned_entry_size rounds up all advances).
            unsafe {
                let dst = self.ring.buf[tail_offset].get();
                std::ptr::copy_nonoverlapping(
                    WRAP_SENTINEL.to_ne_bytes().as_ptr(),
                    dst,
                    HEADER_SIZE,
                );
            }
            self.tail = tail.wrapping_add(contiguous as u64);
            self.write_entry(data);
        } else {
            self.write_entry(data);
        }
        true
    }

    #[inline]
    fn write_entry(&mut self, data: &[u8]) {
        let cap = self.ring.capacity;
        let mask = cap - 1;
        let offset = (self.tail as usize) & mask;
        let len = data.len() as u32;
        // SAFETY: caller guaranteed sufficient contiguous space.
        unsafe {
            let base = self.ring.buf[offset].get();
            std::ptr::copy_nonoverlapping(len.to_ne_bytes().as_ptr(), base, HEADER_SIZE);
            std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(HEADER_SIZE), data.len());
        }
        self.tail = self
            .tail
            .wrapping_add(aligned_entry_size(data.len()) as u64);
    }

    /// Publish all written entries to the consumer. Returns true if
    /// the ring was empty from the consumer's perspective before this
    /// flush (i.e., the consumer had caught up to the previous tail).
    #[inline]
    fn flush(&mut self) -> bool {
        let prev_tail = self.ring.tail.load(Ordering::Relaxed);
        let head = self.ring.head.load(Ordering::Acquire);
        self.ring.tail.store(self.tail, Ordering::Release);
        prev_tail == head
    }
}

impl RingConsumer {
    /// Try to read the next entry. Returns a `(ptr, len)` slice into
    /// the ring buffer. The data is valid until the next `release` call.
    #[inline]
    fn try_peek(&mut self) -> Option<(*const u8, usize)> {
        if self.head == self.cached_tail {
            self.cached_tail = self.ring.tail.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                return None;
            }
        }
        let cap = self.ring.capacity;
        let mask = cap - 1;
        let offset = (self.head as usize) & mask;
        // SAFETY: head..head+4 is within published region.
        let len = unsafe {
            let mut bytes = [0u8; HEADER_SIZE];
            std::ptr::copy_nonoverlapping(
                self.ring.buf[offset].get(),
                bytes.as_mut_ptr(),
                HEADER_SIZE,
            );
            u32::from_ne_bytes(bytes)
        };
        if len == WRAP_SENTINEL {
            let contiguous = cap - offset;
            self.head = self.head.wrapping_add(contiguous as u64);
            let new_offset = (self.head as usize) & mask;
            debug_assert_eq!(new_offset, 0);
            if self.head == self.cached_tail {
                self.cached_tail = self.ring.tail.load(Ordering::Acquire);
                if self.head == self.cached_tail {
                    return None;
                }
            }
            let actual_len = unsafe {
                let mut bytes = [0u8; HEADER_SIZE];
                std::ptr::copy_nonoverlapping(
                    self.ring.buf[new_offset].get(),
                    bytes.as_mut_ptr(),
                    HEADER_SIZE,
                );
                u32::from_ne_bytes(bytes)
            };
            debug_assert_ne!(actual_len, WRAP_SENTINEL);
            Some((
                self.ring.buf[new_offset + HEADER_SIZE].get().cast_const(),
                actual_len as usize,
            ))
        } else {
            Some((
                self.ring.buf[offset + HEADER_SIZE].get().cast_const(),
                len as usize,
            ))
        }
    }

    /// Advance past the last peeked entry and publish the new head.
    #[inline]
    fn advance_and_release(&mut self, len: usize) {
        self.head = self.head.wrapping_add(aligned_entry_size(len) as u64);
        self.ring.head.store(self.head, Ordering::Release);
    }

    fn is_empty(&self) -> bool {
        self.head == self.ring.tail.load(Ordering::Acquire)
    }
}

impl Drop for RingProducer {
    fn drop(&mut self) {
        self.flush();
    }
}

impl Drop for BypassSend {
    fn drop(&mut self) {
        self.pipe.close();
    }
}

impl Drop for BypassRecv {
    fn drop(&mut self) {
        self.pipe.close();
    }
}

// ── Bypass sender / receiver ────────────────────────────────────────

/// Sender half installed on the PUSH socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassSend {
    pub(crate) producer: RingProducer,
    pub(crate) pipe: Arc<InprocPipe>,
}

/// Receiver half installed on the PULL socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassRecv {
    pub(crate) consumer: RingConsumer,
    pub(crate) pipe: Arc<InprocPipe>,
}

/// Create a bypass pair for an inproc connection.
/// `byte_capacity` is the total byte ring size (will be rounded up to power of two).
pub(crate) fn create_bypass(
    byte_capacity: usize,
    recv_notify: crate::notify::RecvNotify,
) -> (BypassSend, BypassRecv) {
    let (producer, consumer) = ring_pair(byte_capacity);
    let pipe = Arc::new(InprocPipe {
        closed: AtomicBool::new(false),
        recv_notify,
        sender_waiting: AtomicBool::new(false),
        sender_thread: std::sync::Mutex::new(None),
    });
    (
        BypassSend {
            producer,
            pipe: pipe.clone(),
        },
        BypassRecv { consumer, pipe },
    )
}

impl BypassSend {
    /// Try to push raw payload bytes. Returns false if full.
    /// Signals the receiver's event on empty-to-non-empty transitions.
    #[inline]
    pub(crate) fn push(&mut self, data: &[u8]) -> Result<(), i32> {
        if self.pipe.closed.load(Ordering::Acquire) {
            return Err(ETERM);
        }
        if !self.producer.try_push(data) {
            return if self.pipe.closed.load(Ordering::Acquire) {
                Err(ETERM)
            } else {
                Err(libc::EAGAIN)
            };
        }
        if self.producer.flush() {
            self.pipe.recv_notify.signal();
        }
        Ok(())
    }

    /// Blocking push: parks the sender thread until ring space is available.
    pub(crate) fn push_blocking(&mut self, data: &[u8]) -> Result<(), i32> {
        match self.push(data) {
            Ok(()) => return Ok(()),
            Err(libc::EAGAIN) => {}
            Err(e) => return Err(e),
        }
        loop {
            if self.pipe.closed.load(Ordering::Acquire) {
                return Err(ETERM);
            }
            {
                let mut guard = self.pipe.sender_thread.lock().unwrap();
                *guard = Some(std::thread::current());
            }
            self.pipe.sender_waiting.store(true, Ordering::Release);
            if self.pipe.closed.load(Ordering::Acquire) {
                self.pipe.sender_waiting.store(false, Ordering::Relaxed);
                return Err(ETERM);
            }
            if !self.producer.try_push(data) {
                std::thread::park();
                self.pipe.sender_waiting.store(false, Ordering::Relaxed);
                continue;
            }
            self.pipe.sender_waiting.store(false, Ordering::Relaxed);
            if self.producer.flush() {
                self.pipe.recv_notify.signal();
            }
            return Ok(());
        }
    }
}

impl BypassRecv {
    /// Peek at the next message's payload. Returns a raw pointer + length
    /// into the ring buffer. Caller must call `advance` after consuming.
    #[inline]
    pub(crate) fn peek(&mut self) -> Option<(*const u8, usize)> {
        self.consumer.try_peek()
    }

    /// Advance past the last peeked entry. Unparks a blocked sender if
    /// one is waiting, since ring space is now available. Drains the
    /// eventfd when the ring becomes empty so `libc::poll` sees the fd
    /// as not-readable.
    #[inline]
    pub(crate) fn advance(&mut self, len: usize) {
        self.consumer.advance_and_release(len);
        self.pipe.unpark_sender();
        if self.consumer.is_empty() {
            self.pipe.recv_notify.drain();
        }
    }

    /// Check if the ring is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.consumer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::NotifyHandle;
    use std::time::Duration;

    #[test]
    fn entry_size_rejects_wrap_sentinel_length() {
        assert_eq!(checked_aligned_entry_size(0), Some(HEADER_SIZE));
        assert!(checked_aligned_entry_size(WRAP_SENTINEL as usize).is_none());
    }

    #[test]
    fn byte_ring_crosses_u32_boundary_without_aliasing() {
        let (mut producer, mut consumer) = ring_pair(64);
        let base = u64::from(u32::MAX) - 3;
        producer.tail = base;
        producer.cached_head = base;
        consumer.head = base;
        consumer.cached_tail = base;
        producer.ring.head.store(base, Ordering::Relaxed);
        producer.ring.tail.store(base, Ordering::Relaxed);

        assert!(producer.try_push(b"abcd"));
        assert!(producer.flush());

        let (ptr, len) = consumer.try_peek().expect("entry visible");
        let got = unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(got, b"abcd");
        consumer.advance_and_release(len);
        assert!(consumer.is_empty());
    }

    #[test]
    fn receiver_drop_wakes_blocked_sender() {
        let notify = crate::notify::create_notify().expect("notify");
        let (mut sender, receiver) = create_bypass(64, notify.recv_notifier());
        let payload = [7u8; 32];
        sender.push(&payload).expect("initial push fills ring");

        let pipe = sender.pipe.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = sender.push_blocking(&payload);
            tx.send(result).expect("send result");
        });

        let wait_until = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < wait_until {
            if pipe.sender_waiting.load(Ordering::Acquire) {
                break;
            }
            std::thread::yield_now();
        }
        assert!(pipe.sender_waiting.load(Ordering::Acquire));

        drop(receiver);

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("sender should wake"),
            Err(ETERM)
        );
        handle.join().expect("sender thread");
    }
}
