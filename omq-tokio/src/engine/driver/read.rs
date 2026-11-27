//! Stream buffers and cancel-safe reads of incomplete payloads.

use super::{PeerDriverConfig, ReceiveProfile};
use bytes::{BufMut, Bytes, BytesMut};
use omq_proto::{
    copy_stats::{self, Site},
    error::Result,
    proto::Connection,
};
use parking_lot::Mutex;
use std::{
    io,
    sync::{Arc, Weak},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Largest buffer a receive pool keeps.
pub(super) const RECV_POOL_MAX_BUFFER_BYTES: usize = 8 * 1024 * 1024;
/// Idle bytes one socket's receive pool keeps.
pub(super) const RECV_POOL_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
/// Idle buffers one socket's receive pool keeps, which bounds each sorted
/// insert and removal.
pub(super) const RECV_POOL_MAX_BUFFERS: usize = 512;
const LARGE_READ_CHUNK: usize = 64 * 1024;

pub(super) const READ_BUF_INITIAL_LATENCY: usize = 4 * 1024;
pub(super) const READ_BUF_INITIAL_THROUGHPUT: usize = 4 * 1024;
pub(super) const READ_BUF_MAX: usize = 128 * 1024;
pub(super) const READ_BUF_GROW_FULL_READS: usize = 2;

/// Reusable large-body buffers that a socket's connections share.
#[derive(Debug, Default)]
pub(crate) struct RecvBufPool {
    pub(super) inner: Mutex<RecvBufPoolInner>,
}

#[derive(Debug, Default)]
pub(super) struct RecvBufPoolInner {
    /// Idle buffers in ascending capacity order.
    pub(super) buffers: Vec<BytesMut>,
    pub(super) retained_bytes: usize,
}

impl RecvBufPool {
    pub(crate) fn new() -> Arc<Self> {
        Arc::default()
    }

    /// An empty buffer for a `capacity`-byte body. Receive budgets charge a
    /// body's whole buffer, so a pooled one serves only up to twice that size.
    pub(super) fn take(&self, capacity: usize) -> BytesMut {
        let pooled = self.inner.lock().take(capacity);
        pooled.unwrap_or_else(|| BytesMut::with_capacity(capacity))
    }

    pub(super) fn give(&self, mut buf: BytesMut) {
        if buf.capacity() > RECV_POOL_MAX_BUFFER_BYTES {
            return;
        }

        buf.clear();
        // The lock guard drops at the end of this statement, so the buffers
        // the pool lets go are freed after unlocking.
        let _released = self.inner.lock().give(buf);
    }

    pub(super) fn wrap(self: &Arc<Self>, buf: BytesMut) -> Bytes {
        Bytes::from_owner(PooledRecvBuf {
            buf,
            pool: Arc::downgrade(self),
        })
    }
}

impl RecvBufPoolInner {
    /// Remove the smallest buffer that holds `capacity` bytes, and of equal
    /// ones the last returned. A buffer over twice `capacity` does not count.
    fn take(&mut self, capacity: usize) -> Option<BytesMut> {
        let smallest = self
            .buffers
            .partition_point(|buf| buf.capacity() < capacity);
        let best = self.buffers.get(smallest)?.capacity();
        if best > capacity.saturating_mul(2) {
            return None;
        }

        let latest = self.buffers.partition_point(|buf| buf.capacity() <= best) - 1;
        self.retained_bytes -= best;

        Some(self.buffers.remove(latest))
    }

    /// Keep `buf` if it fits. A full pool first evicts the buffer farthest
    /// from its size, so pooled sizes follow the received ones. Returns the
    /// buffers the pool lets go.
    fn give(&mut self, buf: BytesMut) -> [Option<BytesMut>; 2] {
        let capacity = buf.capacity();
        let evicted = if self.fits(capacity) {
            None
        } else {
            self.evict_farthest_from(capacity)
        };

        if !self.fits(capacity) {
            return [evicted, Some(buf)];
        }

        let at = self
            .buffers
            .partition_point(|pooled| pooled.capacity() <= capacity);
        self.buffers.insert(at, buf);
        self.retained_bytes += capacity;

        [evicted, None]
    }

    fn fits(&self, capacity: usize) -> bool {
        self.buffers.len() < RECV_POOL_MAX_BUFFERS
            && self.retained_bytes.saturating_add(capacity) <= RECV_POOL_MAX_RETAINED_BYTES
    }

    /// Remove the smallest or the largest buffer, whichever is farther from
    /// `capacity`.
    fn evict_farthest_from(&mut self, capacity: usize) -> Option<BytesMut> {
        let smallest = self.buffers.first()?.capacity();
        let largest = self.buffers.last()?.capacity();
        let buf = if largest.abs_diff(capacity) >= smallest.abs_diff(capacity) {
            self.buffers.pop()?
        } else {
            self.buffers.remove(0)
        };
        self.retained_bytes -= buf.capacity();

        Some(buf)
    }
}

pub(super) struct PooledRecvBuf {
    pub(super) buf: BytesMut,
    pub(super) pool: Weak<RecvBufPool>,
}

impl AsRef<[u8]> for PooledRecvBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledRecvBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        if let Some(pool) = self.pool.upgrade() {
            pool.give(buf);
        }
    }
}

/// Own a claimed payload across cancel-safe, bounded reads in the main select.
#[derive(Debug)]
pub(super) struct PendingLargeRead {
    pub(super) buf: ReceiveBody,
    pub(super) target: usize,
}

#[derive(Debug)]
pub(super) enum ReceiveBody {
    /// A body read into `buf`, which returns to `pool` once its last view
    /// drops; without a pool, the receiver owns `buf` alone.
    Stream {
        buf: BytesMut,
        pool: Option<Arc<RecvBufPool>>,
    },
    /// A body of unlimited declared size, grown as it arrives.
    Growing(Vec<u8>),
    Payload(omq_proto::PayloadBuffer),
}

impl ReceiveBody {
    /// Storage for a `target`-byte stream body that starts with `prefix`.
    fn stream(
        target: usize,
        prefix: &[u8],
        bounded: bool,
        pool: Option<&Arc<RecvBufPool>>,
    ) -> Self {
        // Bodies up to the pool limit get their whole size at once.
        let whole = target <= RECV_POOL_MAX_BUFFER_BYTES;

        if !whole && !bounded {
            // A declaration alone must not reserve an arbitrarily large body.
            let mut buf =
                Vec::with_capacity(prefix.len().saturating_add(LARGE_READ_CHUNK).min(target));
            buf.extend_from_slice(prefix);
            return Self::Growing(buf);
        }

        let pool = pool.filter(|_| whole).cloned();
        let mut buf = if let Some(pool) = &pool {
            pool.take(target)
        } else {
            // Admission already checked the complete frame and multipart size.
            // Allocate once so accepted large bodies never move while filling.
            BytesMut::with_capacity(target)
        };

        buf.extend_from_slice(prefix);
        Self::Stream { buf, pool }
    }
}

impl PendingLargeRead {
    pub(super) fn begin(
        connection: &mut Connection,
        config: &PeerDriverConfig,
        pool: Option<&Arc<RecvBufPool>>,
    ) -> Result<Option<Self>> {
        #[cfg(feature = "ws")]
        let skip_large = connection.is_ws();
        #[cfg(not(feature = "ws"))]
        let skip_large = false;

        if matches!(config.large_message_threshold, 0 | usize::MAX) || skip_large {
            return Ok(None);
        }

        let Some(info) = connection.peek_next_frame_payload_size()? else {
            return Ok(None);
        };

        // Complete frames retain their byte views, including frames left by
        // a bounded codec turn. No pool checkout should make a second copy.
        if info.buffered_payload_prefix == info.payload_len {
            return Ok(None);
        }

        let bounded = connection
            .receive_frame_size_limit(info.flags.command)
            .is_some();

        // A CURVE frame holds ciphertext and its plaintext starts past the
        // MESSAGE header, so it reads into a stream body, never a slot.
        let payload_buffer = if info.flags.command || connection.has_frame_transform() {
            None
        } else {
            connection.try_recv_payload_buffer(info.payload_len)
        };

        if payload_buffer.is_none() && info.payload_len < config.large_message_threshold {
            return Ok(None);
        }

        if let Some(mut buffer) = payload_buffer {
            let Some((target, filled)) =
                connection.begin_supplied_payload_into(buffer.writable())?
            else {
                return Ok(None);
            };

            copy_stats::record(Site::RecvLargePrefix, filled);
            buffer.set_len(filled).expect("selected slot fits body");
            return Ok(Some(Self {
                buf: ReceiveBody::Payload(buffer),
                target,
            }));
        }

        let Some((target, prefix)) = connection.begin_supplied_payload_with_prefix() else {
            return Ok(None);
        };

        // Release the shared input prefix before the caller refills read_buf.
        copy_stats::record(Site::RecvLargePrefix, prefix.len());
        Ok(Some(Self {
            buf: ReceiveBody::stream(target, prefix.as_slice(), bounded, pool),
            target,
        }))
    }

    pub(super) fn filled(&self) -> usize {
        match &self.buf {
            ReceiveBody::Stream { buf, .. } => buf.len(),
            ReceiveBody::Growing(buf) => buf.len(),
            ReceiveBody::Payload(buf) => buf.len(),
        }
    }

    pub(super) fn complete(&self) -> bool {
        self.filled() == self.target
    }

    pub(super) async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> io::Result<usize> {
        let filled = self.filled();
        let remaining = (self.target - filled).min(LARGE_READ_CHUNK);

        let n = match &mut self.buf {
            ReceiveBody::Stream { buf, .. } => {
                buf.reserve(remaining);
                let mut limited = buf.limit(remaining);
                reader.read_buf(&mut limited).await?
            }
            ReceiveBody::Growing(buf) => {
                grow_body(buf, remaining, self.target);
                let mut limited = buf.limit(remaining);
                reader.read_buf(&mut limited).await?
            }
            ReceiveBody::Payload(buf) => {
                let n = reader
                    .read(&mut buf.writable()[filled..filled + remaining])
                    .await?;
                buf.set_len(filled + n).expect("read stays within body");

                n
            }
        };

        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }

        Ok(n)
    }

    pub(super) fn finish(self, connection: &mut Connection) -> Result<()> {
        debug_assert!(self.complete());
        // Stream bodies stay writable until the connection has decrypted a
        // CURVE frame in place; the payload then views the same buffer.
        match self.buf {
            ReceiveBody::Payload(buf) => connection.supply_payload_frame(buf.into_payload()),
            ReceiveBody::Stream { buf, pool } => {
                let capacity = buf.capacity();
                connection.supply_payload_frame_mut(buf, |buf, range| {
                    let bytes = match pool {
                        Some(pool) => pool.wrap(buf),
                        None => buf.freeze(),
                    };
                    omq_proto::Payload::from_bytes_with_retained_size(bytes.slice(range), capacity)
                })
            }
            ReceiveBody::Growing(buf) => {
                let capacity = buf.capacity();
                // A unique `Bytes` converts back without copying.
                let frame = BytesMut::from(Bytes::from(buf));
                connection.supply_payload_frame_mut(frame, |buf, range| {
                    omq_proto::Payload::from_bytes_with_retained_size(
                        buf.freeze().slice(range),
                        capacity,
                    )
                })
            }
        }
    }
}

/// Make room for `additional` more bytes of a `target`-byte body. The capacity
/// doubles, so it stays within twice the bytes received, and stops at `target`.
fn grow_body(buf: &mut Vec<u8>, additional: usize, target: usize) {
    let needed = buf.len() + additional;
    if needed <= buf.capacity() {
        return;
    }

    let capacity = buf.capacity().saturating_mul(2).min(target).max(needed);
    buf.reserve_exact(capacity - buf.len());
}

/// Retain stream buffers and a partially read body across selected operations.
pub(super) struct StreamRead {
    pub(super) buffer: BytesMut,
    target: usize,
    full_reads: usize,
    /// Reusable large-body buffers the socket's connections share. Without
    /// it, each large body gets an allocation its receiver owns alone.
    pool: Option<Arc<RecvBufPool>>,
    pending: Option<PendingLargeRead>,
}

impl StreamRead {
    pub(super) fn new(profile: ReceiveProfile, pool: Option<Arc<RecvBufPool>>) -> Self {
        let target = match profile {
            ReceiveProfile::Throughput => READ_BUF_INITIAL_THROUGHPUT,
            ReceiveProfile::Latency | ReceiveProfile::LatencyReq => READ_BUF_INITIAL_LATENCY,
        };

        Self {
            buffer: BytesMut::with_capacity(target),
            target,
            full_reads: 0,
            pool,
            pending: None,
        }
    }

    pub(super) fn has_pending_body(&self) -> bool {
        self.pending.is_some()
    }

    pub(super) fn finish_pending(
        &mut self,
        connection: &mut Connection,
        config: &PeerDriverConfig,
    ) -> Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(PendingLargeRead::complete)
        {
            self.pending.take().unwrap().finish(connection)?;
            self.pending = PendingLargeRead::begin(connection, config, self.pool.as_ref())?;
        }

        Ok(())
    }

    pub(super) async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> io::Result<usize> {
        match &mut self.pending {
            Some(large) => large.read(reader).await,
            None => reader.read_buf(&mut self.buffer).await,
        }
    }

    pub(super) fn handle_input(
        &mut self,
        n: usize,
        connection: &mut Connection,
        config: &PeerDriverConfig,
        last_input: &mut Instant,
    ) -> Result<()> {
        *last_input = Instant::now();
        if n >= self.target && self.target < READ_BUF_MAX {
            self.full_reads += 1;
            if self.full_reads >= READ_BUF_GROW_FULL_READS {
                self.target = (self.target * 2).min(READ_BUF_MAX);
                self.full_reads = 0;
            }
        } else {
            self.full_reads = 0;
        }

        let chunk = self.buffer.split().freeze();
        connection.handle_input(chunk)?;
        self.pending = PendingLargeRead::begin(connection, config, self.pool.as_ref())?;
        // Parse before reserving so a consumed prefix can release its allocation.
        // The target is a growth hint, not a minimum spare-capacity promise.
        self.buffer
            .reserve(self.target.saturating_sub(self.buffer.capacity()));

        Ok(())
    }
}
