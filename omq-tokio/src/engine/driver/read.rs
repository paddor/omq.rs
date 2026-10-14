//! Stream buffers and cancel-safe reads of incomplete payloads.

use super::{PeerDriverConfig, ReceiveProfile};
use bytes::{BufMut, Bytes, BytesMut};
use omq_proto::{
    copy_stats::{self, Site},
    error::Result,
    proto::Connection,
};
use std::{
    io,
    sync::{Arc, Weak},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(super) const RECV_POOL_MAX_BUFFER_BYTES: usize = 8 * 1024 * 1024;
pub(super) const RECV_POOL_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct RecvBufPool {
    pub(super) inner: std::sync::Mutex<RecvBufPoolInner>,
}

#[derive(Debug, Default)]
pub(super) struct RecvBufPoolInner {
    pub(super) buffers: Vec<BytesMut>,
    pub(super) retained_bytes: usize,
}

impl RecvBufPool {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(RecvBufPoolInner::default()),
        })
    }

    pub(super) fn take(&self, capacity: usize) -> BytesMut {
        let mut pool = self.inner.lock().expect("recv buf pool");
        if let Some(mut buf) = pool.buffers.pop() {
            pool.retained_bytes = pool.retained_bytes.saturating_sub(buf.capacity());
            buf.clear();
            if buf.capacity() < capacity {
                buf.reserve(capacity);
            }
            return buf;
        }
        BytesMut::with_capacity(capacity)
    }

    pub(super) fn give(&self, mut buf: BytesMut) {
        let capacity = buf.capacity();
        if capacity > RECV_POOL_MAX_BUFFER_BYTES {
            return;
        }
        buf.clear();
        let mut pool = self.inner.lock().expect("recv buf pool");
        if pool.retained_bytes.saturating_add(capacity) <= RECV_POOL_MAX_RETAINED_BYTES {
            pool.retained_bytes += capacity;
            pool.buffers.push(buf);
        }
    }

    pub(super) fn wrap(self: &Arc<Self>, buf: BytesMut) -> Bytes {
        Bytes::from_owner(PooledRecvBuf {
            buf,
            pool: Arc::downgrade(self),
        })
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
    Stream { buf: BytesMut, pooled: bool },
    Payload(omq_proto::PayloadBuffer),
}

impl PendingLargeRead {
    pub(super) fn begin(
        connection: &mut Connection,
        config: &PeerDriverConfig,
        pool: &Arc<RecvBufPool>,
    ) -> Result<Option<Self>> {
        #[cfg(feature = "ws")]
        let skip_large = connection.is_ws();
        #[cfg(not(feature = "ws"))]
        let skip_large = false;
        if matches!(config.large_message_threshold, 0 | usize::MAX)
            || connection.has_frame_transform()
            || skip_large
        {
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
        let payload_buffer = if info.flags.command {
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
        let pooled = target <= RECV_POOL_MAX_BUFFER_BYTES;
        let mut buf = if pooled {
            pool.take(target)
        } else {
            BytesMut::with_capacity(target)
        };
        buf.extend_from_slice(prefix.as_slice());
        Ok(Some(Self {
            buf: ReceiveBody::Stream { buf, pooled },
            target,
        }))
    }

    pub(super) fn filled(&self) -> usize {
        match &self.buf {
            ReceiveBody::Stream { buf, .. } => buf.len(),
            ReceiveBody::Payload(buf) => buf.len(),
        }
    }

    pub(super) fn complete(&self) -> bool {
        self.filled() == self.target
    }

    pub(super) async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> io::Result<usize> {
        let filled = self.filled();
        let remaining = (self.target - filled).min(64 * 1024);
        let n = match &mut self.buf {
            ReceiveBody::Stream { buf, .. } => {
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

    pub(super) fn finish(self, connection: &mut Connection, pool: &Arc<RecvBufPool>) -> Result<()> {
        debug_assert!(self.complete());
        let payload = match self.buf {
            ReceiveBody::Payload(buf) => buf.into_payload(),
            ReceiveBody::Stream { buf, pooled } => {
                let capacity = buf.capacity();
                let bytes = if pooled { pool.wrap(buf) } else { buf.freeze() };
                omq_proto::Payload::from_bytes_with_retained_size(bytes, capacity)
            }
        };
        connection.supply_payload_frame(payload)
    }
}

pub(super) const READ_BUF_INITIAL_LATENCY: usize = 4 * 1024;
pub(super) const READ_BUF_INITIAL_THROUGHPUT: usize = 4 * 1024;
pub(super) const READ_BUF_MAX: usize = 128 * 1024;
pub(super) const READ_BUF_GROW_FULL_READS: usize = 2;

/// Retain stream buffers and a partially read body across selected operations.
pub(super) struct StreamRead {
    pub(super) buffer: BytesMut,
    target: usize,
    full_reads: usize,
    pool: Arc<RecvBufPool>,
    pending: Option<PendingLargeRead>,
}

impl StreamRead {
    pub(super) fn new(profile: ReceiveProfile) -> Self {
        let target = match profile {
            ReceiveProfile::Throughput => READ_BUF_INITIAL_THROUGHPUT,
            ReceiveProfile::Latency | ReceiveProfile::LatencyReq => READ_BUF_INITIAL_LATENCY,
        };
        Self {
            buffer: BytesMut::with_capacity(target),
            target,
            full_reads: 0,
            pool: RecvBufPool::new(),
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
            self.pending
                .take()
                .unwrap()
                .finish(connection, &self.pool)?;
            self.pending = PendingLargeRead::begin(connection, config, &self.pool)?;
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
        self.pending = PendingLargeRead::begin(connection, config, &self.pool)?;
        // Parse before reserving so a consumed prefix can release its allocation.
        // The target is a growth hint, not a minimum spare-capacity promise.
        self.buffer
            .reserve(self.target.saturating_sub(self.buffer.capacity()));
        Ok(())
    }
}
