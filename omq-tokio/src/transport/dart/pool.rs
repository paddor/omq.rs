use std::sync::Arc;

use crate::buffer_pool::BufferReturn;
use crate::{BufferPool, MessageBuffer};
use omq_proto::message::{Payload, PayloadOwner};

/// Writable body capacity, independent of the protocol's datagram limit.
pub const BUFFER_CAPACITY: usize = 2048;

#[derive(Debug)]
struct ReceiveCredits {
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
}

impl BufferReturn for ReceiveCredits {
    fn publish(&self, count: usize) {
        self.credits.publish(count);
        self.signal.mark();
    }

    fn wake(&self) {
        self.signal.mark();
    }

    fn own(&self, bytes: Vec<u8>) -> Payload {
        large_payload(bytes, self.credits.clone(), self.signal.clone())
    }
}

pub(super) fn receiver(
    pool: &BufferPool,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
) -> BufferPool {
    pool.with_returns(Arc::new(ReceiveCredits { credits, signal }))
}

#[derive(Debug)]
struct LargeBody {
    bytes: Vec<u8>,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
}

impl AsRef<[u8]> for LargeBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PayloadOwner for LargeBody {
    fn retained_size(&self) -> Option<usize> {
        Some(self.bytes.capacity() + std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
    }
}

impl Drop for LargeBody {
    fn drop(&mut self) {
        // Publish after releasing the reserved body, including final clones.
        drop(std::mem::take(&mut self.bytes));
        self.credits.publish(1);
        self.signal.mark();
    }
}

pub(super) fn large_payload(
    bytes: Vec<u8>,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
) -> Payload {
    Payload::from_shared_owner(Arc::new(LargeBody {
        bytes,
        credits,
        signal,
    }))
}

#[derive(Debug)]
pub(super) enum ReceiveBody {
    Pooled(MessageBuffer),
    Owned(Vec<u8>),
}

impl ReceiveBody {
    pub(super) fn reserve(pool: &BufferPool, length: usize) -> Option<Self> {
        if length <= BUFFER_CAPACITY
            && let Some(buffer) = pool.try_take()
        {
            return Some(Self::Pooled(buffer));
        }
        let mut body = Vec::new();
        body.try_reserve_exact(length).ok()?;
        Some(Self::Owned(body))
    }

    pub(super) fn finish(self, pool: &BufferPool) -> Payload {
        match self {
            Self::Pooled(buffer) => buffer.into_payload(),
            Self::Owned(body) => pool.owned_payload(body),
        }
    }
}

impl AsRef<[u8]> for ReceiveBody {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Pooled(buffer) => buffer.as_ref(),
            Self::Owned(body) => body,
        }
    }
}

impl omq_proto::dart::FragmentBuffer for ReceiveBody {
    fn capacity(&self) -> usize {
        match self {
            Self::Pooled(buffer) => buffer.capacity(),
            Self::Owned(body) => body.capacity(),
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        match self {
            Self::Pooled(buffer) => {
                let start = buffer.len();
                let end = start + bytes.len();
                buffer.writable()[start..end].copy_from_slice(bytes);
                buffer.set_len(end).expect("reserved assembly capacity");
            }
            Self::Owned(body) => {
                assert!(bytes.len() <= body.capacity() - body.len());
                body.extend_from_slice(bytes);
            }
        }
    }
}
