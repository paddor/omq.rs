use std::sync::Arc;

use crate::{PayloadBuffer, PayloadPool};
use omq_proto::message::{Payload, PayloadOwner};
use omq_proto::payload_pool::PayloadRelease;

#[derive(Debug)]
struct ReceiveCredits {
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
}

impl PayloadRelease for ReceiveCredits {
    fn publish(&self, count: usize) {
        self.credits.publish(count);
        self.signal.mark();
    }

    fn wake(&self) {
        self.signal.mark();
    }
}

#[derive(Debug)]
pub(super) struct ReceivePool {
    storage: Option<PayloadPool>,
    received: Option<PayloadPool>,
    returns: Arc<ReceiveCredits>,
}

pub(super) fn receiver(
    pool: Option<&PayloadPool>,
    credits: Arc<omq_proto::dart::CreditCounter>,
    signal: Arc<crate::engine::signal::DataSignal>,
) -> ReceivePool {
    let returns = Arc::new(ReceiveCredits { credits, signal });
    ReceivePool {
        storage: pool.cloned(),
        received: pool.map(|pool| pool.with_release(returns.clone())),
        returns,
    }
}

#[derive(Debug, Default)]
pub(super) struct ReceiveBuffers {
    class: Option<usize>,
    buffers: Vec<PayloadBuffer>,
}

impl ReceiveBuffers {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            class: None,
            buffers: Vec::with_capacity(capacity),
        }
    }
}

impl ReceivePool {
    pub(super) fn storage(&self) -> Option<&PayloadPool> {
        self.received.as_ref()
    }

    pub(super) fn owned_payload(&self, bytes: Vec<u8>) -> Payload {
        large_payload(
            bytes,
            self.returns.credits.clone(),
            self.returns.signal.clone(),
        )
    }

    pub(super) fn copy_received(&self, bytes: &[u8]) -> Option<Payload> {
        let mut body = Vec::new();
        body.try_reserve_exact(bytes.len()).ok()?;
        body.extend_from_slice(bytes);
        Some(self.owned_payload(body))
    }

    pub(super) fn take(&self, size: usize, cache: &mut ReceiveBuffers) -> Option<PayloadBuffer> {
        let pool = self.storage.as_ref()?;
        let target = pool.class_size(size)?;
        if cache.class != Some(target) {
            pool.with_recycling_batch(|| cache.buffers.clear());
            cache.class = Some(target);
        }
        if cache.buffers.is_empty() {
            pool.try_buffers_into(size, 64, &mut cache.buffers);
        }
        cache.buffers.pop()
    }
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
    Pooled(PayloadBuffer),
    Owned(Vec<u8>),
}

impl ReceiveBody {
    pub(super) fn reserve(pool: &ReceivePool, length: usize) -> Option<Self> {
        if let Some(storage) = pool.storage()
            && let Some(buffer) = storage.try_buffer(length)
        {
            return Some(Self::Pooled(buffer));
        }
        let mut body = Vec::new();
        body.try_reserve_exact(length).ok()?;
        Some(Self::Owned(body))
    }

    pub(super) fn finish(self, pool: &ReceivePool) -> Payload {
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
