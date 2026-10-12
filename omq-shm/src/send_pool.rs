//! Socket-owned automatic payload storage, shared by peers and IO lanes.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use omq_proto::{Message, PayloadPool, Result};

use crate::pool::{payload_offset, storage_length};
use crate::{
    BatchKind, COPY_THRESHOLD, MAX_LEASES, batch, find_slot, memory::Budget, pool_storage,
};

/// Automatic outbound SHM storage shared by all connections of one socket.
///
/// Size classes are allocated on demand. The byte cap includes headers and padding
/// and remains charged while any local owner or remote lease retains a mapping.
/// Idle classes are reclaimed under memory pressure. Explicit [`crate::payload_pool`]
/// storage, imported mappings, lease/carrier tables, and heap queues are excluded.
/// A capacity miss returns `None`; the transport then uses its shared carrier.
#[derive(Clone, Debug)]
pub struct SendPool {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    hwm: usize,
    budget: Arc<Budget>,
    classes: Mutex<BTreeMap<usize, Arc<PayloadPool>>>,
}

impl SendPool {
    /// Create empty automatic storage with a socket-wide byte cap.
    ///
    /// `send_hwm` bounds each class's slot count (clamped to 1..=4096).
    /// `maximum = None` disables byte accounting limits. `Some(0)` disables
    /// automatic SHM allocation while preserving carrier delivery.
    #[must_use]
    pub fn new(send_hwm: usize, maximum: Option<usize>) -> Self {
        Self {
            inner: Arc::new(Inner {
                hwm: send_hwm.clamp(1, MAX_LEASES),
                budget: Budget::new(maximum),
                classes: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Current automatically allocated mapping bytes, including retained owners.
    /// This is an observation, not a reservation of remaining capacity.
    #[must_use]
    pub fn memory_used(&self) -> usize {
        self.inner.budget.used()
    }

    /// Copy the final application body into shared socket storage once.
    ///
    /// Inline or already pooled bodies keep their existing representation.
    /// `None` means automatic storage is unavailable; callers may use the carrier.
    ///
    /// # Errors
    /// Returns an error for a missing body, size overflow, or OS allocation failure.
    pub fn prepare(&self, message: &Message) -> Result<Option<Message>> {
        let body = crate::body(message)?;
        if body.len() <= COPY_THRESHOLD || find_slot(body).is_some() {
            return Ok(Some(message.clone()));
        }
        let Some(pool) = self.class(body.len(), 1)? else {
            return Ok(None);
        };
        let Some(copied) = pool.try_message(body.len(), |bytes| bytes.copy_from_slice(body))?
        else {
            return Ok(None);
        };
        Ok(Some(crate::replace_body(message, copied)))
    }

    /// Pack queued publications into this socket's bounded shared storage.
    ///
    /// [`BatchKind::Referenced`] retains prepared source slots without copying
    /// their bodies. `None` means the batch cannot fit or storage is unavailable.
    ///
    /// # Errors
    /// Returns an error for invalid counts/envelopes, size overflow, or OS failure.
    pub fn prepare_batch(&self, messages: &[&Message], kind: BatchKind) -> Result<Option<Message>> {
        let Some(length) = batch::storage_length(messages, kind)? else {
            return Ok(None);
        };
        let Some(pool) = self.class(length, messages.len())? else {
            return Ok(None);
        };
        match kind {
            BatchKind::Copied => crate::prepare_batch(&pool, messages),
            BatchKind::Referenced => crate::prepare_reference_batch(&pool, messages),
        }
    }

    fn class(&self, length: usize, logical_messages: usize) -> Result<Option<Arc<PayloadPool>>> {
        let size = length
            .checked_next_power_of_two()
            .ok_or_else(|| omq_proto::Error::Config("SHM body too large".into()))?;
        let mut classes = self.inner.classes.lock().expect("SHM send pools");
        if let Some(pool) = classes.get(&size) {
            return Ok(Some(pool.clone()));
        }
        let mut requested = self.inner.hwm.div_ceil(logical_messages);
        if logical_messages > 1 {
            // Stage one additional batch while its recipients return credit.
            requested = (requested + 1).min(self.inner.hwm);
        }
        let header = payload_offset(size);
        let mut capacity =
            requested.min(self.inner.budget.remaining().saturating_sub(header) / size);
        if capacity == 0 {
            classes.retain(|_, pool| pool.available() != pool.capacity());
            capacity = requested.min(self.inner.budget.remaining().saturating_sub(header) / size);
        }
        if capacity == 0 {
            return Ok(None);
        }
        let length = storage_length(size, capacity)?;
        let Some(reservation) = Budget::reserve(&self.inner.budget, length) else {
            return Ok(None);
        };
        let mut reservation = Some(reservation);
        let pool = Arc::new(PayloadPool::with_storage(
            [(size, capacity)],
            &crate::LEASE_RECLAIM_BUDGET,
            |size, capacity| pool_storage(size, capacity, reservation.take()),
        )?);
        classes.insert(size, pool.clone());
        Ok(Some(pool))
    }
}
