//! Socket-owned automatic payload storage, shared by peers and IO lanes.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use omq_proto::{Message, PayloadPool, Result};

use crate::mapping::invalid;
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
    classes: ArcSwap<BTreeMap<usize, Arc<PayloadPool>>>,
    allocation: Mutex<()>,
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
                classes: ArcSwap::from_pointee(BTreeMap::new()),
                allocation: Mutex::new(()),
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
        let Some(pool) = self.class(body.len())? else {
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
        let Some(pool) = self.class(length)? else {
            return Ok(None);
        };
        match kind {
            BatchKind::Copied => batch::prepare_copied_validated(&pool, messages, length),
            BatchKind::Referenced => batch::prepare_references_validated(&pool, messages, length),
        }
    }

    /// Prepare one shared record for a message selected by
    /// [`crate::uses_multipart_record`]. Unpooled frames are copied once after
    /// the frame table; larger pooled frames stay in place. Publish it with
    /// [`crate::Connection::encode_multipart_and_frame`].
    /// `None` means storage is unavailable; the message can still be encoded.
    ///
    /// # Errors
    /// Returns an error for an unsupported message, size overflow, or OS failure.
    pub fn prepare_multipart(&self, message: &Message) -> Result<Option<Message>> {
        if !batch::uses_multipart_record(message) {
            return Err(invalid("message does not use an SHM multipart record").into());
        }
        let frames = batch::MultipartFrames::new(message);
        let Some(pool) = self.class(frames.length())? else {
            return Ok(None);
        };
        Ok(batch::prepare_multipart_validated(&pool, message, &frames))
    }

    fn class(&self, length: usize) -> Result<Option<Arc<PayloadPool>>> {
        let size = length
            .checked_next_power_of_two()
            .ok_or_else(|| omq_proto::Error::Config("SHM body too large".into()))?;
        if let Some(pool) = self.inner.classes.load().get(&size).cloned() {
            return Ok(Some(pool));
        }
        self.allocate_class(size)
    }

    fn allocate_class(&self, size: usize) -> Result<Option<Arc<PayloadPool>>> {
        let _allocation = self
            .inner
            .allocation
            .lock()
            .expect("SHM send pool allocation");
        if let Some(pool) = self.inner.classes.load().get(&size).cloned() {
            return Ok(Some(pool));
        }
        // Classes are shared by every peer. Reserve records independently
        // of the first batch's logical count, within the socket byte cap.
        let requested = self.inner.hwm;
        let header = payload_offset(size);
        let mut capacity =
            requested.min(self.inner.budget.remaining().saturating_sub(header) / size);
        if capacity == 0 {
            let mut classes = (**self.inner.classes.load()).clone();
            classes.retain(|_, pool| pool.available() != pool.capacity());
            self.inner.classes.store(Arc::new(classes));
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
        let mut classes = (**self.inner.classes.load()).clone();
        classes.insert(size, pool.clone());
        self.inner.classes.store(Arc::new(classes));
        Ok(Some(pool))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_batch_records_do_not_depend_on_the_first_batch_count() {
        const HWM: usize = 8;
        const MAXIMUM: usize = 1024 * 1024;
        let pool = SendPool::new(HWM, Some(MAXIMUM));
        let messages: Vec<_> = (0..HWM).map(|_| Message::from_slice(&[1; 64])).collect();
        let references: Vec<_> = messages.iter().collect();
        // Eight peers can each retain one HWM-sized batch in this shared class.
        let retained: Vec<_> = (0..HWM)
            .map(|_| {
                pool.prepare_batch(&references, BatchKind::Copied)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        assert!(pool.memory_used() <= MAXIMUM);
        assert!(
            pool.prepare_batch(&references, BatchKind::Copied)
                .unwrap()
                .is_none()
        );
        drop(retained);
        assert!(
            pool.prepare_batch(&references, BatchKind::Copied)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn cached_class_reads_do_not_wait_for_an_allocation() {
        let pool = SendPool::new(8, Some(1024 * 1024));
        let first = pool.class(16 * 1024).unwrap().unwrap();
        let allocation = pool.inner.allocation.lock().unwrap();
        let reader = pool.clone();
        let (done, result) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            done.send(reader.class(16 * 1024).unwrap().unwrap())
                .unwrap();
        });
        let reused = result
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        drop(allocation);
        thread.join().unwrap();
        assert!(Arc::ptr_eq(&first, &reused));
    }

    #[test]
    fn concurrent_class_misses_share_one_reserved_mapping() {
        const SIZE: usize = 16 * 1024;
        const PEERS: usize = 8;
        let maximum = storage_length(SIZE, PEERS).unwrap();
        let pool = SendPool::new(PEERS, Some(maximum));
        let barrier = Arc::new(std::sync::Barrier::new(PEERS));
        let classes = std::thread::scope(|scope| {
            let readers: Vec<_> = (0..PEERS)
                .map(|_| {
                    let pool = &pool;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        pool.class(SIZE).unwrap().unwrap()
                    })
                })
                .collect();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(classes.iter().all(|class| Arc::ptr_eq(class, &classes[0])));
        assert_eq!(pool.memory_used(), maximum);
    }
}
