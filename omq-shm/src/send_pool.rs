//! Socket-owned automatic payload storage, shared by peers and IO lanes.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use arc_swap::ArcSwap;
use omq_proto::{Message, PayloadPool, Result};

use crate::mapping::invalid;
use crate::pool::{payload_offset, storage_length};
use crate::{
    BatchKind, COPY_THRESHOLD, MAX_LEASES, batch, find_slot, memory::Budget, pool_storage,
};

/// Minimum spacing of growth checks. Growth retires a class only after it
/// stayed unused for a whole interval, so classes used in turn are kept.
const GROWTH_CHECK_INTERVAL_MS: u64 = 50;

/// Automatic outbound SHM storage shared by all connections of one socket.
///
/// Size classes are allocated on demand. The byte cap includes headers and padding
/// and remains charged while any local owner or remote lease retains a mapping.
/// A body class starts with its send-HWM window when the budget allows; batch
/// and multipart record classes take at most a quarter of the cap. A full
/// class below the send HWM doubles when the budget allows. Idle classes are
/// reclaimed under memory pressure.
/// Explicit [`crate::payload_pool`] storage, imported mappings, lease/carrier
/// tables, and heap queues are excluded.
/// A capacity miss returns `None`; the transport then uses its shared carrier.
#[derive(Clone, Debug)]
pub struct SendPool {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    hwm: usize,
    budget: Arc<Budget>,
    classes: ArcSwap<BTreeMap<usize, Arc<Class>>>,
    /// Serializes class changes. Holds replaced classes until their last
    /// slot returns; their storage stays charged while recipients hold leases.
    allocation: Mutex<Vec<Arc<Class>>>,
    created: Instant,
    /// Milliseconds after `created` before the next growth check may run.
    next_growth_check: AtomicU64,
}

/// How a new class claims the budget.
#[derive(Clone, Copy)]
enum Sizing {
    /// Single bodies: the whole send-HWM window when it fits.
    Window,
    /// Batch and multipart records, whose sizes vary with each drain: at most
    /// a quarter of the cap, so one record size cannot hold another's budget.
    Share,
}

/// One size class, with its successful preparations for idle detection.
#[derive(Debug)]
struct Class {
    pool: PayloadPool,
    uses: AtomicU64,
    checked_uses: AtomicU64,
}

impl Class {
    fn new(pool: PayloadPool) -> Self {
        Self {
            pool,
            uses: AtomicU64::new(0),
            checked_uses: AtomicU64::new(0),
        }
    }

    fn prepare<T>(&self, prepare: impl Fn(&PayloadPool) -> Result<Option<T>>) -> Result<Option<T>> {
        let prepared = prepare(&self.pool)?;
        if prepared.is_some() {
            self.uses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(prepared)
    }

    fn idle(&self) -> bool {
        self.pool.available() == self.pool.capacity()
    }

    /// Whether no preparation succeeded since the previous call.
    fn unused_since_check(&self) -> bool {
        let uses = self.uses.load(Ordering::Relaxed);
        self.checked_uses.swap(uses, Ordering::Relaxed) == uses
    }
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
                allocation: Mutex::new(Vec::new()),
                created: Instant::now(),
                next_growth_check: AtomicU64::new(0),
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
        let copied = self.with_class(body.len(), Sizing::Window, |pool| {
            pool.try_message(body.len(), |bytes| bytes.copy_from_slice(body))
        })?;
        Ok(copied.map(|copied| crate::replace_body(message, copied)))
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
        self.with_class(length, Sizing::Share, |pool| match kind {
            BatchKind::Copied => batch::prepare_copied_validated(pool, messages, length),
            BatchKind::Referenced => batch::prepare_references_validated(pool, messages, length),
        })
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
        self.with_class(frames.length(), Sizing::Share, |pool| {
            Ok(batch::prepare_multipart_validated(pool, message, &frames))
        })
    }

    /// Prepare in the class for `length`. An undersized class without a free
    /// slot may grow once; otherwise `None` lets the caller use the carrier.
    fn with_class<T>(
        &self,
        length: usize,
        sizing: Sizing,
        prepare: impl Fn(&PayloadPool) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        let size = class_size(length)?;
        let Some(class) = self.class(size, sizing)? else {
            return Ok(None);
        };
        if let Some(prepared) = class.prepare(&prepare)? {
            return Ok(Some(prepared));
        }

        match self.grow_class(size, &class)? {
            Some(larger) => larger.prepare(&prepare),
            None => Ok(None),
        }
    }

    fn class(&self, size: usize, sizing: Sizing) -> Result<Option<Arc<Class>>> {
        if let Some(class) = self.inner.classes.load().get(&size).cloned() {
            return Ok(Some(class));
        }
        self.allocate_class(size, sizing)
    }

    fn allocate_class(&self, size: usize, sizing: Sizing) -> Result<Option<Arc<Class>>> {
        let mut retired = self.lock_allocation();
        if let Some(class) = self.inner.classes.load().get(&size).cloned() {
            return Ok(Some(class));
        }

        // Classes are shared by every peer. Reserve records independently
        // of the first batch's logical count, within the socket byte cap.
        if self.free_slots(size) == 0 {
            self.retire_classes(&mut retired, |_, class| class.idle());
        }
        self.install_class(size, self.initial_capacity(size, sizing), &mut retired)
    }

    /// Slots for a new class: up to the send HWM and the free budget. Record
    /// classes also take at most a quarter of the cap, but at least one slot.
    fn initial_capacity(&self, size: usize, sizing: Sizing) -> usize {
        let share = match (sizing, self.inner.budget.maximum()) {
            (Sizing::Share, Some(maximum)) => class_slots(maximum / 4, size).max(1),
            _ => usize::MAX,
        };
        self.free_slots(size).min(share)
    }

    /// Replace an exhausted class created below the send HWM with one of up
    /// to twice its slots, after classes left unused since the previous check
    /// return their storage. The old class keeps serving its in-flight slots.
    fn grow_class(&self, size: usize, exhausted: &Arc<Class>) -> Result<Option<Arc<Class>>> {
        if exhausted.pool.capacity() >= self.inner.hwm || !self.growth_check_due() {
            return Ok(None);
        }
        let mut retired = self.lock_allocation();
        let current = self.inner.classes.load().get(&size).cloned();
        if let Some(current) = current.filter(|current| !Arc::ptr_eq(current, exhausted)) {
            // Another caller replaced it while this one waited.
            return Ok(Some(current));
        }

        self.retire_classes(&mut retired, |other, class| {
            other != size && class.unused_since_check() && class.idle()
        });
        let capacity = self
            .free_slots(size)
            .min(exhausted.pool.capacity().saturating_mul(2));
        if capacity <= exhausted.pool.capacity() {
            return Ok(None);
        }
        self.install_class(size, capacity, &mut retired)
    }

    /// Claim the next growth check; checks run at most once per interval.
    fn growth_check_due(&self) -> bool {
        let now = u64::try_from(self.inner.created.elapsed().as_millis()).unwrap_or(u64::MAX);
        let next = self.inner.next_growth_check.load(Ordering::Relaxed);
        now >= next
            && self
                .inner
                .next_growth_check
                .compare_exchange(
                    next,
                    now.saturating_add(GROWTH_CHECK_INTERVAL_MS),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    /// Remove classes selected by `retire`, and replaced classes whose last
    /// slot returned, so their storage leaves the budget.
    fn retire_classes(
        &self,
        retired: &mut Vec<Arc<Class>>,
        retire: impl Fn(usize, &Class) -> bool,
    ) {
        let mut classes = (**self.inner.classes.load()).clone();
        classes.retain(|&size, class| !retire(size, class));
        self.inner.classes.store(Arc::new(classes));
        retired.retain(|class| !class.idle());
    }

    /// Slots of `size` that fit the send HWM and the remaining budget.
    fn free_slots(&self, size: usize) -> usize {
        self.inner
            .hwm
            .min(class_slots(self.inner.budget.remaining(), size))
    }

    /// Reserve and map `capacity` slots of `size` as the class, retiring the
    /// class it replaces.
    fn install_class(
        &self,
        size: usize,
        capacity: usize,
        retired: &mut Vec<Arc<Class>>,
    ) -> Result<Option<Arc<Class>>> {
        if capacity == 0 {
            return Ok(None);
        }
        let length = storage_length(size, capacity)?;
        let Some(reservation) = Budget::reserve(&self.inner.budget, length) else {
            return Ok(None);
        };
        let mut reservation = Some(reservation);
        let pool = PayloadPool::with_storage(
            [(size, capacity)],
            &crate::LEASE_RECLAIM_BUDGET,
            |size, capacity| pool_storage(size, capacity, reservation.take()),
        )?;

        let class = Arc::new(Class::new(pool));
        let mut classes = (**self.inner.classes.load()).clone();
        if let Some(replaced) = classes.insert(size, class.clone()) {
            retired.push(replaced);
        }
        self.inner.classes.store(Arc::new(classes));
        Ok(Some(class))
    }

    fn lock_allocation(&self) -> MutexGuard<'_, Vec<Arc<Class>>> {
        self.inner
            .allocation
            .lock()
            .expect("SHM send pool allocation")
    }
}

fn class_size(length: usize) -> Result<usize> {
    length
        .checked_next_power_of_two()
        .ok_or_else(|| omq_proto::Error::Config("SHM body too large".into()))
}

/// Slots of `size` in one region of at most `bytes`, including its header.
fn class_slots(bytes: usize, size: usize) -> usize {
    bytes.saturating_sub(payload_offset(size)) / size
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
    fn a_full_record_class_doubles_into_the_free_budget() {
        const SIZE: usize = 4096;
        // A quarter of this cap holds two batch records; the rest stays free.
        let maximum = 4 * storage_length(SIZE, 2).unwrap();
        let pool = SendPool::new(8, Some(maximum));
        let messages: Vec<_> = (0..2).map(|_| Message::from_slice(&[1; 1024])).collect();
        let references: Vec<_> = messages.iter().collect();
        let retained: Vec<_> = (0..4)
            .map(|_| {
                pool.prepare_batch(&references, BatchKind::Copied)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        assert_eq!(retained.len(), 4);
        assert_eq!(
            pool.memory_used(),
            storage_length(SIZE, 2).unwrap() + storage_length(SIZE, 4).unwrap()
        );
    }

    #[test]
    fn growth_retires_classes_left_unused_for_a_check_interval() {
        const HWM: usize = 8;
        const LARGE: usize = 16 * 1024;
        const SMALL: usize = 4096;
        // The large class takes its whole window and leaves the small class two slots.
        let maximum = storage_length(LARGE, HWM).unwrap() + storage_length(SMALL, 2).unwrap();
        let pool = SendPool::new(HWM, Some(maximum));
        drop(
            pool.prepare(&Message::from_slice(&[1; LARGE]))
                .unwrap()
                .unwrap(),
        );
        let small = Message::from_slice(&[2; SMALL]);
        let retained: Vec<_> = (0..2)
            .map(|_| pool.prepare(&small).unwrap().unwrap())
            .collect();

        // The large class was used before this first growth check, so it stays.
        assert!(pool.prepare(&small).unwrap().is_none());

        // Unused for a whole interval, it returns its budget and the full
        // small class doubles instead of sending bodies through the carrier.
        std::thread::sleep(std::time::Duration::from_millis(
            GROWTH_CHECK_INTERVAL_MS + 10,
        ));
        let grown = pool.prepare(&small).unwrap().unwrap();
        assert!(find_slot(crate::body(&grown).unwrap()).is_some());
        assert_eq!(
            pool.memory_used(),
            storage_length(SMALL, 2).unwrap() + storage_length(SMALL, 4).unwrap()
        );
        for message in &retained {
            assert!(crate::body(message).unwrap().iter().all(|&byte| byte == 2));
        }
    }

    #[test]
    fn cached_class_reads_do_not_wait_for_an_allocation() {
        let pool = SendPool::new(8, Some(1024 * 1024));
        let first = pool.class(16 * 1024, Sizing::Window).unwrap().unwrap();
        let allocation = pool.inner.allocation.lock().unwrap();
        let reader = pool.clone();
        let (done, result) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            done.send(reader.class(16 * 1024, Sizing::Window).unwrap().unwrap())
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
                        pool.class(SIZE, Sizing::Window).unwrap().unwrap()
                    })
                })
                .collect();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(classes.iter().all(|class| Arc::ptr_eq(class, &classes[0])));
        let capacity = classes[0].pool.capacity();
        assert_eq!(pool.memory_used(), storage_length(SIZE, capacity).unwrap());
    }
}
