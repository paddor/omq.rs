//! Explicit size classes and lookup of existing mapped payload slots.
#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use omq_proto::payload_pool::PayloadStorage;
use omq_proto::{Message, PayloadPool, Result};

use crate::lease::{Lease, LeaseGroup};
use crate::mapping::{DATA, HEADER, Mapping};
use crate::memory;
use crate::wire::{MULTIPART, REFERENCE_BATCH};

type Registry = BTreeMap<usize, Weak<PoolRegion>>;
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
// Keep large power-of-two slots aligned for bulk copies instead of inheriting
// the mapping header's fixed offset. Small classes keep their compact header.
const LARGE_CLASS_ALIGNMENT: usize = 4096;
pub(super) const LOOKUP_BATCH: usize = 8;

pub(super) const fn payload_offset(size: usize) -> usize {
    if size >= LARGE_CLASS_ALIGNMENT {
        LARGE_CLASS_ALIGNMENT
    } else {
        HEADER
    }
}

pub(super) fn storage_length(size: usize, capacity: usize) -> Result<usize> {
    size.checked_mul(capacity)
        .and_then(|bytes| bytes.checked_add(payload_offset(size)))
        .ok_or_else(|| omq_proto::Error::Config("SHM pool size overflow".into()))
}

thread_local! {
    pub(super) static LOOKUP: RefCell<LookupCache> = const { RefCell::new(LookupCache {
        slot: Weak::new(), slot_start: 0, slot_size: 0,
        region: Weak::new(), region_start: 0, region_size: 0,
    }) };
}

pub(super) struct LookupCache {
    pub(super) slot: Weak<Slot>,
    pub(super) slot_start: usize,
    pub(super) slot_size: usize,
    pub(super) region: Weak<PoolRegion>,
    pub(super) region_start: usize,
    pub(super) region_size: usize,
}

fn contains(start: usize, size: usize, bytes: &[u8]) -> bool {
    (bytes.as_ptr() as usize)
        .checked_sub(start)
        .is_some_and(|offset| offset < size && bytes.len() <= size - offset)
}

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[derive(Debug)]
pub(super) struct Slot {
    pub(super) mapping: Arc<Mapping>,
    pub(super) offset: usize,
    pub(super) size: usize,
    pub(super) state: Mutex<SlotState>,
    region: Arc<PoolRegion>,
}

impl Slot {
    pub(super) fn contains(&self, bytes: &[u8]) -> bool {
        contains(self.mapping.address() + self.offset, self.size, bytes)
    }

    pub(super) fn track_lease(&self, lease: Lease, kind: u8) {
        const MIN_SCAN_INTERVAL: usize = 32;
        let mut state = self.state.lock().expect("SHM slot leases");
        if matches!(kind, REFERENCE_BATCH | MULTIPART) {
            state
                .published_batch
                .as_ref()
                .expect("prepared SHM reference batch")
                .track(lease);
        } else {
            // Amortize recipient cleanup. Final checkout still acquires every
            // outstanding completion before making this slot writable.
            if state.leases.len() >= state.lease_scan_at {
                state.leases.retain(|lease| !lease.completed());
                state.lease_scan_at = state.leases.len().saturating_mul(2).max(MIN_SCAN_INTERVAL);
            }
            state.leases.push(lease);
        }
    }
}

#[derive(Debug)]
pub(super) struct PoolRegion {
    mapping: Arc<Mapping>,
    payload_offset: usize,
    slot_size: usize,
    slots: OnceLock<Box<[Weak<Slot>]>>,
}

impl PoolRegion {
    fn find(&self, bytes: &[u8]) -> Option<Arc<Slot>> {
        let displacement =
            (bytes.as_ptr() as usize).checked_sub(self.mapping.address() + self.payload_offset)?;
        let slots = self.slots.get()?;
        let slot = slots.get(displacement / self.slot_size)?.upgrade()?;
        let offset = displacement % self.slot_size;
        (bytes.len() <= self.slot_size - offset).then_some(slot)
    }
}

#[derive(Debug, Default)]
pub(super) struct SlotState {
    pub(super) leases: Vec<Lease>,
    pub(super) lease_scan_at: usize,
    pub(super) batches: Vec<Arc<LeaseGroup>>,
    pub(super) published_batch: Option<Arc<LeaseGroup>>,
    pub(super) regions: Vec<Arc<Mapping>>,
}

impl Drop for PoolRegion {
    fn drop(&mut self) {
        registry()
            .lock()
            .expect("SHM registry")
            .remove(&(self.mapping.address() + self.payload_offset));
    }
}

#[derive(Debug)]
pub(super) struct SlotStorage(pub(super) Arc<Slot>);

impl AsRef<[u8]> for SlotStorage {
    fn as_ref(&self) -> &[u8] {
        self.0.mapping.bytes(self.0.offset, self.0.size)
    }
}

impl PayloadStorage for SlotStorage {
    fn reusable(&self) -> bool {
        let mut state = self.0.state.lock().expect("SHM slot leases");
        state.leases.retain(|lease| !lease.completed());
        state.batches.retain(|batch| !batch.completed());
        if state.leases.is_empty()
            && state.batches.is_empty()
            && state
                .published_batch
                .as_ref()
                .is_none_or(|batch| batch.completed())
        {
            state.published_batch = None;
            state.regions.clear();
            true
        } else {
            false
        }
    }
}

/// Preallocate SHM-backed size classes in the existing `PayloadPool` type.
///
/// `try_message` / `try_buffer` never grow this pool. Ordinary `message` and
/// `payload` retain their existing heap fallback when no slot is available.
/// Bodies no larger than the existing inline capacity still remain inline.
/// A pooled body can also be sent over TCP, IPC, or inproc unchanged.
/// This explicit allocation is independent of socket automatic-memory limits.
///
/// # Errors
/// Returns an error for invalid size classes, size overflow, or OS allocation failure.
pub fn payload_pool(classes: impl IntoIterator<Item = (usize, usize)>) -> Result<PayloadPool> {
    PayloadPool::with_storage(classes, &crate::LEASE_RECLAIM_BUDGET, |size, capacity| {
        pool_storage(size, capacity, None)
    })
}

pub(super) fn pool_storage(
    size: usize,
    capacity: usize,
    reservation: Option<memory::Reservation>,
) -> Result<Vec<Box<dyn PayloadStorage>>> {
    if capacity == 0 {
        return Ok(Vec::new());
    }
    let length = storage_length(size, capacity)?;
    let mut mapping = Mapping::create(length, DATA)?;
    mapping.reservation = reservation;
    region_slots(&Arc::new(mapping), size, capacity)
}

fn region_slots(
    mapping: &Arc<Mapping>,
    size: usize,
    capacity: usize,
) -> Result<Vec<Box<dyn PayloadStorage>>> {
    let region = Arc::new(PoolRegion {
        mapping: mapping.clone(),
        payload_offset: payload_offset(size),
        slot_size: size,
        slots: OnceLock::new(),
    });
    let mut references = Vec::with_capacity(capacity);
    let mut slots: Vec<Box<dyn PayloadStorage>> = Vec::new();
    slots
        .try_reserve_exact(capacity)
        .map_err(|_| omq_proto::Error::Config("SHM pool allocation failed".into()))?;
    for index in 0..capacity {
        let slot = Arc::new(Slot {
            mapping: mapping.clone(),
            offset: region.payload_offset + index * size,
            size,
            state: Mutex::new(SlotState::default()),
            region: region.clone(),
        });
        references.push(Arc::downgrade(&slot));
        slots.push(Box::new(SlotStorage(slot)));
    }
    region
        .slots
        .set(references.into_boxed_slice())
        .expect("new SHM region");
    registry().lock().expect("SHM registry").insert(
        mapping.address() + region.payload_offset,
        Arc::downgrade(&region),
    );
    Ok(slots)
}

/// Whether the final body already belongs to a mapped SHM pool slot.
pub fn is_pooled(message: &Message) -> bool {
    message
        .len()
        .checked_sub(1)
        .and_then(|index| message.part_slice(index))
        .is_some_and(|body| find_slot(body).is_some())
}

/// Number of leading messages whose bodies belong to SHM pool slots.
/// Resolves up to eight queued bodies against the same region at a time.
#[must_use]
pub fn pooled_prefix(messages: &[&Message]) -> usize {
    let mut count = 0;
    let mut previous: Option<Arc<Slot>> = None;
    for messages in messages.chunks(LOOKUP_BATCH) {
        let bodies = std::array::from_fn(|index| {
            messages
                .get(index)
                .and_then(|message| crate::body(message).ok())
        });
        if let Some(slot) = &previous
            && bodies
                .iter()
                .take(messages.len())
                .all(|body| body.is_some_and(|body| slot.contains(body)))
        {
            count += messages.len();
            continue;
        }
        for slot in find_slots(bodies).into_iter().take(messages.len()) {
            if slot.is_none() {
                return count;
            }
            count += 1;
            previous = slot;
        }
    }
    count
}

pub(super) fn find_slot(bytes: &[u8]) -> Option<Arc<Slot>> {
    LOOKUP.with(|cached| {
        let mut cached = cached.borrow_mut();
        if contains(cached.slot_start, cached.slot_size, bytes)
            && let Some(slot) = cached.slot.upgrade()
        {
            return Some(slot);
        }
        let region = if contains(cached.region_start, cached.region_size, bytes) {
            cached.region.upgrade()
        } else {
            None
        };
        let region = region.or_else(|| {
            let registry = registry().lock().expect("SHM registry");
            let (_, weak) = registry.range(..=bytes.as_ptr() as usize).next_back()?;
            weak.upgrade()
        })?;
        let slot = region.find(bytes)?;
        cached.slot_start = slot.mapping.address() + slot.offset;
        cached.slot_size = slot.size;
        cached.slot = Arc::downgrade(&slot);
        cached.region_start = region.mapping.address() + region.payload_offset;
        cached.region_size = region.mapping.len() - region.payload_offset;
        cached.region = Arc::downgrade(&region);
        Some(slot)
    })
}

/// Resolve queued bodies together without retaining unused pool slots.
pub(super) fn find_slots(
    bodies: [Option<&[u8]>; LOOKUP_BATCH],
) -> [Option<Arc<Slot>>; LOOKUP_BATCH] {
    let first = bodies[0].and_then(find_slot);
    let region = first.as_ref().map(|slot| &slot.region);
    let size = bodies[0].map(<[u8]>::len);
    let mut slots = std::array::from_fn(|index| {
        if index == 0 {
            return None;
        }
        let body = bodies[index]?;
        if Some(body.len()) == size
            && let Some(slot) = &first
            && contains(slot.mapping.address() + slot.offset, slot.size, body)
        {
            return Some(slot.clone());
        }
        if Some(body.len()) == size
            && let Some(slot) = region.and_then(|region| region.find(body))
        {
            return Some(slot);
        }
        find_slot(body)
    });
    slots[0] = first;
    slots
}
