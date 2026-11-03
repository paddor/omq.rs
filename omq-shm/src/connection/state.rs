//! Connection bookkeeping: outbound lease credit, region registrations sent
//! to the peer, and regions imported from it.

use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::lease::{PENDING, Table, lease_completed};
use crate::mapping::{Mapping, invalid};
use crate::process::Process;
use crate::wire::{self, GENERATION_STEP, MULTIPART, REFERENCE_BATCH, STATE_BITS};
use crate::{MAX_LEASES, batch, socket};

#[derive(Debug)]
pub(super) struct ActiveLease {
    index: usize,
    generation: u64,
    messages: usize,
}

/// Outbound lease credit: free table slots, published leases, and the
/// logical messages they hold against the HWM.
#[derive(Debug)]
pub(super) struct OutboundLeases {
    pub(super) free: VecDeque<usize>,
    pub(super) active: VecDeque<ActiveLease>,
    pub(super) leased_messages: usize,
    generation: u64,
    pub(super) hwm: usize,
}

impl OutboundLeases {
    pub(super) fn new(count: usize) -> Self {
        Self {
            free: (0..count).collect(),
            active: VecDeque::with_capacity(count),
            leased_messages: 0,
            generation: 0,
            hwm: count,
        }
    }

    /// Whether `needed` more logical messages fit under the HWM.
    pub(super) fn has_credit(&self, needed: usize) -> bool {
        self.hwm - self.leased_messages >= needed
    }

    pub(super) fn reclaim(&mut self, outgoing: &Table, peer: &Process, needed: usize) {
        // Leases usually complete in publication order. Returning them from
        // the front on every encode keeps credit flowing a few at a time,
        // instead of one scan of the whole window per HWM of messages.
        while let Some(lease) = self.active.front()
            && lease_completed(outgoing, peer, lease.index, lease.generation)
        {
            let lease = self.active.pop_front().expect("completed active lease");
            self.free.push_back(lease.index);
            self.leased_messages -= lease.messages;
        }
        if self.has_credit(needed) {
            return;
        }
        // Scan the complete bounded window before parking. A batch can need
        // more credit even while a few unused positions remain in the table.
        let mut budget = crate::LEASE_RECLAIM_BUDGET;
        for _ in 0..self.active.len() {
            if budget.exhausted() {
                break;
            }
            let lease = self.active.pop_front().expect("active lease");
            let _ = budget.account(std::mem::size_of::<ActiveLease>());
            if lease_completed(outgoing, peer, lease.index, lease.generation) {
                self.free.push_back(lease.index);
                self.leased_messages -= lease.messages;
            } else {
                self.active.push_back(lease);
            }
        }
    }

    /// Take a free lease and the next generation for `messages` logical
    /// messages. Callers check [`Self::has_credit`] first.
    pub(super) fn reserve(&mut self, messages: usize) -> omq_proto::Result<(usize, u64)> {
        let generation = self
            .generation
            .checked_add(GENERATION_STEP)
            .ok_or_else(|| omq_proto::Error::Protocol("SHM lease generation exhausted".into()))?;
        let index = self.free.pop_front().expect("reserved lease credit");
        self.generation = generation;
        self.leased_messages += messages;
        Ok((index, generation))
    }

    /// Track a published lease until the peer completes it.
    pub(super) fn track(&mut self, index: usize, generation: u64, messages: usize) {
        self.active.push_back(ActiveLease {
            index,
            generation,
            messages,
        });
    }
}

/// Region descriptors queued for the peer, and the regions it already has.
#[derive(Debug)]
pub(super) struct Registrations {
    /// The last `SEND_LIMIT` regions sent, in wire order.
    registered: VecDeque<(u32, u64)>,
    /// Descriptors waiting for the next control write.
    pub(super) descriptors: VecDeque<File>,
    /// Descriptors written so far; the peer acknowledges imports against it.
    pub(super) sent: u64,
}

impl Registrations {
    pub(super) fn new(descriptors: VecDeque<File>) -> Self {
        Self {
            registered: VecDeque::new(),
            descriptors,
            sent: 0,
        }
    }

    /// Queue `mapping`'s descriptor unless the peer already has it. `false`
    /// when the descriptor queue is full.
    pub(super) fn register(&mut self, mapping: &Mapping) -> io::Result<bool> {
        let identity = (mapping.id, mapping.token);
        if self.registered.contains(&identity) {
            return Ok(true);
        }
        if self.descriptors.len() == socket::SEND_LIMIT {
            return Ok(false);
        }
        self.descriptors.push_back(mapping.descriptor()?);
        if self.registered.len() == socket::SEND_LIMIT {
            self.registered.pop_front();
        }
        self.registered.push_back(identity);
        Ok(true)
    }

    /// Count every queued descriptor as written; `false` when none were.
    pub(super) fn mark_sent(&mut self) -> bool {
        if self.descriptors.is_empty() {
            return false;
        }
        self.sent = self.sent.wrapping_add(self.descriptors.len() as u64);
        self.descriptors.clear();
        true
    }
}

/// Regions imported from the peer: its last `SEND_LIMIT` registrations, plus
/// older ones that pending leases still reference.
#[derive(Debug, Default)]
pub(super) struct InboundRegions {
    pub(super) regions: VecDeque<(u32, u64, Arc<Mapping>)>,
    pub(super) retired: VecDeque<(u32, u64, Arc<Mapping>)>,
}

impl InboundRegions {
    pub(super) fn find(&self, id: u32, token: u64) -> Option<&Arc<Mapping>> {
        self.regions
            .iter()
            .chain(self.retired.iter())
            .find(|(registered, nonce, _)| *registered == id && *nonce == token)
            .map(|(_, _, mapping)| mapping)
    }

    /// Import a region, replacing an earlier one with the same identity.
    /// `incoming` holds the peer's leases, which may still reference regions
    /// that fall out of the registration window.
    pub(super) fn insert(&mut self, mapping: Mapping, incoming: &Table) -> io::Result<()> {
        let identity = (mapping.id, mapping.token);
        self.regions
            .retain(|(id, token, _)| (*id, *token) != identity);
        self.retired
            .retain(|(id, token, _)| (*id, *token) != identity);
        self.regions
            .push_back((mapping.id, mapping.token, Arc::new(mapping)));
        if self.regions.len() > socket::SEND_LIMIT {
            // Both sides retain the last 64 registrations in wire order.
            // Older regions remain discoverable while queued references need
            // them; borrowed owners independently retain their mappings.
            let pending = self.pending(incoming);
            self.retired
                .retain(|(id, token, _)| pending.contains(&(*id, *token)));
            let region = self.regions.pop_front().expect("registered region");
            if pending.contains(&(region.0, region.1)) {
                self.retired.push_back(region);
            }
            if self.retired.len() > MAX_LEASES {
                return Err(invalid("too many retained SHM regions"));
            }
        }
        Ok(())
    }

    /// Regions referenced by the peer's pending leases.
    fn pending(&self, table: &Table) -> BTreeSet<(u32, u64)> {
        let mut regions = BTreeSet::new();
        for index in 0..table.count {
            if table.cell(index).load(Ordering::Acquire) & STATE_BITS != PENDING {
                continue;
            }
            let id = table
                .coordinates(index, wire::REGION_ID)
                .load(Ordering::Relaxed) as u32;
            let token = table
                .coordinates(index, wire::REGION_TOKEN)
                .load(Ordering::Relaxed);
            regions.insert((id, token));
            let kind = table
                .coordinates(index, wire::RECORD_KIND)
                .load(Ordering::Relaxed);
            if (kind == u64::from(REFERENCE_BATCH) || kind == u64::from(MULTIPART))
                && let Some(mapping) = self.find(id, token)
            {
                pending_batch_regions(table, index, mapping, &mut regions);
            }
        }
        regions
    }
}

/// Add the regions that the pending batch or multipart table at lease
/// `index`, stored in `mapping`, refers to. Unreadable tables add none.
fn pending_batch_regions(
    table: &Table,
    index: usize,
    mapping: &Mapping,
    regions: &mut BTreeSet<(u32, u64)>,
) {
    let offset = table
        .coordinates(index, wire::BODY_OFFSET)
        .load(Ordering::Relaxed);
    let length = table
        .coordinates(index, wire::BODY_LENGTH)
        .load(Ordering::Relaxed);
    if let (Ok(offset), Ok(length)) = (usize::try_from(offset), usize::try_from(length))
        && mapping.contains(offset, length)
        && let Ok(references) = batch::references(mapping.bytes(offset, length))
    {
        regions.extend(
            references
                .into_iter()
                .map(|reference| (reference.id, reference.token)),
        );
    }
}
