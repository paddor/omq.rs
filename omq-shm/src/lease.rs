//! Generation-tagged completion credit and retained receive owners.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use omq_proto::Result;
use omq_proto::message::PayloadOwner;

use crate::coordination::{CreditSignal, LeaseCell};
use crate::mapping::{HEADER, Mapping, invalid};
use crate::process::{Process, futex_wait, futex_wake};
use crate::wire::{
    self, CELL_BYTES, CELL_FIELDS, READER_CLOSED, STATE_BITS, WAITER_PARKED, WAKE_SEQUENCE,
};

pub(super) use crate::coordination::{BORROWED, PENDING};

#[derive(Debug)]
pub(super) struct Table {
    pub(super) mapping: Mapping,
    pub(super) count: usize,
    pub(super) stopped: AtomicBool,
    pub(super) carrier: crate::carrier::Carrier,
}

impl Table {
    pub(super) fn borrow(&self, reference: wire::Reference) -> Result<()> {
        if reference.lease >= self.count
            || reference.generation == 0
            || reference.generation & STATE_BITS != 0
        {
            return Err(invalid("invalid SHM lease").into());
        }
        let cell = self.lease_cell(reference.lease);
        if !cell.borrow(reference.generation) {
            return Err(invalid("stale or duplicate SHM reference").into());
        }
        if !LeaseCell::matches(
            self.coordinate_cells(reference.lease),
            reference.coordinates(),
        ) {
            self.complete(reference.lease, reference.generation, BORROWED);
            return Err(invalid("SHM coordinates do not match their lease").into());
        }
        Ok(())
    }

    pub(super) fn lease_cell(&self, index: usize) -> LeaseCell<'_> {
        LeaseCell {
            state: self.cell(index),
            closed: self.mapping.atomic64(READER_CLOSED),
        }
    }

    fn coordinate_cells(
        &self,
        index: usize,
    ) -> [&std::sync::atomic::AtomicU64; crate::coordination::COORDINATES] {
        std::array::from_fn(|field| self.coordinates(index, field + 1))
    }

    pub(super) fn publish(&self, reference: wire::Reference) -> bool {
        self.lease_cell(reference.lease).publish(
            reference.generation,
            self.coordinate_cells(reference.lease),
            reference.coordinates(),
        )
    }

    fn credit_signal(&self) -> CreditSignal<'_> {
        CreditSignal {
            sequence: self.mapping.atomic32(WAKE_SEQUENCE),
            parked: self.mapping.atomic32(WAITER_PARKED),
            stopped: &self.stopped,
        }
    }
    pub(super) fn cell(&self, index: usize) -> &std::sync::atomic::AtomicU64 {
        assert!(index < self.count);
        self.mapping.atomic64(HEADER + index * CELL_BYTES)
    }

    pub(super) fn coordinates(&self, index: usize, field: usize) -> &std::sync::atomic::AtomicU64 {
        assert!(index < self.count && (1..CELL_FIELDS).contains(&field));
        self.mapping
            .atomic64(HEADER + index * CELL_BYTES + field * std::mem::size_of::<u64>())
    }

    pub(super) fn sequence(&self) -> u32 {
        self.mapping.atomic32(WAKE_SEQUENCE).load(Ordering::Acquire)
    }

    pub(super) fn notify(&self) {
        // One kernel wake for a parked producer, rather than for every drop.
        if self.credit_signal().notify() {
            futex_wake(self.mapping.atomic32(WAKE_SEQUENCE));
        }
    }

    pub(super) fn complete(&self, index: usize, generation: u64, state: u64) {
        if self.lease_cell(index).complete(generation, state) {
            self.notify();
        }
    }

    pub(super) fn close_reader(&self) {
        crate::coordination::close_reader(self.mapping.atomic64(READER_CLOSED));
        // A table has at most 4096 cells (192 KiB). Borrowed cells are retained
        // until the final application owner drops, even after this close.
        for index in 0..self.count {
            if self.lease_cell(index).cancel_pending() {
                self.notify();
            }
        }
        self.notify();
        let _ = self.carrier.notify_writer(&self.mapping);
    }
}

#[derive(Debug)]
pub(super) struct Lease {
    pub(super) table: Arc<Table>,
    pub(super) peer: Arc<Process>,
    pub(super) index: usize,
    pub(super) generation: u64,
}

#[derive(Debug, Default)]
pub(super) struct LeaseGroup {
    staging: Mutex<Vec<Lease>>,
    published: OnceLock<Vec<Lease>>,
    pub(super) complete: AtomicBool,
}

impl LeaseGroup {
    pub(super) fn track(&self, lease: Lease) {
        self.staging.lock().expect("SHM batch leases").push(lease);
    }

    pub(super) fn seal(&self) {
        let leases = std::mem::take(&mut *self.staging.lock().expect("SHM batch leases"));
        self.published.set(leases).expect("unsealed SHM batch");
    }

    pub(super) fn completed(&self) -> bool {
        if self.complete.load(Ordering::Acquire) {
            return true;
        }
        let Some(leases) = self.published.get() else {
            return false;
        };
        if leases.iter().all(Lease::completed) {
            self.complete.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }
}

impl Lease {
    pub(super) fn completed(&self) -> bool {
        lease_completed(&self.table, &self.peer, self.index, self.generation)
    }
}

pub(super) fn lease_completed(
    table: &Table,
    peer: &Process,
    index: usize,
    generation: u64,
) -> bool {
    table.lease_cell(index).completed(generation) || peer.dead()
}

#[derive(Debug)]
pub(super) struct Borrowed {
    pub(super) mapping: Arc<Mapping>,
    pub(super) offset: usize,
    pub(super) length: usize,
    pub(super) table: Arc<Table>,
    pub(super) index: usize,
    pub(super) generation: u64,
}

impl AsRef<[u8]> for Borrowed {
    fn as_ref(&self) -> &[u8] {
        self.mapping.bytes(self.offset, self.length)
    }
}

impl PayloadOwner for Borrowed {}

impl Drop for Borrowed {
    fn drop(&mut self) {
        self.table.complete(self.index, self.generation, BORROWED);
    }
}

/// A cancellable kernel wait for shared-memory completion credit. It must
/// run on a blocking worker, never on the socket's IO runtime thread.
#[derive(Debug)]
pub struct CreditWait {
    pub(super) table: Arc<Table>,
    pub(super) sequence: u32,
}

impl CreditWait {
    /// Park until a completion changes the sequence, or the connection stops.
    pub fn wait(self) {
        if self.table.credit_signal().arm(self.sequence) {
            futex_wait(self.table.mapping.atomic32(WAKE_SEQUENCE), self.sequence);
        }
    }
}
