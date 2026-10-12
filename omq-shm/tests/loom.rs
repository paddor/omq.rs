#![cfg(all(loom, target_os = "linux"))]

//! Small-state interleavings of the production transitions, not shadow algorithms.

use loom::sync;
use loom::{model, thread};
use sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use sync::{Arc, Mutex};

#[path = "../src/coordination.rs"]
mod coordination;
#[path = "../src/memory.rs"]
mod memory;

use coordination::{
    BORROWED, COORDINATES, CarrierState, CreditSignal, DONE, GENERATION_STEP, LeaseCell, PENDING,
    STATE_BITS,
};

struct Lease {
    state: AtomicU64,
    coordinates: [AtomicU64; COORDINATES],
    closed: AtomicU64,
}

impl Lease {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            coordinates: std::array::from_fn(|_| AtomicU64::new(0)),
            closed: AtomicU64::new(0),
        }
    }
    fn cell(&self) -> LeaseCell<'_> {
        LeaseCell {
            state: &self.state,
            closed: &self.closed,
        }
    }
    fn publish(&self, generation: u64, coordinates: [u64; COORDINATES]) -> bool {
        self.cell()
            .publish(generation, self.coordinate_cells(), coordinates)
    }
    fn matches(&self, coordinates: [u64; COORDINATES]) -> bool {
        LeaseCell::matches(self.coordinate_cells(), coordinates)
    }
    fn coordinate_cells(&self) -> [&AtomicU64; COORDINATES] {
        std::array::from_fn(|index| &self.coordinates[index])
    }
    fn close(&self) {
        coordination::close_reader(&self.closed);
        self.cell().cancel_pending();
    }
}

#[test]
fn publication_and_close_never_leave_an_unread_lease_pinned() {
    model(|| {
        let lease = Arc::new(Lease::new());
        let publisher = {
            let lease = lease.clone();
            thread::spawn(move || {
                lease.publish(GENERATION_STEP, [1, 2, 3, 4, 5]);
            })
        };
        lease.close();
        publisher.join().unwrap();
        assert!(lease.cell().completed(GENERATION_STEP));
        assert_eq!(lease.state.load(Ordering::Acquire) & STATE_BITS, DONE);
    });
}

#[test]
fn borrow_racing_close_either_cancels_or_pins_until_completion() {
    model(|| {
        let lease = Arc::new(Lease::new());
        lease.publish(GENERATION_STEP, [1, 2, 3, 4, 5]);
        let borrower = {
            let lease = lease.clone();
            thread::spawn(move || {
                if lease.cell().borrow(GENERATION_STEP) {
                    assert!(lease.matches([1, 2, 3, 4, 5]));
                    assert!(!lease.cell().completed(GENERATION_STEP));
                    thread::yield_now();
                    assert!(lease.cell().complete(GENERATION_STEP, BORROWED));
                }
            })
        };
        lease.close();
        borrower.join().unwrap();
        assert!(lease.cell().completed(GENERATION_STEP));
    });
}

#[test]
fn acquire_borrow_observes_every_coordinate_and_payload_write() {
    model(|| {
        let lease = Arc::new(Lease::new());
        let data = Arc::new(loom::cell::UnsafeCell::new(0));
        let publisher = {
            let lease = lease.clone();
            let data = data.clone();
            thread::spawn(move || {
                // SAFETY: the producer owns this cell before publication.
                data.with_mut(|ptr| unsafe { *ptr = 7 });
                lease.publish(GENERATION_STEP, [1, 2, 3, 4, 5]);
            })
        };
        if lease.cell().borrow(GENERATION_STEP) {
            assert!(lease.matches([1, 2, 3, 4, 5]));
            // SAFETY: successful borrow acquired the producer's publication.
            data.with(|ptr| assert_eq!(unsafe { *ptr }, 7));
            lease.cell().complete(GENERATION_STEP, BORROWED);
        }
        publisher.join().unwrap();
    });
}

#[test]
fn release_completion_prevents_reuse_until_readers_finish() {
    model(|| {
        let lease = Arc::new(Lease::new());
        let data = Arc::new(loom::cell::UnsafeCell::new(7));
        lease.publish(GENERATION_STEP, [1; COORDINATES]);
        assert!(lease.cell().borrow(GENERATION_STEP));
        let reader = {
            let lease = lease.clone();
            let data = data.clone();
            thread::spawn(move || {
                // SAFETY: this reader holds the borrowed lease.
                data.with(|ptr| assert_eq!(unsafe { *ptr }, 7));
                lease.cell().complete(GENERATION_STEP, BORROWED);
            })
        };
        if lease.cell().completed(GENERATION_STEP) {
            // SAFETY: completion acquired the final reader release.
            data.with_mut(|ptr| unsafe { *ptr = 9 });
            lease.publish(2 * GENERATION_STEP, [2; COORDINATES]);
        }
        reader.join().unwrap();
    });
}

#[test]
fn only_one_receiver_can_borrow_or_complete_a_generation() {
    model(|| {
        let lease = Arc::new(Lease::new());
        let winners = Arc::new(AtomicUsize::new(0));
        lease.publish(GENERATION_STEP, [1; COORDINATES]);
        let borrower = |lease: Arc<Lease>, winners: Arc<AtomicUsize>| {
            if lease.cell().borrow(GENERATION_STEP) {
                winners.fetch_add(1, Ordering::Relaxed);
                assert!(lease.cell().complete(GENERATION_STEP, BORROWED));
                assert!(!lease.cell().complete(GENERATION_STEP, BORROWED));
            }
        };
        let other = {
            let lease = lease.clone();
            let winners = winners.clone();
            thread::spawn(move || borrower(lease, winners))
        };
        borrower(lease.clone(), winners.clone());
        other.join().unwrap();
        assert_eq!(winners.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn stale_completion_cannot_release_a_reused_cell() {
    model(|| {
        let lease = Arc::new(Lease::new());
        lease.publish(2 * GENERATION_STEP, [2; COORDINATES]);
        let stale = {
            let lease = lease.clone();
            thread::spawn(move || {
                assert!(!lease.cell().borrow(GENERATION_STEP));
                assert!(!lease.cell().complete(GENERATION_STEP, PENDING));
                assert!(!lease.cell().complete(GENERATION_STEP, BORROWED));
            })
        };
        assert!(lease.cell().borrow(2 * GENERATION_STEP));
        stale.join().unwrap();
        assert!(!lease.cell().completed(2 * GENERATION_STEP));
        assert!(lease.matches([2; COORDINATES]));
    });
}

struct Ring {
    write: AtomicU64,
    read: AtomicU64,
    imported: AtomicU64,
    reader_parked: AtomicU32,
    writer_parked: AtomicU32,
    capacity: usize,
}
impl Ring {
    fn new(capacity: usize, cursor: u64) -> Self {
        Self {
            write: AtomicU64::new(cursor),
            read: AtomicU64::new(cursor),
            imported: AtomicU64::new(0),
            reader_parked: AtomicU32::new(0),
            writer_parked: AtomicU32::new(0),
            capacity,
        }
    }
    fn state(&self) -> CarrierState<'_> {
        CarrierState {
            write: &self.write,
            read: &self.read,
            imported: &self.imported,
            reader_parked: &self.reader_parked,
            writer_parked: &self.writer_parked,
            capacity: self.capacity,
        }
    }
}

#[test]
fn reader_arm_racing_publication_has_data_or_a_wake() {
    model(|| {
        let ring = Arc::new(Ring::new(1, 0));
        let writer = {
            let ring = ring.clone();
            thread::spawn(move || ring.state().publish(1))
        };
        let ready = ring.state().arm_reader(false).unwrap();
        let notified = writer.join().unwrap();
        assert!(
            ready || notified,
            "reader parked across publication without a wake"
        );
    });
}

#[test]
fn writer_arm_racing_returned_space_has_space_or_a_wake() {
    model(|| {
        let ring = Arc::new(Ring::new(1, 0));
        ring.write.store(1, Ordering::Relaxed);
        let reader = {
            let ring = ring.clone();
            thread::spawn(move || ring.state().return_bytes(1))
        };
        ring.state().park_writer();
        let ready = ring.state().write_window().unwrap().length != 0;
        let notified = reader.join().unwrap();
        assert!(
            ready || notified,
            "writer parked across returned space without a wake"
        );
    });
}

#[test]
fn writer_arm_racing_descriptor_ack_has_acknowledgment_or_a_wake() {
    model(|| {
        let ring = Arc::new(Ring::new(1, 0));
        let receiver = {
            let ring = ring.clone();
            thread::spawn(move || ring.state().acknowledge_imports(1))
        };
        ring.state().park_writer();
        let ready = ring.state().imported() == 1;
        let notified = receiver.join().unwrap();
        assert!(ready || notified);
    });
}

#[test]
fn carrier_acquire_publication_and_return_protect_wrapping_payload_bytes() {
    for cursor in [0, u64::MAX] {
        model(move || {
            let ring = Arc::new(Ring::new(1, cursor));
            let data = Arc::new(loom::cell::UnsafeCell::new(0));
            let writer = {
                let ring = ring.clone();
                let data = data.clone();
                thread::spawn(move || {
                    for value in 1..=2 {
                        let window = ring.state().write_window().unwrap();
                        if window.length != 0 {
                            // SAFETY: the acquired READ cursor returned this slot.
                            data.with_mut(|ptr| unsafe { *ptr = value });
                            ring.state().publish(window.cursor.wrapping_add(1));
                        }
                    }
                })
            };
            let mut last = 0;
            for _ in 0..2 {
                let window = ring.state().read_window().unwrap();
                if window.length != 0 {
                    // SAFETY: acquired WRITE owns this byte until return.
                    data.with(|ptr| {
                        let value = unsafe { *ptr };
                        assert!(value > last);
                        last = value;
                    });
                    ring.state().return_bytes(window.cursor.wrapping_add(1));
                }
            }
            writer.join().unwrap();
        });
    }
}

#[test]
fn polling_and_notification_coalescing_preserve_readiness() {
    model(|| {
        let ring = Ring::new(2, 0);
        assert!(!ring.state().arm_reader(true).unwrap());
        assert!(!ring.state().publish(1));
        assert!(ring.state().arm_reader(false).unwrap());
        assert!(ring.state().publish(2));
        assert!(!ring.state().publish(2));
        ring.state().park_writer();
        assert!(ring.state().return_bytes(1));
        assert!(!ring.state().return_bytes(2));
    });
}

#[test]
fn invalid_carrier_cursors_do_not_grant_storage() {
    for (read, write) in [(0, 3), (2, 0)] {
        model(move || {
            let ring = Ring::new(2, 0);
            ring.read.store(read, Ordering::Relaxed);
            ring.write.store(write, Ordering::Relaxed);
            assert!(ring.state().read_window().is_none());
            assert!(ring.state().write_window().is_none());
        });
    }
}

struct Credit {
    sequence: AtomicU32,
    parked: AtomicU32,
    stopped: AtomicBool,
}
impl Credit {
    fn new(sequence: u32) -> Self {
        Self {
            sequence: AtomicU32::new(sequence),
            parked: AtomicU32::new(0),
            stopped: AtomicBool::new(false),
        }
    }
    fn signal(&self) -> CreditSignal<'_> {
        CreditSignal {
            sequence: &self.sequence,
            parked: &self.parked,
            stopped: &self.stopped,
        }
    }

    fn notify(&self, sleeping: &Mutex<bool>) {
        if self.signal().notify() {
            *sleeping.lock().unwrap() = false;
        }
    }

    fn wait(&self, sequence: u32, sleeping: &Mutex<bool>) {
        if self.signal().arm(sequence) {
            // The kernel serializes futex compare-and-park with wake.
            let mut sleeping = sleeping.lock().unwrap();
            if self.sequence.load(Ordering::Acquire) == sequence {
                *sleeping = true;
            }
        }
    }
}

#[test]
fn completion_before_or_after_arming_cannot_leave_credit_asleep() {
    for sequence in [0, u32::MAX] {
        model(move || {
            let credit = Arc::new(Credit::new(sequence));
            let sleeping = Arc::new(Mutex::new(false));
            let completer = {
                let credit = credit.clone();
                let sleeping = sleeping.clone();
                thread::spawn(move || credit.notify(&sleeping))
            };
            credit.wait(sequence, &sleeping);
            completer.join().unwrap();
            assert!(
                !*sleeping.lock().unwrap(),
                "credit completion missed a parked futex"
            );
        });
    }
}

#[test]
fn shutdown_racing_credit_arm_cannot_park_without_progress() {
    model(|| {
        let credit = Arc::new(Credit::new(0));
        let sleeping = Arc::new(Mutex::new(false));
        let stopper = {
            let credit = credit.clone();
            let sleeping = sleeping.clone();
            thread::spawn(move || {
                credit.stopped.store(true, Ordering::Release);
                credit.notify(&sleeping);
            })
        };
        credit.wait(0, &sleeping);
        stopper.join().unwrap();
        assert!(!*sleeping.lock().unwrap(), "shutdown missed a parked futex");
        assert!(!credit.signal().arm(credit.sequence.load(Ordering::Acquire)));
    });
}

#[test]
fn concurrent_final_view_releases_recycle_exactly_once() {
    model(|| {
        let owner = Arc::new(AtomicUsize::new(0));
        let second = owner.clone();
        let gate = Arc::new(Mutex::new(()));
        let recycled = Arc::new(AtomicUsize::new(0));
        let other = {
            let gate = gate.clone();
            let recycled = recycled.clone();
            thread::spawn(move || {
                if let Some(owner) = coordination::unique_release(second, &gate) {
                    assert_eq!(Arc::strong_count(&owner), 1);
                    recycled.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        if let Some(owner) = coordination::unique_release(owner, &gate) {
            assert_eq!(Arc::strong_count(&owner), 1);
            recycled.fetch_add(1, Ordering::Relaxed);
        }
        other.join().unwrap();
        assert_eq!(recycled.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn simultaneous_memory_reservations_share_and_return_one_limit() {
    model(|| {
        let budget = memory::Budget::new(Some(1));
        let other = {
            let budget = budget.clone();
            thread::spawn(move || {
                let reservation = memory::Budget::reserve(&budget, 1);
                assert!(budget.used() <= 1);
                drop(reservation);
            })
        };
        let reservation = memory::Budget::reserve(&budget, 1);
        assert!(budget.used() <= 1);
        drop(reservation);
        other.join().unwrap();
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.remaining(), 1);
    });
}

#[test]
fn memory_accounting_rejects_overflow_zero_and_over_limit() {
    model(|| {
        assert!(memory::Budget::reserve(&memory::Budget::new(Some(0)), 1).is_none());
        let budget = memory::Budget::new(None);
        let held = memory::Budget::reserve(&budget, usize::MAX).unwrap();
        assert!(memory::Budget::reserve(&budget, 1).is_none());
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(memory::Budget::reserve(&memory::Budget::new(Some(1)), 2).is_none());
    });
}

#[test]
fn every_fanout_recipient_must_finish_before_payload_reuse() {
    model(|| {
        let leases = Arc::new([Lease::new(), Lease::new()]);
        let data = Arc::new(loom::cell::UnsafeCell::new(7));
        for lease in leases.iter() {
            lease.publish(GENERATION_STEP, [1; COORDINATES]);
            assert!(lease.cell().borrow(GENERATION_STEP));
        }
        let readers: Vec<_> = (0..2)
            .map(|index| {
                let leases = leases.clone();
                let data = data.clone();
                thread::spawn(move || {
                    // SAFETY: each reader owns its own outstanding recipient lease.
                    data.with(|ptr| assert_eq!(unsafe { *ptr }, 7));
                    leases[index].cell().complete(GENERATION_STEP, BORROWED);
                })
            })
            .collect();
        if leases
            .iter()
            .all(|lease| lease.cell().completed(GENERATION_STEP))
        {
            // SAFETY: every reader completed with a release acquired above.
            data.with_mut(|ptr| unsafe { *ptr = 9 });
        }
        for reader in readers {
            reader.join().unwrap();
        }
    });
}

#[test]
fn three_final_view_owners_recycle_once_outside_the_release_gate() {
    model(|| {
        let gate = Arc::new(Mutex::new(()));
        let recycled = Arc::new(AtomicUsize::new(0));
        let first = Arc::new(0);
        let second = first.clone();
        let third = first.clone();
        let release = |owner, gate: Arc<Mutex<()>>, recycled: Arc<AtomicUsize>| {
            if coordination::unique_release(owner, &gate).is_some() {
                assert!(
                    gate.try_lock().is_ok(),
                    "nested owner release would deadlock"
                );
                recycled.fetch_add(1, Ordering::Relaxed);
            }
        };
        let threads: Vec<_> = [second, third]
            .into_iter()
            .map(|owner| {
                let gate = gate.clone();
                let recycled = recycled.clone();
                thread::spawn(move || release(owner, gate, recycled))
            })
            .collect();
        release(first, gate, recycled.clone());
        for other in threads {
            other.join().unwrap();
        }
        assert_eq!(recycled.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn simultaneous_publication_borrow_and_close_never_pin_unread_storage() {
    model(|| {
        let lease = Arc::new(Lease::new());
        let data = Arc::new(loom::cell::UnsafeCell::new(0));
        let publisher = {
            let lease = lease.clone();
            let data = data.clone();
            thread::spawn(move || {
                // SAFETY: exclusive producer access before publication.
                data.with_mut(|ptr| unsafe { *ptr = 7 });
                lease.publish(GENERATION_STEP, [1; COORDINATES]);
            })
        };
        let borrower = {
            let lease = lease.clone();
            let data = data.clone();
            thread::spawn(move || {
                if lease.cell().borrow(GENERATION_STEP) {
                    assert!(lease.matches([1; COORDINATES]));
                    // SAFETY: publication was acquired and this lease remains borrowed.
                    data.with(|ptr| assert_eq!(unsafe { *ptr }, 7));
                    assert!(lease.cell().complete(GENERATION_STEP, BORROWED));
                }
            })
        };
        lease.close();
        publisher.join().unwrap();
        borrower.join().unwrap();
        assert!(lease.cell().completed(GENERATION_STEP));
        assert_eq!(lease.state.load(Ordering::Acquire) & STATE_BITS, DONE);
    });
}
