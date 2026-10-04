#![cfg(target_pointer_width = "64")]

use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering, fence};
use loom::sync::{Arc, Mutex};
use loom::thread;

#[test]
fn source_claim_racing_close_cannot_hide_control_work() {
    loom::model(|| {
        const LIVE: u8 = 1;
        const CLAIMED: u8 = 2;
        let status = Arc::new(AtomicU8::new(LIVE));
        let pending = Arc::new(AtomicBool::new(false));
        let closer = {
            let status = status.clone();
            let pending = pending.clone();
            thread::spawn(move || {
                if status.fetch_and(!LIVE, Ordering::AcqRel) & CLAIMED != 0 {
                    pending.swap(true, Ordering::Release);
                }
            })
        };
        if status.fetch_or(CLAIMED, Ordering::AcqRel) & LIVE == 0 {
            pending.swap(true, Ordering::Release);
        }
        closer.join().unwrap();
        assert!(pending.load(Ordering::Acquire));
    });
}

#[test]
fn source_receipt_release_racing_control_scan_is_resumed_or_pending() {
    loom::model(|| {
        const CLAIMED: u8 = 4;
        let status = Arc::new(AtomicU8::new(CLAIMED));
        let pending = Arc::new(AtomicBool::new(false));
        let releaser = {
            let status = status.clone();
            let pending = pending.clone();
            thread::spawn(move || {
                status.fetch_and(!CLAIMED, Ordering::AcqRel);
                pending.swap(true, Ordering::Release);
            })
        };
        let resumed =
            pending.swap(false, Ordering::AcqRel) && status.load(Ordering::Acquire) & CLAIMED == 0;
        releaser.join().unwrap();
        assert!(resumed || pending.load(Ordering::Acquire));
    });
}

#[derive(Debug, Default)]
struct StateSignalState {
    generation: u64,
    waiters: usize,
    woken: bool,
}

#[derive(Debug)]
struct ModelStateSignal {
    state: Mutex<StateSignalState>,
}

impl ModelStateSignal {
    fn new() -> Self {
        Self {
            state: Mutex::new(StateSignalState::default()),
        }
    }

    fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    fn notify_changed(&self) {
        let mut state = self.state.lock().unwrap();
        state.generation = state.generation.wrapping_add(1);
        if state.waiters != 0 {
            state.woken = true;
        }
    }

    fn register_and_check(&self, seen: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.generation != seen {
            return true;
        }
        state.waiters += 1;
        state.generation != seen || state.woken
    }

    fn has_woken_waiter(&self) -> bool {
        self.state.lock().unwrap().woken
    }
}

#[derive(Debug, Default)]
struct DataSignalState {
    state: u8,
}

#[derive(Debug)]
struct ModelDataSignal {
    state: Mutex<DataSignalState>,
}

impl ModelDataSignal {
    fn new() -> Self {
        Self {
            state: Mutex::new(DataSignalState::default()),
        }
    }

    fn mark(&self) {
        let mut state = self.state.lock().unwrap();
        state.state = match state.state {
            0 => 1,
            1 | 3 => state.state,
            2 => 3,
            _ => unreachable!("invalid data signal state"),
        };
    }

    fn begin_drain(&self) {
        let mut state = self.state.lock().unwrap();
        if state.state == 1 {
            state.state = 2;
        }
    }

    fn clear_after(&self, is_empty: bool) {
        let mut state = self.state.lock().unwrap();
        state.state = match (state.state, is_empty) {
            (_, false) | (3, true) => 1,
            (2, true) => 0,
            (0 | 1, true) => state.state,
            _ => unreachable!("invalid data signal state"),
        }
    }

    fn ready(&self) -> bool {
        self.state.lock().unwrap().state != 0
    }
}

/// `DataSignal` with the atomics and orderings of `engine/signal.rs`.
///
/// `ModelDataSignal` above serializes every transition through a mutex, so
/// it cannot show races between the ring tail and the signal state. This
/// model keeps both as independent atomics, as the real code does.
#[derive(Debug)]
struct AtomicDataSignal {
    state: AtomicU8,
    /// `true` models the current code. `false` models the protocol without
    /// the sequentially consistent fences, which can strand an item.
    fenced: bool,
}

impl AtomicDataSignal {
    const IDLE: u8 = 0;
    const PENDING: u8 = 1;
    const DRAINING: u8 = 2;
    const DIRTY: u8 = 3;

    fn new(state: u8, fenced: bool) -> Self {
        Self {
            state: AtomicU8::new(state),
            fenced,
        }
    }

    /// Returns `true` when the mark fired a wake.
    fn mark(&self) -> bool {
        if self.fenced {
            fence(Ordering::SeqCst);
        }
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                Self::IDLE => match self.state.compare_exchange(
                    Self::IDLE,
                    Self::PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(next) => state = next,
                },
                Self::PENDING | Self::DIRTY => return false,
                Self::DRAINING => match self.state.compare_exchange(
                    Self::DRAINING,
                    Self::DIRTY,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return false,
                    Err(next) => state = next,
                },
                _ => unreachable!("invalid data signal state"),
            }
        }
    }

    fn begin_drain(&self) {
        if self.state.load(Ordering::Acquire) == Self::PENDING {
            let _ = self.state.compare_exchange(
                Self::PENDING,
                Self::DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        if self.fenced {
            fence(Ordering::SeqCst);
        }
    }

    fn clear_after(&self, is_empty: bool) {
        if !is_empty {
            self.rearm();
            return;
        }
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                Self::DRAINING => match self.state.compare_exchange(
                    Self::DRAINING,
                    Self::IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(next) => state = next,
                },
                Self::DIRTY => {
                    self.rearm();
                    return;
                }
                Self::PENDING | Self::IDLE => return,
                _ => unreachable!("invalid data signal state"),
            }
        }
    }

    fn rearm(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        while state != Self::PENDING {
            match self.state.compare_exchange(
                state,
                Self::PENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => state = next,
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == Self::IDLE
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelFanoutEntry {
    Plain,
    Dict,
    Compressed(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelLaneEntry {
    CompressionUpdate,
    Dispatch(u8),
}

#[derive(Debug)]
struct ModelLaneDistributorState {
    entries: Vec<ModelLaneEntry>,
    pending_compression: bool,
}

#[derive(Debug)]
struct ModelLaneDistributor {
    state: Mutex<ModelLaneDistributorState>,
    cap: usize,
}

impl ModelLaneDistributor {
    fn new(cap: usize) -> Self {
        Self {
            state: Mutex::new(ModelLaneDistributorState {
                entries: Vec::new(),
                pending_compression: false,
            }),
            cap,
        }
    }

    fn set_compression(&self) {
        self.state.lock().unwrap().pending_compression = true;
    }

    fn try_dispatch(&self, id: u8) -> bool {
        let mut state = self.state.lock().unwrap();
        if !self.try_push_pending_compression(&mut state) {
            return false;
        }
        if state.entries.len() >= self.cap {
            return false;
        }
        state.entries.push(ModelLaneEntry::Dispatch(id));
        true
    }

    fn drain(&self) -> Vec<ModelLaneEntry> {
        std::mem::take(&mut self.state.lock().unwrap().entries)
    }

    fn try_push_pending_compression(&self, state: &mut ModelLaneDistributorState) -> bool {
        if !state.pending_compression {
            return true;
        }
        if state.entries.len() >= self.cap {
            return false;
        }
        state.entries.push(ModelLaneEntry::CompressionUpdate);
        state.pending_compression = false;
        true
    }
}

#[derive(Debug)]
struct ModelFanoutSlot {
    entries: Mutex<Vec<ModelFanoutEntry>>,
    msg_cap: usize,
    dict_queued: AtomicBool,
    dict_shipped: AtomicBool,
}

impl ModelFanoutSlot {
    fn new(msg_cap: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            msg_cap,
            dict_queued: AtomicBool::new(false),
            dict_shipped: AtomicBool::new(false),
        }
    }

    fn push_plain(&self) {
        self.push_unprotected(ModelFanoutEntry::Plain);
    }

    fn push_dict(&self) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if !Self::make_room(&mut entries, self.msg_cap) {
            return false;
        }
        entries.push(ModelFanoutEntry::Dict);
        self.dict_queued.store(true, Ordering::Release);
        true
    }

    fn push_compressed(&self, id: u8) -> bool {
        if !self.dict_ready() {
            return false;
        }
        self.push_unprotected(ModelFanoutEntry::Compressed(id))
    }

    fn drain(&self) -> Vec<ModelFanoutEntry> {
        let mut entries = self.entries.lock().unwrap();
        let drained = std::mem::take(&mut *entries);
        if drained
            .iter()
            .any(|entry| matches!(entry, ModelFanoutEntry::Dict))
        {
            self.dict_queued.store(false, Ordering::Release);
            self.dict_shipped.store(true, Ordering::Release);
        }
        drained
    }

    fn snapshot(&self) -> Vec<ModelFanoutEntry> {
        self.entries.lock().unwrap().clone()
    }

    fn dict_ready(&self) -> bool {
        self.dict_queued.load(Ordering::Acquire) || self.dict_shipped.load(Ordering::Acquire)
    }

    fn push_unprotected(&self, entry: ModelFanoutEntry) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if !Self::make_room(&mut entries, self.msg_cap) {
            return false;
        }
        entries.push(entry);
        true
    }

    fn make_room(entries: &mut Vec<ModelFanoutEntry>, msg_cap: usize) -> bool {
        while entries.len() >= msg_cap {
            let Some(pos) = entries
                .iter()
                .position(|entry| !matches!(entry, ModelFanoutEntry::Dict))
            else {
                return false;
            };
            entries.remove(pos);
        }
        true
    }
}

fn assert_compressed_payloads_follow_dict(entries: &[ModelFanoutEntry]) {
    let mut saw_dict = false;
    for entry in entries {
        match entry {
            ModelFanoutEntry::Dict => saw_dict = true,
            ModelFanoutEntry::Compressed(_) => {
                assert!(
                    saw_dict,
                    "compressed fan-out payload must not overtake or orphan dict: {entries:?}"
                );
            }
            ModelFanoutEntry::Plain => {}
        }
    }
}

#[derive(Debug)]
struct ModelBlockingRecvWaker {
    active: AtomicUsize,
    armed: AtomicBool,
    thread: Mutex<bool>,
    unparked: AtomicBool,
}

impl ModelBlockingRecvWaker {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
            thread: Mutex::new(false),
            unparked: AtomicBool::new(false),
        }
    }

    fn register(&self) {
        let mut thread = self.thread.lock().unwrap();
        *thread = true;
        self.armed.store(true, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::SeqCst);
    }

    fn prepare_sleep(&self) {
        self.armed.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }

    fn cancel_sleep(&self) {
        self.armed.store(false, Ordering::Release);
    }

    fn wake(&self) {
        fence(Ordering::SeqCst);
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        if *self.thread.lock().unwrap() && self.armed.swap(false, Ordering::AcqRel) {
            self.unparked.store(true, Ordering::Release);
        }
    }

    fn was_unparked(&self) -> bool {
        self.unparked.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct ModelBlockingRecvCancel {
    canceled: AtomicBool,
    registered: AtomicBool,
    thread_present: Mutex<bool>,
    unparked: AtomicBool,
}

impl ModelBlockingRecvCancel {
    fn new() -> Self {
        Self {
            canceled: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            thread_present: Mutex::new(false),
            unparked: AtomicBool::new(false),
        }
    }

    fn register_current_thread_once(&self) {
        if self
            .registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        *self.thread_present.lock().unwrap() = true;
        if self.canceled.load(Ordering::Acquire) {
            self.unparked.store(true, Ordering::Release);
        }
    }

    fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
        if *self.thread_present.lock().unwrap() {
            self.unparked.store(true, Ordering::Release);
        }
    }

    fn was_unparked(&self) -> bool {
        self.unparked.load(Ordering::Acquire)
    }
}

#[test]
fn state_signal_catches_change_between_check_and_wait_registration() {
    loom::model(|| {
        let signal = Arc::new(ModelStateSignal::new());
        let full = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));

        let waiter_signal = signal.clone();
        let waiter_full = full.clone();
        let waiter_observed = observed.clone();
        let waiter = thread::spawn(move || {
            let seen = waiter_signal.generation();
            if !waiter_full.load(Ordering::SeqCst) {
                waiter_observed.store(true, Ordering::SeqCst);
                return;
            }

            thread::yield_now();

            if waiter_signal.register_and_check(seen) || !waiter_full.load(Ordering::SeqCst) {
                waiter_observed.store(true, Ordering::SeqCst);
            }
        });

        let releaser_signal = signal.clone();
        let releaser_full = full.clone();
        let releaser = thread::spawn(move || {
            releaser_full.store(false, Ordering::SeqCst);
            releaser_signal.notify_changed();
        });

        waiter.join().unwrap();
        releaser.join().unwrap();

        assert!(
            observed.load(Ordering::SeqCst) || signal.has_woken_waiter(),
            "generation change must be observed or wake a registered waiter"
        );
    });
}

/// Two send futures share one PEER producer. The first has already observed
/// a full ring and registered the bridge waker. The second can observe the
/// consumer's released slot before fanring delivers the capacity wake.
fn peer_shared_sender_capacity_handoff_model(relay_ready: bool, parked: bool) {
    let full = Arc::new(AtomicBool::new(true));
    let waiting = Arc::new(AtomicBool::new(true));
    let registered = Arc::new(Mutex::new(true));
    let signal = Arc::new(ModelStateSignal::new());
    let seen = signal.generation();
    if parked {
        assert!(!signal.register_and_check(seen));
    }
    let consumer = {
        let full = full.clone();
        let waiting = waiting.clone();
        let registered = registered.clone();
        let signal = signal.clone();
        thread::spawn(move || {
            // fanring publishes credits before notifying its single waiter.
            full.store(false, Ordering::Release);
            let wake = std::mem::replace(&mut *registered.lock().unwrap(), false);
            if wake {
                // peer_send::SpaceWake::wake
                waiting.store(false, Ordering::Release);
                signal.notify_changed();
            }
        })
    };
    let sender = {
        let waiting = waiting.clone();
        let registered = registered.clone();
        let signal = signal.clone();
        thread::spawn(move || {
            // peer_send::Producer::try_send, serialized with the first
            // caller by the lane mutex. Both calls use the same bridge waker.
            let was_waiting = waiting.load(Ordering::Acquire);
            waiting.store(true, Ordering::Release);
            if full.load(Ordering::Acquire) {
                *registered.lock().unwrap() = true;
                if full.load(Ordering::Acquire) {
                    return;
                }
            }
            // fanring::Sender::poll_ready cancels its registration on Ready.
            *registered.lock().unwrap() = false;
            waiting.store(false, Ordering::Release);
            if relay_ready && was_waiting {
                signal.notify_changed();
            }
        })
    };

    consumer.join().unwrap();
    sender.join().unwrap();
    // Cover a caller already parked and one not yet polling changed_after.
    // Registration racing with notify is covered independently above.
    let observed = !parked && signal.register_and_check(seen);
    assert!(
        observed || signal.has_woken_waiter(),
        "shared sender parked after another caller canceled its capacity wake"
    );
}

#[test]
fn peer_shared_sender_capacity_handoff_wakes_existing_waiters() {
    for parked in [false, true] {
        loom::model(move || peer_shared_sender_capacity_handoff_model(true, parked));
    }
}

#[test]
#[should_panic(expected = "shared sender parked")]
fn peer_shared_sender_without_capacity_handoff_can_lose_wake() {
    loom::model(|| peer_shared_sender_capacity_handoff_model(false, true));
}

#[test]
fn blocking_recv_cancel_registration_cannot_lose_cancel_wake() {
    loom::model(|| {
        let cancel = Arc::new(ModelBlockingRecvCancel::new());

        let register_cancel = cancel.clone();
        let register = thread::spawn(move || {
            thread::yield_now();
            register_cancel.register_current_thread_once();
        });

        let fire_cancel = cancel.clone();
        let fire = thread::spawn(move || {
            thread::yield_now();
            fire_cancel.cancel();
        });

        register.join().unwrap();
        fire.join().unwrap();

        assert!(
            cancel.was_unparked(),
            "cancel racing with thread registration must leave an unpark token"
        );
    });
}

#[test]
fn blocking_recv_lazy_registration_and_rearm_cannot_lose_publication() {
    for rearm in [false, true] {
        loom::model(move || {
            let waker = Arc::new(ModelBlockingRecvWaker::new());
            if rearm {
                waker.register();
                waker.cancel_sleep();
            }
            let message = Arc::new(AtomicBool::new(false));
            let receiver = {
                let waker = waker.clone();
                let message = message.clone();
                thread::spawn(move || {
                    if !rearm {
                        waker.register();
                    }
                    waker.prepare_sleep();
                    // Exactly one queue recheck before parking, with no final
                    // load that could hide a missing publication wake.
                    message.load(Ordering::Acquire)
                })
            };
            let sender = {
                let waker = waker.clone();
                thread::spawn(move || {
                    message.store(true, Ordering::Release);
                    waker.wake();
                })
            };
            let observed = receiver.join().unwrap();
            sender.join().unwrap();
            assert!(
                observed || waker.was_unparked(),
                "registered receiver lost publication"
            );
        });
    }
}

#[test]
fn blocking_recv_waker_does_not_lose_wake_around_sleep_prepare() {
    loom::model(|| {
        let waker = Arc::new(ModelBlockingRecvWaker::new());
        let has_message = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));
        let parked = Arc::new(AtomicBool::new(false));
        let lost = Arc::new(AtomicBool::new(false));

        let recv_waker = waker.clone();
        let recv_has_message = has_message.clone();
        let recv_observed = observed.clone();
        let recv_parked = parked.clone();
        let recv_lost = lost.clone();
        let receiver = thread::spawn(move || {
            recv_waker.register();
            if recv_has_message.load(Ordering::Acquire) {
                recv_observed.store(true, Ordering::Release);
                return;
            }

            thread::yield_now();
            recv_waker.prepare_sleep();
            thread::yield_now();

            if recv_has_message.load(Ordering::Acquire) {
                recv_waker.cancel_sleep();
                recv_observed.store(true, Ordering::Release);
                return;
            }

            thread::yield_now();
            if recv_has_message.load(Ordering::Acquire) {
                recv_waker.cancel_sleep();
                recv_observed.store(true, Ordering::Release);
                return;
            }

            recv_parked.store(true, Ordering::Release);
            thread::yield_now();
            if recv_has_message.load(Ordering::Acquire) && !recv_waker.was_unparked() {
                recv_lost.store(true, Ordering::Release);
            }
        });

        let send_waker = waker.clone();
        let send_has_message = has_message.clone();
        let sender = thread::spawn(move || {
            thread::yield_now();
            send_has_message.store(true, Ordering::Release);
            send_waker.wake();
        });

        receiver.join().unwrap();
        sender.join().unwrap();

        assert!(
            !lost.load(Ordering::Acquire),
            "message became available while receiver could park without an unpark token"
        );
        assert!(
            observed.load(Ordering::Acquire)
                || !parked.load(Ordering::Acquire)
                || waker.was_unparked(),
            "receiver must observe message or get an unpark token"
        );
    });
}

#[test]
fn data_signal_rearm_catches_push_between_clear_and_next_wait() {
    loom::model(|| {
        let signal = Arc::new(ModelDataSignal::new());
        let empty = Arc::new(AtomicBool::new(true));

        let consumer_signal = signal.clone();
        let consumer_empty = empty.clone();
        let consumer = thread::spawn(move || {
            consumer_signal.begin_drain();
            thread::yield_now();
            consumer_signal.clear_after(consumer_empty.load(Ordering::SeqCst));
        });

        let producer_signal = signal.clone();
        let producer_empty = empty.clone();
        let producer = thread::spawn(move || {
            producer_empty.store(false, Ordering::SeqCst);
            producer_signal.mark();
        });

        consumer.join().unwrap();
        producer.join().unwrap();

        assert!(
            signal.ready(),
            "data signal must stay ready when producer races with drain clear"
        );
    });
}

#[test]
fn fanout_dict_entry_stays_before_compressed_payloads_under_hwm() {
    loom::model(|| {
        let slot = Arc::new(ModelFanoutSlot::new(2));
        let wire = Arc::new(Mutex::new(Vec::new()));

        slot.push_plain();

        let sender_slot = slot.clone();
        let sender = thread::spawn(move || {
            assert!(sender_slot.push_dict(), "dict must fit by evicting plain");
            thread::yield_now();
            let _ = sender_slot.push_compressed(1);
            thread::yield_now();
            let _ = sender_slot.push_compressed(2);
        });

        let drain_slot = slot.clone();
        let drain_wire = wire.clone();
        let drainer = thread::spawn(move || {
            for _ in 0..3 {
                thread::yield_now();
                let drained = drain_slot.drain();
                thread::yield_now();
                drain_wire.lock().unwrap().extend(drained);
            }
        });

        sender.join().unwrap();
        drainer.join().unwrap();

        let mut observed = wire.lock().unwrap().clone();
        observed.extend(slot.snapshot());
        assert_compressed_payloads_follow_dict(&observed);
    });
}

#[test]
fn fanout_pending_compression_update_precedes_accepted_try_send_dispatches() {
    loom::model(|| {
        let distributor = Arc::new(ModelLaneDistributor::new(1));

        distributor.set_compression();

        let sender_dist = distributor.clone();
        let sender = thread::spawn(move || {
            let mut accepted = Vec::new();
            if sender_dist.try_dispatch(1) {
                accepted.push(1);
            }
            thread::yield_now();
            if sender_dist.try_dispatch(2) {
                accepted.push(2);
            }
            accepted
        });

        let drain_dist = distributor.clone();
        let drainer = thread::spawn(move || {
            let mut observed = Vec::new();
            thread::yield_now();
            observed.extend(drain_dist.drain());
            thread::yield_now();
            observed.extend(drain_dist.drain());
            observed
        });

        let accepted = sender.join().unwrap();
        let mut observed = drainer.join().unwrap();

        observed.extend(distributor.drain());
        let update_pos = observed
            .iter()
            .position(|entry| *entry == ModelLaneEntry::CompressionUpdate);

        for id in accepted {
            let dispatch_pos = observed
                .iter()
                .position(|entry| *entry == ModelLaneEntry::Dispatch(id))
                .unwrap_or_else(|| panic!("accepted dispatch {id} missing from {observed:?}"));
            assert!(
                update_pos.is_some_and(|pos| pos < dispatch_pos),
                "accepted dispatch {id} must follow compression update: {observed:?}"
            );
        }
    });
}

#[test]
fn space_signal_catches_release_or_drop_after_full_retry() {
    loom::model(|| {
        let signal = Arc::new(ModelStateSignal::new());
        let full = Arc::new(AtomicBool::new(true));
        let alive = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(AtomicBool::new(false));

        let sender_signal = signal.clone();
        let sender_full = full.clone();
        let sender_alive = alive.clone();
        let sender_observed = observed.clone();
        let sender = thread::spawn(move || {
            if !sender_full.load(Ordering::SeqCst) || !sender_alive.load(Ordering::SeqCst) {
                sender_observed.store(true, Ordering::SeqCst);
                return;
            }
            let seen = sender_signal.generation();
            thread::yield_now();
            if sender_signal.register_and_check(seen)
                || !sender_full.load(Ordering::SeqCst)
                || !sender_alive.load(Ordering::SeqCst)
            {
                sender_observed.store(true, Ordering::SeqCst);
            }
        });

        let releaser_signal = signal.clone();
        let releaser_full = full.clone();
        let releaser_alive = alive.clone();
        let releaser = thread::spawn(move || {
            releaser_full.store(false, Ordering::SeqCst);
            releaser_signal.notify_changed();
            thread::yield_now();
            releaser_alive.store(false, Ordering::SeqCst);
            releaser_signal.notify_changed();
        });

        sender.join().unwrap();
        releaser.join().unwrap();

        assert!(
            observed.load(Ordering::SeqCst) || signal.has_woken_waiter(),
            "space wait must observe either capacity release or pipe teardown"
        );
    });
}

#[test]
fn pipe_wait_tracks_space_and_route_activation() {
    loom::model(|| {
        let pipe_space = Arc::new(ModelStateSignal::new());
        let route_changed = Arc::new(ModelStateSignal::new());
        let pipe_full = Arc::new(AtomicBool::new(true));
        let route_available = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));

        let sender_pipe_space = pipe_space.clone();
        let sender_route_changed = route_changed.clone();
        let sender_pipe_full = pipe_full.clone();
        let sender_route_available = route_available.clone();
        let sender_observed = observed.clone();
        let sender = thread::spawn(move || {
            let pipe_seen = sender_pipe_space.generation();
            let route_seen = sender_route_changed.generation();
            thread::yield_now();
            if sender_pipe_space.register_and_check(pipe_seen)
                || sender_route_changed.register_and_check(route_seen)
                || !sender_pipe_full.load(Ordering::SeqCst)
                || sender_route_available.load(Ordering::SeqCst)
            {
                sender_observed.store(true, Ordering::SeqCst);
            }
        });

        let releaser_pipe_space = pipe_space.clone();
        let releaser_route_changed = route_changed.clone();
        let releaser_pipe_full = pipe_full.clone();
        let releaser_route_available = route_available.clone();
        let releaser = thread::spawn(move || {
            releaser_pipe_full.store(false, Ordering::SeqCst);
            releaser_pipe_space.notify_changed();
            thread::yield_now();
            releaser_route_available.store(true, Ordering::SeqCst);
            releaser_route_changed.notify_changed();
        });

        sender.join().unwrap();
        releaser.join().unwrap();

        assert!(
            observed.load(Ordering::SeqCst)
                || pipe_space.has_woken_waiter()
                || route_changed.has_woken_waiter(),
            "pipe wait must wake on either pipe space or route activation"
        );
    });
}

/// A producer that publishes with a release store and then finds the
/// signal already pending skips its wake. The consumer must still see that
/// item before it parks, or the item waits for the next send.
#[test]
fn data_signal_skipped_mark_cannot_strand_published_item() {
    loom::model(|| data_signal_skipped_mark_model(true));
}

/// Without the fences the same handoff loses the item. This keeps the
/// model above honest: it must be able to observe the race it rules out.
#[test]
#[should_panic(expected = "consumer parked without a wake")]
fn data_signal_without_fences_can_strand_published_item() {
    loom::model(|| data_signal_skipped_mark_model(false));
}

fn data_signal_skipped_mark_model(fenced: bool) {
    {
        // One item is published and marked. The consumer has not drained.
        let tail = Arc::new(AtomicUsize::new(1));
        let signal = Arc::new(AtomicDataSignal::new(AtomicDataSignal::PENDING, fenced));

        let producer = {
            let tail = tail.clone();
            let signal = signal.clone();
            thread::spawn(move || {
                // yring flush, then DataSignal::mark.
                tail.store(2, Ordering::Release);
                signal.mark();
            })
        };

        // Lane worker: drain passes until the signal lets it park.
        let mut consumed = 0;
        for _ in 0..3 {
            signal.begin_drain();
            consumed = tail.load(Ordering::Acquire);
            let is_empty = tail.load(Ordering::Acquire) == consumed;
            signal.clear_after(is_empty);
            if signal.is_idle() {
                break;
            }
        }
        let parked = signal.is_idle();

        producer.join().unwrap();
        // A parked consumer with no pending wake must have seen every item.
        assert!(
            !(parked && signal.is_idle() && consumed < 2),
            "item 2 is published but the consumer parked without a wake"
        );
    }
}

// Compile the production transitions against Loom's admission mutex.
#[path = "../src/engine/write_ownership.rs"]
mod write_ownership;

#[test]
fn direct_idle_publication_cannot_miss_queue_admission() {
    loom::model(|| {
        use write_ownership::WriteOwnership;
        let owner = Arc::new(Mutex::new(WriteOwnership::new()));
        let queued = Arc::new(AtomicBool::new(false));
        let producer = {
            let owner = owner.clone();
            let queued = queued.clone();
            thread::spawn(move || {
                let mut owner = owner.lock().unwrap();
                if !owner.is_idle() {
                    assert!(owner.claim_driver());
                    queued.store(true, Ordering::Release);
                }
            })
        };
        owner
            .lock()
            .unwrap()
            .publish_idle(|| !queued.load(Ordering::Acquire));
        producer.join().unwrap();
        assert!(!owner.lock().unwrap().is_idle() || !queued.load(Ordering::Acquire));
    });
}

#[test]
fn pending_write_excludes_direct_send_until_completion_or_close() {
    loom::model(|| {
        use write_ownership::WriteOwnership;
        let owner = Arc::new(Mutex::new(WriteOwnership::new()));
        let pending = Arc::new(AtomicBool::new(true));
        let sender = {
            let owner = owner.clone();
            let pending = pending.clone();
            thread::spawn(move || {
                let mut owner = owner.lock().unwrap();
                if owner.is_idle() {
                    assert!(!pending.load(Ordering::Acquire));
                } else if !owner.is_closed() {
                    owner.claim_driver();
                }
            })
        };
        let closer = {
            let owner = owner.clone();
            thread::spawn(move || owner.lock().unwrap().close())
        };
        // No mutex held while the driver's write future is pending/canceled.
        thread::yield_now();
        pending.store(false, Ordering::Release);
        owner
            .lock()
            .unwrap()
            .publish_idle(|| !pending.load(Ordering::Acquire));
        sender.join().unwrap();
        closer.join().unwrap();
        assert!(owner.lock().unwrap().is_closed());
    });
}
