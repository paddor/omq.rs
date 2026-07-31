#![cfg(target_pointer_width = "64")]

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelFanoutEntry {
    Plain,
    Dict,
    Compressed(u8),
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
    registered: AtomicBool,
    sleeping: AtomicBool,
    unparked: AtomicBool,
}

impl ModelBlockingRecvWaker {
    fn new() -> Self {
        Self {
            registered: AtomicBool::new(false),
            sleeping: AtomicBool::new(false),
            unparked: AtomicBool::new(false),
        }
    }

    fn register(&self) {
        self.registered.store(true, Ordering::Release);
    }

    fn prepare_sleep(&self) {
        self.sleeping.store(true, Ordering::Release);
    }

    fn cancel_sleep(&self) {
        self.sleeping.store(false, Ordering::Release);
    }

    fn wake(&self) {
        if !self.sleeping.load(Ordering::Acquire) {
            return;
        }
        if self
            .sleeping
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self.registered.load(Ordering::Acquire)
        {
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
