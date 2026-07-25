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
fn fallback_wait_tracks_queue_space_and_active_peer_changes() {
    loom::model(|| {
        let queue_space = Arc::new(ModelStateSignal::new());
        let active_changed = Arc::new(ModelStateSignal::new());
        let queue_full = Arc::new(AtomicBool::new(true));
        let active_peer = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));

        let sender_queue_space = queue_space.clone();
        let sender_active_changed = active_changed.clone();
        let sender_queue_full = queue_full.clone();
        let sender_active_peer = active_peer.clone();
        let sender_observed = observed.clone();
        let sender = thread::spawn(move || {
            let queue_seen = sender_queue_space.generation();
            let active_seen = sender_active_changed.generation();
            thread::yield_now();
            if sender_queue_space.register_and_check(queue_seen)
                || sender_active_changed.register_and_check(active_seen)
                || !sender_queue_full.load(Ordering::SeqCst)
                || sender_active_peer.load(Ordering::SeqCst)
            {
                sender_observed.store(true, Ordering::SeqCst);
            }
        });

        let releaser_queue_space = queue_space.clone();
        let releaser_active_changed = active_changed.clone();
        let releaser_queue_full = queue_full.clone();
        let releaser_active_peer = active_peer.clone();
        let releaser = thread::spawn(move || {
            releaser_queue_full.store(false, Ordering::SeqCst);
            releaser_queue_space.notify_changed();
            thread::yield_now();
            releaser_active_peer.store(true, Ordering::SeqCst);
            releaser_active_changed.notify_changed();
        });

        sender.join().unwrap();
        releaser.join().unwrap();

        assert!(
            observed.load(Ordering::SeqCst)
                || queue_space.has_woken_waiter()
                || active_changed.has_woken_waiter(),
            "fallback wait must wake on either queue space or pipe activation"
        );
    });
}
