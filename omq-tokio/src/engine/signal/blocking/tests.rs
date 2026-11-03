use std::sync::Arc;

use super::*;

#[test]
fn blocking_waker_can_reenter_registration() {
    struct Reenter(Arc<BlockingSignal>);
    impl std::task::Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            assert!(self.0.waiters.try_lock().is_ok());
            drop(self.0.register());
        }
    }
    let signal = BlockingSignal::new();
    let registration = signal.register_waker(Arc::new(Reenter(signal.clone())).into());
    signal.wake();
    drop(registration);
    assert_eq!(signal.active.load(Ordering::Acquire), 0);
}

#[test]
fn panicking_waker_does_not_poison_waiter_registration() {
    struct Panics;
    impl std::task::Wake for Panics {
        fn wake(self: Arc<Self>) {
            panic!("user waker");
        }
    }
    let signal = BlockingSignal::new();
    let registration = signal.register_waker(Arc::new(Panics).into());
    assert!(std::panic::catch_unwind(|| signal.wake()).is_err());
    drop(registration);
    let waiter = signal.register();
    signal.wake();
    assert!(!waiter.state.armed.load(Ordering::Acquire));
}

#[test]
fn blocking_task_waker_releases_at_thread_exit_and_allows_late_tls_calls() {
    struct LateCall(std::sync::mpsc::Sender<()>);
    impl Drop for LateCall {
        fn drop(&mut self) {
            block_on(std::future::ready(()));
            self.0.send(()).unwrap();
        }
    }
    thread_local! {
        static LATE: RefCell<Option<LateCall>> = const { RefCell::new(None) };
    }
    let (completed, completion) = std::sync::mpsc::channel();
    let cached = std::thread::spawn(move || {
        // TLS drops in reverse order. This destructor runs after the
        // cached wake handle has been destroyed.
        LATE.with(|late| *late.borrow_mut() = Some(LateCall(completed)));
        block_on(std::future::ready(()));
        BLOCKING_UNPARK.with(Arc::downgrade)
    })
    .join()
    .unwrap();
    completion.recv().unwrap();
    assert!(cached.upgrade().is_none());
}

#[test]
fn cached_blocking_waiters_keep_nested_registrations_independent() {
    let signal = BlockingSignal::new();
    let first = signal.register();
    let pointer = Arc::as_ptr(&first.state);
    let second = signal.register();
    assert!(!Arc::ptr_eq(&first.state, &second.state));
    drop(first);
    let reused = signal.register();
    assert_eq!(Arc::as_ptr(&reused.state), pointer);
    assert_eq!(signal.active.load(Ordering::Acquire), 2);
    signal.wake();
    assert!(!second.state.armed.load(Ordering::Acquire));
    assert!(!reused.state.armed.load(Ordering::Acquire));
    drop(second);
    drop(reused);
    assert_eq!(signal.active.load(Ordering::Acquire), 0);
}

#[test]
fn blocking_waiter_cache_retains_no_socket_or_exited_thread() {
    let signal = BlockingSignal::new();
    let background = signal.clone();
    let state = std::thread::spawn(move || {
        let waiter = background.register();
        Arc::downgrade(&waiter.state)
    })
    .join()
    .unwrap();
    assert!(state.upgrade().is_none());
    let weak = Arc::downgrade(&signal);
    drop(signal.register());
    drop(signal);
    assert!(weak.upgrade().is_none());
}
